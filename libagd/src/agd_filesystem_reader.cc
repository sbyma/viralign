
#include "agd_filesystem_reader.h"
#include "filemap.h"
#include "parser.h"
#include <thread>
#include <chrono>

using namespace std::chrono_literals;

namespace agd {

Status AGDFileSystemReader::Create(
    std::vector<std::string> columns, InputQueueType* input_queue,
    size_t threads, ObjectPool<Buffer>& buf_pool,
    std::unique_ptr<AGDFileSystemReader>& reader) {
  reader.reset(new AGDFileSystemReader(columns, buf_pool, input_queue));
  return reader->Initialize(threads);
}

Status AGDFileSystemReader::Initialize(size_t threads) {

  output_queue_ = std::make_unique<OutputQueueType>(5);
  inter_queue_ = std::make_unique<InterQueueType>(5);

  auto reader_func = [this]() {
    while (!done_) {
      InputQueueItem item;
      if (!input_queue_->pop(item)) continue;

      InterQueueItem out_item;
      out_item.mapped_files.reserve(columns_.size());
      for (const auto& col : columns_) {
        auto filepath = absl::StrCat(item, ".", col);

        //std::cout << "[AGDFSReader] reader mapping file: " << filepath << "\n";
        std::pair<char*, uint64_t> mapped_file;
        Status s = mmap_file(filepath, &mapped_file.first, &mapped_file.second);

        if (!s.ok()) {
          std::cout << "[AGDFSReader] WARNING: Could not map file. Thread exiting.\n";
          return;
        }
        out_item.mapped_files.push_back(mapped_file);
      }

      //std::cout << "[AGDFSReader] pushing to inter_queue_: \n";
      out_item.name = std::move(item);
      inter_queue_->push(std::move(out_item));
    }
  };


  auto parser_func = [this]() {
    RecordParser parser;
    std::string record_id;
    //std::cout << "[AGDFSReader] parser thread starting.\n";
    while (!parser_done_) {
      InterQueueItem item;
      if (!inter_queue_->pop(item)) continue;

      OutputQueueItem out_item;
      out_item.name = std::move(item.name);
      for (auto& col_file : item.mapped_files) {

        auto buf = buf_pool_->get();
        uint64_t first_ordinal;
        uint32_t num_records;

        Status s = parser.ParseNew(col_file.first, col_file.second, false, buf.get(), &first_ordinal, &num_records, record_id);
        //std::cout << "[AGDFSReader] parsed chunk with : " << num_records << " records.\n";
        unmap_file(col_file.first, col_file.second);

        if (!s.ok()) {
          std::cout << "[AGDFSReader] WARNING: Error decompressing chunk: " << s.error_message() << "\n";
          return;
        }
        out_item.col_bufs.push_back(std::move(buf));
        out_item.chunk_size = num_records;
        out_item.first_ordinal = first_ordinal;
      }

      //std::cout << "[AGDFSReader] pushing to output queue.\n";
      output_queue_->push(std::move(out_item));
    }
  };

  // make threads
  // use one reader for now
  read_thread_ = std::thread(reader_func);
  parse_threads_.resize(threads);
  for (auto& t : parse_threads_) {
    t = std::thread(parser_func);
  }

  return Status::OK();
}


AGDFileSystemReader::OutputQueueType* AGDFileSystemReader::GetOutputQueue() {
  return output_queue_.get();
}

void AGDFileSystemReader::Stop() {
  // this doesnt own input_queue_, should it really be stopping it?
  std::cout << "[AGDFSReader] Stopping ...\n";
  while (!input_queue_->empty()) std::this_thread::sleep_for(1ms);
  // may already be unblocked by the owner, but its fine
  input_queue_->unblock();

  done_ = true;
  read_thread_.join();

  while (!inter_queue_->empty()) std::this_thread::sleep_for(1ms);
  inter_queue_->unblock();

  parser_done_ = true;
  for (auto& t : parse_threads_) {
    t.join();
  }
}

} // namespace agd
