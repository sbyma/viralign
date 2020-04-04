
#include "agd_filesystem_reader.h"
#include "filemap.h"
#include "parser.h"

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
      for (const auto& col : columns_) {
        auto filepath = absl::StrCat(item, ".", col);

        std::pair<char*, uint64_t> mapped_file;
        Status s = mmap_file(filepath, &mapped_file.first, &mapped_file.second);

        if (!s.ok()) {
          std::cout << "[AGDFSReader] WARNING: Could not map file. Thread exiting.\n";
          return; 
        }
        out_item.push_back(mapped_file);
      }

      inter_queue_->push(std::move(out_item));
    }
  };


  auto parser_func = [this]() {
    RecordParser parser;
    std::string record_id;
    while (!done_) {
      InterQueueItem item;
      if (!inter_queue_->pop(item)) continue;

      OutputQueueItem out_item;
      for (auto& col_file : item) {

        auto buf = buf_pool_->get();
        uint64_t first_ordinal;
        uint32_t num_records;

        Status s = parser.ParseNew(col_file.first, col_file.second, false, buf.get(), &first_ordinal, &num_records, record_id);

        if (!s.ok()) {
          std::cout << "[AGDFSReader] WARNING: Error decompressing chunk: " << s.error_message() << "\n";
          return; 
        }
        out_item.col_bufs.push_back(std::move(buf));
        out_item.chunk_size = num_records;
        out_item.first_ordinal = first_ordinal;
      }

      output_queue_->push(std::move(out_item));
    }
  };

  // make threads
  read_thread_ = std::thread(reader_func);
  for (auto& t : parse_threads_) {
    t = std::thread(parser_func);
  }
  
  return Status::OK(); 
}

}