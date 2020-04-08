
#include "agd_filesystem_writer.h"

#include <chrono>
#include <fstream>

#include "compression.h"

using namespace std::chrono_literals;

namespace agd {

Status AGDFileSystemWriter::Create(
    std::vector<std::string> columns, InputQueueType* input_queue,
    size_t threads, ObjectPool<Buffer>& buf_pool,
    std::unique_ptr<AGDFileSystemWriter>& writer) {
  writer.reset(new AGDFileSystemWriter(columns, buf_pool, input_queue));
  return writer->Initialize(threads);
}

Status AGDFileSystemWriter::Initialize(size_t threads) {
  column_map_["base"] = {
      agd::format::RecordType::TEXT,
      agd::format::CompressionType::GZIP};  // todo store as compacted bases
  column_map_["qual"] = {agd::format::RecordType::TEXT,
                         agd::format::CompressionType::GZIP};
  column_map_["meta"] = {agd::format::RecordType::TEXT,
                         agd::format::CompressionType::GZIP};
  column_map_["aln"] = {agd::format::RecordType::STRUCTURED,
                        agd::format::CompressionType::GZIP};
  inter_queue_ = std::make_unique<InterQueueType>(5);

  auto compress_func = [this]() {
    while (!compress_done_) {
      InputQueueItem item;
      if (!input_queue_->pop(item)) continue;

      InterQueueItem out_item;
      if (item.col_buf_pairs.size() != columns_.size()) {
        std::cout << "[AGDFSWriter] expected " << columns_.size()
                  << " columns, got " << item.col_buf_pairs.size() << "\n";
        return;
      }

      out_item.col_bufs.reserve(columns_.size());
      for (auto& col : item.col_buf_pairs) {
        auto compress_buf = buf_pool_->get();
        compress_buf->reserve(col->data().size() + col->index().size());
        compress_buf->reset();
        AppendingGZIPCompressor compressor(*compress_buf.get());
        Status s = Status::OK();
        s = compressor.init();
        if (!s.ok()) {
          std::cout << "[AGDFSWriter] Error: couldn't init compressor: "
                    << s.error_message() << "\n";
          exit(0);
        }
        s = compressor.appendGZIP(col->index().data(), col->index().size());
        if (!s.ok()) {
          std::cout << "[AGDFSWriter] Error: couldn't compress data: "
                    << s.error_message() << "\n";
          exit(0);
        }
        s = compressor.appendGZIP(col->data().data(), col->data().size());
        if (!s.ok()) {
          std::cout << "[AGDFSWriter] Error: couldn't compress data: "
                    << s.error_message() << "\n";
          exit(0);
        }

        out_item.col_bufs.push_back(std::move(compress_buf));
      }

      // std::cout << "[AGDFSReader] pushing to inter_queue_: \n";
      out_item.name = std::move(item.name);
      out_item.chunk_size = item.chunk_size;
      out_item.first_ordinal = item.first_ordinal;
      inter_queue_->push(std::move(out_item));
    }
  };

  auto writer_func = [this]() {
    // std::cout << "[AGDFSReader] parser thread starting.\n";
    while (!done_) {
      InterQueueItem item;
      if (!inter_queue_->pop(item)) continue;

      size_t buf_idx = 0;
      auto pos = item.name.find_last_of("/") + 1;
      auto pos2 = item.name.find_last_of("_");
      auto name = item.name.substr(pos, pos2 - pos);

      std::cout << "[AGDFSWriter] dataset name is " << name << "\n";
      for (auto& col : columns_) {
        auto& buf = item.col_bufs[buf_idx];

        agd::format::FileHeader header;

        const auto& types = column_map_[col];
        header.record_type = types.type;
        header.compression_type = types.compress_type;

        memset(header.string_id, 0, sizeof(agd::format::FileHeader::string_id));
        auto copy_size =
            std::min({name.size(), sizeof(agd::format::FileHeader::string_id)});
        strncpy(&header.string_id[0], name.c_str(), copy_size);

        header.first_ordinal = item.first_ordinal;
        header.last_ordinal = item.first_ordinal + item.chunk_size;

        auto file_name = absl::StrCat(item.name, ".", col);
        std::cout << "[AGDFSWriter] writing file " << file_name << "\n";

        std::ofstream out_file(file_name, std::ios::binary);
        out_file.write(reinterpret_cast<const char*>(&header), sizeof(header));
        out_file.write(buf->data(), buf->size());

        if (!out_file.good()) {
          std::cout << "[AGDFSWriter] Failed to write bases file " << file_name
                    << "\n";
        }
        out_file.close();
        num_written_++;
        buf_idx++;
      }
    }
  };

  // make threads
  // use one reader for now
  write_thread_ = std::thread(writer_func);
  compress_threads_.resize(threads);
  for (auto& t : compress_threads_) {
    t = std::thread(compress_func);
  }

  return Status::OK();
}

void AGDFileSystemWriter::Stop() {
  // this doesnt own input_queue_, should it really be stopping it?
  std::cout << "[AGDFSWriter] Stopping ...\n";

  while (!input_queue_->empty()) std::this_thread::sleep_for(1ms);
  // may already be unblocked by the owner, but its fine
  input_queue_->unblock();

  compress_done_ = true;
  for (auto& t : compress_threads_) {
    t.join();
  }

  while (!inter_queue_->empty()) std::this_thread::sleep_for(1ms);
  inter_queue_->unblock();

  done_ = true;
  write_thread_.join();
}

}  // namespace agd