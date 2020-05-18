#include "agd_ceph_writer.h"

#include "absl/strings/str_format.h"
#include "compression.h"

using namespace std::chrono_literals;

namespace agd {

Status AGDCephWriter::Create(std::vector<std::string> columns,
                             const std::string& cluster_name,
                             const std::string& user_name,
                             const std::string& name_space,
                             const std::string& ceph_conf_file,
                             InputQueueType* input_queue, size_t threads,
                             ObjectPool<Buffer>& buf_pool,
                             std::unique_ptr<AGDCephWriter>& writer) {
  writer.reset(new AGDCephWriter(columns, buf_pool, input_queue));
  return writer->Initialize(cluster_name, user_name, name_space, ceph_conf_file,
                            threads);
}

Status AGDCephWriter::setup_ceph_connection(const std::string& cluster_name,
                                            const std::string& user_name,
                                            const std::string& ceph_conf_file) {
  int ret = 0;

  ret = cluster_.init2(user_name.c_str(), cluster_name.c_str(), 0);
  if (ret < 0) {
    return Internal(
        "[AGDCephWriter] Couldn't intialize the cluster handle! error ", ret);
  } else {
    std::cout << absl::StreamFormat(
        "[AGDCephWriter] Created a cluster handle.\n");
  }

  ret = cluster_.conf_read_file(ceph_conf_file.c_str());
  if (ret < 0) {
    return Internal(
        "[AGDCephWriter] Couldn't read the Ceph configuration file! error ",
        ret);
  } else {
    std::cout << absl::StreamFormat(
        "[AGDCephWriter] Read the Ceph configuration file.\n");
  }

  ret = cluster_.connect();
  if (ret < 0) {
    return Unavailable("[AGDCephWriter] Couldn't connect to cluster! error ",
                       ret);
  } else {
    std::cout << absl::StreamFormat(
        "[AGDCephWriter] Connected to the cluster.\n");
  }

  return Status::OK();
}

void AGDCephWriter::create_io_ctx(const InputQueueItem& item,
                                  const std::string& name_space,
                                  librados::IoCtx* io_ctx) {
  int ret = cluster_.ioctx_create(item.pool.c_str(), *io_ctx);
  if (ret < 0) {
    std::cerr << absl::StreamFormat(
        "[AGDCephWriter] Couldn't set up ioctx! error %d. Thread exiting.\n",
        ret);
    exit(EXIT_FAILURE);
  } else {
    io_ctx->set_namespace(name_space);
    std::cout << absl::StreamFormat(
        "[AGDCephWriter] Created ioctx for namespace %s.\n", name_space);
  }
}

Status AGDCephWriter::Initialize(const std::string& cluster_name,
                                 const std::string& user_name,
                                 const std::string& name_space,
                                 const std::string& ceph_conf_file,
                                 size_t threads) {
  
  output_queue_.reset(new OutputQueueType(30)); // is 5 big enough?

  column_map_["base"] = {
      agd::format::RecordType::TEXT,
      agd::format::CompressionType::GZIP};  // todo store as compacted bases
  column_map_["qual"] = {agd::format::RecordType::TEXT,
                         agd::format::CompressionType::GZIP};
  column_map_["meta"] = {agd::format::RecordType::TEXT,
                         agd::format::CompressionType::GZIP};
  column_map_["aln"] = {agd::format::RecordType::STRUCTURED,
                        agd::format::CompressionType::GZIP};

  setup_ceph_connection(cluster_name, user_name, ceph_conf_file);

  auto compress_and_write_func = [this, name_space]() {
    InputQueueItem item;
    while (!done_) {
      std::cout << absl::StreamFormat("[AGDCephWriter] Trying to pop queue\n");
      if (!input_queue_->pop(item)) continue;
      std::cout << absl::StreamFormat(
          "[AGDCephWriter] input_queue = {%s, %d, %d, %s}\n", item.pool,
          item.chunk_size, item.first_ordinal, item.name);

      if (item.col_buf_pairs.size() != columns_.size()) {
        std::cerr << absl::StreamFormat(
            "[AGDCephWriter] expected %d columns, got %d\n", columns_.size(),
            item.col_buf_pairs.size());
        return;
      }

      // We use a single io_ctx per thread for writing all columns.
      librados::IoCtx io_ctx;
      create_io_ctx(item, name_space, &io_ctx);

      auto name = item.name.substr(item.name.find_last_of("/") + 1,
                                   item.name.find_last_of("_"));

      for (size_t buf_idx = 0; buf_idx < columns_.size(); buf_idx++) {
        // Compress.
        const auto& colbufpair = item.col_buf_pairs[buf_idx];
        auto compress_buf = buf_pool_->get();
        compress_buf->reserve(colbufpair->data().size() +
                              colbufpair->index().size());
        compress_buf->reset();
        AppendingGZIPCompressor compressor(*compress_buf.get());
        Status s = Status::OK();
        s = compressor.init();
        if (!s.ok()) {
          std::cerr << absl::StreamFormat(
              "[AGDCephWriter] Error: couldn't init compressor: %s\n",
              s.error_message());
          exit(EXIT_FAILURE);
        }
        s = compressor.appendGZIP(colbufpair->index().data(),
                                  colbufpair->index().size());
        if (!s.ok()) {
          std::cerr << absl::StreamFormat(
              "[AGDCephWriter] Error: couldn't compress data: %s\n",
              s.error_message());
          exit(EXIT_FAILURE);
        }
        s = compressor.appendGZIP(colbufpair->data().data(),
                                  colbufpair->data().size());
        if (!s.ok()) {
          std::cerr << absl::StreamFormat(
              "[AGDCephWriter] Error: couldn't compress data: %s\n",
              s.error_message());
          exit(EXIT_FAILURE);
        }
        s = compressor.finish();
        if (!s.ok()) {
          std::cerr << absl::StreamFormat(
              "[AGDCephWriter] Error: couldn't close compressor: %s\n",
              s.error_message());
          exit(EXIT_FAILURE);
        }

        // Write.
        const auto& colname = columns_[buf_idx];
        agd::format::FileHeader header;
        const auto& types = column_map_[colname];
        header.record_type = types.type;
        header.compression_type = types.compress_type;

        memset(header.string_id, 0, sizeof(agd::format::FileHeader::string_id));
        auto copy_size =
            std::min({name.size(), sizeof(agd::format::FileHeader::string_id)});
        strncpy(&header.string_id[0], name.c_str(), copy_size);

        header.first_ordinal = item.first_ordinal;
        header.last_ordinal = item.first_ordinal + item.chunk_size;

        // Send to Ceph. Strip any file path prefix. 
        std::string obj_base =
            item.name.substr(item.name.find_last_of('/') + 1);
        std::string objId = absl::StrCat(obj_base, ".", colname);

        librados::bufferlist bl =
            bl.static_from_mem((char*)&header, sizeof(header));
        bl.append(compress_buf->data(), compress_buf->size());
        std::cout << absl::StreamFormat(
            "Writing %d bytes to object %s in ceph\n", bl.length(), objId);
        io_ctx.write_full(objId, bl);

        num_written_++;

        OutputQueueItem output_item;
        output_item.objName = std::move(item.name);
        output_item.pool = std::move(item.pool);
        output_queue_->push(output_item);
      }
    }
  };

  // Make threads and assign compress_and_write_func to each.
  compress_and_write_threads_.resize(threads);
  for (auto& t : compress_and_write_threads_) {
    t = std::thread(compress_and_write_func);
  }

  return Status::OK();
}

void AGDCephWriter::Stop() {
  // this doesnt own input_queue_, should it really be stopping it?
  std::cout << absl::StreamFormat("[AGDCephWriter] Stopping ...\n");
  while (!input_queue_->empty()) std::this_thread::sleep_for(1ms);
  // may already be unblocked by the owner, but its fine
  input_queue_->unblock();

  done_ = true;

  for (auto& t : compress_and_write_threads_) {
    t.join();
  }
}

}  // namespace agd
