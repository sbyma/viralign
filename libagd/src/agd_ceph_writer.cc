#include "agd_ceph_writer.h"
#include "compression.h"

using namespace std::chrono_literals;

namespace agd {

Status AGDCephWriter::Create(std::vector<std::string> columns,
                             const std::string& cluster_name,
                             const std::string& user_name,
                             const std::string& name_space,
                             const std::string& ceph_conf_file,
                             InputQueueType* input_queue,
                             size_t threads,
                             ObjectPool<Buffer>& buf_pool,
                             std::unique_ptr<AGDCephWriter>& writer) {
  writer.reset(new AGDCephWriter(columns, buf_pool, input_queue));
  return writer->Initialize(cluster_name, user_name, name_space, ceph_conf_file, threads);
}

Status AGDCephWriter::setup_ceph_connection(const std::string& cluster_name,
                                            const std::string& user_name,
                                            const std::string& ceph_conf_file) {
  int ret = 0;

  ret = cluster_.init2(user_name.c_str(), cluster_name.c_str(), 0);
  if (ret < 0) {
    return Internal("[AGDCephWriter] Couldn't intialize the cluster handle! error ", ret);
  } else {
    std::cout << "[AGDCephWriter] Created a cluster handle." << std::endl;
  }

  ret = cluster_.conf_read_file(ceph_conf_file.c_str());
  if (ret < 0) {
    return Internal("[AGDCephWriter] Couldn't read the Ceph configuration file! error ", ret);
  } else {
    std::cout << "[AGDCephWriter] Read the Ceph configuration file." << std::endl;
  }

  ret = cluster_.connect();
  if (ret < 0) {
    return Unavailable("[AGDCephWriter] Couldn't connect to cluster! error ", ret);
  } else {
    std::cout << "[AGDCephWriter] Connected to the cluster." << std::endl;
  }

  return Status::OK();
}

void AGDCephWriter::create_io_ctx(const InputQueueItem& item,
                                  const std::string& name_space,
                                  librados::IoCtx* io_ctx) {
  int ret = cluster_.ioctx_create(item.pool.c_str(), *io_ctx);
  if (ret < 0) {
    std::cerr << "[AGDCephWriter] Couldn't set up ioctx! error " << ret << ". Thread exiting."<< std::endl;
    exit(EXIT_FAILURE);
  } else {
    io_ctx->set_namespace(name_space);
    std::cout << "[AGDCephWriter] Created ioctx for namespace " << name_space << "." << std::endl;
  }
}

ceph::bufferlist AGDCephWriter::write_file(const std::string& objId, librados::IoCtx& io_ctx) {
  int ret;
  size_t file_size;
  time_t pmtime;
  ret = io_ctx.stat(objId, &file_size, &pmtime);
  if (ret != 0) {
    std::cerr << "[AGDCephWriter] io_ctx.stat() return " << ret << " for key " << objId << std::endl;
    exit(EXIT_FAILURE);
  }

  std::cout << "[AGDCephWriter] filesize = " << file_size << "." << std::endl;

  size_t data_read = 0;
  size_t read_len;
  size_t size_to_read = 4 * 1024 * 1024; // 4 MB. I chose this arbitrarily.

  librados::bufferlist read_buf;
  while (data_read < file_size) {
    read_len = std::min(size_to_read, file_size - data_read);

    std::cout << "[AGDCephWriter] Trying to read " << read_len << " bytes from " << objId << "." << std::endl;

    // Create I/O Completion.
    librados::AioCompletion *read_completion = librados::Rados::aio_create_completion();
    ret = io_ctx.aio_read(objId, read_completion, &read_buf, read_len, data_read);
    if (ret < 0) {
      std::cerr << "[AGDCephWriter] Couldn't start read object! error " << ret << std::endl;
      exit(EXIT_FAILURE);
    }

    data_read += read_len;

    // Wait for the request to complete, and check that it succeeded.
    read_completion->wait_for_complete();
    ret = read_completion->get_return_value();
    if (ret < 0) {
      std::cerr << "[AGDCephWriter] Couldn't read object! error " << ret << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  return read_buf;
}

Status AGDCephWriter::Initialize(const std::string& cluster_name,
                                 const std::string& user_name,
                                 const std::string& name_space,
                                 const std::string& ceph_conf_file,
                                 size_t threads) {
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
    while (!done_) {
      InputQueueItem item;
      std::cout << "[AGDCephWriter] Trying to pop queue" << std::endl;
      if (!input_queue_->pop(item)) continue;
      std::cout << "[AGDCephWriter] input_queue = {"
                << item.pool << ", "
                << item.chunk_size << ", "
                << item.first_ordinal << ", "
                << item.name << "}"
                << std::endl;

      if (item.col_buf_pairs.size() != columns_.size()) {
        std::cerr << "[AGDCephWriter] expected " << columns_.size()
                  << " columns, got " << item.col_buf_pairs.size() << std::endl;
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
        compress_buf->reserve(colbufpair->data().size() + colbufpair->index().size());
        compress_buf->reset();
        AppendingGZIPCompressor compressor(*compress_buf.get());
        Status s = Status::OK();
        s = compressor.init();
        if (!s.ok()) {
          std::cerr << "[AGDCephWriter] Error: couldn't init compressor: "
                    << s.error_message() << std::endl;
          exit(EXIT_FAILURE);
        }
        s = compressor.appendGZIP(colbufpair->index().data(), colbufpair->index().size());
        if (!s.ok()) {
          std::cerr << "[AGDCephWriter] Error: couldn't compress data: "
                    << s.error_message() << std::endl;
          exit(EXIT_FAILURE);
        }
        s = compressor.appendGZIP(colbufpair->data().data(), colbufpair->data().size());
        if (!s.ok()) {
          std::cerr << "[AGDCephWriter] Error: couldn't compress data: "
                    << s.error_message() << std::endl;
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

        // Send to Ceph.
        std::string objId = item.name + "." + colname;
        librados::bufferlist bl = bl.static_from_mem((char*) &header, sizeof(header));
        bl.append(compress_buf->data(), compress_buf->size());
        io_ctx.write_full(objId, bl);

        num_written_++;
      }


      // OutputQueueItem out_item;
      // out_item.name = std::move(item.objName);

      // for (const auto& col : columns_) {
      //   std::string objId = out_item.name + "." + col;
      //   auto read_buf = read_file(objId, io_ctx);
      //   auto out_buf = buf_pool_->get();
      //   uint64_t first_ordinal;
      //   uint32_t num_records;

      //   Status s = parser.ParseNew(read_buf.c_str(), read_buf.length(), false, out_buf.get(), &first_ordinal, &num_records, record_id);
      //   std::cout << "[AGDCephWriter] Parsed chunk with " << num_records << " records." << std::endl;

      //   if (!s.ok()) {
      //     std::cerr << "[AGDCephWriter] WARNING: Error decompressing chunk: " << s.error_message() << std::endl;
      //     return;
      //   }

      //   out_item.col_bufs.push_back(std::move(out_buf));
      //   out_item.chunk_size = num_records;
      //   out_item.first_ordinal = first_ordinal;
      // }

      // output_queue_->push(std::move(out_item));
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
  std::cout << "[AGDCephWriter] Stopping ...\n";
  while (!input_queue_->empty()) std::this_thread::sleep_for(1ms);
  // may already be unblocked by the owner, but its fine
  input_queue_->unblock();

  done_ = true;

  for (auto& t : compress_and_write_threads_) {
    t.join();
  }
}

} // namespace agd
