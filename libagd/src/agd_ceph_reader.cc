#include "agd_ceph_reader.h"
#include "parser.h"

using namespace std::chrono_literals;

namespace agd {

Status AGDCephReader::Create(std::vector<std::string> columns,
                             const std::string& cluster_name,
                             const std::string& user_name,
                             const std::string& ceph_conf_file,
                             InputQueueType* input_queue,
                             size_t threads,
                             ObjectPool<Buffer>& buf_pool,
                             std::unique_ptr<AGDCephReader>& reader) {
  reader.reset(new AGDCephReader(columns, buf_pool, input_queue));
  return reader->Initialize(cluster_name, user_name, ceph_conf_file, threads);
}

ceph::bufferlist AGDCephReader::read_file(const std::string& objId, librados::IoCtx& io_ctx) {
  int ret;
  size_t file_size;
  time_t pmtime;
  ret = io_ctx.stat(objId, &file_size, &pmtime);
  if (ret != 0) {
    std::cerr << "[AGDCephReader] io_ctx.stat() return " << ret << " for key " << objId << std::endl;
    exit(EXIT_FAILURE);
  }

  size_t data_read = 0;
  size_t read_len;
  size_t size_to_read = 4 * 1024 * 1024; // 4 MB. I chose this arbitrarily.

  librados::bufferlist read_buf;
  while (data_read < file_size) {
    read_len = std::min(size_to_read, file_size - data_read);

    // Create I/O Completion.
    librados::AioCompletion *read_completion = librados::Rados::aio_create_completion();
    ret = io_ctx.aio_read(objId, read_completion, &read_buf, read_len, data_read);
    if (ret < 0) {
      std::cerr << "[AGDCephReader] Couldn't start read object! error " << ret << std::endl;
      exit(EXIT_FAILURE);
    }

    // Wait for the request to complete, and check that it succeeded.
    read_completion->wait_for_complete();
    ret = read_completion->get_return_value();
    if (ret < 0) {
      std::cerr << "[AGDCephReader] Couldn't read object! error " << ret << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  return read_buf;
}

std::vector<std::pair<std::string, librados::IoCtx>> AGDCephReader::create_io_ctxs(const InputQueueItem& item) {
  std::vector<std::pair<std::string, librados::IoCtx>> metadatas;

  // Create an IoCtx per column to read.
  for (const auto& col : columns_) {
    int ret;
    librados::IoCtx io_ctx;

    ret = cluster_.ioctx_create(item.pool.c_str(), io_ctx);
    if (ret < 0) {
      std::cerr << "[AGDCephReader] Couldn't set up ioctx! error " << ret << ". Thread exiting."<< std::endl;
      exit(EXIT_FAILURE);
    }

    std::string objId = item.objName + "." + col;
    metadatas.push_back(std::make_pair(objId, io_ctx));
  }

  return metadatas;
}

Status AGDCephReader::Initialize(const std::string& cluster_name,
                                 const std::string& user_name,
                                 const std::string& ceph_conf_file,
                                 size_t threads) {
  int ret = 0;

  ret = cluster_.init2(user_name.c_str(), cluster_name.c_str(), 0);
  if (ret < 0) {
    return Internal("[AGDCephReader] Couldn't intialize the cluster handle! error ", ret);
  } else {
    std::cout << "[AGDCephReader] Created a cluster handle." << std::endl;
  }

  ret = cluster_.conf_read_file(ceph_conf_file.c_str());
  if (ret < 0) {
    return Internal("[AGDCephReader] Couldn't read the Ceph configuration file! error ", ret);
  } else {
    std::cout << "[AGDCephReader] Read the Ceph configuration file." << std::endl;
  }

  ret = cluster_.connect();
  if (ret < 0) {
    return Unavailable("[AGDCephReader] Couldn't connect to cluster! error ", ret);
  } else {
    std::cout << "[AGDCephReader] Connected to the cluster." << std::endl;
  }

  auto read_and_parse_func = [this]() {
    RecordParser parser;
    std::string record_id;

    while (!done_) {
      InputQueueItem item;
      if (!input_queue_->pop(item)) continue;

      auto metadatas = create_io_ctxs(item);
      OutputQueueItem out_item;
      out_item.name = std::move(item.objName);

      for (auto& meta : metadatas) {
        std::string objId;
        librados::IoCtx io_ctx;
        std::tie(objId, io_ctx) = meta;
        auto read_buf = read_file(objId, io_ctx);
        auto out_buf = buf_pool_->get();
        uint64_t first_ordinal;
        uint32_t num_records;

        Status s = parser.ParseNew(read_buf.c_str(), read_buf.length(), false, out_buf.get(), &first_ordinal, &num_records, record_id);
        std::cout << "[AGDCephReader] Parsed chunk with " << num_records << " records." << std::endl;

        if (!s.ok()) {
          std::cerr << "[AGDCephReader] WARNING: Error decompressing chunk: " << s.error_message() << std::endl;
          return;
        }

        out_item.col_bufs.push_back(std::move(out_buf));
        out_item.chunk_size = num_records;
        out_item.first_ordinal = first_ordinal;
      }

      output_queue_->push(std::move(out_item));
    }
  };

  // Make threads and assign read_and_parse_func to each.
  read_and_parse_threads_.resize(threads);
  for (auto& t : read_and_parse_threads_) {
    t = std::thread(read_and_parse_func);
  }

  return Status::OK();
}

AGDCephReader::OutputQueueType* AGDCephReader::GetOutputQueue() {
  return output_queue_.get();
}

void AGDCephReader::Stop() {
  // this doesnt own input_queue_, should it really be stopping it?
  std::cout << "[AGDCephReader] Stopping ...\n";
  while (!input_queue_->empty()) std::this_thread::sleep_for(1ms);
  // may already be unblocked by the owner, but its fine
  input_queue_->unblock();

  done_ = true;

  for (auto& t : read_and_parse_threads_) {
    t.join();
  }
}

} // namespace agd
