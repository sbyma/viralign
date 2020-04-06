#pragma once

#include <rados/librados.hpp>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "buffer.h"
#include "concurrent_queue/concurrent_queue.h"
#include "liberr/errors.h"
#include "object_pool.h"

using namespace errors;

namespace agd {

// read chunks from multiple columns from ceph and put them in a queue
// input queue contains name, pool pairs of chunks to read
class AGDCephReader {
 public:
  using InputQueueItem = struct {
    std::string pool;
    std::string objName;
  };
  using InputQueueType = ConcurrentQueue<InputQueueItem>;
  struct OutputQueueItem {
    std::vector<ObjectPool<Buffer>::ptr_type> col_bufs;
    uint32_t chunk_size;
    uint64_t first_ordinal;
    std::string name;
  };
  using OutputQueueType = ConcurrentQueue<OutputQueueItem>;

  static Status Create(std::vector<std::string> columns,
                       const std::string& cluster_name,
                       const std::string& user_name,
                       const std::string& ceph_conf_file,
                       InputQueueType* input_queue,
                       size_t threads,
                       ObjectPool<Buffer>& buf_pool,
                       std::unique_ptr<AGDCephReader>& reader);

  OutputQueueType* GetOutputQueue();

  void Stop();

 private:
  AGDCephReader() = delete;
  AGDCephReader(std::vector<std::string>& columns,
                ObjectPool<Buffer>& buf_pool,
                InputQueueType* input_queue)
      : columns_(columns), buf_pool_(&buf_pool), input_queue_(input_queue) {}

  Status Initialize(const std::string& cluster_name,
                    const std::string& user_name,
                    const std::string& ceph_conf_file,
                    size_t threads);

  ceph::bufferlist read_file(const std::string& objId, librados::IoCtx& io_ctx);
  std::vector<std::pair<std::string, librados::IoCtx>> create_io_ctxs(const InputQueueItem& item);

  std::vector<std::string> columns_;
  ObjectPool<Buffer>* buf_pool_;  // does not own

  InputQueueType* input_queue_;

  std::unique_ptr<OutputQueueType> output_queue_;

  librados::Rados cluster_;

  volatile bool done_ = false;

  std::vector<std::thread> read_and_parse_threads_;
};

}  // namespace agd
