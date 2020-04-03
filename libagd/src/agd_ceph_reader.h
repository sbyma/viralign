
#include <rados/librados.hpp>
#include <string>
#include <thread>
#include <vector>

#include "buffer.h"
#include "concurrent_queue/concurrent_queue.h"
#include "liberr/errors.h"
#include "object_pool.h"

using namespace errors;

namespace agd {

// read chunks from multiple columsn from ceph and put them in a queue
// input queue contains name, pool pairs of chunks to read
class AGDCephReader {
 public:
  using InputQueueType = ConcurrentQueue<std::tuple<std::string, std::string>>;
  using OutputQueueType =
      ConcurrentQueue<std::vector<ObjectPool<Buffer>::ptr_type>>;

  static Status CreateAGDCephReader(std::vector<std::string> columns,
                                    const std::string& cluster_name,
                                    const std::string& user_name,
                                    const std::string& ceph_conf_file,
                                    InputQueueType* input_queue, size_t threads,
                                    ObjectPool<Buffer>& buf_pool);

  // Status GetNextChunk(std::vector<ObjectPool<Buffer>::ptr_type>& chunk_bufs);

  OutputQueueType* GetOutputQueue();

  void Stop();

 private:
  AGDCephReader() = delete;
  AGDCephReader(std::vector<std::string>& columns, ObjectPool<Buffer>& buf_pool,
                InputQueueType* input_queue)
      : columns_(columns), buf_pool_(&buf_pool), input_queue_(input_queue) {}

  Status Initialize(const std::string& cluster_name,
                    const std::string& user_name,
                    const std::string& ceph_conf_file, size_t threads);

  std::vector<std::string> columns_;
  ObjectPool<Buffer>* buf_pool_;  // does not own

  InputQueueType* input_queue_;
  std::unique_ptr<OutputQueueType> output_queue_;
  librados::Rados cluster_;

  std::vector<std::thread> threads_;
};

}  // namespace agd