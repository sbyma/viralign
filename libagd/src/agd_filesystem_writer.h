#pragma once

#include <string>
#include <thread>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "buffer_pair.h"
#include "concurrent_queue/concurrent_queue.h"
#include "format.h"
#include "liberr/errors.h"
#include "object_pool.h"

using namespace errors;

namespace agd {

// read chunks from multiple columsn from FS and put them in a queue
// input queue contains names of chunks to read
class AGDFileSystemWriter {
 public:
  struct InputQueueItem {
    std::vector<ObjectPool<BufferPair>::ptr_type> col_buf_pairs;
    uint32_t chunk_size;
    uint64_t first_ordinal;
    std::string name;  // full path without ext, e.g. path/to/dataset/test_1000
  };
  using InputQueueType = ConcurrentQueue<InputQueueItem>;

  static Status Create(std::vector<std::string> columns,
                       InputQueueType* input_queue, size_t threads,
                       ObjectPool<Buffer>& buf_pool,
                       std::unique_ptr<AGDFileSystemWriter>& writer);

  uint32_t GetNumWritten() { return num_written_.load(); };

  void Stop();

 private:
  struct InterQueueItem {
    std::vector<ObjectPool<Buffer>::ptr_type> col_bufs;
    uint32_t chunk_size;
    uint64_t first_ordinal;
    std::string name;
  };
  using InterQueueType = ConcurrentQueue<InterQueueItem>;

  AGDFileSystemWriter() = delete;
  AGDFileSystemWriter(std::vector<std::string>& columns,
                      ObjectPool<Buffer>& buf_pool, InputQueueType* input_queue)
      : columns_(columns), buf_pool_(&buf_pool), input_queue_(input_queue) {}

  Status Initialize(size_t threads);

  struct FormatValue {
    format::RecordType type;
    format::CompressionType compress_type;
  };

  // this could really just be an array but whatever
  absl::flat_hash_map<absl::string_view, FormatValue> column_map_;

  std::vector<std::string> columns_;
  ObjectPool<Buffer>* buf_pool_;  // does not own

  InputQueueType* input_queue_;

  std::unique_ptr<InterQueueType> inter_queue_;

  volatile bool done_ = false;
  volatile bool compress_done_ = false;

  std::vector<std::thread> compress_threads_;
  std::thread write_thread_;

  std::atomic_uint32_t num_written_{0};
};

}  // namespace agd