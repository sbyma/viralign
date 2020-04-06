#pragma once

#include <string>
#include <thread>
#include <vector>

#include "buffer.h"
#include "concurrent_queue/concurrent_queue.h"
#include "liberr/errors.h"
#include "object_pool.h"

using namespace errors;

namespace agd {

// read chunks from multiple columns from FS and put them in a queue
// input queue contains names of chunks to read
class AGDFileSystemReader {
 public:
  using InputQueueItem = std::string;
  using InputQueueType = ConcurrentQueue<InputQueueItem>;
  struct OutputQueueItem {
    std::vector<ObjectPool<Buffer>::ptr_type> col_bufs;
    uint32_t chunk_size;
    uint64_t first_ordinal;
    std::string name;
  };
  using OutputQueueType = ConcurrentQueue<OutputQueueItem>;

  static Status Create(std::vector<std::string> columns,
                       InputQueueType* input_queue, size_t threads,
                       ObjectPool<Buffer>& buf_pool,
                       std::unique_ptr<AGDFileSystemReader>& reader);

  OutputQueueType* GetOutputQueue();

  void Stop();

 private:
  struct InterQueueItem {
    std::vector<std::pair<char*, uint64_t>> mapped_files;
    std::string name;
  };
  using InterQueueType = ConcurrentQueue<InterQueueItem>;

  AGDFileSystemReader() = delete;
  AGDFileSystemReader(std::vector<std::string>& columns,
                      ObjectPool<Buffer>& buf_pool, InputQueueType* input_queue)
      : columns_(columns), buf_pool_(&buf_pool), input_queue_(input_queue) {}

  Status Initialize(size_t threads);

  std::vector<std::string> columns_;
  ObjectPool<Buffer>* buf_pool_;  // does not own

  InputQueueType* input_queue_;

  std::unique_ptr<OutputQueueType> output_queue_;
  std::unique_ptr<InterQueueType> inter_queue_;

  volatile bool done_ = false;
  volatile bool parser_done_ = false;

  std::vector<std::thread> parse_threads_;
  std::thread read_thread_;
};

}  // namespace agd
