#pragma once

#include "buffer.h"
#include "buffer_pair.h"
#include "concurrent_queue/concurrent_queue.h"
#include "object_pool.h"

namespace agd {

// there is some redundancy in data between types, but having different names
// makes everything more explicit and easier to understand

// filename or ceph object (name + pool)
// AGDFSReader will ignore pool
struct ReadQueueItem {
  std::string pool;
  std::string objName;
};

using ReadQueueType = ConcurrentQueue<ReadQueueItem>;

// bufs containing decompressed columns for processing
struct ChunkQueueItem {
  std::string pool;
  std::vector<ObjectPool<Buffer>::ptr_type> col_bufs;
  uint32_t chunk_size;
  uint64_t first_ordinal;
  std::string name;
};

using ChunkQueueType = ConcurrentQueue<ChunkQueueItem>;

// buf pair (data, index) to be written to columns
struct WriteQueueItem {
  std::string pool;
  std::vector<ObjectPool<BufferPair>::ptr_type> col_buf_pairs;
  uint32_t chunk_size;
  uint64_t first_ordinal;
  std::string name;  // full path without ext, e.g. path/to/dataset/test_1000
};
using WriteQueueType = ConcurrentQueue<WriteQueueItem>;

// name (+ pool) to signal this chunk's processing is complete
struct OutputQueueItem {
  std::string pool;
  std::string objName;
};
using OutputQueueType = ConcurrentQueue<OutputQueueItem>;

}  // namespace agd
