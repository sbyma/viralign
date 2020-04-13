
#pragma once

#include <thread>
#include "concurrent_queue/concurrent_priority_queue.h"
#include "concurrent_queue/concurrent_queue.h"
#include "concurrent_queue/concurrent_priority_queue.h"
#include "json.hpp"
#include "libagd/src/queue_defs.h"
#include "liberr/errors.h"
#include "snap-master/SNAPLib/Bam.h"

using json = nlohmann::json;
using namespace errors;

class BamBuilder {
 public:
  BamBuilder(agd::ChunkQueueType* input_queue, size_t max_chunks)
      : input_queue_(input_queue), max_chunks_(max_chunks) {}

  errors::Status Run();
  errors::Status Init(const json& agd_metadata, size_t threads);

 private:
  agd::ChunkQueueType* input_queue_;
  size_t max_chunks_;

  agd::ObjectPool<agd::Buffer> buf_pool_;

  bool file_exists_;

  int32_t count_ = 0;
  std::string header_;

  bool first_ = true;
  uint32_t current_index_ = 0;

  const uint64_t buffer_size_ = 64 * 1024;  // 64Kb
  FILE* bam_fp_ = nullptr;

  Status compute_status_ = Status::OK();

  volatile bool run_compress_ = true;
  volatile bool run_write_ = true;
  std::atomic<uint32_t> num_active_threads_;
  int thread_id_ = 0;
  int num_threads_;

  char* scratch_;
  size_t scratch_pos_;

  agd::ObjectPool<agd::Buffer>::ptr_type current_buf_ref_;

  typedef std::tuple<agd::ObjectPool<agd::Buffer>::ptr_type, uint32_t,
                     agd::ObjectPool<agd::Buffer>::ptr_type, uint32_t, uint32_t>
      CompressItem;  // inbuffer, isize, outbuffer, osize, index (ordering)
  std::unique_ptr<ConcurrentQueue<CompressItem>> compress_queue_;

  struct WriteItem {
    agd::ObjectPool<agd::Buffer>::ptr_type buf;
    size_t size;
    uint32_t index;
    bool operator<(const WriteItem& other) const { return index > other.index; }
  };

  std::unique_ptr<ConcurrentPriorityQueue<WriteItem>> write_queue_;

  std::vector<std::thread> compress_threads_;
  std::thread writer_thread_;

  Status CompressToBuffer(char* in_buf, uint32_t in_size, char* out_buf,
                          uint32_t out_size, size_t& compressed_size);
  void ThreadInit();
};