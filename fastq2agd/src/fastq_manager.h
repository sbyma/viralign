
#pragma once

#include "absl/strings/string_view.h"
#include "concurrent_queue/concurrent_queue.h"
#include "fastq_chunker.h"
#include "liberr/errors.h"

using namespace errors;

struct FastqQueueItem {
  FastqChunk chunk_1;
  FastqChunk chunk_2; // will be invalid if single fastq
  size_t first_ordinal;
};

typedef ConcurrentQueue<FastqQueueItem> QueueType;
// manage the chunking, etc. of an input fastq (or pair)
// parses fastq into chunks so workers can process in parallel
class FastqManager {
 public:
  static Status CreateFastqManager(const std::string& file_path,
                                   const size_t chunk_size, QueueType* work_queue, std::unique_ptr<FastqManager>& manager);
  static Status CreatePairedFastqManager(const std::string& file_path_1,
                                         const std::string& file_path_2,
                                         const size_t chunk_size, QueueType* work_queue, std::unique_ptr<FastqManager>& manager);

  // run until done
  Status Run();

  uint32_t TotalChunks() const { return total_chunks_; }
 
 private:
  FastqManager() = default;
  FastqManager(char* file_data, const uint64_t file_size, char* file_data_2, const uint64_t file_size_2, const size_t chunk_size, QueueType* work_queue);

  char* file_data_1_ = nullptr;
  uint64_t file_size_1_ = 0;
  char* file_data_2_ = nullptr;
  uint64_t file_size_2_ = 0;
  size_t chunk_size_ = 0;
  QueueType* work_queue_ = nullptr;

  uint64_t current_ordinal_ = 0;
  uint32_t total_chunks_ = 0;
};