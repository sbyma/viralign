#pragma once

#include <vector>
#include <thread>
#include "snap_single_aligner.h"
#include "libagd/src/queue_defs.h"
#include "libagd/src/buffer_pair.h"
#include "liberr/errors.h"
// class to manage aligning chunks in parallel

class ParallelAligner {
 public:
  using InputQueueItem = agd::ChunkQueueItem;
  using InputQueueType = agd::ChunkQueueType;
  using OutputQueueItem = agd::WriteQueueItem;
  using OutputQueueType = agd::WriteQueueType;

  static errors::Status Create(size_t threads, GenomeIndex* index,
                       AlignerOptions* options, InputQueueType* input_queue, int filter_contig_index,
                       std::unique_ptr<ParallelAligner>& aligner);

  OutputQueueType* GetOutputQueue() { return output_queue_.get(); }

  void Stop();


 private:
  ParallelAligner(GenomeIndex* index, AlignerOptions* options, InputQueueType* input_queue,  size_t filter_contig_index)
      : genome_index_(index), options_(options), input_queue_(input_queue), filter_contig_index_(filter_contig_index) {}

  errors::Status Init(size_t threads);

  std::vector<std::thread> aligner_threads_;
  agd::ObjectPool<agd::BufferPair> bufpair_pool_;

  GenomeIndex* genome_index_;
  AlignerOptions* options_;
  InputQueueType* input_queue_;
  std::unique_ptr<OutputQueueType> output_queue_;
  volatile bool done_ = false;

  std::atomic_uint64_t num_aligned_{0};
  
  // if not -1, output 0 entry for any alignment not mapping to this contig
  int filter_contig_index_ = -1;
};