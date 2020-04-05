#pragma once

#include "snap_single_aligner.h"
#include "libagd/src/agd_filesystem_reader.h"
#include "libagd/src/agd_filesystem_writer.h"
#include "libagd/src/buffer_pair.h"
// class to manage aligning chunks in parallel

class ParallelAligner {
 public:
  using InputQueueItem = agd::AGDFileSystemReader::OutputQueueItem;
  using InputQueueType = ConcurrentQueue<InputQueueItem>;
  using OutputQueueItem = agd::AGDFileSystemWriter::InputQueueItem;
  using OutputQueueType = ConcurrentQueue<OutputQueueItem>;

  static Status Create(size_t threads, GenomeIndex* index,
                       AlignerOptions* options, InputQueueType* input_queue,
                       std::unique_ptr<ParallelAligner>& aligner);

  OutputQueueType* GetOutputQueue() { return output_queue_.get(); }

  void Stop();


 private:
  ParallelAligner(GenomeIndex* index, AlignerOptions* options, InputQueueType* input_queue)
      : genome_index_(index), options_(options), input_queue_(input_queue) {}

  Status Init(size_t threads);

  std::vector<std::thread> aligner_threads_;
  agd::ObjectPool<agd::BufferPair> bufpair_pool_;

  GenomeIndex* genome_index_;
  AlignerOptions* options_;
  InputQueueType* input_queue_;
  std::unique_ptr<OutputQueueType> output_queue_;
  volatile bool done_ = false;
};