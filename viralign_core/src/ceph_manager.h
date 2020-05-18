#pragma once

#include "libagd/src/agd_record_reader.h"
#include "liberr/errors.h"
#include "parallel_aligner.h"

struct CephManagerParams {
  agd::ReadQueueType* input_queue;
  uint32_t max_records;
  absl::string_view ceph_config_json_path;
  int filter_contig_index;
  GenomeIndex* index;
  AlignerOptions* options;
  size_t aligner_threads;
  size_t reader_threads;
  size_t writer_threads;
  absl::string_view redis_addr;
  absl::string_view queue_name;
};

class CephManager {
 public:
  static errors::Status Run(const CephManagerParams& params);
};