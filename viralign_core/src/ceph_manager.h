#pragma once

#include "libagd/src/agd_record_reader.h"
#include "liberr/errors.h"
#include "parallel_aligner.h"

class CephManager {
 public:
  static errors::Status Run(agd::ReadQueueType* input_queue, uint32_t max_records,
                            absl::string_view ceph_config_json_path,
                            int filter_contig_index, GenomeIndex* index,
                            AlignerOptions* options, size_t threads);
};