#pragma once

#include "libagd/src/agd_record_reader.h"
#include "libagd/src/queue_defs.h"
#include "liberr/errors.h"
#include "genes.h"

struct CephManagerParams {
  agd::ReadQueueType* input_queue;
  absl::string_view ceph_config_json_path;
  size_t reader_threads;
  absl::string_view output_filename;
  uint32_t max_chunks;
  const IntervalForest* interval_forest;
  const GeneIdMap* genes;
};

class CephManager {
 public:
  static errors::Status Run(const CephManagerParams& params);
};