#pragma once

#include "libagd/src/agd_record_reader.h"
#include "liberr/errors.h"
#include "parallel_aligner.h"

class CephManager {
 public:
  static errors::Status Run(absl::string_view agd_meta_json_path,
                    absl::string_view ceph_config_json_path, GenomeIndex* index,
                    AlignerOptions* options);
};