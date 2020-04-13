
#pragma once

#include "libagd/src/agd_record_reader.h"
#include "libagd/src/queue_defs.h"
#include "liberr/errors.h"

class CephManager {
 public:
  static errors::Status Run(agd::ReadQueueType* input_queue, size_t chunks, const std::string& agd_metadata_path,
                            absl::string_view ceph_config_json_path, size_t threads);
};