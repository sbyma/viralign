#pragma once 

#include "json.hpp"
#include "libagd/src/queue_defs.h"
#include "liberr/errors.h"

using json = nlohmann::json;

class FileSystemManager {
 public:
  static errors::Status Run(agd::ReadQueueType* input_queue, size_t threads, size_t chunks, const std::string& agd_metadata_path);
};