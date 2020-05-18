#pragma once

#include "libagd/src/queue_defs.h"
#include "src/sw/redis++/redis++.h"
#include "absl/strings/str_cat.h"

using namespace sw::redis;

class RedisPusher {
 public:
  RedisPusher(agd::OutputQueueType* input_queue, absl::string_view redis_addr, absl::string_view queue_name)
      : input_queue_(input_queue), queue_name_(queue_name) {

    auto full_addr = absl::StrCat("tcp://", redis_addr);
    std::cout << "[RedisPusher] Creating and connecting to " << full_addr
              << "\n";

    redis_.reset(new Redis(full_addr));
  }

  errors::Status Run();

  void Stop() { std::cout << "[RedisPusher] stop not implemented\n"; };

 private:
  std::unique_ptr<sw::redis::Redis> redis_;
  agd::OutputQueueType* input_queue_;
  absl::string_view queue_name_;
};