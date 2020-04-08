#pragma once 

#include "libagd/src/queue_defs.h"
#include "liberr/errors.h"
#include "redox.hpp"

constexpr absl::string_view RdxQueueName = "queue:viralign";

// redox::Command<std::string>& cmd = rdx_.commandSync<std::string>({"RPUSH",
// worker_id_finished_name_, std::to_string(worker_id_)});

// maintains a connection to a redis queue, fetches items, and puts them in an
// AGD input queue
class RedoxFetcher {
 public:
  static errors::Status Create(const std::string& addr, int port, const std::string& queue_name,
                               std::unique_ptr<RedoxFetcher>& fetcher);

  agd::ReadQueueType* GetInputQueue() { return input_queue_.get(); }

 private:
  
  RedoxFetcher(const std::string& queue_name) : queue_name_(queue_name) {};

  errors::Status Init();

  redox::Redox rdx_;
  std::string queue_name_;
  std::unique_ptr<agd::ReadQueueType> input_queue_;
  std::thread loop_thread_;
  volatile bool done_ = false;
};