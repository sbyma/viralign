#pragma once 

#include "fetcher.h"
#include "redox.hpp"


// redox::Command<std::string>& cmd = rdx_.commandSync<std::string>({"RPUSH",
// worker_id_finished_name_, std::to_string(worker_id_)});

// maintains a connection to a redis queue, fetches items, and puts them in an
// AGD input queue
class RedoxFetcher : public InputFetcher {
 public:
  static errors::Status Create(const std::string& addr, int port, const std::string& queue_name,
                               std::unique_ptr<RedoxFetcher>& fetcher);


  errors::Status Run() override;

  void Stop() override;

  // we never know how many records to expect from Redis
  uint32_t MaxRecords() const override { return 0; }

 private:
  
  RedoxFetcher(const std::string& queue_name) : queue_name_(queue_name) {};

  redox::Redox rdx_;
  std::string queue_name_;
  std::thread loop_thread_;
  volatile bool done_ = false;
};