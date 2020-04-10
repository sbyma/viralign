#pragma once 

#include <thread>
#include "fetcher.h"
#include "src/sw/redis++/redis++.h"

// maintains a connection to a redis queue, fetches items, and puts them in an
// AGD input queue
class RedisFetcher : public InputFetcher {
 public:
  static errors::Status Create(const std::string& addr, const std::string& queue_name,
                               std::unique_ptr<InputFetcher>& fetcher);


  errors::Status Run() override;

  void Stop() override;

  // we never know how many records to expect from Redis
  uint32_t MaxRecords() const override { return 0; }

 private:
  
  RedisFetcher(const std::string& queue_name) : queue_name_(queue_name) {
    input_queue_ = std::make_unique<agd::ReadQueueType>(5);
  };

  std::unique_ptr<sw::redis::Redis> redis_;
  std::string queue_name_;
  std::thread loop_thread_;
  volatile bool done_ = false;
};