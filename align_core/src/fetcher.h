#pragma once

#include "liberr/errors.h"
#include "libagd/src/queue_defs.h"

// input fetch base class provides interface for access to an AGD read queue
// inheritors are responsible for init of the queue
class InputFetcher {
 public:
  virtual errors::Status Run() = 0;
  virtual void Stop() = 0;
  agd::ReadQueueType* GetInputQueue() { return input_queue_.get(); }
  virtual uint32_t MaxRecords() const = 0;

 protected:
  std::unique_ptr<agd::ReadQueueType> input_queue_;
};