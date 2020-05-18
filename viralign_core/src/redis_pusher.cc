
#include "redis_pusher.h"


errors::Status RedisPusher::Run() {

  agd::OutputQueueItem item;
  while (true) {
    if (!input_queue_->pop(item)) {
      continue;
    }

    // why doesn't it work with absl::string_view ???? :-(
    sw::redis::StringView name(item.objName);
    sw::redis::StringView queue_name(queue_name_.data(), queue_name_.size());
    auto ret = redis_->rpush(queue_name, name);

    std::cout << "[RedisPusher] Pushed chunk name " << item.objName << " to queue " << queue_name_ << "\n";
  }

  return errors::Status::OK();
}