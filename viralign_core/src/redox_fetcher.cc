#include "redox_fetcher.h"

#include <string>

#include "json.hpp"

using json = nlohmann::json;
using namespace errors;

Status RedoxFetcher::Create(const std::string& addr, int port,
                            const std::string& queue_name,
                            std::unique_ptr<RedoxFetcher>& fetcher) {
  fetcher.reset(new RedoxFetcher(queue_name));
  auto ret = fetcher->rdx_.connect(addr, port);
  if (!ret) {
    return errors::Internal("Failed to connect to redis\n");
  }

  return Status::OK();
}

Status RedoxFetcher::Run() {
  input_queue_ = std::make_unique<agd::ReadQueueType>(5);

  auto loop_func = [this]() {
    json j;
    std::string test;
    while (!done_) {
      agd::ReadQueueItem item;
      // issue LPOP command
      redox::Command<std::string>& cmd =
          rdx_.commandSync<std::string>({"BLPOP", queue_name_});

      if (cmd.status() == redox::Command<std::string>::NIL_REPLY) {
        std::cout << "[RedoxFetcher] received NIL reply, exiting ...\n";
        done_ = false;
      } else if (cmd.ok()) {
        // parse string to json
        j = json::parse(cmd.reply());
        item.objName = j["obj_name"];
        item.pool = j["pool"];  // will be empty if data is in FS
        std::cout << "[RedoxFetcher] pull queue item with name: "
                  << item.objName << " and pool: " << item.pool << "\n";
        input_queue_->push(std::move(item));
      }

      cmd.free();
    }
  };

  loop_thread_ = std::thread(loop_func);

  return Status::OK();
}

void RedoxFetcher::Stop() {
  done_ = false;
  loop_thread_.join();
}
