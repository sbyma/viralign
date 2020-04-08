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

  return fetcher->Init();
}

Status RedoxFetcher::Init() {
  auto loop_func = [this]() {
    json j;
    std::string test;
    while (!done_) {
      agd::ReadQueueItem item;
      // issue LPOP command
      redox::Command<std::string>& cmd =
          rdx_.commandSync<std::string>({"BLPOP", queue_name_});

      if (cmd.status() == redox::Command<std::string>::NIL_REPLY) {
        std::cout << "[RedoxFetch] received NIL reply, exiting ...\n";
        done_ = false;
      } else if (cmd.ok()) {
        // parse string to json
        j = json::parse(cmd.reply());
        item.objName = j["obj_name"];
        item.pool = j["pool"]; // will be empty if data is in FS
        input_queue_->push(std::move(item));
      }

      cmd.free();
    }
  };

  loop_thread_ = std::thread(loop_func);

  return Status::OK();
}