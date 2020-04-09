#include "redis_fetcher.h"

#include <string>

#include "json.hpp"

using json = nlohmann::json;
using namespace errors;
using namespace sw::redis;

Status RedisFetcher::Create(const std::string& addr,
                            const std::string& queue_name,
                            std::unique_ptr<InputFetcher>& fetcher) {
  auto nf = new RedisFetcher(queue_name);
  auto full_addr = absl::StrCat("tcp://", addr);
  std::cout << "[RedisFetcher] Creating and connecting to " << full_addr << "\n";

  nf->redis_.reset(new Redis(full_addr));

  std::cout << "[RedisFetcher] Connection success.\n";
  
  fetcher.reset(nf);

  return Status::OK();
}

Status RedisFetcher::Run() {

  std::cout << "[RedisFetcher] Running ...\n";

  auto loop_func = [this]() {
    json j;
    std::string test;
    while (!done_) {
      agd::ReadQueueItem item;
      // issue LPOP command
      std::cout << "[RedisFetcher] Issuing LPOP ...\n";

      auto response = redis_->blpop({queue_name_});

      if (response) {
        std::cout << "[RedisFetcher] The reply is: " << response.value().first << " and " << response.value().second << "\n";
      }

      if (!response) {
        std::cout << "[RedisFetcher] received NIL reply, exiting ...\n";
        done_ = false;
      } else {
        // parse string to json
        j = json::parse(response.value().second);
        item.objName = j["obj_name"];
        item.pool = j["pool"];  // will be empty if data is in FS
        std::cout << "[RedisFetcher] pull queue item with name: "
                  << item.objName << " and pool: " << item.pool << "\n";
        input_queue_->push(std::move(item));
      }

    }
  };

  loop_thread_ = std::thread(loop_func);

  return Status::OK();
}

void RedisFetcher::Stop() {
  done_ = false;
  loop_thread_.join();
}
