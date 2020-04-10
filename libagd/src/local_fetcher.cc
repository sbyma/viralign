
#include "local_fetcher.h"

#include <fstream>

#include "json.hpp"

using json = nlohmann::json;

errors::Status LocalFetcher::Run() {

  input_queue_ = std::make_unique<agd::ReadQueueType>(5);
  std::ifstream i(agd_meta_path_.data());
  i >> agd_metadata_;
  i.close();

  const auto& records = agd_metadata_["records"];

  max_records_ = records.size();

  auto run_func = [this]() {

    const auto& records = agd_metadata_["records"];

    auto file_path_base =
        agd_meta_path_.substr(0, agd_meta_path_.find_last_of('/') + 1);

    std::cout << "[LocalFetcher] base path is " << file_path_base << "\n";

    std::string pool("");

    try {
      pool = agd_metadata_["pool"];
    } catch (...) {
      // no pool exists, its fine
    }

    for (const auto& rec : records) {
      agd::ReadQueueItem item;

      // objects in a ceph pool do not need a path base
      if (pool != "") {
        item.objName = rec["path"].get<std::string>();
      } else {
        item.objName =
            absl::StrCat(file_path_base, rec["path"].get<std::string>());
      }
      item.pool = pool;
      std::cout << "[LocalFetcher] chunk path / obj name is: " << item.objName
                << ", pool name is: " << item.pool << "\n";

      input_queue_->push(std::move(item));
    }
  };

  fetch_thread_ = std::thread(run_func);
  return errors::Status::OK();
}
