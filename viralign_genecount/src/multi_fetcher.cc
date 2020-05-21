
#include "multi_fetcher.h"
#include <fstream>

errors::Status MultiFetcher::Run() {
  input_queue_ = std::make_unique<agd::ReadQueueType>(5);

  std::ifstream i(metadata_list_json_path_.data());
  i >> metadata_list_;
  i.close();

  for (const auto& meta : metadata_list_) {
    json agd_metadata;
    const auto& meta_path = meta.get<std::string>();
    std::ifstream i(meta_path);
    i >> agd_metadata;
    i.close();

    max_records_ += agd_metadata["records"].size();
  }

  std::cout << "[MultiFetcher] Max chunks: " << max_records_ << "\n";

  auto run_func = [this]() {
    for (const auto& meta : metadata_list_) {
      json agd_metadata;
      const auto& meta_path = meta.get<std::string>();
      std::cout << "[MultiFetcher] processing metadata file " << meta_path
                << "\n";
      std::ifstream i(meta_path);
      i >> agd_metadata;
      i.close();

      const auto& records = agd_metadata["records"];
      auto file_path_base =
          meta_path.substr(0, meta_path.find_last_of('/') + 1);

      std::cout << "[MultiFetcher] base path is " << file_path_base << "\n";

      std::string pool("");

      try {
        pool = agd_metadata["pool"];
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
        std::cout << "[MultiFetcher] chunk path / obj name is: " << item.objName
                  << ", pool name is: " << item.pool << "\n";

        input_queue_->push(std::move(item));
      }
    }
  };

  fetch_thread_ = std::thread(run_func);
  return errors::Status::OK();
}