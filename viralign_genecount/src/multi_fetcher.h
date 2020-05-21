
#pragma once

#include <thread>
#include "json.hpp"
#include "libagd/src/fetcher.h"

using json = nlohmann::json;

// class to
class MultiFetcher : public InputFetcher {
 public:
  MultiFetcher(absl::string_view metadata_list_json_path)
      : metadata_list_json_path_(metadata_list_json_path) {}

  errors::Status Run() override;
  void Stop() override { fetch_thread_.join(); };
  uint32_t MaxRecords() const override { return max_records_; }

 private:
  absl::string_view metadata_list_json_path_;
  uint32_t max_records_;
  json metadata_list_;
  std::thread fetch_thread_;
};