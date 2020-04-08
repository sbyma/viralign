#include "liberr/errors.h"
#include "fetcher.h"
#include <thread>
#include "json.hpp"

using json = nlohmann::json;

// fill a agd read queue from a local agd metadata file
class LocalFetcher : public InputFetcher {
 public:
  LocalFetcher(const std::string& agd_meta_path)
      : agd_meta_path_(agd_meta_path) {}

  errors::Status Run() override;
  void Stop() override { fetch_thread_.join(); };

  uint32_t MaxRecords() const override { return max_records_; }
 private:

  std::string agd_meta_path_;
  std::thread fetch_thread_;
  json agd_metadata_;
  int max_records_ = -1;
};