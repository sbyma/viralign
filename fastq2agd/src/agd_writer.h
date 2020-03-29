
#include "json.hpp"
#include "agd_chunk_converter.h"
#include "concurrent_queue/concurrent_queue.h"
#include <vector>
#include <tuple>

struct OutputQueueItem {
  FastqColumns columns;
  uint64_t first_ordinal;
};

typedef ConcurrentQueue<OutputQueueItem> OutputQueueType;

typedef std::vector<nlohmann::json> RecordVec;
// class to facilitate writing AGD dataset to disk.
// chunks can come in any order, so need to build the json metadata output as we go
class AGDWriter {
  public:
    AGDWriter(const std::string& dataset_name) : dataset_name_(dataset_name) {}

    // call first, or writes may fail
    Status Init(const std::string& output_dir);

    Status Write(const OutputQueueItem& item);

    Status WriteMetadata();

    const RecordVec& GetRecordVec() const { return records_; }

    private:
      std::string dataset_name_;
      std::string output_dir_;
      RecordVec records_;
};