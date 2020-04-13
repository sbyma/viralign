#pragma once

#include <vector>
#include "liberr/errors.h"
#include "format.h"
#include "libagd/src/proto/alignment.pb.h"

namespace agd {

using namespace errors;

/*
 * A class that provides a "view" over the data in the resource container.
 * Does not take ownership of the underlying data
 *
 * This is the class to inherit from if you want another interface to
 * AGD chunk records.
 */
class AGDRecordReader {
 public:
  AGDRecordReader(const char* resource, size_t num_records);

  void Reset();
  int NumRecords() { return num_records_; }

  Status GetNextRecord(const char** data, size_t* size);
  Status PeekNextRecord(const char** data, size_t* size);

  Status GetRecordAt(size_t index, const char** data, size_t* size);

  size_t GetCurrentIndex() { return cur_record_; }

 private:
  const format::RelativeIndex* index_;
  const char *data_, *cur_data_;
  size_t cur_record_ = 0;
  std::vector<size_t> absolute_index_;

  void InitializeIndex();

 protected:
  size_t num_records_;
};

class AGDResultReader : public AGDRecordReader {
  public:

    // class Position is defined by alignment.pb.h

    // metadata column is required to disambiguate results that mapped to 
    // the same position.
    // if null is passed, GetResultAtLocation will not work.
    AGDResultReader(const char* resource, size_t num_records, AGDRecordReader* metadata=nullptr);

    // Get the result at specified GenomeLocation. Uses a binary search
    // for log(n) performance. metadata may be used to disambiguate reads
    // that have mapped to the same position, which is likely. Also returns
    // the index position the read was found at.
    Status GetResultAtPosition(Position& position, const char* metadata,
        size_t metadata_len, Alignment& result, size_t* index=nullptr);

    // Get or peek next alignment result in the index. 
    // NB again cigar is not a proper c-string
    Status GetNextResult(Alignment& result);
    Status PeekNextResult(Alignment& result);

    // Get a result at a specific index offset in the chunk
    // uses the Absolute index created on construction
    Status GetResultAtIndex(size_t index, Alignment& result);

    // is this location possibly contained
    // i.e. start_location_ <= location <= end_location_
    bool IsPossiblyContained(Position& position) {
      return position.ref_index() >= start_position_.ref_index() && position.ref_index() <= end_position_.ref_index()
              && position.position() >= start_position_.position() && position.position() <= end_position_.position();
    }

  private:

    Position start_position_;
    Position end_position_;
    AGDRecordReader* metadata_;

  };
}  // namespace agd
