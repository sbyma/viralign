#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "buffer_list.h"
#include "format.h"
#include "libagd/src/proto/alignment.pb.h"

namespace agd {

// given a buffer pair (for data and index), provides an interface to append
// records, and compress the final chunk payload does not include the chunk
// header, which must be added when written to disk
class ColumnBuilder {
 public:
  virtual ~ColumnBuilder(){};
  virtual void SetBufferPair(BufferPair* data);

  virtual void AppendRecord(const char* data, const std::size_t size);

  // finish 
  Status CompressChunk(agd::Buffer* output_buf);

 protected:
  Buffer *data_ = nullptr, *index_ = nullptr;
};

class AlignmentResultBuilder : public ColumnBuilder {
 public:
  using ColumnBuilder::AppendRecord;
  using ColumnBuilder::SetBufferPair;
  /*
    Append the current alignment result to the internal result buffer
    result_size is needed to ensure that the result is the correct size

    The AppendAlignmentResult Methods take a character index, which is passed in
    from outside to enable the aligner kernel to pass in a buffer from a pool,
    to build up a result in it.

    records_ is used to build up the actual reads, which are appended into the
    chunk.
  */

  // void AppendAlignmentResult(const SingleAlignmentResult &result, const
  // std::string &var_string, const int flag);

  // void AppendAlignmentResult(const SingleAlignmentResult &result);

  // This is the only one we should use now

  void AppendAlignmentResult(const Alignment& result);
  // sometimes we want to append an empty result
  // e.g. not all reads will generate X secondary alignments (so some columns
  // will have gaps)
  void AppendEmpty();

  // void AppendAlignmentResult(const PairedAlignmentResult &result, const
  // std::size_t result_idx);

 private:
  std::vector<char> scratch_;
};

}  // namespace agd
