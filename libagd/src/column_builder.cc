#include "column_builder.h"
#include "compression.h"
#include <iostream>
#include "util.h"

namespace agd {

using namespace std;
using namespace format;

void AlignmentResultBuilder::AppendEmpty() {
  format::RelativeIndex val = 0;
  index_->AppendBuffer(reinterpret_cast<const char*>(&val),
                       sizeof(RelativeIndex));
}

void AlignmentResultBuilder::AppendAlignmentResult(const Alignment& result) {
  // LOG(INFO) << "appending alignment result location: " << result.location_ <<
  // " mapq: " << result.mapq_ << " flags: "
  //<< result.flag_ << " next: " << result.next_location_ << " template_len: "
  //<< result.template_length_ << " cigar: " << var_string;
  size_t size = result.ByteSizeLong();
  scratch_.resize(size);
  result.SerializeToArray(&scratch_[0], size);
  ColumnBuilder::AppendRecord(&scratch_[0], size);
}

void ColumnBuilder::SetBufferPair(BufferPair* data) {
  data->reset();
  data_ = &data->data();
  index_ = &data->index();
}

void ColumnBuilder::AppendRecord(const char* data, const size_t size) {
  if (size > format::MAX_INDEX_SIZE)
    std::cout << "WARNING: Appending data larger than " << UINT8_MAX
              << " bytes not supported by AGD.";
  if (size > 0)  // could be a zero record
    data_->AppendBuffer(data, size);
  format::RelativeIndex cSize = static_cast<uint16_t>(size);
  index_->AppendBuffer(reinterpret_cast<const char*>(&cSize), sizeof(cSize));
}


Status ColumnBuilder::CompressChunk(agd::Buffer* output_buf) {
  output_buf->reserve(data_->size() + index_->size());
  output_buf->reset();
  agd::AppendingGZIPCompressor compressor(
      *output_buf);  // destructor releases GZIP resources
  ERR_RETURN_IF_ERROR(compressor.init());
  ERR_RETURN_IF_ERROR(compressor.appendGZIP(index_->data(), index_->size()));
  ERR_RETURN_IF_ERROR(compressor.appendGZIP(data_->data(), data_->size()));
  return Status::OK();
}

}  // namespace agd
