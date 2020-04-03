
#include "agd_chunk_converter.h"

AGDChunkConverter::AGDChunkConverter() {
  base_bufpair_.data().reserve(1024 * 1024);
  base_bufpair_.index().reserve(1024 * 100);
  qual_bufpair_.data().reserve(1024 * 1024);
  qual_bufpair_.index().reserve(1024 * 100);
  meta_bufpair_.data().reserve(1024 * 1024);
  meta_bufpair_.index().reserve(1024 * 100);
}

Status AGDChunkConverter::Convert(FastqChunk &fastq_chunk,
                                  FastqColumns *output_cols) {
  const char *base, *qual, *meta;
  size_t base_len, meta_len;
  auto s = Status::OK();

  base_bufpair_.reset();
  qual_bufpair_.reset();
  meta_bufpair_.reset();

  agd::ColumnBuilder base_builder, qual_builder, meta_builder;
  base_builder.SetBufferPair(&base_bufpair_);
  qual_builder.SetBufferPair(&qual_bufpair_);
  meta_builder.SetBufferPair(&meta_bufpair_);

  s = fastq_chunk.GetNextRecord(&base, &base_len, &qual, &meta, &meta_len);
  while (s.ok()) {
    base_builder.AppendRecord(base, base_len);
    qual_builder.AppendRecord(qual, base_len);
    meta_builder.AppendRecord(meta, meta_len);
    s = fastq_chunk.GetNextRecord(&base, &base_len, &qual, &meta, &meta_len);
  }

  output_cols->chunk_size = fastq_chunk.NumRecords();
  ERR_RETURN_IF_ERROR(CompressBuffer(base_bufpair_, output_cols->base));
  ERR_RETURN_IF_ERROR(CompressBuffer(qual_bufpair_, output_cols->qual));
  ERR_RETURN_IF_ERROR(CompressBuffer(meta_bufpair_, output_cols->meta));

  return Status::OK();
}

Status AGDChunkConverter::ConvertPaired(FastqChunk &fastq_chunk_1,
                                        FastqChunk &fastq_chunk_2,
                                        FastqColumns *output_cols) {
  const char *base, *qual, *meta;
  size_t base_len, meta_len;
  auto s = Status::OK();
  
  base_bufpair_.reset();
  qual_bufpair_.reset();
  meta_bufpair_.reset();

  agd::ColumnBuilder base_builder, qual_builder, meta_builder;
  base_builder.SetBufferPair(&base_bufpair_);
  qual_builder.SetBufferPair(&qual_bufpair_);
  meta_builder.SetBufferPair(&meta_bufpair_);

  s = fastq_chunk_1.GetNextRecord(&base, &base_len, &qual, &meta, &meta_len);
  while (s.ok()) {
    base_builder.AppendRecord(base, base_len);
    qual_builder.AppendRecord(qual, base_len);
    meta_builder.AppendRecord(meta, meta_len);
    s = fastq_chunk_2.GetNextRecord(&base, &base_len, &qual, &meta, &meta_len);
    if (!s.ok()) {
      return errors::Internal(
          "Fastq chunk 2 did not have entry where expected.");
    }
    base_builder.AppendRecord(base, base_len);
    qual_builder.AppendRecord(qual, base_len);
    meta_builder.AppendRecord(meta, meta_len);
    s = fastq_chunk_1.GetNextRecord(&base, &base_len, &qual, &meta, &meta_len);
  }

  // output chunk size will be twice that of
  // input, because we interleave paired reads
  output_cols->chunk_size = fastq_chunk_1.NumRecords() * 2;
  ERR_RETURN_IF_ERROR(base_builder.CompressChunk(output_cols->base.get()));
  ERR_RETURN_IF_ERROR(qual_builder.CompressChunk(output_cols->qual.get()));
  ERR_RETURN_IF_ERROR(meta_builder.CompressChunk(output_cols->meta.get()));
  
  /*ERR_RETURN_IF_ERROR(CompressBuffer(base_bufpair_, output_cols->base));
  ERR_RETURN_IF_ERROR(CompressBuffer(qual_bufpair_, output_cols->qual));
  ERR_RETURN_IF_ERROR(CompressBuffer(meta_bufpair_, output_cols->meta));*/

  return Status::OK();
}

Status AGDChunkConverter::CompressBuffer(agd::BufferPair &buf_pair,
                                         ObjectPool<agd::Buffer>::ptr_type& buf) {
  buf->reserve(buf_pair.data().size() + buf_pair.index().size());
  agd::AppendingGZIPCompressor compressor(
      *buf);  // destructor releases GZIP resources
  ERR_RETURN_IF_ERROR(compressor.init());
  auto &index = buf_pair.index();
  ERR_RETURN_IF_ERROR(compressor.appendGZIP(index.data(), index.size()));
  auto &data = buf_pair.data();
  ERR_RETURN_IF_ERROR(compressor.appendGZIP(data.data(), data.size()));
  return Status::OK();
}