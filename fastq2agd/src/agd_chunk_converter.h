
#pragma once

#include "fastq_chunk.h"
#include "libagd/src/buffer_pair.h"
#include "libagd/src/column_builder.h"
#include "libagd/src/compression.h"
#include "libagd/src/object_pool.h"

// contains compressed chunks for output.
struct FastqColumns {
  ObjectPool<agd::Buffer>::ptr_type base;
  ObjectPool<agd::Buffer>::ptr_type qual;
  ObjectPool<agd::Buffer>::ptr_type meta;
  size_t chunk_size;
};

// provide methods to parse a fastq chunk and output corresponding AGD column
// chunks
class AGDChunkConverter {
 public:
  AGDChunkConverter();

  // build columns, and compress to output bufs
  Status Convert(FastqChunk& fastq_chunk, FastqColumns* output_cols);
  
  Status ConvertPaired(FastqChunk& fastq_chunk_1, FastqChunk& fastq_chunk_2, FastqColumns* output_cols);

 private:
  // reused for every conversion call
  agd::BufferPair base_bufpair_;
  agd::BufferPair qual_bufpair_;
  agd::BufferPair meta_bufpair_;

  Status CompressBuffer(agd::BufferPair& buf_pair, ObjectPool<agd::Buffer>::ptr_type& buf);
};
