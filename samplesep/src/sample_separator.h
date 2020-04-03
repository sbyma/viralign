
#pragma once

#include "fastq_parser.h"
#include "libagd/src/buffer.h"
#include "absl/container/flat_hash_map.h"
#include "libagd/src/dataset_writer.h"
#include "libagd/src/column_builder.h"
#include "libagd/src/object_pool.h"
#include "liberr/errors.h"

using namespace errors;

class SampleSeparator {
 public:
  SampleSeparator(FastqParser* fastq_parser, FastqParser* sample_fastq_parser, size_t chunk_size,
                  const std::string& output_dir)
      : fastq_parser_(fastq_parser),
        sample_fastq_parser_(sample_fastq_parser),
        chunk_size_(chunk_size),
        output_dir_(output_dir) {}

  Status Separate();

 private:
  // build each sample one buffer at a time
  // the pair lets us build the data block and relative index at the same time
  // when at chunk_size_, we can push the SmapleChunk to its associated
  // DatasetWriter for output
  struct SampleChunk {
    agd::ObjectPool<agd::BufferPair>::ptr_type base_buf;
    agd::ObjectPool<agd::BufferPair>::ptr_type qual_buf;
    agd::ObjectPool<agd::BufferPair>::ptr_type meta_buf;
    agd::ColumnBuilder base_builder;
    agd::ColumnBuilder qual_builder;
    agd::ColumnBuilder meta_builder;
    uint64_t first_ordinal;
    size_t current_size;
  };

  absl::flat_hash_map<absl::string_view, SampleChunk> sample_map_;
  absl::flat_hash_map<absl::string_view, std::unique_ptr<agd::DatasetWriter>>
      writer_map_;

  FastqParser* fastq_parser_;
  FastqParser* sample_fastq_parser_;
  agd::ObjectPool<agd::Buffer> buf_pool_;
  agd::ObjectPool<agd::BufferPair> bufpair_pool_;

  size_t chunk_size_;

  std::string output_dir_;
};