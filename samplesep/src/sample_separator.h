
#pragma once

#include "absl/container/flat_hash_map.h"
#include "fastq_parser.h"
#include "libagd/src/buffer.h"
#include "libagd/src/column_builder.h"
#include "libagd/src/dataset_writer.h"
#include "libagd/src/object_pool.h"
#include "liberr/errors.h"

using namespace errors;

class SampleSeparator {
 public:
  using BarcodeIndices = std::pair<uint32_t, uint32_t>;
  using BarcodeMap = absl::flat_hash_map<std::string, std::string>;

  SampleSeparator(FastqParser* fastq_parser, FastqParser* sample_fastq_parser,
                  size_t chunk_size, const std::string& output_dir,
                  BarcodeIndices indices)
      : fastq_parser_(fastq_parser),
        sample_fastq_parser_(sample_fastq_parser),
        chunk_size_(chunk_size),
        output_dir_(output_dir),
        barcode_indices_(indices) {
    barcode_length_ = indices.second - indices.first;
  }

  Status Separate(const BarcodeMap& barcode_map);

 private:
  // build each sample one buffer at a time
  // the pair lets us build the data block and relative index at the same time
  // when at chunk_size_, we can push the SampleChunk to its associated
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

  // map from sample barcode to current chunk
  // depending on the number of samples and chunk size,
  // this could eat a lot of memory
  absl::flat_hash_map<absl::string_view, SampleChunk> sample_map_;

  absl::flat_hash_map<absl::string_view, std::unique_ptr<agd::DatasetWriter>>
      writer_map_;

  FastqParser* fastq_parser_;
  FastqParser* sample_fastq_parser_;
  agd::ObjectPool<agd::Buffer> buf_pool_;
  agd::ObjectPool<agd::BufferPair> bufpair_pool_;

  size_t chunk_size_;
  std::string output_dir_;

  BarcodeIndices barcode_indices_;
  uint32_t barcode_length_;

  uint32_t allowed_diffs_ = 1; // TODO make parameter
  std::vector<uint32_t> diffs_;
  std::vector<absl::string_view> barcodes_;

  bool SaveBarcode(absl::string_view barcode, absl::string_view* saved);

  // stats
  uint64_t num_bad_barcodes_ = 0;
  uint64_t num_saved_barcodes_ = 0;
};