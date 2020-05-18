
#include "sample_separator.h"
#include <fstream>

Status SampleSeparator::Separate(const BarcodeMap& barcode_map) {
  auto start_time = std::chrono::high_resolution_clock::now();

  for (const auto& barcode_kv : barcode_map) {
    barcodes_.push_back(absl::string_view(barcode_kv.first));
  }

  const char *base, *qual, *meta, *barcode_base, *barcode_qual, *barcode_meta;
  size_t base_len, meta_len, barcode_base_len, barcode_meta_len;
  Status s = Status::OK();

  uint64_t reads_processed = 0;
  s = fastq_parser_->GetNextRecord(&base, &base_len, &qual, &meta, &meta_len);

  while (s.ok()) {
    /*std::cout << "[samplesep] processing read:\n"
              << std::string(meta, meta_len) << "\n"
              << std::string(base, base_len) << "\n"
              << std::string(qual, base_len) << "\n";*/

    s = sample_fastq_parser_->GetNextRecord(&barcode_base, &barcode_base_len,
                                            &barcode_qual, &barcode_meta,
                                            &barcode_meta_len);
    ERR_RETURN_IF_ERROR(s);

    // use the barcode bases to look up the appropriate dataset writer
    // not found barcodes with diff of one from existing can be "saved"

    /*std::cout << "Looking up barcode: "
              << absl::string_view(barcode_base + barcode_indices_.first,
                                   barcode_length_)
              << "\n";*/

    absl::string_view sample_key(barcode_base + barcode_indices_.first,
                                 barcode_length_);

    if (!barcode_map.contains(sample_key)) {
      // not a known barcode!
      // try to "save" it if we can
      absl::string_view saved;
      if (SaveBarcode(sample_key, &saved)) {
        sample_key = saved;
        num_saved_barcodes_++;
      } else {
        num_bad_barcodes_++;
        /*std::cout << "[samplesep] Unable to save barcode " << sample_key
                  << ", skipping ...\n";*/
        s = fastq_parser_->GetNextRecord(&base, &base_len, &qual, &meta,
                                         &meta_len);
        reads_processed++;
        continue;
      }
    }

    if (sample_map_.contains(sample_key)) {
      auto& sample_chunk = sample_map_[sample_key];
      sample_chunk.base_builder.AppendRecord(base, base_len);
      sample_chunk.qual_builder.AppendRecord(qual, base_len);
      sample_chunk.meta_builder.AppendRecord(meta, meta_len);
      sample_chunk.current_size++;

      // if we are at the chunk size, write it out
      if (sample_chunk.current_size == chunk_size_) {
        // find the associated dataset writer and send this chunk for writing
        auto& dataset_writer = writer_map_[sample_key];
        std::vector<agd::ObjectPool<agd::BufferPair>::ptr_type> col_bufs;
        col_bufs.reserve(3);
        col_bufs.push_back(std::move(sample_chunk.base_buf));
        col_bufs.push_back(std::move(sample_chunk.qual_buf));
        col_bufs.push_back(std::move(sample_chunk.meta_buf));
        ERR_RETURN_IF_ERROR(dataset_writer->WriteChunks(
            col_bufs, chunk_size_, sample_chunk.first_ordinal));

        if (sample_chunk.base_buf.get() != nullptr) {
          std::cout << "[samplesep] the buf was not moved? what the fuck\n";
        }
        // give fresh bufs for the next chunk, advance first ordinal
        sample_chunk.base_buf = std::move(bufpair_pool_.get());
        sample_chunk.qual_buf = std::move(bufpair_pool_.get());
        sample_chunk.meta_buf = std::move(bufpair_pool_.get());
        sample_chunk.base_builder.SetBufferPair(sample_chunk.base_buf.get());
        sample_chunk.qual_builder.SetBufferPair(sample_chunk.qual_buf.get());
        sample_chunk.meta_builder.SetBufferPair(sample_chunk.meta_buf.get());
        sample_chunk.first_ordinal += chunk_size_;
        sample_chunk.current_size = 0;
      }

    } else {
      // it doesnt exist, initialize
      std::cout << "[samplesep] creating new writer for new sample: "
                << sample_key << "\n";
      SampleChunk newchunk;
      newchunk.base_buf = std::move(bufpair_pool_.get());
      newchunk.qual_buf = std::move(bufpair_pool_.get());
      newchunk.meta_buf = std::move(bufpair_pool_.get());
      newchunk.base_builder.SetBufferPair(newchunk.base_buf.get());
      newchunk.qual_builder.SetBufferPair(newchunk.qual_buf.get());
      newchunk.meta_builder.SetBufferPair(newchunk.meta_buf.get());
      // append the record
      newchunk.base_builder.AppendRecord(base, base_len);
      newchunk.qual_builder.AppendRecord(qual, base_len);
      newchunk.meta_builder.AppendRecord(meta, meta_len);
      newchunk.first_ordinal = 0;
      newchunk.current_size = 1;
      sample_map_.insert_or_assign(sample_key, std::move(newchunk));

      // create the writer
      const auto& name = barcode_map.at(sample_key);
      std::cout << "[samplesep] New dataset name is " << name << "\n";

      std::unique_ptr<agd::DatasetWriter> writer;
      auto out_dir = absl::StrCat(output_dir_, name, "/");

      ERR_RETURN_IF_ERROR(agd::DatasetWriter::CreateDatasetWriter(
          1, 1, name, out_dir, {"base", "qual", "meta"}, writer, &buf_pool_));

      writer_map_.insert_or_assign(sample_key, std::move(writer));
    }

    s = fastq_parser_->GetNextRecord(&base, &base_len, &qual, &meta, &meta_len);
    reads_processed++;

    if (reads_processed % 1000000 == 0) {
      auto now = std::chrono::high_resolution_clock::now();
      auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
                        now - start_time)
                        .count();
      std::cout << "[samplesep] Processed " << reads_processed << " reads in "
                << float(millis) / 1000.0f << " seconds ...\n";
    }
  }

  // write out last chunks that weren't full size
  for (auto& chunk : sample_map_) {
    if (chunk.second.current_size > 0) {
      auto& sample_chunk = chunk.second;
      auto& dataset_writer = writer_map_[chunk.first];
      std::vector<agd::ObjectPool<agd::BufferPair>::ptr_type> col_bufs;
      col_bufs.reserve(3);
      col_bufs.push_back(std::move(sample_chunk.base_buf));
      col_bufs.push_back(std::move(sample_chunk.qual_buf));
      col_bufs.push_back(std::move(sample_chunk.meta_buf));
      ERR_RETURN_IF_ERROR(dataset_writer->WriteChunks(
          col_bufs, sample_chunk.current_size, sample_chunk.first_ordinal));
    }
  }

  std::cout << "[samplesep] fastq processing complete, wrote out "
            << writer_map_.size() << " sample datasets from " << reads_processed
            << " reads.\n";
  auto now = std::chrono::high_resolution_clock::now();
  auto millis =
      std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time)
          .count();
  std::cout << "[samplesep] Elapsed time: " << float(millis) / 1000.0f << " seconds\n";
  std::cout << "[samplesep] # bad barcodes: " << num_bad_barcodes_ << "\n";
  std::cout << "[samplesep] # saved barcodes: " << num_saved_barcodes_ << "\n";
  
  std::ofstream stats_output("samplesep_datasets.csv");
  stats_output << "Name, Path\n";
  for (auto& v : writer_map_) {
    stats_output << v.second->Name() << ", " << v.second->Path() << "\n";
    v.second->Stop();
    v.second->WriteMetadata();
  }

  return Status::OK();
}

// adapted from
// https://github.com/DeplanckeLab/BRB-seqTools/blob/master/src/tools/Utils.java#L186
// see if there are any existing barcodes with max differences of
// `allowed_diffs_` if more than one exists, don't use any
bool SampleSeparator::SaveBarcode(absl::string_view barcode,
                                  absl::string_view* saved) {
  diffs_.resize(barcodes_.size());
  memset(&diffs_[0], 0,
         sizeof(uint32_t) * diffs_.size());  // zero the diffs array

  for (size_t i = 0; i < barcode.size(); i++) {
    for (size_t j = 0; j < barcodes_.size(); j++) {
      if (barcode[i] != barcodes_[j][i]) {
        diffs_[j]++;
      }
    }
  }

  absl::string_view savedbc;
  for (size_t i = 0; i < diffs_.size(); i++) {
    if (diffs_[i] <= allowed_diffs_) {
      if (!savedbc.empty()) {
        // there is more than one <= allowed_diffs_
        return false;
        break;
      }
      savedbc = barcodes_[i];
    }
  }

  if (savedbc.empty()) {
    return false;
  } else {
    *saved = savedbc;
    /*std::cout << "[samplesep] Saved barcode " << barcode << ", is now "
              << savedbc << "\n";*/
    return true;
  }
}