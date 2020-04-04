
#include "sample_separator.h"

Status SampleSeparator::Separate() {
  const char *base, *qual, *meta, *sample_base, *sample_qual, *sample_meta;
  size_t base_len, meta_len, sample_base_len, sample_meta_len;
  Status s = Status::OK();

  s = fastq_parser_->GetNextRecord(&base, &base_len, &qual, &meta, &meta_len);

  while (s.ok()) {
    s = sample_fastq_parser_->GetNextRecord(&sample_base, &sample_base_len,
                                            &sample_qual, &sample_meta,
                                            &sample_meta_len);
    ERR_RETURN_IF_ERROR(s);

    // use the sample bases to look up the appropriate dataset writer
    // unclear how accurate the sample barcodes will be
    // its possible we need to preprocess just the sample file in order to
    // filter out bad barcodes
    // TODO ask ioannis what expected bad barcode rates will be, and ask his
    // advice on dealing with that

    absl::string_view sample_key(sample_base, sample_base_len);

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
        std::vector<agd::ObjectPool<agd::BufferPair>::ptr_type> col_bufs(3);
        col_bufs.push_back(std::move(sample_chunk.base_buf));
        col_bufs.push_back(std::move(sample_chunk.qual_buf));
        col_bufs.push_back(std::move(sample_chunk.meta_buf));
        dataset_writer->WriteChunks(col_bufs, chunk_size_,
                                    sample_chunk.first_ordinal);

        if (sample_chunk.base_buf.get() != nullptr) {
          std::cout << "the buf was not moved? what the fuck\n";
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
      sample_map_.insert_or_assign(sample_key, newchunk);

      // create the writer
      std::unique_ptr<agd::DatasetWriter> writer;
      auto out_dir = absl::StrCat(output_dir_, sample_key, "/");
      ERR_RETURN_IF_ERROR(agd::DatasetWriter::CreateDatasetWriter(
          3, 1, std::string(sample_key), out_dir, {"base", "qual", "meta"},
          writer, &buf_pool_));
      writer_map_.insert_or_assign(sample_key, writer);
    }

    s = fastq_parser_->GetNextRecord(&base, &base_len, &qual, &meta, &meta_len);
  }

  std::cout << "[samplesep] fastq processing complete\n";
  for (auto& v : writer_map_) {
    v.second->Stop();
    v.second->WriteMetadata();
  }

  return Status::OK();
}

