
#include "parallel_aligner.h"

#include <chrono>

#include "libagd/src/agd_record_reader.h"
#include "libagd/src/column_builder.h"

using namespace std::chrono_literals;
using namespace errors;

Status ParallelAligner::Create(size_t threads, GenomeIndex* index,
                               AlignerOptions* options,
                               InputQueueType* input_queue,
                               int filter_contig_index,
                               std::unique_ptr<ParallelAligner>& aligner) {
  aligner.reset(
      new ParallelAligner(index, options, input_queue, filter_contig_index));
  ERR_RETURN_IF_ERROR(aligner->Init(threads));
  return Status::OK();
}

Status ParallelAligner::Init(size_t threads) {
  aligner_threads_.resize(threads);
  output_queue_ = std::make_unique<OutputQueueType>(5);

  auto aligner_func = [this]() {
    const char *base, *qual;
    size_t base_len, qual_len;

    SingleAligner aligner(genome_index_, options_);

    while (!done_) {
      InputQueueItem item;
      if (!input_queue_->pop(item)) continue;

      if (item.col_bufs.size() != 2) {
        std::cout << "[ParallelAligner] Error: Expected 2 columns but got "
                  << item.col_bufs.size() << ", skipping ...\n";
        continue;
      }

      agd::AGDRecordReader base_reader(item.col_bufs[0]->data(),
                                       item.chunk_size);
      agd::AGDRecordReader qual_reader(item.col_bufs[1]->data(),
                                       item.chunk_size);

      auto out_buf_pair = bufpair_pool_.get();
      agd::AlignmentResultBuilder builder;
      builder.SetBufferPair(out_buf_pair.get());

      Status s = base_reader.GetNextRecord(&base, &base_len);
      while (s.ok()) {
        s = qual_reader.GetNextRecord(&qual, &qual_len);
        if (!s.ok()) {
          std::cout << "[ParallelAligner] no corresponding qual for base, "
                       "thread ending ...\n";
          return;
        }
        // std::cout << "[ParallelAligner] Aligning read: \n"
        //<< std::string(base, base_len) << "\n"
        //<< std::string(qual, qual_len) << "\n\n";

        Read read;
        read.init("", 0, base, qual, base_len);
        Alignment aln;
        GenomeLocation loc;

        s = aligner.AlignRead(read, aln, loc);

        // std::cout << "[ParallelAligner] aligned to location: " <<
        // aln.DebugString() << "\n";
        if (filter_contig_index_ >= 0 &&
            aln.position().ref_index() != filter_contig_index_) {
          builder.AppendEmpty();
        } else {
          builder.AppendAlignmentResult(aln);
        }

        if (aln.position().ref_index() != -1) {
          num_mapped_++;
        }

        num_aligned_++;
        s = base_reader.GetNextRecord(&base, &base_len);
      }

      OutputQueueItem out_item;
      out_item.col_buf_pairs.push_back(std::move(out_buf_pair));
      out_item.chunk_size = item.chunk_size;
      out_item.pool = item.pool;
      out_item.first_ordinal = item.first_ordinal;
      out_item.name = std::move(item.name);
      output_queue_->push(std::move(out_item));
    }
  };

  for (auto& t : aligner_threads_) {
    t = std::thread(aligner_func);
  }

  return Status::OK();
}

void ParallelAligner::Stop() {
  while (!input_queue_->empty()) std::this_thread::sleep_for(1ms);

  input_queue_->unblock();
  done_ = true;

  for (auto& t : aligner_threads_) {
    t.join();
  }

  std::cout << "[ParallelAligner] aligned " << num_aligned_.load() << " reads, "
            << num_mapped_.load() << " successfully mapped ("
            << (float(num_mapped_.load()) / float(num_aligned_.load()))*100.0f << "%)\n";
}
