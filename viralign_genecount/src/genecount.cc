
#include <fstream>

#include "genecount.h"
#include "libagd/src/agd_record_reader.h"
#include "libagd/src/proto/alignment.pb.h"

using namespace errors;

inline int parseNextOp(const char* ptr, char& op, int& num) {
  num = 0;
  const char* begin = ptr;
  for (char curChar = ptr[0]; curChar != 0; curChar = (++ptr)[0]) {
    int digit = curChar - '0';
    if (digit >= 0 && digit <= 9)
      num = num * 10 + digit;
    else
      break;
  }
  op = (ptr++)[0];
  return ptr - begin;
}

uint32_t ParseCigarLen(const char* cigar, size_t cigar_len) {
  // cigar parsing adapted from samblaster
  uint32_t total_len = 0;
  char op;
  int op_len;
  while (cigar_len > 0) {
    size_t len = parseNextOp(cigar, op, op_len);
    cigar += len;
    cigar_len -= len;
    if (op == 'M' || op == 'I' || op == 'S' || op == '=' || op == 'X') {
      total_len += op_len;
    }
  }
  return total_len;
}

errors::Status CountGenes(uint32_t max_chunks, agd::ChunkQueueType* input_queue,
                          const IntervalForest* interval_forest, const GeneIdMap& genes) {
  // get chunk
  // for each alignment
  // process based on cigar string, looking up intervals
  // map < sample, map < gene_id, int > > to track reads mapping to genes
  // TODO parallelize and multi thread this code, should be fairly straightforward as the interval tree is read only

  absl::flat_hash_map<std::string, absl::flat_hash_map<std::string, uint32_t>>
      sample_genecount_map;

  agd::ChunkQueueItem item;
  Alignment aln;

  std::vector<const TreeValue*> found_intervals;
  found_intervals.reserve(15);

  uint64_t num_alignments = 0;
  uint64_t num_mapped_alignments = 0;
  uint64_t num_samples = 0;
  for (uint32_t i = 0; i < max_chunks; i++) {
    input_queue->pop(item);

    assert(item.col_bufs.size() == 1);

    agd::AGDResultReader aln_reader(item.col_bufs[0]->data(), item.chunk_size);

    if (!sample_genecount_map.contains(item.name)) {
      sample_genecount_map.insert_or_assign(
          item.name, absl::flat_hash_map<std::string, uint32_t>());
      num_samples++;
    }

    auto& sample_genemap = sample_genecount_map[item.name];

    bool done_chunk = false;
    while (!done_chunk) {
      // process this alignment
      Status s = aln_reader.GetNextResult(aln);
      if (IsResourceExhausted(s)) {
        done_chunk = true;
        continue;
      } else if (IsUnavailable(s)) {
        continue;  // empty result
      }

      num_alignments++;

      const auto& cigar = aln.cigar();
      auto cigar_len = ParseCigarLen(cigar.data(), cigar.size());
      std::cout << "[viralign-countgenes] Alignment length for cigar " << cigar
                << " was " << cigar_len << "\n";

      const auto& contig_name = aln.position().contig();
      int start = aln.position().position();  // + 1 ?
      int end = start + cigar_len;

      if (!interval_forest->contains(contig_name)) {
        continue;  // read maps to no known contig / chr
      } else {
        found_intervals.clear();
        interval_forest->at(contig_name)
            .findOverlappingValues(start, end, found_intervals);
      }

      std::cout << "[viralign-genecount] read mapped to "
                << found_intervals.size() << " genes\n";
      for (auto vptr : found_intervals) {
         // ignore the strand for now
        if (!sample_genemap.contains(vptr->gene_id)) {
          sample_genemap.insert_or_assign(vptr->gene_id, 1);
        } else {
          sample_genemap[vptr->gene_id]++;
        }
        num_mapped_alignments++;
      }
    }
  }

  std::cout << "[viralign-genecount] Processed " << num_alignments << " from "
            << num_samples << " of which " << num_mapped_alignments
            << " were mapped to genes.\n";

  // build a csv, columns are genes, < 1 col per sample >

  std::ofstream output_matrix("genecount.csv");
  output_matrix << "gene_id, ";
  size_t max_samples = sample_genecount_map.size();
  size_t i = 0;
  for (const auto& sample : sample_genecount_map) {
    output_matrix << sample.first;
    if (i != max_samples - 1) {
      output_matrix << ", ";
    } 
    i++;
  }
  output_matrix << "\n";

  for (const auto& gene : genes) {
    output_matrix << gene.first << "(";
    for (const auto& name : gene.second) {
      output_matrix << name << " ";
    }
    output_matrix << "), ";
    i = 0;
    for (const auto& sample : sample_genecount_map) {
      if (sample.second.contains(gene.first)) {
        output_matrix << sample.second.at(gene.first);
      } else {
        output_matrix << "0";
      }
      if (i != max_samples - 1) {
        output_matrix << ", ";
      } 
      i++;
    }
  }
  
  return Status::OK();

}