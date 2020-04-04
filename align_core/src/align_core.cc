
#include <fstream>
#include <iostream>
#include <thread>

#include "absl/strings/str_split.h"
#include "args.h"
#include "libagd/src/agd_dataset.h"
#include "snap-master/SNAPLib/Bam.h"
#include "snap-master/SNAPLib/Read.h"
#include "snap_single_aligner.h"

using json = nlohmann::json;
using namespace errors;

void CheckStatus(Status& s) {
  if (!s.ok()) {
    std::cout << "Error: " << s.error_message() << "\n";
    exit(0);
  }
}

int main(int argc, char** argv) {
  args::ArgumentParser parser(
      "align-core",
      "Align reads using SNAP from either Ceph or Local disk AGD files");
  args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
  args::ValueFlag<unsigned int> threads_arg(
      parser, "threads",
      absl::StrCat("Number of threads to use for all stages [",
                   std::thread::hardware_concurrency(), "]"),
      {'t', "threads"});
  args::ValueFlag<std::string> queue_arg(
      parser, "redis queue", "Address of the redis queue to pull work from.",
      {'q', "queue"});
  args::ValueFlag<std::string> ceph_json_arg(parser, "ceph config file json",
                                             "Ceph config json path",
                                             {'c', "ceph_config"});
  args::ValueFlag<std::string> snap_args_arg(
      parser, "snap args", "Any args to pass to SNAP", {'s', "snap_args"});
  args::ValueFlag<std::string> genome_location_arg(
      parser, "genomeloc", "SNAP Genome Index location", {'g', "genome_loc"});
  args::ValueFlag<std::string> agd_metadata_args(
      parser, "agd args", "For testing, an AGD metadata to align some stuff",
      {'i', "input_metadata"});

  try {
    parser.ParseCLI(argc, argv);
  } catch (args::Help) {
    std::cout << parser;
    return 0;
  } catch (args::ParseError e) {
    std::cerr << e.what() << std::endl;
    std::cerr << parser;
    return 1;
  } catch (args::ValidationError e) {
    std::cerr << e.what() << std::endl;
    std::cerr << parser;
    return 1;
  }

  InitializeSeedSequencers();

  std::string snap_cmd("");
  if (snap_args_arg) {
    snap_cmd = args::get(snap_args_arg);
  }

  std::string genome_location;
  if (!genome_location_arg) {
    std::cout << "Genome index is required\n";
    exit(0);
  } else {
    genome_location = args::get(genome_location_arg);
  }

  GenomeIndex* genome_index = GenomeIndex::loadFromDirectory(
      const_cast<char*>(genome_location.c_str()), true, true);

  if (!genome_index) {
    std::cout << "Index load failed.\n";
    return 0;
  }

  std::unique_ptr<AlignerOptions> options =
      std::make_unique<AlignerOptions>("-=");

  std::vector<std::string> split_cmd = absl::StrSplit(snap_cmd, ' ');
  const char* snapargv[split_cmd.size()];
  for (size_t i = 0; i < split_cmd.size(); i++) {
    snapargv[i] = split_cmd[i].c_str();
  }
  int snapargc = split_cmd.size();
  bool done;
  for (int i = 0; i < snapargc; i++) {
    if (!options->parse(snapargv, snapargc, i, &done)) {
      std::cout << "Could not parse snap arg " << std::string(snapargv[i])
                << " \n";
    }
    if (done) break;
  }

  SingleAligner aligner(genome_index, options.get());

  std::string agd_meta_path = args::get(agd_metadata_args);

  std::unique_ptr<agd::AGDBufferedDataset> dataset;
  auto s = agd::AGDBufferedDataset::Create(agd_meta_path, dataset);
  if (!s.ok()) {
    std::cout << "Error opening AGD dataset: " << s.error_message() << "\n";
    return 0;
  }

  agd::AGDBufferedDataset::ColumnIterator c_base, c_qual, c_meta;

  s = dataset->Column("base", &c_base);
  CheckStatus(s);

  s = dataset->Column("qual", &c_qual);
  CheckStatus(s);

  const char *base, *qual;
  size_t base_len, qual_len;

  std::string meta_placeholder("metafake");
  size_t count = 0;
  s = c_base.GetNextRecord(&base, &base_len);
  while (s.ok()) {
    s = c_qual.GetNextRecord(&qual, &qual_len);
    CheckStatus(s);

    Read read;
    Alignment result;
    read.init(meta_placeholder.c_str(), meta_placeholder.size(), base, qual,
              base_len);

    GenomeLocation location;
    s = aligner.AlignRead(read, result, location);
    CheckStatus(s);

    auto substr =
        genome_index->getGenome()->getSubstring(location, read.getDataLength());
    std::cout << "Read " << count
              << " aligned to contig: " << result.position().ref_index()
              << " at position: " << result.position().position()
              << " with mapq: " << result.mapping_quality()
              << " with cigar: " << result.cigar().c_str() << "\n"
              << std::string(base, base_len) << "\n"
              << std::string(substr, read.getDataLength()) << "\n\n";
    count++;
    s = c_base.GetNextRecord(&base, &base_len);
  }

  return 0;
}