
#include <algorithm>
#include <fstream>
#include <iostream>
#include <thread>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "args.h"
#include "ceph_manager.h"
#include "filesystem_manager.h"
#include "json.hpp"
#include "parallel_aligner.h"

using json = nlohmann::json;
using namespace errors;

void CheckStatus(Status& s) {
  if (!s.ok()) {
    std::cout << "Error: " << s.error_message() << "\n";
    exit(0);
  }
}

constexpr absl::string_view SarsCov2Contig = "MN985325";

int main(int argc, char** argv) {
  args::ArgumentParser parser(
      "align-core",
      "Align reads using SNAP from either Ceph or Local disk AGD files, only "
      "logging SarsCov2 mapping reads (for now).");
  args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
  args::ValueFlag<unsigned int> threads_arg(
      parser, "threads",
      absl::StrCat("Number of threads to use for all stages [",
                   std::thread::hardware_concurrency(), "]"),
      {'t', "threads"});
  args::ValueFlag<std::string> redis_arg(
      parser, "redis queue", "Address of the redis queue to pull work from [localhost:1234]",
      {'r', "redis_queue"});
  args::ValueFlag<std::string> queue_arg(
      parser, "redis queue resource name", "Name of the redis resource to get work from [queue:viralign]",
      {'q', "queue_name"});
  args::ValueFlag<std::string> ceph_json_arg(parser, "ceph config file json",
                                             "Ceph config json path",
                                             {'c', "ceph_config"});
  args::ValueFlag<std::string> snap_args_arg(
      parser, "snap args", "Any args to pass to SNAP", {'s', "snap_args"});
  args::ValueFlag<std::string> genome_location_arg(
      parser, "genomeloc", "SNAP Genome Index location", {'g', "genome_loc"});
  args::ValueFlag<std::string> agd_metadata_args(
      parser, "agd args", "For testing, an AGD metadata to align some stuff. Overrides -r.",
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

  unsigned int threads;
  if (threads_arg) {
    threads = args::get(threads_arg);
    threads = std::min({std::thread::hardware_concurrency(), threads});
  } else {
    threads = std::thread::hardware_concurrency();
  }

  std::string snap_cmd("");
  if (snap_args_arg) {
    snap_cmd = args::get(snap_args_arg);
  }

  std::string genome_location;
  if (!genome_location_arg) {
    std::cout << "[align-core] Genome index is required\n";
    exit(0);
  } else {
    genome_location = args::get(genome_location_arg);
  }

  std::cout << "[align-core] Loading genome index: " << genome_location
            << " ...\n";
  GenomeIndex* genome_index = GenomeIndex::loadFromDirectory(
      const_cast<char*>(genome_location.c_str()), true, true);

  if (!genome_index) {
    std::cout << "[align-core] Index load failed.\n";
    return 0;
  }

  const Genome* genome = genome_index->getGenome();

  const Genome::Contig* contigs = genome->getContigs();
  auto num_contigs = genome->getNumContigs();

  std::cout << "[align-core] Genome loaded, there are " << num_contigs
            << " contigs.\n";

  int sars_cov2_contig_idx = -1;
  for (int i = 0; i < num_contigs; i++) {
    auto contig_name =
        absl::string_view(contigs[i].name, contigs[i].nameLength);
    if (absl::StrContains(contig_name, SarsCov2Contig)) {
      sars_cov2_contig_idx = i;
      break;
    }
  }

  if (sars_cov2_contig_idx == -1) {
    std::cout << "[align-core] did not find covid contig index\n";
  } else {
    std::cout << "[align-core] SarsCov2 contig index is "
              << sars_cov2_contig_idx << "\n";
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
      std::cout << "[align-core] Could not parse snap arg "
                << std::string(snapargv[i]) << " \n";
    }
    if (done) break;
  }

  // determine source for data input (-i or -r)
  //if (agd_metadata_args) 


  std::string agd_meta_path = args::get(agd_metadata_args);

  Status s = Status::OK();
  if (ceph_json_arg) {
    // we will do IO from ceph

    const auto& ceph_conf_json_path = args::get(ceph_json_arg);
    s = CephManager::Run(agd_meta_path, ceph_conf_json_path, sars_cov2_contig_idx, genome_index,
                         options.get());

  } else {
    // we will IO from file system

    s = FileSystemManager::Run(agd_meta_path, sars_cov2_contig_idx, genome_index, options.get());
  }

  if (!s.ok()) {
    std::cout << "[align-core] Error: " << s.error_message() << "\n";
  }

  return 0;
}