
#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <thread>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "args.h"
#include "ceph_manager.h"
#include "filesystem_manager.h"
#include "json.hpp"
#include "local_fetcher.h"
#include "parallel_aligner.h"
#include "redis_fetcher.h"

using json = nlohmann::json;
using namespace errors;

void CheckStatus(Status& s) {
  if (!s.ok()) {
    std::cout << "Error: " << s.error_message() << "\n";
    exit(0);
  }
}

constexpr absl::string_view SarsCov2Contig = "MN985325";

// if not present, add the "aln" column to an existing AGD metadata json
Status AddColumn(const std::string& agd_meta_path) {
  std::ifstream i(agd_meta_path.data());
  if (!i.good()) return errors::Internal("Couldn't open file ", agd_meta_path);
  json agd_metadata;
  i >> agd_metadata;
  i.close();

  const auto& cols = agd_metadata["columns"];
  if (std::find(cols.begin(), cols.end(), "aln") == cols.end()) {
    agd_metadata["columns"].push_back("aln");
    std::ofstream o(agd_meta_path);
    if (!o.good())
      return errors::Internal("Couldn't open file ", agd_meta_path);
    o << std::setw(4) << agd_metadata;
  }

  return Status::OK();
}

int main(int argc, char** argv) {
  // TODO Separate thread args for other stages e.g. IO, parsing, compression?
  args::ArgumentParser parser(
      "viralign-core",
      "Align reads using SNAP from either Ceph or Local disk AGD files, only "
      "logging SarsCov2 mapping reads (for now).");
  args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
  args::ValueFlag<unsigned int> threads_arg(
      parser, "threads",
      absl::StrCat("Number of threads to use for all alignment [",
                   std::thread::hardware_concurrency(), "]"),
      {'t', "threads"});
  args::ValueFlag<std::string> redis_arg(
      parser, "redis queue",
      "Address of the redis queue to pull work from <host>:<port>",
      {'r', "redis_queue"});
  args::ValueFlag<std::string> queue_arg(
      parser, "redis queue resource name",
      "Name of the redis resource to get work from [queue:viralign]",
      {'q', "queue_name"});
  args::ValueFlag<std::string> ceph_json_arg(
      parser, "ceph config file json",
      "Ceph config json path. If not provided, filesystem access is assumed.",
      {'c', "ceph_config"});
  args::ValueFlag<std::string> snap_args_arg(
      parser, "snap args", "Any args to pass to SNAP", {'s', "snap_args"});
  args::ValueFlag<std::string> genome_location_arg(
      parser, "genomeloc", "SNAP Genome Index location", {'g', "genome_loc"});
  args::ValueFlag<std::string> agd_metadata_args(
      parser, "agd args",
      "For testing, an AGD metadata to align some stuff. Overrides -r.",
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

  std::cout << "[viralign-core] Using " << threads
            << " threads for alignment\n";

  std::string snap_cmd("");
  if (snap_args_arg) {
    snap_cmd = args::get(snap_args_arg);
  }

  std::string genome_location;
  if (!genome_location_arg) {
    std::cout << "[viralign-core] Genome index is required\n";
    exit(0);
  } else {
    genome_location = args::get(genome_location_arg);
  }

  std::cout << "[viralign-core] Loading genome index: " << genome_location
            << " ...\n";
  GenomeIndex* genome_index = GenomeIndex::loadFromDirectory(
      const_cast<char*>(genome_location.c_str()), true, true);

  if (!genome_index) {
    std::cout << "[viralign-core] Index load failed.\n";
    return 0;
  }

  const Genome* genome = genome_index->getGenome();

  const Genome::Contig* contigs = genome->getContigs();
  auto num_contigs = genome->getNumContigs();

  std::cout << "[viralign-core] Genome loaded, there are " << num_contigs
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
    std::cout << "[viralign-core] did not find covid contig index\n";
  } else {
    std::cout << "[viralign-core] SarsCov2 contig index is "
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
      std::cout << "[viralign-core] Could not parse snap arg "
                << std::string(snapargv[i]) << " \n";
    }
    if (done) break;
  }

  std::unique_ptr<InputFetcher> fetcher;

  auto t1 = std::chrono::high_resolution_clock::now();

  // determine source for data input (-i or -r)
  if (agd_metadata_args) {
    // create local fetcher and run
    std::string agd_meta_path = args::get(agd_metadata_args);
    fetcher.reset(new LocalFetcher(agd_meta_path));
    Status fs = fetcher->Run();
    if (!fs.ok()) {
      std::cout << "[viralign-core] Unable to create fetcher: "
                << fs.error_message() << "\n";
    }
  } else {
    // this is the "run forever" case
    if (redis_arg) {
      // create redox fetcher and run TODO
      std::string redis_addr = args::get(redis_arg);

      std::string queue_name("queue:viralign");

      if (queue_arg) {
        queue_name = args::get(queue_arg);
      }

      Status rs = RedisFetcher::Create(redis_addr, queue_name, fetcher);

      if (!rs.ok()) {
        std::cout << "[viralign-core] Unable to create redox fetcher: " << rs.error_message() << "\n";
        exit(0);
      }

      rs = fetcher->Run();
      
      if (!rs.ok()) {
        std::cout << "[viralign-core] Error running redox fetcher: " << rs.error_message() << "\n";
        exit(0);
      }

    } else {
      std::cout << "[viralign-core] Need either -i or -r for data input. "
                   "Exiting ... \n";
      exit(0);
    }
  }

  auto input_queue = fetcher->GetInputQueue();
  int max_records = fetcher->MaxRecords();  // run forever

  Status s = Status::OK();
  if (ceph_json_arg) {
    // we will do IO from ceph

    const auto& ceph_conf_json_path = args::get(ceph_json_arg);
    s = CephManager::Run(input_queue, max_records, ceph_conf_json_path,
                         sars_cov2_contig_idx, genome_index, options.get(),
                         threads);

  } else {
    // we will IO from file system

    s = FileSystemManager::Run(input_queue, max_records, sars_cov2_contig_idx,
                               genome_index, options.get(), threads);
  }

  if (!s.ok()) {
    std::cout << "[viralign-core] Error: " << s.error_message() << "\n";
    return 0;
  }

  if (agd_metadata_args) {
    std::string agd_meta_path = args::get(agd_metadata_args);
    s = AddColumn(agd_meta_path);
  }

  if (!s.ok()) {
    std::cout << "[viralign-core] Error: " << s.error_message() << "\n";
    return 0;
  }

  auto t2 = std::chrono::high_resolution_clock::now();
  auto total =
      std::chrono::duration_cast<std::chrono::seconds>(t2 - t1).count();

  std::cout << "[viralign-core] Alignment time: " << total << " seconds.\n";

  return 0;
}