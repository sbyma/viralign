
#include <fstream>
#include <iostream>
#include <chrono>
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/str_split.h"
#include "args.hxx"
#include "ceph_manager.h"
#include "filesystem_manager.h"
#include "genes.h"
#include "multi_fetcher.h"

using namespace errors;

// adapted from https://github.com/DeplanckeLab/BRB-seqTools/blob/master/src/model/GTF.java
Status ParseGTF(std::unique_ptr<IntervalForest>& forest,
                GeneIdMap& gene_id_name_map, const std::string& gtf_path) {

  auto t1 = std::chrono::high_resolution_clock::now();

  // read GTF
  std::cout << "[viralign-genecount] Reading GTF ... \n";
  IntervalMap intervals;

  std::ifstream gtf_input(gtf_path);
  if (!gtf_input.good()) {
    return errors::Internal("Could not open file ", gtf_path);
  }

  std::string line;
  getline(gtf_input, line);

  while (line[0] == '#' && !gtf_input.eof()) {
    getline(gtf_input, line);
  }

  std::vector<std::string> fields;
  std::vector<std::string> params;
  std::string gene_name, gene_id;

  uint32_t num_genes = 0;
  uint32_t num_exons = 0;

  do {
    //std::cout << "[viralign-genecount] parsing GTF line: " << line << "\n";
    gene_name.clear();
    gene_id.clear();
    fields = absl::StrSplit(line, '\t');
    params = absl::StrSplit(fields[8], ';');

    int start = std::stoi(fields[3]);
    int end = std::stoi(fields[4]);

    const std::string& chr = fields[0];
    const std::string& type = fields[2];

    bool strand = fields[6] == "+";

    for (const auto& param : params) {
      std::vector<std::string> values =
          absl::StrSplit(param, ' ', absl::SkipEmpty());
      if (values.size() >= 2) {
        values[1] = absl::StrReplaceAll(values[1], {{"\"", ""}});
        if (values[0] == "gene_name")
          gene_name = values[1];
        else if (values[0] == "gene_id")
          gene_id = values[1];
      }
    }
    if (gene_name.empty()) gene_name = gene_id;

    if (gene_id.empty()) {
      std::cout << "[viralign-genecount] Gene ID empty, skipping GTF line: "
                << line << "\n";
    } else {
      if (!gene_id_name_map.contains(gene_id)) {
        gene_id_name_map[gene_id] = absl::flat_hash_set<std::string>();
        num_genes++;
      }
      gene_id_name_map[gene_id].insert(gene_name);

      if (type == "exon") {
        num_exons++;
        if (!intervals.contains(chr)) {
          intervals[chr] = std::vector<Interval<int, TreeValue>>();
        }
        auto& interval_vec = intervals[chr];
        TreeValue v;
        v.gene_id = gene_id;
        v.strand = strand;
        v.gene_name = gene_name;
        Interval<int, TreeValue> i(start, end, std::move(v));
        interval_vec.push_back(std::move(i));
      }
    }

    getline(gtf_input, line);
  } while (!gtf_input.eof());

  std::cout << "[viralign-genecount] " << num_exons
            << " 'exons' are annotating " << num_genes
            << " unique gene_ids in the provided GTF file.\n";
  // now build the interval trees from the intervals
  forest.reset(new IntervalForest());
  for (auto& iv : intervals) {
    std::cout << "[viralign-genecount] building tree " << iv.first << " out of "
              << iv.second.size() << " intervals.\n";
    forest->insert_or_assign(iv.first,
                             GeneIntervalTree(std::move(iv.second)));
    /*std::cout << "[viralign-genecount] Constructed interval tree is: "
              << forest->at(iv.first) << "\n";*/
  }
  
  auto t2 = std::chrono::high_resolution_clock::now();
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
  std::cout << "[viralign-genecount] GTF parsing took " << float(ms) / 1000.0f << " seconds.\n";

  return Status::OK();
}

int main(int argc, char** argv) {
  args::ArgumentParser parser("viralign-genecount",
                              "For given datasets, count which reads map to "
                              "which genes and output a CSV");
  args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
  args::ValueFlag<unsigned int> threads_arg(
      parser, "threads", "Number of threads to use for I/O [4]",
      {'t', "threads"});
  args::ValueFlag<std::string> gtf_arg(
      parser, "GTF file", "GTF file indicating genes to count reads for.",
      {'g', "gtf_file"});
  args::ValueFlag<std::string> ceph_json_arg(
      parser, "ceph config file json",
      "Ceph config json path. If not provided, filesystem access is assumed. "
      "Required format: {\"conf_file\": <ceph conf file>, \"cluster\": <ceph "
      "cluster name>, \"client\": <ceph client name>, \"namespace\": <required "
      "namespace if any>}",
      {'c', "ceph_config"});
  args::Positional<std::string> input_arg(
      parser, "input datasets",
      "Input json file containing a list of datasets with aligned reads");

  try {
    parser.ParseCLI(argc, argv);
  } catch (const args::Completion& e) {
    std::cout << e.what();
    return 0;
  } catch (const args::Help&) {
    std::cout << parser;
    return 0;
  } catch (const args::ParseError& e) {
    std::cerr << e.what() << std::endl;
    std::cerr << parser;
    return 1;
  }

  uint32_t threads = 4;
  if (threads_arg) {
    threads =
        std::max(args::get(threads_arg), std::thread::hardware_concurrency());
  }
  // build interval tree from GTF file
  std::unique_ptr<IntervalForest> interval_forest;
  GeneIdMap gene_id_name_map;

  if (!gtf_arg) {
    std::cout << "[viralign-genecount] GTF file (-g) is required.\n";
    exit(0);
  } else {
    const auto& gtf_path = args::get(gtf_arg);
    Status s = ParseGTF(interval_forest, gene_id_name_map, gtf_path);
  }

  return 0;
  /*std::vector<const TreeValue*> values;
  interval_forest->at("NC_045512.2")
      .findOverlappingValues(21000, 26524, values);

  for (auto& vptr : values) {
    std::cout << "[viralign-genecount] Found overlapping value: " << *vptr
              << "\n";
  }*/

  const auto& input_list_json_path = args::get(input_arg);

  std::unique_ptr<InputFetcher> input_fetcher(
      new MultiFetcher(absl::string_view(input_list_json_path)));

  input_fetcher->Run();
  std::cout << "[viralign-genecount] Max chunks is " << input_fetcher->MaxRecords() << "\n";

  auto t1 = std::chrono::high_resolution_clock::now();

  if (ceph_json_arg) {
    // io from ceph
    CephManagerParams params;
    params.ceph_config_json_path = args::get(ceph_json_arg);
    params.genes = &gene_id_name_map;
    params.input_queue = input_fetcher->GetInputQueue();
    params.max_chunks = input_fetcher->MaxRecords();
    params.interval_forest = interval_forest.get();
    params.output_filename = "genecount.csv";
    params.reader_threads = threads;

    Status s = CephManager::Run(params);
    if (!s.ok()) {
      std::cout << "[viralign-genecount] Error: " << s.error_message() << "\n";
    }
  } else {
    // io from FS
    FileSystemManagerParams params;
    params.genes = &gene_id_name_map;
    params.input_queue = input_fetcher->GetInputQueue();
    params.max_chunks = input_fetcher->MaxRecords();
    params.interval_forest = interval_forest.get();
    params.output_filename = "genecount.csv";
    params.reader_threads = threads;
    Status s = FileSystemManager::Run(params);
    if (!s.ok()) {
      std::cout << "[viralign-genecount] Error: " << s.error_message() << "\n";
    }
  }

  auto t2 = std::chrono::high_resolution_clock::now();
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();

  std::cout << "[viralign-genecount] Elapsed time: " << float(ms) / 1000.0f << " seconds.\n";
  
  return 0;
}