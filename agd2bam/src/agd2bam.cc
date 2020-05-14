
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "args.hxx"
#include "ceph_manager.h"
#include "filesystem_manager.h"
#include "json.hpp"
#include "libagd/src/local_fetcher.h"

using json = nlohmann::json;
using namespace errors;

void CheckStatus(Status& s) {
  if (!s.ok()) {
    std::cout << "[agd2bam] Error: " << s.error_message() << "\n";
    exit(0);
  }
}

int main(int argc, char** argv) {
  args::ArgumentParser parser(
      "agd2bam", "Convert AGD to BAM. Requires dataset to be aligned.");
  args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
  args::ValueFlag<std::string> ceph_json_arg(
      parser, "ceph config file json",
      "Ceph config json path. If not provided, filesystem access is assumed.",
      {'c', "ceph_config"});
  args::Positional<std::string> agd_metadata_args(
      parser, "agd args", "AGD metadata of dataset to convert.");

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

  const auto& agd_meta_path = args::get(agd_metadata_args);

  std::unique_ptr<InputFetcher> fetcher;

  fetcher.reset(new LocalFetcher(agd_meta_path));
  Status fs = fetcher->Run();
  CheckStatus(fs);

  auto input_queue = fetcher->GetInputQueue();
  auto max_chunks = fetcher->MaxRecords();

  // need a thread arg?
  if (ceph_json_arg) {
    const auto& ceph_json_path = args::get(ceph_json_arg);
    auto s = CephManager::Run(input_queue, max_chunks, agd_meta_path,
                              ceph_json_path, 5);
    CheckStatus(s);
  } else {
    auto s = FileSystemManager::Run(input_queue, 5, max_chunks, agd_meta_path);
    CheckStatus(s);
  }

  return 0;
}