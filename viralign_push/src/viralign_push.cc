#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "args.h"
#include "json.hpp"
#include "redox.hpp"

using json = nlohmann::json;

int main(int argc, char** argv) {
  // TODO Separate thread args for other stages e.g. IO, parsing, compression?
  args::ArgumentParser parser("viralign-push",
                              "Push AGD dataset chunknames to queue.");
  args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
  args::ValueFlag<std::string> redis_arg(
      parser, "redis addr", "Redis address to connect to [localhost:6379]",
      {'q', "queue_name"});
  args::ValueFlag<std::string> queue_arg(
      parser, "redis queue resource name",
      "Name of the redis resource to push stuff to [queue:viralign]",
      {'q', "queue_name"});
  args::Positional<std::string> agd_metadata_arg(
      parser, "agd args", "The dataset chunk names to push");

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

  std::string agd_meta_path;

  if (agd_metadata_arg) {
    agd_meta_path = args::get(agd_metadata_arg);
  } else {
    std::cout << "[viralign-push] AGD metadata JSON file is required.\n";
  }

  std::string queue_name("queue:viralign");

  if (queue_arg) {
    queue_name = args::get(queue_arg);
  }

  std::string redis_addr("localhost:6379");

  if (redis_arg) {
    redis_addr = args::get(redis_arg);
  }

  std::vector<std::string> addr_port = absl::StrSplit(redis_addr, ":");

  if (addr_port.size() != 2) {
    std::cout << "[viralign-push] Unable to parse redis addr: " << redis_addr
              << "\n";
    exit(0);
  }

  const std::string& addr = addr_port[0];
  int port;
  bool port_good = absl::SimpleAtoi(addr_port[1], &port);

  if (!port_good) {
    std::cout << "[viralign-push] Bad port: " << addr_port[1] << "\n";
    exit(0);
  }

  redox::Redox rdx;
  bool connected = rdx.connect(addr, port);

  if (!connected) {
    std::cout << "[viralign-push] Unable to connect to Redis.\n ";
    exit(0);
  }

  std::ifstream i(agd_meta_path.data());
  json agd_metadata;
  i >> agd_metadata;
  i.close();

  const auto& records = agd_metadata["records"];

  auto file_path_base =
      agd_meta_path.substr(0, agd_meta_path.find_last_of('/') + 1);

  std::cout << "[viralign-push] base path is " << file_path_base << "\n";

  std::string pool("");

  try {
    pool = agd_metadata["pool"];
  } catch (...) {
    // no pool exists, its fine
  }

  json j;
  for (const auto& record : records) {
    j["obj_name"] = absl::StrCat(file_path_base, record["path"].get<std::string>());
    j["pool"] = pool;
    auto to_send = j.dump();
    std::cout << "[viralign-push] Pushing: " << j["obj_name"] << "\n";

    const auto& cmd =
        rdx.commandSync({"RPUSH", queue_name, to_send});

    if (!cmd) {
      std::cout << "[viralign-push] Push failed!\n";
      exit(0);
    }
  }

  std::cout << "[viralign-push] Pushed all values.\n";
   
  return 0;
}