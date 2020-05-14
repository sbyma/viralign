#include <filesystem>
#include <iostream>
#include <fstream>
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "args.hxx"
#include "json.hpp"
#include "src/sw/redis++/redis++.h"

using json = nlohmann::json;
namespace fs = std::filesystem;

int main(int argc, char** argv) {
  // TODO Separate thread args for other stages e.g. IO, parsing, compression?
  args::ArgumentParser parser("viralign-push",
                              "Push AGD dataset chunknames to queue.");
  args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
  args::ValueFlag<std::string> redis_arg(
      parser, "redis addr", "Redis address to connect to [localhost:6379]",
      {'r', "redis_addr"});
  args::ValueFlag<std::string> queue_arg(
      parser, "redis queue resource name",
      "Name of the redis resource to push stuff to [queue:viralign]",
      {'q', "queue_name"});
  args::Positional<std::string> agd_metadata_arg(
      parser, "agd args", "The AGD dataset to push");

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

  auto full_addr = absl::StrCat("tcp://", redis_addr);
  sw::redis::Redis redis(full_addr);

  std::ifstream i(agd_meta_path);
  json agd_metadata;
  i >> agd_metadata;
  i.close();

  const auto& records = agd_metadata["records"];

  fs::path path(agd_meta_path);

  fs::path abs_path = fs::absolute(path);
  fs::path abs_dir = abs_path.parent_path();

  std::cout << "[viralign-push] Absolute path is " << abs_path << "\n";
  std::cout << "[viralign-push] Absolute dir path is " << abs_dir << "\n";

  std::string pool("");

  try {
    pool = agd_metadata["pool"];
  } catch (...) {
    // no pool exists, its fine
    pool = "";
  }

  json j;
  for (const auto& record : records) {
    j["obj_name"] = absl::StrCat(abs_dir.c_str(), "/", record["path"].get<std::string>());
    j["pool"] = pool;
    auto to_send = j.dump();
    std::cout << "[viralign-push] Pushing: " << j["obj_name"] << "\n";

    try {
      auto resp = redis.rpush(queue_name, {to_send});
      std::cout << "[viralign-push] Response was " << resp << "\n";
    } catch (...) {
      std::cout << "[viralign-push] Push failed!\n";
      exit(0);
    }
  }

  std::cout << "[viralign-push] Pushed all values.\n";
   
  return 0;
}