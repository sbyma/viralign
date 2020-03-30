
#include <sys/stat.h>
#include <sys/types.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <thread>
#include "absl/strings/str_cat.h"
#include "agd_chunk_converter.h"
#include "agd_writer.h"
#include "args.h"
#include "object_pool.h"
#include "fastq_manager.h"

using namespace std;
using namespace errors;
using namespace std::chrono_literals;

int main(int argc, char** argv) {
  args::ArgumentParser parser("fastq2agd", "Convert FASTQ to AGD format.");
  args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
  args::ValueFlag<unsigned int> threads_arg(
      parser, "threads",
      absl::StrCat("Number of threads to use for all stages [",
                   std::thread::hardware_concurrency(), "]"),
      {'t', "threads"});
  args::ValueFlag<std::string> name_arg(
      parser, "name", "A name for the output dataset [fastq filename]",
      {'n', "name"});
  args::ValueFlag<std::string> outdir_arg(
      parser, "output dir",
      "Output directory for the AGD dataset [new dir named `name_arg`]",
      {'o', "output_dir"});
  args::ValueFlag<unsigned int> chunk_size_arg(parser, "chunksize",
                                               "AGD output chunk size [100000]",
                                               {'c', "chunksize"});
  args::PositionalList<std::string> fastq_files(
      parser, "datasets",
      "FASTQ  dataset to convert. Provide one for single end, two for paired "
      "end. ");

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

  unique_ptr<FastqManager> fastq_manager;
  QueueType chunk_queue(10);

  auto& fastq_files_vec = args::get(fastq_files);
  auto chunk_size = args::get(chunk_size_arg);
  std::cout << "Using chunk size: " << chunk_size << "\n";
  Status s;

  if (fastq_files_vec.size() > 2) {
    std::cout << "You must provide 2 or fewer fastq files.\n";
    return 0;
  } else if (args::get(fastq_files).size() == 2) {
    // setup paired
    s = FastqManager::CreatePairedFastqManager(fastq_files_vec[0],
                                               fastq_files_vec[1], chunk_size,
                                               &chunk_queue, fastq_manager);
  } else {
    // setup single
    s = FastqManager::CreateFastqManager(fastq_files_vec[0], chunk_size,
                                         &chunk_queue, fastq_manager);
  }

  if (!s.ok()) {
    cout << s.error_message() << "\n";
    return 0;
  }

  std::string dataset_name;
  if (name_arg) {
    dataset_name = args::get(name_arg);
  } else {
    auto position = fastq_files_vec[0].find_last_of('.');
    dataset_name = fastq_files_vec[0].substr(0, position);
    std::cout << "Using dataset name \"" << dataset_name << "\"\n";
  }

  std::string output_dir;
  if (outdir_arg) {
    output_dir = args::get(outdir_arg);
    if (output_dir.back() != '/') {
      output_dir.append("/");
    }
  } else {
    output_dir = dataset_name + "/";
  }

  unsigned int threads = std::thread::hardware_concurrency();
  if (threads_arg) {
    threads = args::get(threads_arg);
    // do not allow more than hardware threads
    if (threads > std::thread::hardware_concurrency()) {
      threads = std::thread::hardware_concurrency();
    }
  }

  cout << "Using " << threads << " threads for conversion and chunk compression\n";

  auto chunker_thread = std::thread([&fastq_manager]() {
    Status s = fastq_manager->Run();
    if (!s.ok()) {
      cout << "Chunker Thread: fastq manager Run ended with error: "
           << s.error_message() << "\n";
    }
    cout << "Run is complete\n";
  });

  // test fastq parsing code
  /*FastqQueueItem item;

  const char* bases;
  const char* meta;
  const char* qual;
  size_t bases_size, meta_size;

  while (chunk_queue.pop(item)) {
    cout << "chunk size is " << item.chunk_1.NumRecords() << "\n";
    cout << "chunk 1:\n";
    Status stat = item.chunk_1.GetNextRecord(&bases, &bases_size, &qual, &meta,
  &meta_size); while (stat.ok()) { cout << string(meta, meta_size) << "\n" <<
  string(bases, bases_size) << "\n" << string(qual, bases_size) << "\n" << "\n";
      stat = item.chunk_1.GetNextRecord(&bases, &bases_size, &qual, &meta,
                                        &meta_size);
    }

    cout << "chunk 2:\n";
    stat = item.chunk_2.GetNextRecord(&bases, &bases_size, &qual, &meta,
  &meta_size); while (stat.ok()) { cout << string(meta, meta_size) << "\n" <<
  string(bases, bases_size) << "\n" << string(qual, bases_size) << "\n" << "\n";
      stat = item.chunk_2.GetNextRecord(&bases, &bases_size, &qual, &meta,
  &meta_size);
    }
  }*/

  volatile bool done = false;
  OutputQueueType output_queue(10);

  std::vector<std::thread> converter_threads(threads);

  ObjectPool<agd::Buffer> buffer_pool;
  
  for (auto& t : converter_threads) {
    t = std::thread([&chunk_queue, &output_queue, &done, &buffer_pool]() {
      FastqQueueItem item;
      AGDChunkConverter converter;
      Status s;
      while (!done) {
        FastqColumns output_cols;
        output_cols.base = std::move(buffer_pool.get());
        output_cols.qual = std::move(buffer_pool.get());
        output_cols.meta = std::move(buffer_pool.get());
        if (!chunk_queue.pop(item)) continue;

        if (item.chunk_2.IsValid()) {
          s = converter.ConvertPaired(item.chunk_1, item.chunk_2, &output_cols);
        } else {
          s = converter.Convert(item.chunk_1, &output_cols);
        }

        if (!s.ok()) {
          cout << "Convert failed to successfully convert with error: "
              << s.error_message() << "\n";
          exit(0);
        }

        OutputQueueItem item_out;
        item_out.columns = std::move(output_cols);
        item_out.first_ordinal = item.first_ordinal;
        output_queue.push(std::move(item_out));
      }
    });
  }


  volatile bool writer_done = false;

  absl::Mutex mu;
  RecordVec all_records;
  std::atomic_uint32_t chunk_count{0};

  auto writer_thread = std::thread([&]() {
    AGDWriter writer(dataset_name);
    writer.Init(output_dir);
    Status s = Status::OK();

    while (!writer_done) {
      OutputQueueItem item;
      if (!output_queue.pop(item)) continue;

      s = writer.Write(item);
      if (!s.ok()) {
        cout << "Failed to write chunks: " << s.error_message() << "\n";
        exit(0);
      }
      chunk_count++;
    }

    const auto& recs = writer.GetRecordVec();
    absl::MutexLock l(&mu);
    all_records.insert(all_records.begin(), recs.begin(), recs.end());
  });
  
  auto measure_thread = std::thread([&done, &chunk_queue, &output_queue](){
    int time_count = 0;
    ofstream csv_out("queue_levels.csv");
    while (!done) {
      csv_out << time_count << "," << chunk_queue.size() << "," << output_queue.size() << "\n";
      time_count++;
      std::this_thread::sleep_for(1s);
    }
    csv_out.close();
  });


  chunker_thread.join();
  while (chunk_count.load() != fastq_manager->TotalChunks()) {
    std::this_thread::sleep_for(500ms);
  }

  chunk_queue.unblock();
  done = true;
  for (auto& t : converter_threads) {
    t.join();
  }
  writer_done = true;
  output_queue.unblock();
  writer_thread.join();
  measure_thread.join();

  nlohmann::json metadata_json;

  metadata_json["columns"] = {"base", "qual", "meta"};
  metadata_json["version"] = 1;
  metadata_json["name"] = dataset_name;
  metadata_json["records"] = all_records;

  std::ofstream o(absl::StrCat(output_dir, "metadata.json"));
  o << std::setw(4) << metadata_json << std::endl;

  return 0;
}