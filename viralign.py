import argparse
import subprocess
import os
import redis
import json

def count_chunks(metadata_path):
    with open(metadata_path) as json_file:
        data = json.load(json_file)
        return len(data["records"])
        
def main():
    parser = argparse.ArgumentParser(
        description="Tool to easily run the viralign pipeline. Assumes Redis server and viralign-core containers are already running."
    )
    parser.add_argument("barcodes", help="The input multiplexed fastq dataset barcodes")
    parser.add_argument("reads", help="The input multiplexed fastq dataset reads")
    parser.add_argument(
        "-b",
        "--barcode_config",
        default="lib_example_barcode.txt",
        help="The BRBSeqTool barcode file defining sample barcodes",
    )
    parser.add_argument(
        "-c", "--chunk_size", type=int, default=50000, help="Output dataset chunk size"
    )
    parser.add_argument(
        "-C",
        "--ceph_conf",
        default="",
        help="Ceph config file, indicates to use Ceph for IO",
    )
    parser.add_argument(
        "-r",
        "--redis_addr",
        default="localhost:6379",
        help="Redis server address for sending work/receiving results",
    )
    parser.add_argument(
        "-q",
        "--queue_name",
        default="queue:viralign",
        help="Redis queue name to send work on [queue:viralign (_return)]",
    )
    parser.add_argument(
        "-g",
        "--gtf_file",
        default="",
        help="The GTF file defining the genes to map reads to",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        default="viralign_out",
        help="viralign output directory, will contain separated samples",
    )
    args = parser.parse_args()

    print("[viralign] The sample barcode file is: {}".format(args.barcodes))
    print("[viralign] The reads file is: {}".format(args.reads))
    # call samplesep

    if args.output_dir[-1] != '/':
        args.output_dir += '/'

    samplesep_cmd = [
        "./bazel-bin/samplesep/samplesep",
        "-o",
        args.output_dir,
        "-b",
        args.barcode_config,
        "-c",
        str(args.chunk_size),
        args.barcodes,
        args.reads,
    ]
    
    print("[viralign] Running samplesep with args: {}".format(samplesep_cmd))

    subprocess.run(samplesep_cmd)
    # should now have file samplesep_datasets.csv

    # align datasets
    # enumerate dataset metadata files
    # run viralign-push for each one
    total_chunks = 0
    datasets = []

    with open("samplesep_datasets.csv") as f:
        lines = f.readlines()
        # first line is Name, Path
        for l in lines[1:]:
            name, path = l.split(',')
            name = name.strip()
            path = path.strip()
            print("[viralign] Found separated dataset \"{}\" with path: {}".format(name, path))

            metadata_path = path + "metadata.json"

            if not os.path.exists(metadata_path):
                print("[viralign] Path \"{}\" does not exist. Is the file not named \"metadata.json\"?".format(metadata_path))
            else: 
                chunks = count_chunks(metadata_path)
                total_chunks += chunks
                print("[viralign] Pushing dataset {} to alignment, had {} chunks".format(metadata_path, chunks))
                push_cmd = ["./bazel-bin/viralign_push/viralign-push", "-r", args.redis_addr, "-q", args.queue_name, metadata_path]
                print("[viralign] Push cmd: {}".format(push_cmd))
                datasets.append(metadata_path)
                subprocess.run(push_cmd)

    print("[viralign] All datasets: {}".format(datasets))

    host, port = args.redis_addr.split(':')

    print("[viralign] Connecting to {}:{}".format(host, port))

    r = redis.Redis(host=host, port=int(port))

    return_queue = args.queue_name + "_return"
    for c in range(total_chunks):
        resp, other = r.blpop(return_queue)
        print("[viralign] Got alignment response {}, {}".format(resp, other))

    print("[viralign] All chunks aligned.")

    with open("aligned_datasets.json") as f:
        json.dump(datasets, f)

    # count covid genes

    # call viralign genecount 
    count_cmd = ["./bazel-bin/viralign_genecount/viralign-genecount", "-g", args.gtf_file, "aligned_datasets.json"]
    subprocess.run(count_cmd)




if __name__ == "__main__":
    main()
