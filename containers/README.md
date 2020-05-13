
# Viralign containers

Here is a basic container for running `viralign_core` to do parallel distributed alignment. 
The idea is to run a bunch of containers running `viralign_core` with each of them pulling work (in the form of AGD chunks to align) from a redis queue.
As such, it is generally required to use the `-r` flag to specify the redis endpoint. 

If using NFS or other mounted storage, make sure to mount the volume and ensure that AGD datasets pushed to the redis queue using `viralign_push` correctly point to the chunks stored on the volume.
You will also need a solution to make your reference genome available to each container.
Include it in a mounted volume, or copy it over into each container. 

Basic setup would be as follows, assuming datasets and ref genome mounted in NFS or similar:

1. Run a number of containers, eaching running `viralign_core` using a command similar to `$ ./viralign_core -t <threads> -r redisendpoi.nt:6379 -q "queue:viralign" -g <genome index location> -s <snap args, optional>`
2. Push data to the queue using `viralign_push`. `$ viralign_push -r redisendpoi.nt6379 -q "queue:viralign" <agd_metadata.json>`
3. Once complete the dataset will have an `aln` column containing the read alignments.