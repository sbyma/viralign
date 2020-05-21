
#pragma once

#include "genes.h"
#include "libagd/src/queue_defs.h"
#include "liberr/errors.h"

errors::Status CountGenes(uint32_t max_chunks, agd::ChunkQueueType* input_queue,
                          const IntervalForest* interval_forest,
                          const GeneIdMap& genes);