
#include <memory>
#include <string>

#include "libagd/src/proto/alignment.pb.h"
#include "liberr/errors.h"
#include "snap-master/SNAPLib/AlignerOptions.h"
#include "snap-master/SNAPLib/BaseAligner.h"
#include "snap-master/SNAPLib/GenomeIndex.h"
#include "snap-master/SNAPLib/SeedSequencer.h"
#include "snap-master/SNAPLib/SAM.h"
#include "snap-master/SNAPLib/AlignmentResult.h"


class SingleAligner {
 public:
  SingleAligner(GenomeIndex* index, AlignerOptions* options);

  errors::Status AlignRead(Read& snap_read, Alignment& result, GenomeLocation& loc);

 private:
  GenomeIndex* index_;
  const Genome* genome_;
  AlignerOptions* options_;
  BaseAligner* base_aligner_;
  std::unique_ptr<BigAllocator> allocator_;
  unsigned alignmentResultBufferCount_;

  SingleAlignmentResult primaryResult_;
  std::vector<SingleAlignmentResult> secondaryResults_;
  LandauVishkinWithCigar lvc_;

  errors::Status WriteSingleResult(Read& snap_read, SingleAlignmentResult& result,
                           Alignment& format_result,
                           const Genome* genome, LandauVishkinWithCigar* lvc,
                           bool is_secondary, bool use_m);

  errors::Status PostProcess(const Genome* genome, LandauVishkinWithCigar* lv,
                     Read* read, AlignmentResult result, int mapQuality,
                     GenomeLocation genomeLocation, Direction direction,
                     bool secondaryAlignment, Alignment& finalResult,
                     std::string& cigar, int* addFrontClipping, bool useM,
                     bool hasMate = false, bool firstInPair = false,
                     Read* mate = NULL, AlignmentResult mateResult = NotFound,
                     GenomeLocation mateLocation = 0,
                     Direction mateDirection = FORWARD,
                     bool alignedAsPair = false);
};