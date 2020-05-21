
#include "snap_single_aligner.h"

SingleAligner::SingleAligner(GenomeIndex *index, AlignerOptions *options)
    : index_(index), options_(options) {
  genome_ = index_->getGenome();

  if (options_->maxSecondaryAlignmentAdditionalEditDistance < 0) {
    alignmentResultBufferCount_ = 1;  // For the primary alignment
  } else {
    alignmentResultBufferCount_ =
        BaseAligner::getMaxSecondaryResults(
            options_->numSeedsFromCommandLine, options_->seedCoverage,
            MAX_READ_LENGTH, options_->maxHits, index_->getSeedLength()) +
        1;  // +1 for the primary alignment
  }
  size_t alignmentResultBufferSize =
      sizeof(SingleAlignmentResult) *
      (alignmentResultBufferCount_ + 1);  // +1 is for primary result

  allocator_.reset(new BigAllocator(
      BaseAligner::getBigAllocatorReservation(
          index_, true, options_->maxHits, MAX_READ_LENGTH,
          index_->getSeedLength(), options_->numSeedsFromCommandLine,
          options_->seedCoverage, options_->maxSecondaryAlignmentsPerContig) +
      alignmentResultBufferSize));

  base_aligner_ = new (allocator_.get()) BaseAligner(
      index_, options_->maxHits, options_->maxDist, MAX_READ_LENGTH,
      options_->numSeedsFromCommandLine, options_->seedCoverage,
      options_->minWeightToCheck, options_->extraSearchDepth, false, false,
      false,  // stuff that would decrease performance without impacting quality
      options_->maxSecondaryAlignmentsPerContig, nullptr,
      nullptr,  // Uncached Landau-Vishkin
      nullptr,  // No need for stats
      allocator_.get());

  allocator_->checkCanaries();

  base_aligner_->setExplorePopularSeeds(options_->explorePopularSeeds);
  base_aligner_->setStopOnFirstHit(options_->stopOnFirstHit);

  secondaryResults_.resize(alignmentResultBufferCount_);
}

errors::Status SingleAligner::AlignRead(Read &snap_read, Alignment &result, GenomeLocation& loc) {
  snap_read.clip(options_->clipping);
  if (snap_read.getDataLength() < options_->minReadLength ||
      snap_read.countOfNs() > options_->maxDist) {
    primaryResult_.status = AlignmentResult::NotFound;
    primaryResult_.location = InvalidGenomeLocation;
    primaryResult_.mapq = 0;
    primaryResult_.direction = FORWARD;
  }

  int num_secondary_results;
  base_aligner_->AlignRead(
      &snap_read, &primaryResult_,
      options_->maxSecondaryAlignmentAdditionalEditDistance,
      alignmentResultBufferCount_, &num_secondary_results,
      0,                     // maximum number of secondary results
      &secondaryResults_[0]  // secondaryResults
  );
  loc = primaryResult_.location;
  auto s = WriteSingleResult(snap_read, primaryResult_, result, genome_, &lvc_,
                             false, options_->useM);

  return s;
}

errors::Status SingleAligner::WriteSingleResult(Read &snap_read,
                                                SingleAlignmentResult &result,
                                                Alignment &format_result,
                                                const Genome *genome,
                                                LandauVishkinWithCigar *lvc,
                                                bool is_secondary, bool use_m) {
  std::string cigar;
  // Alignment format_result;
  snap_read.setAdditionalFrontClipping(0);

  int addFrontClipping = -1;
  GenomeLocation finalLocation =
      result.status != NotFound ? result.location : InvalidGenomeLocation;
  unsigned nAdjustments = 0;
  int cumulativeAddFrontClipping = 0;
  while (addFrontClipping != 0) {
    addFrontClipping = 0;
    errors::Status s =
        PostProcess(genome, lvc, &snap_read, result.status, result.mapq,
                    finalLocation, result.direction, is_secondary,
                    format_result, cigar, &addFrontClipping, use_m);
    // redo if read modified (e.g. to add soft clipping, or move alignment for a
    // leading I.
    if (addFrontClipping != 0) {
      nAdjustments++;
      const Genome::Contig *originalContig =
          result.status == NotFound
              ? NULL
              : genome->getContigAtLocation(result.location);
      const Genome::Contig *newContig =
          result.status == NotFound
              ? NULL
              : genome->getContigAtLocation(result.location + addFrontClipping);
      if (newContig == NULL || newContig != originalContig ||
          finalLocation + addFrontClipping >
              originalContig->beginningLocation + originalContig->length -
                  genome->getChromosomePadding() ||
          nAdjustments > snap_read.getDataLength()) {
        //
        // Altering this would push us over a contig boundary, or we're stuck in
        // a loop.  Just give up on the read.
        //
        result.status = NotFound;
        result.location = InvalidGenomeLocation;
        finalLocation = InvalidGenomeLocation;
      } else {
        cumulativeAddFrontClipping += addFrontClipping;
        if (addFrontClipping > 0) {
          snap_read.setAdditionalFrontClipping(cumulativeAddFrontClipping);
        }
        finalLocation = result.location + cumulativeAddFrontClipping;
      }
    }
  }

  // format_result.set_cigar(cigar);
  // result_column.AppendAlignmentResult(format_result);
  return errors::Status::OK();
}

errors::Status SingleAligner::PostProcess(
    const Genome *genome, LandauVishkinWithCigar *lv, Read *read,
    AlignmentResult result, int mapQuality, GenomeLocation genomeLocation,
    Direction direction, bool secondaryAlignment, Alignment &finalResult,
    std::string &cigar, int *addFrontClipping, bool useM, bool hasMate,
    bool firstInPair, Read *mate, AlignmentResult mateResult,
    GenomeLocation mateLocation, Direction mateDirection, bool alignedAsPair) {
  cigar = "*";
  const int MAX_READ = MAX_READ_LENGTH;
  char data[MAX_READ];
  char quality[MAX_READ];
  /*const int cigarBufSize = MAX_READ * 2;
  char cigarBuf[cigarBufSize];
  const int cigarBufWithClippingSize = MAX_READ * 2 + 32;
  char cigarBufWithClipping[cigarBufWithClippingSize];
  int flags = 0;
  const char *cigar = "*";
  const char *matecontigName = "*";
  int mateContigIndex = -1;
  GenomeDistance matePositionInContig = 0;
  _int64 templateLength = 0;
  char data[MAX_READ];
  char quality[MAX_READ];
  const char* clippedData;
  unsigned fullLength;
  unsigned clippedLength;
  unsigned basesClippedBefore;
  GenomeDistance extraBasesClippedBefore;   // Clipping added if we align before
  the beginning of a chromosome unsigned basesClippedAfter; int editDistance =
  -1;*/

  *addFrontClipping = 0;
  const char *contigName = "*";
  const char *matecontigName = "*";
  int contigIndex = -1;
  GenomeDistance positionInContig = 0;
  int mateContigIndex = -1;
  GenomeDistance matePositionInContig = 0;

  GenomeDistance extraBasesClippedBefore;  // Clipping added if we align before
                                           // the beginning of a chromosome
  _int64 templateLength = 0;
  const char *clippedData;
  unsigned fullLength;
  unsigned clippedLength;
  unsigned basesClippedBefore;
  unsigned basesClippedAfter;
  int editDistance = -1;
  uint16_t flags = 0;
  GenomeLocation orig_location = genomeLocation;

  if (secondaryAlignment) {
    flags |= SAM_SECONDARY;
  }

  //
  // If the aligner said it didn't find anything, treat it as such.  Sometimes
  // it will emit the best match that it found, even if it's not within the
  // maximum edit distance limit (but will then say NotFound).  Here, we force
  // that to be SAM_UNMAPPED.
  //
  if (NotFound == result) {
    genomeLocation = InvalidGenomeLocation;
  }

  if (InvalidGenomeLocation == genomeLocation) {
    //
    // If it's unmapped, then always emit it in the forward direction.  This is
    // necessary because we don't even include the SAM_REVERSE_COMPLEMENT flag
    // for unmapped reads, so there's no way to tell that we reversed it.
    //
    direction = FORWARD;
  }

  clippedLength = read->getDataLength();
  fullLength = read->getUnclippedLength();

  if (direction == RC) {
    for (unsigned i = 0; i < fullLength; i++) {
      data[fullLength - 1 - i] = COMPLEMENT[read->getUnclippedData()[i]];
      quality[fullLength - 1 - i] = read->getUnclippedQuality()[i];
    }
    clippedData =
        &data[fullLength - clippedLength - read->getFrontClippedLength()];
    basesClippedBefore =
        fullLength - clippedLength - read->getFrontClippedLength();
    basesClippedAfter = read->getFrontClippedLength();
  } else {
    clippedData = read->getData();
    basesClippedBefore = read->getFrontClippedLength();
    basesClippedAfter = fullLength - clippedLength - basesClippedBefore;
  }

  if (genomeLocation != InvalidGenomeLocation) {
    if (direction == RC) {
      flags |= SAM_REVERSE_COMPLEMENT;
    }
    const Genome::Contig *contig = genome->getContigForRead(
        genomeLocation, read->getDataLength(), &extraBasesClippedBefore);
    _ASSERT(NULL != contig && contig->length > genome->getChromosomePadding());
    genomeLocation += extraBasesClippedBefore;

    contigName = contig->name;
    contigIndex = (int)(contig - genome->getContigs());
    positionInContig =
        genomeLocation - contig->beginningLocation;  // SAM is 1-based
    mapQuality = max(0, min(70, mapQuality));  // FIXME: manifest constant.
  } else {
    flags |= SAM_UNMAPPED;
    mapQuality = 0;
    extraBasesClippedBefore = 0;
  }

  finalResult.mutable_next_position()->set_position(-1);
  finalResult.mutable_next_position()->set_ref_index(-1);
  finalResult.mutable_next_position()->set_contig("");

  if (hasMate) {
    flags |= SAM_MULTI_SEGMENT;
    flags |= (firstInPair ? SAM_FIRST_SEGMENT : SAM_LAST_SEGMENT);
    if (mateLocation != InvalidGenomeLocation) {
      GenomeDistance mateExtraBasesClippedBefore;
      const Genome::Contig *mateContig = genome->getContigForRead(
          mateLocation, mate->getDataLength(), &mateExtraBasesClippedBefore);
      mateLocation += mateExtraBasesClippedBefore;
      matecontigName = mateContig->name;
      mateContigIndex = (int)(mateContig - genome->getContigs());
      matePositionInContig = mateLocation - mateContig->beginningLocation;

      if (mateDirection == RC) {
        flags |= SAM_NEXT_REVERSED;
      }

      if (genomeLocation == InvalidGenomeLocation) {
        //
        // The SAM spec says that for paired reads where exactly one end is
        // unmapped that the unmapped half should just have RNAME and POS copied
        // from the mate.
        //
        contigName = matecontigName;
        contigIndex = mateContigIndex;
        matecontigName = "=";
        positionInContig = matePositionInContig;
      }

    } else {
      flags |= SAM_NEXT_UNMAPPED;
      //
      // The mate's unmapped, so point it at us.
      //  in AGD this doesnt matter
      matecontigName = "=";
      mateContigIndex = contigIndex;
      matePositionInContig = positionInContig;
    }

    if (genomeLocation != InvalidGenomeLocation &&
        mateLocation != InvalidGenomeLocation) {
      if (alignedAsPair) {
        flags |= SAM_ALL_ALIGNED;
      }
      // Also compute the length of the whole paired-end string whose ends we
      // saw. This is slightly tricky because (a) we may have clipped some bases
      // before/after each end and (b) we need to give a signed result based on
      // whether our read is first or second in the pair.
      GenomeLocation myStart = genomeLocation - basesClippedBefore;
      GenomeLocation myEnd = genomeLocation + clippedLength + basesClippedAfter;
      _int64 mateBasesClippedBefore = mate->getFrontClippedLength();
      _int64 mateBasesClippedAfter = mate->getUnclippedLength() -
                                     mate->getDataLength() -
                                     mateBasesClippedBefore;
      GenomeLocation mateStart =
          mateLocation - (mateDirection == RC ? mateBasesClippedAfter
                                              : mateBasesClippedBefore);
      GenomeLocation mateEnd =
          mateLocation + mate->getDataLength() +
          (mateDirection == FORWARD ? mateBasesClippedAfter
                                    : mateBasesClippedBefore);
      if (contigName ==
          matecontigName) {  // pointer (not value) comparison, but that's OK.
        if (myStart < mateStart) {
          templateLength = mateEnd - myStart;
        } else {
          templateLength = -(myEnd - mateStart);
        }
      }  // otherwise leave TLEN as zero.
    }

    if (contigName == matecontigName) {
      matecontigName =
          "=";  // SAM Spec says to do this when they're equal (and not *, which
                // won't happen because this is a pointer, not string, compare)
    }
    finalResult.mutable_next_position()->set_position(matePositionInContig);
    finalResult.mutable_next_position()->set_ref_index(mateContigIndex);
    finalResult.mutable_next_position()->set_contig(matecontigName);
  }

  finalResult.set_mapping_quality(mapQuality);
  finalResult.set_flag(flags);
  finalResult.set_template_length(templateLength);
  finalResult.mutable_position()->set_position(positionInContig);
  finalResult.mutable_position()->set_ref_index(contigIndex);
  finalResult.mutable_position()->set_contig(contigName);

  const int cigarBufSize = MAX_READ * 2;
  char cigarBuf[cigarBufSize];

  const int cigarBufWithClippingSize = MAX_READ * 2 + 32;
  char cigarBufWithClipping[cigarBufWithClippingSize];

  if (orig_location != InvalidGenomeLocation) {
    const char *thecigar = SAMFormat::computeCigarString(
        genome, lv, cigarBuf, cigarBufSize, cigarBufWithClipping,
        cigarBufWithClippingSize, clippedData, clippedLength,
        basesClippedBefore, extraBasesClippedBefore, basesClippedAfter,
        read->getOriginalFrontHardClipping(),
        read->getOriginalBackHardClipping(), orig_location, direction, useM,
        &editDistance, addFrontClipping);

    // VLOG(INFO) << "cigar output was : " << thecigar << " and frontclipping
    // was " << *addFrontClipping;

    if (*addFrontClipping != 0) {
      // higher up the call stack deals with this
      // return errors::Internal("something went horribly wrong creating a cigar
      // string");
    } else {
      cigar = thecigar;
      finalResult.set_cigar(thecigar);
    }
  }

  return errors::Status::OK();
}