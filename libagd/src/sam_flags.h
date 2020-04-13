#pragma once
#include "libagd/src/proto/alignment.pb.h"
#include "liberr/errors.h"

namespace agd {

enum ResultFlag {  // if the field is set, then ...
  MULTI = 0x1,  // there are multiple segments (usually, this is part of a read
                // pair)
  ALL_MAPPED = 0x2,             // all segments (reads) mapped properly
  UNMAPPED = 0x4,               // this segment (read) unmapped
  NEXT_UNMAPPED = 0x8,          // the next read is unmapped
  REVERSE_COMPLEMENTED = 0x10,  // this read is mapped as a reverse complemente
  NEXT_REVERSE_COMPLEMENTED =
      0x20,               // the next read is mapped as a reverse complemented
  FIRST = 0x40,           // this is the first read
  LAST = 0x80,            // this is the last read
  SECONDARY = 0x100,      // this is a secondary alignment
  FILTERED = 0x200,       // this read was filtered for some reason
  PCR_DUPLICATE = 0x400,  // this read is a duplicate
  SUPPLEMENTAL = 0x800    // this is a supplemental alignment
};

// a bunch of helper functions for flags (currently equivalent to the SAM
// format)
inline bool IsPrimary(uint32_t flag) {
  return !(flag & ResultFlag::SECONDARY || flag & ResultFlag::SUPPLEMENTAL);
}

inline bool IsPaired(uint32_t flag) { return (flag & ResultFlag::MULTI) != 0; }

inline bool IsDiscordant(uint32_t flag) {
  return (flag & ResultFlag::ALL_MAPPED) == 0;
}

inline bool IsMapped(uint32_t flag) { return (flag & ResultFlag::UNMAPPED) == 0; }

inline bool IsUnmapped(uint32_t flag) { return !IsMapped(flag); }

inline bool IsNextUnmapped(uint32_t flag) {
  return (flag & ResultFlag::NEXT_UNMAPPED) != 0;
}

inline bool IsNextMapped(uint32_t flag) { return !IsNextUnmapped(flag); }

inline bool IsReverseStrand(uint32_t flag) {
  return (flag & ResultFlag::REVERSE_COMPLEMENTED) != 0;
}

inline bool IsForwardStrand(uint32_t flag) { return !IsReverseStrand(flag); }

inline bool IsFirstRead(uint32_t flag) { return (flag & ResultFlag::FIRST) != 0; }

inline bool IsLastRead(uint32_t flag) { return (flag & ResultFlag::LAST) != 0; }

inline bool IsSecondary(uint32_t flag) {
  return (flag & ResultFlag::SECONDARY) != 0;
}

inline bool IsSupplemental(uint32_t flag) {
  return (flag & ResultFlag::SUPPLEMENTAL) != 0;
}

inline bool IsDuplicate(uint32_t flag) {
  return (flag & ResultFlag::PCR_DUPLICATE) != 0;
}

}  // namespace agd
