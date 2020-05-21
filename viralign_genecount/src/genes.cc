
#include "genes.h"

std::ostream& operator<<(std::ostream& out, const TreeValue& t) {
  return out << "TreeValue gene_id: " << t.gene_id << ", strand: " << t.strand;
}