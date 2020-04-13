
#include "bam_builder.h"

#include <stdio.h>
#include <zlib.h>
#include "libagd/src/agd_record_reader.h"
#include "libagd/src/sam_flags.h"
#include "libagd/src/proto/alignment.pb.h"

using namespace std;
using namespace errors;

// Most of this implementation is adapted from the Bam writer code in SNAP
//https://github.com/amplab/snap/blob/master/SNAPLib/Bam.cpp


inline int parseNextOp(const char* ptr, char& op, int& num) {
  num = 0;
  const char* begin = ptr;
  for (char curChar = ptr[0]; curChar != 0; curChar = (++ptr)[0]) {
    int digit = curChar - '0';
    if (digit >= 0 && digit <= 9)
      num = num * 10 + digit;
    else
      break;
  }
  op = (ptr++)[0];
  return ptr - begin;
}

Status ParseCigar(const char* cigar, size_t cigar_len,
                  vector<uint32_t>& cigar_vec) {
  // cigar parsing adapted from samblaster
  cigar_vec.clear();
  char op;
  int op_len;
  while (cigar_len > 0) {
    size_t len = parseNextOp(cigar, op, op_len);
    cigar += len;
    cigar_len -= len;
    uint32_t val = (op_len << 4) | BAMAlignment::CigarToCode[op];
    cigar_vec.push_back(val);
  }
  return Status::OK();
}

errors::Status BamBuilder::Run() {
  size_t num_chunks = 0;
  while (num_chunks != max_chunks_) {
    agd::ChunkQueueItem item;
    if (!input_queue_->pop(item)) {
      std::cout << "failed to pop contuing \n";
      continue;
    }

    std::cout << "[BamBuilder] processing chunk " << item.name  << "\n";

    agd::AGDRecordReader base_reader(item.col_bufs[0]->data(), item.chunk_size);
    agd::AGDRecordReader qual_reader(item.col_bufs[1]->data(), item.chunk_size);
    agd::AGDRecordReader meta_reader(item.col_bufs[2]->data(), item.chunk_size);
    agd::AGDResultReader aln_reader(item.col_bufs[3]->data(), item.chunk_size);

    Alignment result;
    const char *meta, *base, *qual;
    const char* cigar;
    size_t meta_len, base_len, qual_len, cigar_len;
    //int ref_index, mate_ref_index;
    vector<uint32_t> cigar_vec;
    cigar_vec.reserve(20);  // should usually be enough

    Status s = Status::OK();
    while (s.ok()) {
      ERR_RETURN_IF_ERROR(meta_reader.GetNextRecord(&meta, &meta_len));
      ERR_RETURN_IF_ERROR(base_reader.GetNextRecord(&base, &base_len));
      ERR_RETURN_IF_ERROR(qual_reader.GetNextRecord(&qual, &qual_len));
      Status aln_s = aln_reader.GetNextResult(result);

      if (IsUnavailable(aln_s)) {  // a null alignment, skip it
        continue;
      }

      const char* occ = strchr(meta, ' ');
      if (occ) meta_len = occ - meta;

      cigar = result.cigar().c_str();
      cigar_len = result.cigar().length();
      ERR_RETURN_IF_ERROR(ParseCigar(cigar, cigar_len, cigar_vec));

      size_t bamSize = BAMAlignment::size(
          (unsigned)meta_len + 1, cigar_vec.size(), base_len, /*auxLen*/ 0);
      if ((buffer_size_ - scratch_pos_) < bamSize) {
        // full buffer, push to compress queue and get a new buffer
        // LOG(INFO) << "main is getting buf for compress";
        // BufferRef compress_ref;
        // buffer_queue_->pop(compress_ref);
        auto compress_ref = buf_pool_.get();
        compress_ref->reset();
        compress_ref->reserve(buffer_size_);
        // LOG(INFO) << "main is pushing to compress";
        compress_queue_->push(make_tuple(std::move(current_buf_ref_),
                                         scratch_pos_, std::move(compress_ref),
                                         buffer_size_, current_index_));

        // LOG(INFO) << "main is getting fresh buf";
        current_index_++;
        // buffer_queue_->pop(current_buf_ref_);
        current_buf_ref_ = buf_pool_.get();
        compress_ref->reset();
        compress_ref->reserve(buffer_size_);
        scratch_ = current_buf_ref_->mutable_data();
        scratch_pos_ = 0;
      }

      BAMAlignment* bam = (BAMAlignment*)(scratch_ + scratch_pos_);
      bam->block_size = (int)bamSize - 4;

      bam->refID = result.position().ref_index();
      bam->pos = result.position().position();
      bam->l_read_name = (_uint8)meta_len + 1;
      bam->MAPQ = result.mapping_quality();
      bam->next_refID = result.next_position().ref_index();
      bam->next_pos = result.next_position().position();

      int refLength = cigar_vec.size() > 0 ? 0 : base_len;
      for (size_t i = 0; i < cigar_vec.size(); i++) {
        refLength += BAMAlignment::CigarCodeToRefBase[cigar_vec[i] & 0xf] *
                     (cigar_vec[i] >> 4);
      }

      if (agd::IsUnmapped(result.flag())) {
        if (agd::IsNextUnmapped(result.flag())) {
          bam->bin = BAMAlignment::reg2bin(-1, 0);
        } else {
          bam->bin = BAMAlignment::reg2bin(bam->next_pos, bam->next_pos + 1);
        }
      } else {
        bam->bin = BAMAlignment::reg2bin(bam->pos, bam->pos + refLength);
      }

      bam->n_cigar_op = cigar_vec.size();
      bam->FLAG = result.flag();
      bam->l_seq = base_len;
      bam->tlen = (int)result.template_length();
      memcpy(bam->read_name(), meta, meta_len);
      bam->read_name()[meta_len] = 0;
      memcpy(bam->cigar(), &cigar_vec[0], cigar_vec.size() * 4);
      BAMAlignment::encodeSeq(bam->seq(), base, base_len);
      memcpy(bam->qual(), qual, qual_len);
      for (unsigned i = 0; i < qual_len; i++) {
        bam->qual()[i] -= '!';
      }
      bam->validate();

      scratch_pos_ += bamSize;
      s = base_reader.PeekNextRecord(&base, &base_len);
    }

    num_chunks++;
  }

  // all chunks processed, finish

  while (compress_queue_->size() > 0) {
    this_thread::sleep_for(chrono::milliseconds(10));
  }

  // now stop the threads
  run_compress_ = false;
  compress_queue_->unblock();
  // LOG(INFO) << "Stopping c threads ...";
  while (num_active_threads_.load() > 1) {
    this_thread::sleep_for(chrono::milliseconds(10));
  }

  for (auto&t : compress_threads_) {
    t.join();
  }
  // it may be that the compress threads give one last
  // block to write, we dont want to lose it, so stop the writer
  // after
  while (write_queue_->size() > 0) {
    this_thread::sleep_for(chrono::milliseconds(10));
  }
  run_write_ = false;
  write_queue_->unblock();
  while (num_active_threads_.load() > 0) {
    this_thread::sleep_for(chrono::milliseconds(10));
  }

  writer_thread_.join();
  // if we have a partial buffer, compress and write it out

  if (scratch_pos_ != 0) {
    std::cout << "[BamBuilder] Finishing writing file ...\n";
    auto buf = buf_pool_.get();
    buf->reset();
    buf->reserve(buffer_size_);
    //buffer_queue_->pop(buf);
    size_t compressed_size = 0;
    Status s = CompressToBuffer(scratch_, scratch_pos_, buf->mutable_data(),
                                buffer_size_, compressed_size);
    if (!s.ok() || compressed_size == 0) {
      cout << "[BamBuilder] Error in final compress and write, compressed size = "
                 << compressed_size;
    }
    int status = fwrite(buf->data(), compressed_size, 1, bam_fp_);

    if (status < 0)
      cout << "[BamBuilder] WARNING: Final write of BAM failed with " << status;
  }
  // EOF marker for bam BGZF format
  static _uint8 eof[] = {0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00,
                         0x00, 0x00, 0xff, 0x06, 0x00, 0x42, 0x43,
                         0x02, 0x00, 0x1b, 0x00, 0x03, 0x00, 0x00,
                         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

  // append the final EOF marker
  int status = fwrite(eof, sizeof(eof), 1, bam_fp_);

  if (status < 0)
    cout << "[BamBuilder] WARNING: Final write of BAM eof marker failed with "
              << status;

  status = fclose(bam_fp_);

  if (status == EOF)
    cout << "[BamBuilder] WARNING: Failed to close BAM file pointer: " << status;

  return Status::OK();
}

errors::Status BamBuilder::Init(const json& agd_metadata, size_t threads) {
  const auto& ref_seqs = agd_metadata["ref_genome"];
  num_threads_ = threads;
  stringstream header_ss;
  header_ss << "@HD\tVN:1.4\tSO:";
  header_ss << "unsorted" << endl;

  auto path = absl::StrCat(agd_metadata["name"].get<string>(), ".bam");

  file_exists_ = true;
  // open the file, we dont write yet
  bam_fp_ = fopen(path.c_str(), "w");
  header_ = header_ss.str();

  if (bam_fp_ == nullptr) {
    return Internal("[BamBuilder] bam fp is ", bam_fp_, " errno is ", errno);
  }

  compress_queue_.reset(new ConcurrentQueue<CompressItem>(num_threads_ * 2));
  write_queue_.reset(new ConcurrentPriorityQueue<WriteItem>(num_threads_ * 2));

  ThreadInit();

  // buffer_queue_->pop(current_buf_ref_);

  current_buf_ref_ = buf_pool_.get();
  current_buf_ref_->resize(buffer_size_);

  scratch_ = current_buf_ref_->mutable_data();
  scratch_pos_ = 0;
  // and write the header into the first buffer
  BAMHeader* bamHeader = (BAMHeader*)scratch_;
  bamHeader->magic = BAMHeader::BAM_MAGIC;
  size_t samHeaderSize = header_.length();
  memcpy(bamHeader->text(), header_.c_str(), samHeaderSize);
  bamHeader->l_text = (int)samHeaderSize;
  scratch_pos_ = BAMHeader::size((int)samHeaderSize);

  // viral genomes may make this too big?
  bamHeader->n_ref() = ref_seqs.size();
  BAMHeaderRefSeq* refseq = bamHeader->firstRefSeq();
  for (const auto& inrefseq : ref_seqs) {
    std::string inseqname = inrefseq["name"];
    int len = inseqname.length() + 1;
    scratch_pos_ += BAMHeaderRefSeq::size(len);
    refseq->l_name = len;
    memcpy(refseq->name(), inseqname.c_str(), len);
    refseq->l_ref() = (int)(inrefseq["length"].get<int>());
    refseq = refseq->next();
    std::cout << "[BamBuilder] adding ref seq " << inseqname << " of size " << inrefseq["length"] << "\n";
    _ASSERT((char*)refseq - header == scratch_pos_);
  }

  return Status::OK();
}

Status BamBuilder::CompressToBuffer(char* in_buf, uint32_t in_size,
                                    char* out_buf, uint32_t out_size,

                                    size_t& compressed_size) {
  if (in_size == 0) return Internal("[BamBuilder] attempting to compress and write 0 bytes");
  // set up BAM header structure
  gz_header header;
  _uint8 bamExtraData[6];
  header.text = false;
  header.time = 0;
  header.xflags = 0;
  header.os = 0;
  header.extra = bamExtraData;
  header.extra_len = 6;
  header.extra_max = 6;
  header.name = NULL;
  header.name_max = 0;
  header.comment = NULL;
  header.comm_max = 0;
  header.hcrc = false;
  header.done = true;
  bamExtraData[0] = 'B';
  bamExtraData[1] = 'C';
  bamExtraData[2] = 2;
  bamExtraData[3] = 0;
  bamExtraData[4] = 3;  // will be filled in later
  bamExtraData[5] = 7;  // will be filled in later

  z_stream zstream;
  zstream.zalloc = Z_NULL;
  zstream.zfree = Z_NULL;
  const int windowBits = 15;
  const int GZIP_ENCODING = 16;
  zstream.next_in = (Bytef*)in_buf;
  zstream.avail_in = (uInt)in_size;
  zstream.next_out = (Bytef*)out_buf;
  zstream.avail_out = (uInt)out_size;
  uInt oldAvail;
  int status;

  status = deflateInit2(&zstream, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                        windowBits | GZIP_ENCODING, 8, Z_DEFAULT_STRATEGY);
  if (status < 0)
    return Internal("[BamBuilder] libz deflate init failed with ", to_string(status));

  status = deflateSetHeader(&zstream, &header);
  if (status != Z_OK) {
    return Internal("[BamBuilder] libz: defaultSetHeader failed with", status);
  }

  oldAvail = zstream.avail_out;
  status = deflate(&zstream, Z_FINISH);

  if (status < 0 && status != Z_BUF_ERROR) {
    return Internal("[BamBuilder] libz: deflate failed with ", status);
  }
  if (zstream.avail_in != 0) {
    return Internal("[BamBuilder] libz: default failed to read all input");
  }
  if (zstream.avail_out == oldAvail) {
    return Internal("[BamBuilder] libz: default failed to write output");
  }
  status = deflateEnd(&zstream);
  if (status < 0) {
    return Internal("[BamBuilder] libz: deflateEnd failed with ", status);
  }

  size_t toUsed = out_size - zstream.avail_out;
  // backpatch compressed block size into gzip header
  if (toUsed >= BAM_BLOCK) {
    return Internal("[BamBuilder] exceeded BAM chunk size");
  }
  *(_uint16*)(out_buf + 16) = (_uint16)(toUsed - 1);

  // status = fwrite(scratch_compress_.get(), toUsed, 1, bam_fp_);

  // if (status < 0)
  // return Internal("write compressed block to bam failed: ", status);

  if (!((BgzfHeader*)(out_buf))->validate(toUsed, in_size)) {
    return Internal("[BamBuilder] bgzf validation failed");
  }

  compressed_size = toUsed;
  return Status::OK();
}

void BamBuilder::ThreadInit() {
  auto compress_func = [this]() {
    size_t compressed_size;
    CompressItem item;
    while (run_compress_) {
      if (!compress_queue_->pop(item)) {
        continue;
      }

      auto& in_buf = get<0>(item);
      auto in_size = get<1>(item);
      auto& out_buf = get<2>(item);
      auto out_size = get<3>(item);
      auto index = get<4>(item);

      compressed_size = 0;
      // LOG(INFO) << my_id <<  " compressor compressing index " << index << "
      // at size " << in_size << " bytes to "
      // << " output buf with size " << out_size;
      Status s = CompressToBuffer(in_buf->mutable_data(), in_size, out_buf->mutable_data(),
                                  out_size, compressed_size);
      if (!s.ok()) {
        std::cout << "[BamBuilder] Error in compress and write";
        compute_status_ = s;
        return;
      }
      // LOG(INFO) << my_id << " compressed into " << compressed_size << "
      // bytes.";

      WriteItem wr_item;
      wr_item.buf = std::move(out_buf);
      wr_item.size = compressed_size;
      wr_item.index = index;
      // LOG(INFO)<< my_id  << " compressor pushing " << index << " to writer ";
      write_queue_->push(std::move(wr_item));
    }
    num_active_threads_--;
  };

  auto writer_func = [this]() {
    uint32_t index = 0;
    WriteItem item;
    const WriteItem* peek_item;
    while (run_write_) {
      if (!write_queue_->peek(&peek_item)) {
        continue;
      }
      // its possible the next index is not in the queue yet
      // wait for it
      // only works if there is one thread doing this
      if (item.index != index) continue;

      // got the right index now, works because
      // we know a lower index than `index` will never
      // happen
      // LOG(INFO) << my_id << " writer popping";
      write_queue_->pop(item);

      auto size = item.size;
      auto idx = item.index;

      // LOG(INFO) << my_id << " writer writing index " << idx;
      if (idx != index) {
        // LOG(INFO) << my_id << " got " << idx << " for index " << index;
        compute_status_ = Internal("[BamBuilder] Did not get block index in order!");
        return;
      }

      int status = fwrite(item.buf->data(), size, 1, bam_fp_);
      if (status < 0) {
        compute_status_ = Internal("[BamBuilder] Failed to write to bam file");
        return;
      }
      index++;

      // recycle the buffer
      // LOG(INFO) << my_id << "writer recycling";
    }
    num_active_threads_--;
  };

  compress_threads_.resize(num_threads_ - 1);
  for (int i = 0; i < num_threads_ - 1; i++)
    compress_threads_[i] = std::thread(compress_func);

  writer_thread_ = std::thread(writer_func);

  num_active_threads_ = num_threads_;
}