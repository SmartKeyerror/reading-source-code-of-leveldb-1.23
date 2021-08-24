// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;              /* Data Block Options */
  Options index_block_options;  /* Index Block Options */
  WritableFile* file;           /* 抽象类，决定了如何进行文件的写入，主要实现为 PosixWritableFile */
  uint64_t offset;              /* Data Block 在 SSTable 中的文件偏移量 */
  Status status;                /* 操作状态 */
  BlockBuilder data_block;      /* 构建 Data Block 所需的 BlockBuilder */
  BlockBuilder index_block;     /* 构建 Index Block 所需的 BlockBuilder */
  std::string last_key;         /* 当前 Data Block 的最后一个写入 key */
  int64_t num_entries;          /* 当前 Data Block 的写入数量 */
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block; /* 构建 Filter Block 所需的 BlockBuilder */

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;  /* 压缩之后的 Data Block */
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

/* 此时的 key 仍然为 InternalKey，也就是 User Key + Sequence Number | Value Type */
void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);   /* 判断当前 Build 过程是否结束 */
  if (!ok()) return;
  if (r->num_entries > 0) {
    /* 判断当前 key 是否大于 last_key */
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  /* 在构建下一个 Data Block 之前，将 Index Block 构建出来 */
  if (r->pending_index_entry) {
    assert(r->data_block.empty());

    /* 通过 last_key 和 当前 key 计算得到一个 X，使得 last_entry <= X < key  */
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);

    /* 向 Index Block 中添加上一个 Data Block 的 Index */
    r->index_block.Add(r->last_key, Slice(handle_encoding));

    /* 上一个 Data Block 的 Index Block 已经写完，故更新 pending_index_entry 为 false */
    r->pending_index_entry = false;
  }

  /* 若指定了 FilterPolicy，那么就会写入 Filter Block */
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  /* 更新 last_key */
  r->last_key.assign(key.data(), key.size());
  /* 更新 Key-Value 写入数量 */
  r->num_entries++;
  /* 将数据添加至 Data Block 中 */
  r->data_block.Add(key, value);

  /* Data Block 的默认大小为 4KB */
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    /* 结束当前 Block 的构建，Flush() 方法内部将会把 pending_index_entry 置为 True */
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);

  /* 对 Data Block 进行压缩，并生成 Block Handle */
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    /* 设置 pending_index_entry 为 true，下一次写入 Data Block 时，需构建 Index Block */
    r->pending_index_entry = true;
    /* 将数据写入至内核缓冲区 */
    r->status = r->file->Flush();
  }
  if (r->filter_block != nullptr) {
    /* 创建一个新的 Filter Block */
    r->filter_block->StartBlock(r->offset);
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;

  /* 获取 Data Block 的全部数据 */
  Slice raw = block->Finish();

  Slice block_contents;

  /* 默认压缩方式为 kSnappyCompression */
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;

      /* 进行 snappy 压缩，并且只有在压缩率大于 12.5 时才会选用压缩结果 */
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        /* 未配置压缩算法，或者是使用 snappy 压缩时压缩率低于 12.5% */
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  /* 将处理后的 block contents、压缩类型以及 block handle 写入到文件中 */
  WriteRawBlock(block_contents, type, handle);
  /* 清空临时存储 buffer */
  r->compressed_output.clear();
  /* 清空 Data Block */
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish() {
  Rep* r = rep_;

  /* 将最后一个 Data Block 写入 */
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      /* 若使用 Bloom Filter，key 的值为 filter.leveldb.BuiltinBloomFilter2 */
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    /* 写入 Metaindex Block */
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
