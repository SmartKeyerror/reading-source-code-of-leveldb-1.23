// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  FilterBlockBuilder(const FilterBlockBuilder&) = delete;
  FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;

  /* 开始构建新的 Filter Block */
  void StartBlock(uint64_t block_offset);

  /*添加一个新的 key，将在 `TableBuilder` 中被调用*/
  void AddKey(const Slice& key);

  /*结束 Filter Block 的构建，并返回 Filter Block 的完整内容*/
  Slice Finish();

 private:
  void GenerateFilter();          /* 构建一个 Filter */

  const FilterPolicy* policy_;    /* filter 类型，如 BloomFilterPolicy */
  std::string keys_;              /* User Keys，全部塞到一个 string 中 */
  std::vector<size_t> start_;     /* 每一个 User Key 在 keys_ 中的起始位置 */
  std::string result_;            /* keys_ 通过 policy_ 计算出来的 filtered data */
  std::vector<Slice> tmp_keys_;   /* policy_->CreateFilter() 的参数 */

  /* filter 在 result_ 中的位置, filter_offsets_.size() 就是 Bloom Filter 的数量 */
  std::vector<uint32_t> filter_offsets_;
};

class FilterBlockReader {
 public:
  // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  const FilterPolicy* policy_;
  const char* data_;    // Pointer to filter data (at block-start)
  const char* offset_;  // Pointer to beginning of offset array (at block-end)
  size_t num_;          // Number of entries in offset array
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
