// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

void FilterBlockBuilder::StartBlock(uint64_t block_offset) {

  /* block_offset 可以认为是 Data Block 的结束偏移量，kFilterBase 的值其实就是 2048，
   * 即 2KB，filter_index 就表示需要创建多少个 Bloom Filter */
  uint64_t filter_index = (block_offset / kFilterBase);

  assert(filter_index >= filter_offsets_.size());

  /* filter_offsets_ 用于保存每个 Bloom Filter 的起始偏移量，因为每一个 Bloom Filter 的
   * 长度可能是不同的。*/
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

/* 此时的 key 为 InternalKey，也就是在尾部追加了 `Sequence Number | Value Type` 的结果 */
void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  /* 注意这里是 keys_.size()，而不是 key.size()，记录的是每一个 key 在 keys_ 中的起始地址 */
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    GenerateFilter();
  }

  const uint32_t array_offset = result_.size();
  /* 将所有的偏移量放到 result_ 尾部，偏移量为定长编码 */
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  /* 将 Bloom Filters 的个数扔到 result_ 尾部*/
  PutFixed32(&result_, array_offset);
  /* 将 "base" 的大小放入，因为 kFilterBaseLg 可能会被修改 */
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

/* `GenerateFilter()` 方法用于构建一个具体的 Filter Block K，其内部将会调用 `policy_->CreateFilter`
 * 创建一个布隆过滤器，并将结果位数组（string 实现）返回 */
void FilterBlockBuilder::GenerateFilter() {

  /* 获取 key 的数量 */
  const size_t num_keys = start_.size();

  /* 如果 num_keys 为 0 的话，直接压入 result_.size() 并返回 */
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation

  /* tmp_keys_ 主要的作用就是作为 CreateFilter() 方法参数构建 Bloom Filter */
  tmp_keys_.resize(num_keys);

  /* 逐一取出 keys_ 中的所有 InternalKey，并扔到 tmp_keys_ 中 */
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];  /* 取得第 i 个 key 的起始地址 */
    size_t length = start_[i + 1] - start_[i];    /* 取得第 i 个 key 的长度 */
    tmp_keys_[i] = Slice(base, length);           /* 通过 Slice 构造 key */
  }

  /* result_.size() 的初始值为 0，所以 filter_offsets_ 记录的是 Bloom Filter 结果的起始偏移量 */
  filter_offsets_.push_back(result_.size());
  /* 构建 Bloom Filter */
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n - 1];
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  data_ = contents.data();
  offset_ = data_ + last_word;
  num_ = (n - 5 - last_word) / 4;
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
