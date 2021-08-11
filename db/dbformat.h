// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DBFORMAT_H_
#define STORAGE_LEVELDB_DB_DBFORMAT_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

// Grouping of constants.  We may want to make some of these
// parameters set via options.
namespace config {
static const int kNumLevels = 7;

// Level-0 compaction is started when we hit this many files.
/* 当 Level-0 存在 4 个 SSTable 时将会触发 Level-0 向其它 level 的 Compaction */
static const int kL0_CompactionTrigger = 4;

// Soft limit on number of level-0 files.  We slow down writes at this point.
/* 当 Level-0 的 SSTable 数量达到 8 时，将会减缓 leveldb 的写入速率，其实就是睡眠 1 秒。
 * 具体的代码逻辑可参考 db_impl.cc/MakeRoomForWrite() 方法 */
static const int kL0_SlowdownWritesTrigger = 8;

// Maximum number of level-0 files.  We stop writes at this point.
/* Level-0 最多只能有 12 个 SSTable，一旦达到该阈值，将会停止外部写入，等到 Level-0 向下
 * Compaction 的完成 */
static const int kL0_StopWritesTrigger = 12;

// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap.  We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations.  We do not push all the way to
// the largest level since that can generate a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
static const int kMaxMemCompactLevel = 2;

// Approximate gap in bytes between samples of data read during iteration.
static const int kReadBytesPeriod = 1048576;

}  // namespace config

/* leveldb 是一个 K-V 存储引擎，因此 Key 的设计就非常重要的了。
 * 一方面需要保存用户自己的 User Key，另一方面还需要有一个序列号来标识同一个 Key 的多个版本更新。
 *
 * 在 InnoDB 中，为了支持 MVCC 所以 InnoDB 直接将 Transaction ID 写入了 B+Tree 聚簇索引记录中。
 * leveldb 则使用一个全局递增的 Sequence Number 来标识 K-V 操作的先后顺序。比如对于同一个 Key 来说，
 * leveldb 存储了 "username-1"、"username-2"，那么当我们进行 Compaction 时，"username-1" 将会被
 * "username-2" 所覆盖。
 *
 * 最后，leveldb 中删除 Key 时实际上是追加一个带有删除标志位的 Key，因此，我们还需要将标志位也放到 K
 * 的组中当中去。这样一来就得到了 InternalKey 的 3 个组成部分:
 *
 * - User Key: 用户传入的 Key
 * - Sequence Number: leveldb 全局递增的序列号，表示操作顺序
 * - Value Type: 枚举值，表示一个 Key 是否被删除。
 *
 * InternalKey 实际上就是把上面的 3 部分组合起来形成一个完整的 K，而 下面的 ParsedInternalKey
 * 则是将 InternalKey 拆解开来，得到 User Key、Sequence Number 以及 Value Type。我们可以简单
 * 地使用 Encode 和 Decode 的方式来理解 InternalKey 和 ParsedInternalKey 之间的关系:
 *
 * User Key + Sequence Number + Value Type  =>  InternalKey
 *                                        Encode
 *
 * InternalKey  =>  User Key + Sequence Number + Value Type
 *            Decode
 *
 * 下图为 InternalKey 的实际格式:
 * ┌──────────┬──────────────────────────────────────┐
 * │ User Key │ (Sequence Number << 8) | Value Type  │
 * └──────────┴──────────────────────────────────────┘
 * */
class InternalKey;

// Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.

/* 因为 leveldb 采用的是 Append 的方式删除数据，因此使用一个标志位来表示数据被删除，也就是
 * kTypeDeletion，这个枚举值将会被添加到 User Key 中，组成 InternalKey 或 ParsedInternalKey */
enum ValueType { kTypeDeletion = 0x0, kTypeValue = 0x1 };
// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
static const ValueType kValueTypeForSeek = kTypeValue;

typedef uint64_t SequenceNumber;

// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
static const SequenceNumber kMaxSequenceNumber = ((0x1ull << 56) - 1);


/* ParsedInternalKey 实际上就是 Internal Key 拆解之后的产物 */
struct ParsedInternalKey {
  Slice user_key;
  SequenceNumber sequence;
  ValueType type;

  ParsedInternalKey() {}  // Intentionally left uninitialized (for speed)
  ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t)
      : user_key(u), sequence(seq), type(t) {}
  std::string DebugString() const;
};

// Return the length of the encoding of "key".
inline size_t InternalKeyEncodingLength(const ParsedInternalKey& key) {
  return key.user_key.size() + 8;
}

// Append the serialization of "key" to *result.
/* 将 ParsedInternalKey 中的三个组件打包成 InternalKey，并存放到 result 中 */
void AppendInternalKey(std::string* result, const ParsedInternalKey& key);

// Attempt to parse an internal key from "internal_key".  On success,
// stores the parsed data in "*result", and returns true.
//
// On error, returns false, leaves "*result" in an undefined state.
/* 将 InternalKey 拆解成三个组件并扔到 result 的相应字段中 */
bool ParseInternalKey(const Slice& internal_key, ParsedInternalKey* result);

// Returns the user key portion of an internal key.
inline Slice ExtractUserKey(const Slice& internal_key) {
  assert(internal_key.size() >= 8);
  return Slice(internal_key.data(), internal_key.size() - 8);
}

// A comparator for internal keys that uses a specified comparator for
// the user key portion and breaks ties by decreasing sequence number.
class InternalKeyComparator : public Comparator {
 private:
  const Comparator* user_comparator_;

 public:
  explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c) {}
  const char* Name() const override;
  int Compare(const Slice& a, const Slice& b) const override;
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override;
  void FindShortSuccessor(std::string* key) const override;

  const Comparator* user_comparator() const { return user_comparator_; }

  int Compare(const InternalKey& a, const InternalKey& b) const;
};

// Filter policy wrapper that converts from internal keys to user keys
class InternalFilterPolicy : public FilterPolicy {
 private:
  const FilterPolicy* const user_policy_;

 public:
  explicit InternalFilterPolicy(const FilterPolicy* p) : user_policy_(p) {}
  const char* Name() const override;
  void CreateFilter(const Slice* keys, int n, std::string* dst) const override;
  bool KeyMayMatch(const Slice& key, const Slice& filter) const override;
};

// Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparisons instead of an InternalKeyComparator.
/* InternalKey 由 3 部分组成: User Key + Sequence Number + Value Type。
 * Value Type 实际上只有两种，要么表示新增/更新一个 K-V，要么表示删除一个 Key */
class InternalKey {
 private:
  std::string rep_;

 public:
  InternalKey() {}  // Leave rep_ as empty to indicate it is invalid
  InternalKey(const Slice& user_key, SequenceNumber s, ValueType t) {
    AppendInternalKey(&rep_, ParsedInternalKey(user_key, s, t));
  }

  bool DecodeFrom(const Slice& s) {
    rep_.assign(s.data(), s.size());
    return !rep_.empty();
  }

  Slice Encode() const {
    assert(!rep_.empty());
    return rep_;
  }

  Slice user_key() const { return ExtractUserKey(rep_); }

  void SetFrom(const ParsedInternalKey& p) {
    rep_.clear();
    AppendInternalKey(&rep_, p);
  }

  void Clear() { rep_.clear(); }

  std::string DebugString() const;
};

inline int InternalKeyComparator::Compare(const InternalKey& a,
                                          const InternalKey& b) const {
  return Compare(a.Encode(), b.Encode());
}

inline bool ParseInternalKey(const Slice& internal_key,
                             ParsedInternalKey* result) {
  const size_t n = internal_key.size();
  if (n < 8) return false;
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  uint8_t c = num & 0xff;
  result->sequence = num >> 8;
  result->type = static_cast<ValueType>(c);
  result->user_key = Slice(internal_key.data(), n - 8);
  return (c <= static_cast<uint8_t>(kTypeValue));
}

// A helper class useful for DBImpl::Get()
/* MemTable 的 Get() 方法需要传入 LookupKey 实例，从其构造函数中我们可以看出它就是由
 * User Key、Sequence Number 以及内部的 ValueType 所组成的。
 *
 *  ┌───────────────┬─────────────────┬────────────────────────────┐
 *  │ size(varint32)│ User Key(string)│Sequence Number | kValueType│
 *  └───────────────┴─────────────────┴────────────────────────────┘
 *  start_         kstart_                                        end_
 *
 *  因为 LookupKey 的 size 是变长存储的，因此需要使用 kstart_ 来标识 User Key 的起始地址
 * */
class LookupKey {
 public:
  // Initialize *this for looking up user_key at a snapshot with
  // the specified sequence number.
  LookupKey(const Slice& user_key, SequenceNumber sequence);

  LookupKey(const LookupKey&) = delete;
  LookupKey& operator=(const LookupKey&) = delete;

  ~LookupKey();

  // Return a key suitable for lookup in a MemTable.
  /* 可以看到，MemTable 获取的 end_ - start_，也就是说前面的变长 size 也会作为 MemTable Key
   * 的一部分 */
  Slice memtable_key() const { return Slice(start_, end_ - start_); }

  // Return an internal key (suitable for passing to an internal iterator)
  /* InternalKey 就是标准的三个组件 */
  Slice internal_key() const { return Slice(kstart_, end_ - kstart_); }

  // Return the user key
  /* User Key 的话需要刨去最后的 (Sequence Number << 8) | Value Type */
  Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); }

 private:
  // We construct a char array of the form:
  //    klength  varint32               <-- start_
  //    userkey  char[klength]          <-- kstart_
  //    tag      uint64
  //                                    <-- end_
  // The array is a suitable MemTable key.
  // The suffix starting with "userkey" can be used as an InternalKey.
  const char* start_;
  const char* kstart_;
  const char* end_;
  char space_[200];  // Avoid allocation for short keys
};

inline LookupKey::~LookupKey() {
  if (start_ != space_) delete[] start_;
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DBFORMAT_H_
