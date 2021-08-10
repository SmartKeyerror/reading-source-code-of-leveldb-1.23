// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Endian-neutral encoding:
// * Fixed-length numbers are encoded with least-significant byte first
// * In addition we support variable length "varint" encoding
// * Strings are encoded prefixed by their length in varint format

#ifndef STORAGE_LEVELDB_UTIL_CODING_H_
#define STORAGE_LEVELDB_UTIL_CODING_H_

#include <cstdint>
#include <cstring>
#include <string>

#include "leveldb/slice.h"
#include "port/port.h"

namespace leveldb {

// Standard Put... routines append to a string
void PutFixed32(std::string* dst, uint32_t value);
void PutFixed64(std::string* dst, uint64_t value);
void PutVarint32(std::string* dst, uint32_t value);
void PutVarint64(std::string* dst, uint64_t value);
void PutLengthPrefixedSlice(std::string* dst, const Slice& value);

// Standard Get... routines parse a value from the beginning of a Slice
// and advance the slice past the parsed value.
bool GetVarint32(Slice* input, uint32_t* value);
bool GetVarint64(Slice* input, uint64_t* value);
bool GetLengthPrefixedSlice(Slice* input, Slice* result);

// Pointer-based variants of GetVarint...  These either store a value
// in *v and return a pointer just past the parsed value, or return
// nullptr on error.  These routines only look at bytes in the range
// [p..limit-1]
const char* GetVarint32Ptr(const char* p, const char* limit, uint32_t* v);
const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* v);

// Returns the length of the varint32 or varint64 encoding of "v"
int VarintLength(uint64_t v);

// Lower-level versions of Put... that write directly into a character buffer
// and return a pointer just past the last byte written.
// REQUIRES: dst has enough space for the value being written
char* EncodeVarint32(char* dst, uint32_t value);
char* EncodeVarint64(char* dst, uint64_t value);

// Lower-level versions of Put... that write directly into a character buffer
// REQUIRES: dst has enough space for the value being written

/* 简单地将 unint32 写入至 dst 中，并没有使用 varint */
inline void EncodeFixed32(char* dst, uint32_t value) {

  /* 这里补充一个 static_cast 和 reinterpret_cast 之间的区别
   *
   * static_cast 主要用于"相关类型"之间的强制类型转换，比如 double 转 int，double 转 float。
   * 相关类型是指转换类型和待转换类型之间多少还有点儿关系，比如 double 和 int，它们都属于数字。
   * 又比如将 void * 指针转换成其它类型的指针，比如 int *，这也没有问题。
   * 但是如果把一个 long long 类型转换成一个指针类型，这就不属于"相关类型" 之间的范畴了。
   *
   * 另外一点就是基类指针转换成派生类指针时，虽然它们确实有关系，但是应该使用 dynamic_cast 完成。
   *
   * reinterpret_cast 则用于"完全不相关类型"之间的强制类型转换。reinterpret 可以翻译为重新释义，也就是
   * 对目标对象的内存进行重新解释。此时我们可以将一个 long long 对象转换成一个指针类型，或者是将一个 char *
   * 转换成一个 int *。
   * */
  uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);

  /* 依次将 value 的 4 字节写入至 buffer 中，也就是写入到 dst 中。
   * 并且可以看到，buffer[0] 写入的是 value 低 8 位，buffer[1] 写入的是第二个低 8 位。
   * 因此，数据存放的方式是按照先低位后高位的顺序存放的，也就是说，leveldb 采用的是小端存储（Little-Endian）*/

  // Recent clang and gcc optimize this to a single mov / str instruction.
  buffer[0] = static_cast<uint8_t>(value);
  buffer[1] = static_cast<uint8_t>(value >> 8);
  buffer[2] = static_cast<uint8_t>(value >> 16);
  buffer[3] = static_cast<uint8_t>(value >> 24);
}

/* 简单地将 unint64 写入至 dst 中，并没有使用 varint */
inline void EncodeFixed64(char* dst, uint64_t value) {
  uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);

  // Recent clang and gcc optimize this to a single mov / str instruction.
  buffer[0] = static_cast<uint8_t>(value);
  buffer[1] = static_cast<uint8_t>(value >> 8);
  buffer[2] = static_cast<uint8_t>(value >> 16);
  buffer[3] = static_cast<uint8_t>(value >> 24);
  buffer[4] = static_cast<uint8_t>(value >> 32);
  buffer[5] = static_cast<uint8_t>(value >> 40);
  buffer[6] = static_cast<uint8_t>(value >> 48);
  buffer[7] = static_cast<uint8_t>(value >> 56);
}

// Lower-level versions of Get... that read directly from a character buffer
// without any bounds checking.

/* 将通过 EncodeFixed32() 函数编码的 char * 转换成 uint32 */
inline uint32_t DecodeFixed32(const char* ptr) {
  const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);

  // Recent clang and gcc optimize this to a single mov / ldr instruction.
  return (static_cast<uint32_t>(buffer[0])) |
         (static_cast<uint32_t>(buffer[1]) << 8) |
         (static_cast<uint32_t>(buffer[2]) << 16) |
         (static_cast<uint32_t>(buffer[3]) << 24);
}

/* 将通过 EncodeFixed64() 函数编码的 char * 转换成 uint64 */
inline uint64_t DecodeFixed64(const char* ptr) {
  const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);

  // Recent clang and gcc optimize this to a single mov / ldr instruction.
  return (static_cast<uint64_t>(buffer[0])) |
         (static_cast<uint64_t>(buffer[1]) << 8) |
         (static_cast<uint64_t>(buffer[2]) << 16) |
         (static_cast<uint64_t>(buffer[3]) << 24) |
         (static_cast<uint64_t>(buffer[4]) << 32) |
         (static_cast<uint64_t>(buffer[5]) << 40) |
         (static_cast<uint64_t>(buffer[6]) << 48) |
         (static_cast<uint64_t>(buffer[7]) << 56);
}

// Internal routine for use by fallback path of GetVarint32Ptr
const char* GetVarint32PtrFallback(const char* p, const char* limit,
                                   uint32_t* value);
inline const char* GetVarint32Ptr(const char* p, const char* limit,
                                  uint32_t* value) {
  if (p < limit) {
    uint32_t result = *(reinterpret_cast<const uint8_t*>(p));
    if ((result & 128) == 0) {
      *value = result;
      return p + 1;
    }
  }
  return GetVarint32PtrFallback(p, limit, value);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_CODING_H_
