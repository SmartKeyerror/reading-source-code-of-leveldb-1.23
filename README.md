
## leveldb 源码阅读

### 1. Build && Install && Debug

leveldb 本身是一个 Key-Value 存储引擎，因此并没有提供 `main` 入口函数，所以需要自行添加。笔者将其放到了 `debug/leveldb_debug.cc` 文件中，并在 `CMakeLists.txtx` 中将其加入:

```bash
  leveldb_test("util/env_test.cc")
  leveldb_test("util/status_test.cc")
  leveldb_test("util/no_destructor_test.cc")
  
  if(NOT BUILD_SHARED_LIBS)
    leveldb_test("debug/leveldb_debug.cc")    # 个人可执行文件
    leveldb_test("db/autocompact_test.cc")
```

```bash
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug .. && cmake --build .
make && make install
gdb leveldb_debug   # 此时 leveldb_debug 就在 build 目录下，可直接进行 gdb 调试
```

如果使用 CLion 的话，可以直接对 `leveldb_debug.cc` 进行 debug，比 gdb 要更方便一些。

### 2. leveldb 核心流程梳理

1. leveldb 概述与 LSM-Tree
2. leveldb 中的常用数据结构
3. [leveldb 中的 varint 与 Key 组成](/debug/articles/03-varint-and-key-format/README.md)
4. [leveldb Key-Value 写入流程分析](/debug/articles/04-write-process/README.md)
5. [leveldb 预写日志格式及其读写流程](/debug/articles/05-WAL/README.md)
6. [SSTable(01)——概览与 Data Block](/debug/articles/06-SSTable-data-block/README.md)
7. SSTable(02)——Bloom Filter 与 Meta Block
8. SSTable(02)——Table Builder