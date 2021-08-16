
## leveldb 源码解析

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