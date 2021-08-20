
#include <cassert>
#include <iostream>
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"


int main(){
  leveldb::DB* db;
  leveldb::Options options;

  options.create_if_missing = true;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);

  leveldb::Status status = leveldb::DB::Open(options,"/Users/smartkeyerror/leveldb", &db);

  leveldb::WriteOptions writeOptions;
  writeOptions.sync = true;

  std::string name = "smartkeyerror";
  std::string email = "smartkeyerror@gmail.com";

  for (int i = 0; i < 10000; i++) {
    status = db->Put(writeOptions, name, email);
    assert(status.ok());
  }

  leveldb::ReadOptions readOptions;

  std::string result;
  status = db->Get(readOptions, name, &result);
  assert(status.ok());
  std::cout << email << std::endl;

  return 0;
}
