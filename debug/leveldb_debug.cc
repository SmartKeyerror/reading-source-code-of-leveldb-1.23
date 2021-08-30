
#include <vector>
#include <thread>
#include <cassert>
#include <iostream>
#include <random>
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"


std::string decimalTo62(long long n) {
  char characters[] = "0123456789abcdefghijklmnopqrstuvwxyz"
                      "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  std::string result;

  while (n) {
    result.push_back(characters[n % 62]);
    n = n / 62;
  }

  while (result.size() < 6) {
    result.push_back('0');
  }

  reverse(result.begin(), result.end());
  return result;
}

void putData(leveldb::DB *db, leveldb::WriteOptions *writeOptions, int keyCount,
             int init, int steps) {

  int decimal = init;
  while (keyCount > 0) {
    std::string key = decimalTo62(decimal);
    std::string value = key + key;
    db->Put(*writeOptions, key, value);
    decimal += steps;
    keyCount--;
  }
}


int main(){
  leveldb::DB* db;
  leveldb::Options options;

  options.create_if_missing = true;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);

  leveldb::Status status = leveldb::DB::Open(options,"/Users/smartkeyerror/leveldb", &db);

  leveldb::WriteOptions writeOptions;
  writeOptions.sync = true;

  int numThreads = 16;
  int total = 500000;

  std::vector<std::thread> threads(numThreads);

  for (int i = 0; i < numThreads; i++) {
    threads[i] = std::thread(putData, db, &writeOptions, total / numThreads, i, numThreads);
  }

  for (auto& t : threads) {
    t.join();
  }

  return 0;
}
