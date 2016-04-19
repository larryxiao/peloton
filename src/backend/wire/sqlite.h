//
// Created by Zrs_y on 4/18/16.
//

#ifndef PELOTON_SQLITE_H
#define PELOTON_SQLITE_H

#include "database.h"
#include <stdlib.h>
#include <sqlite3.h>
#include <stdio.h>

namespace peloton {
namespace wiredb {


class Sqlite : public DataBase {
public:
  Sqlite() {
    // sqlite3_open(filename, sqlite3 **db)
    // filename is null for in memory db
    auto rc = sqlite3_open(nullptr, &db);
    if (rc) {
      fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
      LOG_ERROR("Can't open database %s", sqlite3_errmsg(db));
      exit(0);
    } else {
      fprintf(stderr, "Opened database successfully\n");
    }

    // TODO: remove test
    test();
  }

  virtual ~Sqlite() {
    sqlite3_close(db);
  }


  virtual int Exec(const char *query, std::vector<ResType> &res, std::string &errMsg) {
    LOG_INFO("receive %s", query);
    char *zErrMsg;
    auto rc = sqlite3_exec(db, query, callback, (void *)&res, &zErrMsg);
    if( rc != SQLITE_OK ){
      LOG_INFO("error for %s %s", query, zErrMsg);
      if (zErrMsg != NULL)
        errMsg = std::string(zErrMsg);
      sqlite3_free(zErrMsg);
      return 1;
    }else {
      return 0;
    }
  }



private:
  void test() {
    std::vector<ResType> res;
    std::string err;
    Exec("CREATE TABLE A (id INT PRIMARY KEY, data TEXT);", res, err);
    res.clear();
    Exec("INSERT INTO A VALUES (1, 'abc'); ", res, err);
    res.clear();
    Exec("SELECT * FROM A;", res, err);

    for(auto item : res) {
      for(char c : item.first) {
        LOG_INFO("%c", c);
      }
      LOG_INFO("\n");
      for(char c : item.second) {
        LOG_INFO("%c", c);
      }
    }

    res.clear();
    Exec("DROP TABLE A", res, err);
  }
  static inline void copyFromTo(const char *src, std::vector<char> &dst) {
    if (src == nullptr) {
      return;
    }

    for(unsigned int i = 0; i < strlen(src); i++){
      dst.push_back(src[i]);
    }
  }

  static int callback(void *res, int argc, char **argv, char **azColName){
    auto output = (std::vector<ResType> *)res;
    for(int i = 0; i < argc; i++){
      output->push_back(ResType());
      if (argv[i] == NULL) {
        LOG_INFO("value is null");
      }else if(azColName[i] == NULL) {
        LOG_INFO("name is null");
      }else {
        LOG_INFO("res %s %s", azColName[i], argv[i]);
      }
      copyFromTo(azColName[i], output->at(i).first);
      copyFromTo(argv[i], output->at(i).second);
    }

    return 0;
  }

private:
  sqlite3 *db;
};
}
}

#endif //PELOTON_SQLITE_H
