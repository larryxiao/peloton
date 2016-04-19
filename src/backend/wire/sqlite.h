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

#define UNUSED __attribute__((unused))
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


  virtual int PortalExec(const char *query, std::vector<ResType> &res, std::string &errMsg) {
    LOG_INFO("receive %s", query);
    char *zErrMsg = 0;
    auto rc = sqlite3_exec(db, query, execCallback, (void *)&res, &zErrMsg);
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


  int PortalDesc(const char *table_name, std::vector<FieldInfoType> &field_info, std::string &errMsg) {

    char *zErrMsg = 0;

    std::string query = "PRAGMA table_info(" + std::string(table_name) + ");";
    LOG_INFO("describle query %s", query.c_str());
    auto rc = sqlite3_exec(db, query.c_str(), descCallback, (void *) &field_info, &zErrMsg);
    if (rc != SQLITE_OK) {
      LOG_INFO("error in des %s", zErrMsg);
      if (zErrMsg != NULL)
        errMsg = std::string(zErrMsg);
        sqlite3_free(zErrMsg);
      return 1;
    } else {
      return 0;
    }
  }

  int InitBindPrepStmt(const char *query, std::vector<std::pair<int, std::string>> parameters UNUSED, void ** stmt, std::string &errMsg) {
    sqlite3_stmt *sql_stmt = nullptr;
    int rc = sqlite3_prepare_v2(db, query, -1, &sql_stmt, NULL);
    if (rc != SQLITE_OK) {
      errMsg = std::string(sqlite3_errmsg(db));
      return 1;
    }

    for (int i = 0; i < (int)parameters.size(); i++) {
      auto &item = parameters[i];
      switch (item.first) {
        case WIRE_INTEGER:
          rc = sqlite3_bind_int(sql_stmt, i, std::stoi(item.second));
          if (rc != SQLITE_OK) {
            errMsg = std::string(sqlite3_errmsg(db));
            return 1;
          }
          break;
        case WIRE_FLOAT:
          rc = sqlite3_bind_double(sql_stmt, i, std::stod(item.second));
          if (rc != SQLITE_OK) {
            errMsg = std::string(sqlite3_errmsg(db));
            return 1;
          }
          break;
        case WIRE_TEXT:
          rc = sqlite3_bind_text(sql_stmt, i, item.second.c_str(), (int)item.second.size(), 0);
          if (rc != SQLITE_OK) {
            errMsg = std::string(sqlite3_errmsg(db));
            return 1;
          }
          break;
      }
    }

    *(sqlite3_stmt **)stmt = sql_stmt;
    return 0;
  }

  int ExecPrepStmt(void *stmt, std::vector<ResType> &res, std::string &errMsg) {
    auto sql_stmt = (sqlite3_stmt *)stmt;
    auto ret = sqlite3_step(sql_stmt);
    auto col_num = sqlite3_column_count(sql_stmt);
    while (ret == SQLITE_ROW) {
      for (int i = 0; i < col_num; i++) {
        int t = sqlite3_column_type(sql_stmt, i);
        const char *name = sqlite3_column_name(sql_stmt, i);
        std::string value;
        switch (t) {
          case SQLITE_INTEGER: {
            int v = sqlite3_column_int(sql_stmt, i);
            value = std::to_string(v);
            break;
          }
          case SQLITE_FLOAT: {
            float v = (float)sqlite3_column_double(sql_stmt, i);
            value = std::to_string(v);
            break;
          }
          case SQLITE_TEXT: {
            const char *v = (char *)sqlite3_column_text(sql_stmt, i);
            value = std::string(v);
            break;
          }
          default: break;
        }
        res.push_back(ResType());
        LOG_INFO("res from exeStmt %s %s\n", name, value.c_str());
        copyFromTo(name, res.back().first);
        copyFromTo(value.c_str(), res.back().second);
      }
      ret = sqlite3_step(sql_stmt);
    }

    if (ret != SQLITE_DONE) {
      LOG_INFO("ret num %d, err is %s", ret, sqlite3_errmsg(db));
      errMsg = std::string(sqlite3_errmsg(db));
      return 1;
    }

    return 0;
  }


private:
  void test() {
    std::vector<ResType> res;
    std::string err;
    PortalExec("CREATE TABLE A (id INT PRIMARY KEY, data TEXT);", res, err);
    res.clear();
    PortalExec("INSERT INTO A VALUES (1, 'abc'); ", res, err);
    res.clear();
    PortalExec("SELECT * FROM A;", res, err);

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
    PortalExec("DROP TABLE A", res, err);
  }
  static inline void copyFromTo(const char *src, std::vector<char> &dst) {
    if (src == nullptr) {
      return;
    }

    for(unsigned int i = 0; i < strlen(src); i++){
      dst.push_back(src[i]);
    }
  }

  static int execCallback(void *res, int argc, char **argv, char **azColName){
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

  static int getSize(const std::string& type UNUSED) {
    return 0;
  }
  static int descCallback(void *res, int argc, char **argv, char **azColName){
    auto output = (std::vector<FieldInfoType> *)res;
    std::string name, type;
    for(int i = 0; i < argc; i++){
      output->push_back(FieldInfoType());
      if (argv[i] == NULL) {
        LOG_INFO("value is null");
      }else if(azColName[i] == NULL) {
        LOG_INFO("name is null");
      }else {
        if(strcmp(azColName[i], "name") == 0) {
          LOG_INFO("name is %s", argv[i]);
          name = std::string(argv[i]);
        }else if(strcmp(azColName[i], "type") == 0) {
          LOG_INFO("type is %s", argv[i]);
          type = std::string(argv[i]);
        }
      }

      int size = getSize(type);
      output->push_back(std::make_tuple(name, type, size));
      return 0;
    }

    return 0;
  }

private:
  sqlite3 *db;
};
}
}

#endif //PELOTON_SQLITE_H
