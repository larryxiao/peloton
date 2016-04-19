//
// Created by Zrs_y on 4/18/16.
//

#ifndef PELOTON_DATABASE_H
#define PELOTON_DATABASE_H

#include <vector>
#include <string>
#include "backend/logging/logger.h"

namespace peloton {
namespace wiredb {
typedef std::pair<std::vector<char>, std::vector<char>> ResType;


class DataBase {
public:
  DataBase() { }

  virtual ~DataBase() { }

  virtual int Exec(const char *query, std::vector<ResType> &res, std::string &errMsg) = 0;
};

}
}

#endif //PELOTON_DATABASE_H
