//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ddl.h
//
// Identification: src/backend/bridge/ddl/ddl.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "nodes/nodes.h"

#include <mutex>

namespace peloton {
namespace bridge {

static std::mutex parsetree_stack_mutex;

//===--------------------------------------------------------------------===//
// DDL
//===--------------------------------------------------------------------===//

class DDL {
 public:
  DDL(const DDL &) = delete;
  DDL &operator=(const DDL &) = delete;
  DDL(DDL &&) = delete;
  DDL &operator=(DDL &&) = delete;

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  static void ProcessUtility(Node *parsetree);
};

}  // namespace bridge
}  // namespace peloton
