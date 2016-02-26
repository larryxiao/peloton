#include "gtest/gtest.h"
#include "harness.h"

#include "backend/common/logger.h"
#include "backend/index/index_factory.h"
#include "backend/storage/tuple.h"

#include "backend/index/bwtree.h"
#include "backend/index/index_key.h"

namespace peloton {
namespace test {

catalog::Schema *key_schema = nullptr;
catalog::Schema *tuple_schema = nullptr;

#define KeyType IntsKey<1>
#define ValueType ItemPointer
#define KeyComparator IntsComparator<1>
#define KeyEqualityChecker IntsEqualityChecker<1>

ItemPointer item0(120, 5);
ItemPointer item1(120, 7);
ItemPointer item2(123, 19);

// get a simple bwtree
BTreeIndex *BuildBWTree() {
  // Build tuple and key schema
  std::vector<catalog::Column> columns;
  std::vector<catalog::Schema *> schemas;
  IndexType index_type = INDEX_TYPE_BWTREE;

  catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "Key", true);
  catalog::Column column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "Value", true);

  columns.push_back(column1);

  // INDEX KEY SCHEMA -- {column1}
  key_schema = new catalog::Schema(columns);
  key_schema->SetIndexedColumns({0});

  columns.push_back(column2);

  // TABLE SCHEMA -- {column1, column2}
  tuple_schema = new catalog::Schema(columns);

  // Build index metadata
  const bool unique_keys = false;

  // what is oid_t? (125)
  index::IndexMetadata *index_metadata = new index::IndexMetadata(
    "test_index", 125, index_type, INDEX_CONSTRAINT_TYPE_DEFAULT,
    tuple_schema, key_schema, unique_keys);

  // Build bwtree
  BWTree *bwtree = new BWTree<KeyType, ValueType, KeyComparator,
  KeyEqualityChecker> bwtree(metadata);
  EXPECT_TRUE(index != NULL);

  return bwtree;
}

TEST(MergeTest, SimpleMergeLeaf) {
  // init storage
  auto pool = TestingHarness::GetInstance().GetTestingPool();

  // init with a tree
  std::unique_ptr<BTreeIndex> bwtree(BuildBWTree());

  // build a dummy tree
  pid_t newRoot = static_cast<pid_t>(pid_gen_++);
  pid_t leafRight = static_cast<pid_t>(pid_gen_++);
  pid_t leafLeft = static_cast<pid_t>(pid_gen_++);
  InnerNode* newInnerNode = new InnerNode(newRoot, 1, NULL_PID, NULL_PID);
  InnerNode* newLeafLeft = new LeafNode(leafLeft, leafRight);
  InnerNode* newLeafRight = new LeafNode(leafRight, NULL_PID);

  // keys
  std::unique_ptr<storage::Tuple> key0(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key1(new storage::Tuple(key_schema, true));
  std::unique_ptr<storage::Tuple> key2(new storage::Tuple(key_schema, true));

  key0->SetValue(0, ValueFactory::GetIntegerValue(1), pool);
  key1->SetValue(0, ValueFactory::GetIntegerValue(10), pool);
  key2->SetValue(0, ValueFactory::GetIntegerValue(100), pool);

  // value vector
  std::vector<std::pair<KeyType, pid_t >> rootVec = 
    std::vector<std::pair<KeyType, pid_t >> {std::pair<KeyType, pid_t >(key0, leafLeft),
      std::pair<KeyType, pid_t >(key2, leafRight)};
  std::vector<std::pair<KeyType, ValueType >> leftVec =
      std::vector<std::pair<KeyType, ValueType >> {std::pair<KeyType, ValueType >(key0, item0),
        std::pair<KeyType, ValueType >(key1, item1)};
  std::vector<std::pair<KeyType, ValueType >> rightVec =
        std::vector<std::pair<KeyType, ValueType >> {std::pair<KeyType, ValueType >(key2, item2)};

  newInnerNode->key_values = rootVec;
  newInnerNode->record_count = rootVec.size();
  newLeafLeft->key_values = leftVec;
  newLeafLeft->record_count = leftVec.size();
  newLeafRight->key_values = rightVec;
  newLeafRight->record_count = rightVec.size();

  bwtree->mapping_table_.insert_new_pid(newRoot, newInnerNode);
  bwtree->mapping_table_.insert_new_pid(leafLeft, newLeafLeft);
  bwtree->mapping_table_.insert_new_pid(leafRight, newLeafRight);
  bwtree->root_.store(newRoot, std::memory_order_release); // install new root
  // call merge
  bwtree->merge_page(leafLeft, leafRight, newRoot);
  // check result
  // check for remove node delta, node merge delta, index term delete delta
  Node* current_root = bwtree->mapping_table_.get_phy_ptr(newRoot);
  Node* current_left = bwtree->mapping_table_.get_phy_ptr(leafLeft);
  Node* current_right = bwtree->mapping_table_.get_phy_ptr(leafRight);
  // node type
  EXPECT_EQ(current_root->type, BWTree::NodeType::deleteIndex);
  EXPECT_EQ(current_left->type, BWTree::NodeType::mergeLeaf);
  EXPECT_EQ(current_right->type, BWTree::NodeType::removeNode);
  // TODO node fields
}

TEST(MergeTest, SimpleMergeInner) {
}

}  // End test namespace
}  // End peloton namespace