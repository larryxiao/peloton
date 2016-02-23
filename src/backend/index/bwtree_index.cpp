//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// btree_index.cpp
//
// Identification: src/backend/index/btree_index.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/index/index_key.h"
#include "backend/storage/tuple.h"

#include "backend/index/bwtree_index.h"

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyComparator,
    class ValueComparator, class KeyEqualityChecker>
BWTreeIndex<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
BWTreeIndex(IndexMetadata *metadata)
  : Index(metadata),
    container(metadata),
    equals(metadata),
    comparator() {
  // Add your implementation here
}

template <typename KeyType, typename ValueType, class KeyComparator,
    class ValueComparator, class KeyEqualityChecker>
BWTreeIndex<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
~BWTreeIndex() {
  // Add your implementation here
  container.Cleanup();
}

template <typename KeyType, typename ValueType, class KeyComparator,
    class ValueComparator, class KeyEqualityChecker>
bool BWTreeIndex<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
InsertEntry(
       __attribute__((unused)) const storage::Tuple *key,
       __attribute__((unused)) const ItemPointer location) {
  // Add your implementation here
  KeyType index_key;
  index_key.SetFromKey(key);
  container.Insert(index_key, location);
  return true;
}

template <typename KeyType, typename ValueType, class KeyComparator,
    class ValueComparator, class KeyEqualityChecker>
bool BWTreeIndex<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
DeleteEntry(
       __attribute__((unused)) const storage::Tuple *key,
__attribute__((unused)) const ItemPointer location) {
  // Add your implementation here
  KeyType index_key;
  index_key.SetFromKey(key);
  container.Delete(index_key, location);
  return true;
}

template <typename KeyType, typename ValueType, class KeyComparator,
    class ValueComparator, class KeyEqualityChecker>
std::vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
Scan(
  __attribute__((unused)) const std::vector<Value> &values,
  __attribute__((unused)) const std::vector<oid_t> &key_column_ids,
  __attribute__((unused)) const std::vector<ExpressionType> &expr_types,
  __attribute__((unused)) const ScanDirectionType& scan_direction) {
  std::vector<ItemPointer> result;
  // Add your implementation here
  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator,
    class ValueComparator, class KeyEqualityChecker>
std::vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
ScanAllKeys() {
  std::vector<ItemPointer> result;
  // Add your implementation here
  return result;
}

/**
 * @brief Return all locations related to this key.
 */
template <typename KeyType, typename ValueType, class KeyComparator,
    class ValueComparator, class KeyEqualityChecker>
std::vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
ScanKey(__attribute__((unused)) const storage::Tuple *key) {
  std::vector<ItemPointer> result;
  // Add your implementation here
  KeyType index_key;
  index_key.SetFromKey(key);
  return container.Search(index_key);
}

template <typename KeyType, typename ValueType, class KeyComparator,
    class ValueComparator, class KeyEqualityChecker>
std::string BWTreeIndex<KeyType, ValueType, KeyComparator, ValueComparator, KeyEqualityChecker>::
GetTypeName() const {
  return "BWTree";
}

// Explicit template instantiation
template class BWTreeIndex<IntsKey<1>, ItemPointer, ItemPointerComparator, IntsComparator<1>,
                           IntsEqualityChecker<1>>;
template class BWTreeIndex<IntsKey<2>, ItemPointer, ItemPointerComparator, IntsComparator<2>,
                           IntsEqualityChecker<2>>;
template class BWTreeIndex<IntsKey<3>, ItemPointer, ItemPointerComparator, IntsComparator<3>,
                           IntsEqualityChecker<3>>;
template class BWTreeIndex<IntsKey<4>, ItemPointer, ItemPointerComparator, IntsComparator<4>,
                           IntsEqualityChecker<4>>;

template class BWTreeIndex<GenericKey<4>, ItemPointer, ItemPointerComparator, GenericComparator<4>,
                           GenericEqualityChecker<4>>;
template class BWTreeIndex<GenericKey<8>, ItemPointer, ItemPointerComparator, GenericComparator<8>,
                           GenericEqualityChecker<8>>;
template class BWTreeIndex<GenericKey<12>, ItemPointer, ItemPointerComparator, GenericComparator<12>,
                           GenericEqualityChecker<12>>;
template class BWTreeIndex<GenericKey<16>, ItemPointer, ItemPointerComparator, GenericComparator<16>,
                           GenericEqualityChecker<16>>;
template class BWTreeIndex<GenericKey<24>, ItemPointer, ItemPointerComparator, GenericComparator<24>,
                           GenericEqualityChecker<24>>;
template class BWTreeIndex<GenericKey<32>, ItemPointer, ItemPointerComparator, GenericComparator<32>,
                           GenericEqualityChecker<32>>;
template class BWTreeIndex<GenericKey<48>, ItemPointer, ItemPointerComparator, GenericComparator<48>,
                           GenericEqualityChecker<48>>;
template class BWTreeIndex<GenericKey<64>, ItemPointer, ItemPointerComparator, GenericComparator<64>,
                           GenericEqualityChecker<64>>;
template class BWTreeIndex<GenericKey<96>, ItemPointer, ItemPointerComparator, GenericComparator<96>,
                           GenericEqualityChecker<96>>;
template class BWTreeIndex<GenericKey<128>, ItemPointer, ItemPointerComparator, GenericComparator<128>,
                           GenericEqualityChecker<128>>;
template class BWTreeIndex<GenericKey<256>, ItemPointer, ItemPointerComparator, GenericComparator<256>,
                           GenericEqualityChecker<256>>;
template class BWTreeIndex<GenericKey<512>, ItemPointer, ItemPointerComparator, GenericComparator<512>,
                           GenericEqualityChecker<512>>;

template class BWTreeIndex<TupleKey, ItemPointer, ItemPointerComparator, TupleKeyComparator,
                           TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace
