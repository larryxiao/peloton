//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bwtree.cpp
//
// Identification: src/backend/index/bwtree.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/index/bwtree.h"

namespace peloton {
namespace index {

	template <typename KeyType, typename ValueType, class KeyComparator>
	int BWTree::node_key_search(const DeltaChain &chain, const KeyType &key, const node_search_mode &mode) {
		// first search the delta records
		while()
		// Assuming no delta records, the delta chain just contains
		// a Node type (i.e. either inner or leaf node)
		Node *node = reinterpret_cast<Node *>(chain.head);

		// empty node? return index of 0th element
		if(node->keys.size() == 0) return 0;

		// set the binary search range
		int min = 0, max = node->keys.size() - 1;

		//used to store comparison result after switch case
		bool compare_result;

		while (min < max) {
			// find middle element
			int mid = (min + max) >> 1;

			switch(mode){
				case GTE:
					compare_result = key_compare_lte(key, node->keys[mid]);
					break;
				case GT:
					compare_result = key_compare_lt(key, node->keys[mid]);
					break;
				default:
					//default case is GTE for now
					compare_result = key_compare_lte(key, node->keys[mid]);
			}

			if (compare_result) {
				max = mid;
			} else {
				min = mid+1;
			}
		}
		return min;
	}
}  // End index namespace
}  // End peloton namespace
