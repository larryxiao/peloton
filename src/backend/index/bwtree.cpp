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
	int BWTree::find_lower(const DeltaChain& chain, const KeyType &key) {
		// Assuming no delta records, the delta chain just contains
		// a Node type (i.e. either inner or leaf node)
		Node *node = reinterpret_cast<Node *>(chain.head);

		//empty node? return index of 0th element
		if(node->keys.size() == 0) return 0;

		//set the binary search range
		int min = 0, max = node->keys.size() - 1;

		while (min < max) {
			//find middle element
			int mid = (min + max) >> 1;


		}


	}
}  // End index namespace
}  // End peloton namespace
