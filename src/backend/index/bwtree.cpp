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

	template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
	int BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
	node_key_search(const pid_t node_pid, const KeyType &key, const node_search_mode &mode) {

		// Lookup logical ptr from mapping table
		// and cast as tree node
		TreeNode *node = static_cast<TreeNode *>(mapping_table_.get_phy_ptr(node_pid));

		// empty node? return index of 0th element
		if(node->keys.size() == 0) return -1;

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

	template <typename KeyType, typename ValueType, class KeyComparator,
			class KeyEqualityChecker>
	TreeOpResult BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
	do_tree_operation(const pid_t node_pid, const KeyType &key,
										const LeafOperation *leaf_operation,
										const OperationType &type) {
		Node *node = mapping_table_.get_phy_ptr(node_pid);

		if (node->get_type() == NodeType::removeNode) {
			// we have a remove node delta, end search and try again
			return get_failed_result();
		}

		if (node->chain_length > consolidate_threshold_inner_) {
			consolidate(node);
		}

		// flag to indicate if iteration should be continued
		bool continue_itr = true;

		// store result from recursion, if any
		TreeOpResult *result = nullptr;

		//iterate through the delta chain, based on case
		while (continue_itr && node != nullptr) {
			// Note: explicitly set continue_itr state only if it goes to false

			// switch on the type
			switch(node->get_type()) {
				case indexDelta: {
					// cast the node
					auto delta_node = static_cast<IndexDelta *>(node);
					// Kp <= key, key < Kq?
					if(key_compare_lte(delta_node->low, key) && key_compare_lt(key, delta_node->high)) {
						// recurse into shortcut pointer
						*result = do_tree_operation(delta_node->new_node, key, leaf_operation, type);
						// don't iterate anymore
						continue_itr = false;
					}
					// else, continue
				}

				case deleteIndex: {
					auto delta_node = static_cast<DeleteIndex *>(node);
					// Kp <= key, key < Kq?
					if(key_compare_lte(delta_node->low, key) && key_compare_lt(key, delta_node->high)) {
						// recurse into shortcut pointer
						*result = do_tree_operation(delta_node->merge_node, key, leaf_operation, type);
						// don't iterate anymore
						continue_itr = false;
					}
					// else, continue
				}

				case deltaSplitInner: {
					auto delta_node = static_cast<DeltaSplitInner *>(node);
					// splitkey <= Key?
					if(key_compare_lte(delta_node->splitKey, key)){
						// recurse into new nodenew  
						*result = do_tree_operation(delta_node->new_node, key, leaf_operation, type);

					}
				}
			}
		}

	}
}  // End index namespace
}  // End peloton namespace
