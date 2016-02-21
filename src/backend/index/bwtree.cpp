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
#define TODO

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

	template <typename KeyType, typename ValueType, class KeyComparator>
	LeafIterator BWTree::tree_search(const KeyType &key, const node_search_mode &mode) {

		// first search the delta records
		for(auto it=chain->begin(); chain->end() != it; it++){
			//check for key match in delta record
			if(key_compare_eq((*it)->key, key)){
				//insert record?
			}
		}
	}

    void BwTree::merge(PID pid_l, PID pid_r, PID pid_parent) {
        Node *ptr_l = get_phy_ptr(pid_l);
        Node *ptr_r = get_phy_ptr(pid_r);
        Node *ptr_parent = get_phy_ptr(pid_parent);
        bool leaf = ptr_r.is_leaf();

        // Step 1 marking for delete
        // create remove node delta node
        RemoveNode *remove_node_delta = new RemoveNode();
        remove_node_delta.set_next(ptr_r);
        mapping_table_.install_node(pid_r, ptr_r, remove_node_delta);
        // Step 2 merging children
        // create node merge delta
        // TODO key range
        int record_count = ptr_l->record_count + ptr_r->record_count;
        if (leaf) {
            MergeLeaf *node_merge_delta = new MergeLeaf(ptr_r->low, ptr_r, record_count);
        } else {
            MergeInner *node_merge_delta = new MergeInner(ptr_r->low, ptr_r, record_count);
        }
        remove_node_delta.set_next(ptr_l);
        mapping_table_.install_node(pid_l, ptr_l, (Node *) node_merge_delta);
        // Step 3 parent update
        // create index term delete delta
        IndexDelta *index_term_delete_delta = new IndexDelta(ptr_l->low, ptr_r->high, pid_l);
        index_term_delete_delta->set_next = ptr_parent;
        mapping_table_.install_node(pid_parent, ptr_parent, (Node *) index_term_delete_delta);
    }

    bool BwTree::cleanup() {
        return true;
    }

    }  // End index namespace
}  // End peloton namespace
