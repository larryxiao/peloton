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
	int BWTree::node_key_search(const pid_t node_pid, const KeyType &key, const node_search_mode &mode) {

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

	Node* getHeadNodePointerForPID(const pid_t pPID) {
		//check if null?
		return table[pPID];
	}


	template <typename KeyType, typename ValueType, class KeyComparator>
	bool BWTree::splitPage(pid_t pPID,pid_t pParentPID){

		pid_t qPID;

		auto headNodeP = getHeadNodePointerForPID(pPID); //get the head node of the deltachain
		auto headNodeParentP = getHeadNodePointerForPID(pParentPID);

		KeyType Kp = getSeperatorKeyForSplit(pPID); //TODO
		KeyType Kq = getHighKeyForP(pParentPID, Kp); //TODO

		if(pPID.isLeaf()) //TODO
		{
			qPID=allocateLeaf(); //TODO
			//insert all the key-values in Q for > Kp , delta chain needs to be traversed
		}
		else
		{
			qPID=allocateInner(); //TODO
			//insert all the keys in Q for <= Kp , delta chain needs to be traversed
		}

		auto splitDel = createSplitDeltaNodePointer(qPID, Kp); //sibling pointer to Q //TODO

		if(!mapping[pPID].compare_exchange_weak(headNodeP, splitNode)) //TODO - mapping table structure, CAS
		{
			//CAS failed
			//TODO: Free allocated nodes?
			return false;
		}

		//need to handle any specific delta record nodes?1

//		if(!(std::atomic_compare_exchange_weak_explicit(
//				headNodeP, *headNodeP, splitDel, false, std::memory_order_release,
//				std::memory_order_relaxed)))
//			return false; //no need to clear the new node?

		//what if P disppears or the parent disappears, or R disappears
		setSibling(pPID, nullptr); //set the sibling pointer to null

		//CAS update with number of nodes(after split) on P
		auto sepDel = createSeperatorDeltaPointer(pParentPID, Kp, Kq, qPID); //it sets pointer to Q if range matches //TODO

		if(!mapping[pParentPID].compare_exchange_weak(headNodeParentP,sepDel))
		{
			//CAS failed
			//TODO: Free allocated nodes?
			return false;
		}

		return true;
	}
}  // End index namespace
}  // End peloton namespace
