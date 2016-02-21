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

	template <typename KeyType, typename ValueType, class KeyComparator>
	void BWTree::setSibling(Node* node,Node* sibling){
		while(node->next!= nullptr)
		{
			node=node->next;
		}
		node->sidelink = sibling;
	}

	template <typename KeyType, typename ValueType, class KeyComparator>
	std::tuple<std::vector<KeyType>,std::vector<ValueType>> BWTree::getCurrentCenterKey(Node* headNodeP){
		Node *copyHeadNodeP = headNodeP;
		while(headNodeP->next!= nullptr){
			headNodeP=headNodeP->next;
		}
		//TODO:yet to handle for duplicate keys case
		std::vector<KeyType> overallKeys = headNodeP->keys;
//		std::vector<KeyType> insertedKeys;
		std::vector<KeyType> deletedKeys;

		//handling deltaInsert, deltaDelete, removeNode
		while(copyHeadNodeP->next!= nullptr){
			switch (copyHeadNodeP->get_type()){
				case deltaInsert:
					overallKeys.push_back(copyHeadNodeP->key);
					break;
				case deltaDelete:
					deletedKeys.push_back(copyHeadNodeP->key);
					break;
				default:
					break;
			}
			copyHeadNodeP=copyHeadNodeP->next;
		}

		for(auto const& elem: deletedKeys){
			auto it = find(overallKeys.begin(), overallKeys.end(),elem);
			if(it != overallKeys.end()){
				overallKeys.erase(it);
			}
		}

		std::sort(overallKeys.begin(), overallKeys.end(), less_comparator_);


	};

	template <typename KeyType, typename ValueType, class KeyComparator>
	bool BWTree::checkIfRemoveDelta(Node* head){
		while(head->next!= nullptr)
		{
			switch(head->get_type())
			{
				case removeNode:
					return true;
				default:
					break;
			}
			head=head->next;
		}
		return false;
	}

	template <typename KeyType, typename ValueType, class KeyComparator>
	bool BWTree::splitPage(pid_t pPID,pid_t pParentPID){

		pid_t qPID;Node* splitNode;

		Node *headNodeP = mapping_table_.get_phy_ptr(pPID); //get the head node of the deltachain
		Node *headNodeParentP = mapping_table_.get_phy_ptr(pParentPID);

		if(checkIfRemoveDelta(headNodeP) || checkIfRemoveDelta(headNodeParentP))
			return false;

//		std::tie(prev, next, hadInfinityElement) = getConsolidatedInnerData(startNode, needSplitPage, nodes);

		KeyType Kp,Kq;
//		= getCurrentCenterKey(headNodeP);
//		KeyType Kq = getHighKey(headNodeParentP, Kp); //TODO

		if(headNodeP->is_leaf())
		{
			Node* newLeafNode = new LeafNode((TreeNode*)headNodeP->sidelink); // P->R was there before
			newLeafNode->set_next(nullptr);
			qPID = static_cast<pid_t>(pid_gen_++);
			mapping_table_.insert_new_pid(qPID, newLeafNode);

			splitNode = new DeltaSplitLeaf(Kp, qPID, qPID->record_count);
		}
		else
		{
			Node* newInnerNode = new InnerNode((TreeNode*)headNodeP->sidelink);
			newInnerNode->set_next(nullptr);
			qPID = static_cast<pid_t>(pid_gen_++);
			mapping_table_.insert_new_pid(qPID, newInnerNode);

			splitNode = new DeltaSplitInner(Kp, qPID, qPID->record_count);
		}

		splitNode->set_next(headNodeP);
		mapping_table_.install_node(pPID, headNodeP, splitNode);

		//need to handle any specific delta record nodes?1

		//what if P disppears or the parent disappears, or R disappears
		setSibling(headNodeP,nullptr);

		//CAS update with number of nodes(after split) on P //TODO

		Node* sepDel = new IndexDelta(Kp,Kq,qPID); //what if the range doesn't match
		sepDel->set_next(headNodeParentP);
		mapping_table_.install_node(pParentPID, headNodeParentP, sepDel);

		return true;
	}
}  // End index namespace
}  // End peloton namespace
