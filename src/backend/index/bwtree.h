//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// BWTree.h
//
// Identification: src/backend/index/BWTree.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>
#include <memory>
#include <vector>
#include <bits/atomic_base.h>

//Null page id
#define NULL_PID 0

namespace peloton {
namespace index {

// Look up the stx btree interface for background.
// peloton/third_party/stx/btree.h
template <typename KeyType, typename ValueType, class KeyComparator>
class BWTree {

public:
// typedefs of all the template parameters for visibility
//	typedef KeyType KeyType;
//
//	typedef ValueType ValueType;
//
//	typedef KeyComparator KeyComparator;

	//key of mapping table
	typedef unsigned short pid_t;

	//Mapping table type
	typedef std::unordered_map<pid_t, DeltaChain*> MappingTableType;

private:

	MappingTableType mapping_table_;

	//pid generator for nodes
	std::atomic_ushort pid_gen_;

	//root pid
	pid_t root_;

	//comparator, assuming the default comparator is lt
	KeyComparator less_comparator_;

	//Logical pointer to head leaf page
	pid_t head_leaf_page_;

	//Logical pointer to the tail leaf page
	pid_t tail_leaf_page_;


	struct Node{

		//level of this node
		unsigned short level;

		//node's key vector (all nodes have keys)
		std::vector<KeyType> keys;

		//TODO: parameters to be configured
		unsigned short node_size;
		unsigned short underflow_thresh;
		unsigned short overflow_thresh;

		inline Node(const unsigned short l)
				: level(l)
		{}

		// check if this node is a leaf
		inline bool isLeaf(){
			return (level == 0);
		}

		//overflow checker
		inline bool isoverflow() {
			return (keys.size() > overflow_thresh);
		}

		//underflow checker
		inline bool isunderflow() {
			return (keys.size() < underflow_thresh);
		}

	};

	//a generic delta chain entry
	struct DeltaChainType {
		//if this node is a delta record or bwtree node
		bool is_delta_record;

		//next node in the chain
		DeltaChainType* next = nullptr;

		inline DeltaChainType(bool is_delta_record)
				:is_delta_record(is_delta_record)
		{}
	};

	//incremental delta records in the delta chain
	struct DeltaRecord : public DeltaChainType {
		KeyType key;

		ValueType value;

		//true, if insert record; false, if delete record
		bool is_insert_record;

		inline DeltaRecord(bool is_insert, const KeyType key, ValueType value = nullptr)
			: DeltaChainType(true), is_insert_record(is_insert), key(key), value(value)
		{}
	};

	//wrapper for children vector for simplifying mapping table dereference
	struct ChildrenVector {
		std::vector<pid_t> children;
		MappingTableType mapping_table;


		inline ChildrenVector(MappingTableType& mapping_table) : mapping_table(mapping_table)
		{}

		inline void push_back(const DeltaChain* chain){
			children.push_back(chain->pid);
		}

		//overload [] operator to directly return mapping table pointer
		inline DeltaChain*& operator[](int i){
			return mapping_table[children[i]];
		}
	};

	struct InnerNode : public Node, public DeltaChainType {

		//wrapper for the children pid vector
		ChildrenVector children;

		MappingTableType mapping_table;

		//not a delta record, set delta chain type as false
		inline InnerNode(const unsigned short l, MappingTableType& mapping_table)
				: Node(l), DeltaChainType(false), mapping_table(mapping_table),
					children(mapping_table)
		{}

	};

	struct LeafNode : public Node, public DeltaChainType {

		//logical pointer to previous leaf node
		pid_t prevleaf;

		//logical pointer to next leaf
		pid_t nextleaf;

		//bwtree mapping table
		MappingTableType mapping_table;

		//node's key's values vector (only leaves have values)
		std::vector<ValueType> values;

		//leaf nodes are always level 0
		inline LeafNode(MappingTableType& mapping_table)
				: Node(0), DeltaChainType(false), prevleaf(NULL_PID),
					nextleaf(NULL_PID), mapping_table(mapping_table)
		{}

	};

	//Lock free delta chain that wraps each bwtree node
	struct DeltaChain
	{
		//head of the chain
		DeltaChainType* head;

		//BWtree Node reference at the end of the chain
		Node* tree_node;

		//page id
		pid_t pid;

		//size of the delta chain
		int len;

		inline DeltaChain(DeltaChainType *head, Node *node, const pid_t pid) :
				head(head), tree_node(node), pid(pid), len(1)
		{}
	};

	//allocte an inner node
	inline pid_t allocate_inner_node(const unsigned short level) {
		pid_t pid = static_cast<pid_t>(pid_gen_++);
		InnerNode *node = new InnerNode(level, mapping_table_);
		DeltaChain *chain = new DeltaChain(node, node, pid);
		mapping_table_[pid] = chain;
		return pid;
	}

	//allocate a leaf node and assign a delta chain
	inline pid_t allocate_leaf_node() {
		pid_t pid = static_cast<pid_t >(pid_gen_++);
		LeafNode *node = new LeafNode(mapping_table_);
		DeltaChain *chain = new DeltaChain(node, node, pid);
		mapping_table_[pid] = chain;
		return pid;
	}

	//deletes each element of the inner node chain and the chain itself
	inline void delete_inner_node(const pid_t pid) {
		DeltaChain *chain = mapping_table_[pid];
		auto temp = chain->head;
		while(temp != nullptr) {
			//save reference to the node to be deleted
			auto delete_node = temp;
			temp = temp->next;
			delete delete_node;
		}
		delete chain;
	}

	//deletes each element of the leaf node chain and the chain itself
	inline void delete_leaf_node(const pid_t pid) {
		DeltaChain *chain = mapping_table_[pid];
		auto temp = chain->head;
		while(temp != nullptr) {
			//save reference for node to be deleted
			auto delete_node = temp;
			temp = temp->next;
			delete delete_node;
		}
		delete chain;
	}


public:

	//by default, start the pid generator at 1, 0 is NULL page
	inline BWTree() : pid_gen_(NULL_PID+1)
	{}

	class LeafIterator {
	private:
		//logical pointer to the current node we are at
		pid_t currnode;

		//mapping table for logical ptr conversion
		MappingTableType mapping_table;

		//index of currnode's key vector we are at
		unsigned int currslot;

	public:
		inline LeafIterator() : currnode(nullptr), currslot(0)
		{}

		inline LeafIterator(pid_t node, unsigned int slot,
												MappingTableType& mapping_table)
				: currnode(node), currslot(slot),
					mapping_table(mapping_table)
		{}

		//constructor used for end() to assign to last slot
		inline LeafIterator(pid_t node, MappingTableType& mapping_table) :
				currnode(node), mapping_table(mapping_table)
		{
			//set slot as last position
			LeafNode *leafnode =
					reinterpret_cast<LeafNode*>(mapping_table[node]->tree_node);
			currslot = leafnode->keys.size();
		}

		inline ValueType& operator *() {
			//get physical pointer of the tree node and dereference
			LeafNode *node =
					reinterpret_cast<LeafNode*>(mapping_table[currnode]->tree_node);
			return node->values[currslot];
		}

		inline const KeyType & key() const {
			//get physical pointer of the tree node and dereference
			LeafNode *node =
					reinterpret_cast<LeafNode*>(mapping_table[currnode]->tree_node);
			return node->keys[currslot];
		}

	};

	inline LeafIterator begin() {
		return LeafIterator(head_leaf_page_, 0, mapping_table_);
	}

	inline LeafIterator end() {
		return LeafIterator(tail_leaf_page_, mapping_table_);
	}
		
	// Compares two keys and returns true if a <= b
	inline bool key_compare_lte(const KeyType &a, const KeyType &b) {
		return !less_comparator_(b,a);
	}

	// Compares two keys and returns true if a < b
	inline bool key_compare_lt(const KeyType &a, const KeyType &b) {
		return less_comparator_(a,b);
	}

	//Available modes: Greater than equal to, Greater tham
	enum node_search_mode {
		GTE, GT
	};

	// Performs a binary search on a tree node to find the position of
	// the key nearest to the search key, depending on the mode
	inline int node_key_search(const DeltaChain& chain,  const KeyType& key,
														 const node_search_mode& mode);

};

}  // End index namespace
}  // End peloton namespace
