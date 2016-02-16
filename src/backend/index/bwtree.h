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

namespace peloton {
namespace index {

// Look up the stx btree interface for background.
// peloton/third_party/stx/btree.h
template <typename KeyType, typename ValueType, class KeyComparator>
class BWTree {

public:
	//key of mapping table
	typedef unsigned short pid_t;

	//Mapping table type
	typedef std::unordered_map<pid_t, DeltaChain*> MappingTableType;

private:

	MappingTableType mapping_table;

	//pid generator for nodes
	std::atomic_ushort pid_gen;

	//root pid
	pid_t root;

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

		//pointer to previous leaf node
		LeafNode *prevleaf;

		//pointer to next leaf
		LeafNode *nextleaf;

		//bwtree mapping table
		MappingTableType mapping_table;

		//node's key's values vector (only leaves have values)
		std::vector<ValueType> values;

		//leaf nodes are always level 0
		inline LeafNode(MappingTableType& mapping_table)
				: Node(0), DeltaChainType(false), prevleaf(nullptr),
					nextleaf(nullptr), mapping_table(mapping_table)
		{}

	};

	//Lock free delta chain that wraps each bwtree node
	struct DeltaChain
	{
		//head of the chain
		DeltaChainType* head;

		//page id
		pid_t pid;

		//size of the delta chain
		int len;

		inline DeltaChain(DeltaChainType *head, const pid_t pid) : head(head), pid(pid), len(1)
		{}
	};

	//allocte an inner node
	inline pid_t allocate_inner_node(const unsigned short level) {
		pid_t pid = static_cast<pid_t>(pid_gen++);
		InnerNode *node = new InnerNode(level, mapping_table);
		DeltaChain *chain = new DeltaChain(node, pid);
		mapping_table[pid] = chain;
		return pid;
	}

	//allocate a leaf node and assign a delta chain
	inline pid_t allocate_leaf_node() {
		pid_t pid = static_cast<pid_t >(pid_gen++);
		LeafNode *node = new LeafNode(mapping_table);
		DeltaChain *chain = new DeltaChain(node, pid);
		mapping_table[pid] = chain;
		return pid;
	}

	//deletes each element of the inner node chain and the chain itself
	inline void delete_inner_node(const pid_t pid) {
		DeltaChain *chain = mapping_table[pid];
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
		DeltaChain *chain = mapping_table[pid];
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

	//by default, start the pid generator at 0
	inline BWTree() : pid_gen(0)
	{}


	// Performs a binary search on a tree node to find the position of
	// the key greater than or equal to the search key
	inline int find_lower(const DeltaChain& chain,  const KeyType& key);

};

}  // End index namespace
}  // End peloton namespace
