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

	struct DeltaChain {
		pid_t pid;
	};

	MappingTableType mapping_table;

	//pid generator for nodes
	pid_t pid_gen;

	//root pid
	pid_t root;

	struct node{

		//page id of this node
		std::atomic_ushort pid;

		//level of this node
		unsigned short level;

		//num of slots used
		unsigned short slotused;

		//node's key vector (all nodes have keys)
		std::vector<KeyType> keys;

		//TODO: parameters to be configured
		unsigned short node_size;
		unsigned short underflow_thresh;
		unsigned short overflow_thresh;

		inline node(const unsigned short l, const pid_t pid, const unsigned short slotused = 0)
				: level(l), pid(pid), slotused(slotused)
		{}

		// check if this node is a leaf
		inline bool isLeaf(){
			return (level == 0);
		}

		//overflow checker
		inline bool isoverflow() {
			return (slotused > overflow_thresh);
		}

		//underflow checker
		inline bool isunderflow() {
			return (slotused < underflow_thresh);
		}

	};

	struct children_vector {
		std::vector<pid_t> children;
		MappingTableType mapping_table;

		inline children_vector(const MappingTableType &mapping_table) : mapping_table(mapping_table)
		{}

		inline void push_back(const DeltaChain* chain){
			children.push_back(chain->pid);
		}
	};

	struct inner_node : public node {

		//vector of this node's children pids
		std::vector<pid_t> children;

		inline inner_node(const unsigned short l, pid_t pid) : node(l, pid)
		{}

	};

	struct leaf_node : public node {

		//pointer to previous leaf node
		leaf_node *prevleaf;

		//pointer to next leaf
		leaf_node *nextleaf;

		//node's key's values vector (only leaves have values)
		std::vector<ValueType> values;

		//leaf nodes are always level 0
		inline leaf_node(const pid_t pid) : node(0, pid), prevleaf(nullptr), nextleaf(nullptr)
		{}

	};

public:

	//by default, start the pid generator at 0
	inline BWTree() : pid_gen(0){
	}

};

}  // End index namespace
}  // End peloton namespace
