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
#include <atomic>
#include <bits/atomic_base.h>

//Null page id
#define NULL_PID 0

namespace peloton {
namespace index {

// Look up the stx btree interface for background.
// peloton/third_party/stx/btree.h
template <typename KeyType, typename ValueType, class KeyComparator>
class BWTree {

private:
	enum NodeType : std::int8_t {
		leaf,
		inner,
		deltaInsert,
		deltaDelete,
		deltaSplitInner,
		deltaSplitLeaf,
		deltaSplit,
		separator,
	};

	struct Node{

	protected:
		//type of this node
		NodeType type;

	public:
		//ptr to the next node in delta chain
		Node* next;

		//used to track length of the chain till this node
		int chain_length;

		inline void set_next(Node *next_node) {
			next = next_node;
			chain_length = next_node->chain_length + 1;
		}

	};

	// TODO: performance issues?
	struct LockFreeTable {
		// NOTE: STL containers can support single writer and concurrent readers
		std::unordered_map<pid_t, std::atomic<Node*>> table;

		// used to manage concurrent inserts on different keys
		std::atomic<bool> flag;

		// flag is intially available
		inline LockFreeTable() : flag(true)
		{}

		// lookup and return physical pointer corresponding to pid
		inline Node* get_phy_ptr(const pid_t& pid) {
			// return the node pointer
			return table[pid].load(std::memory_order_relaxed);
		}

		// Used for first time insertion of this pid into the map
		inline void insert_new_pid(const pid_t& pid, Node* node) {
			bool expected = true;
			while(!(std::atomic_compare_exchange_weak_explicit(
					&flag, &expected, false, std::memory_order_release,
					std::memory_order_relaxed)));

			std::atomic<Node *> atomic_ptr;

			// only thread here, relaxed access
			atomic_ptr.store(node, std::memory_order_relaxed);

			// insert into the map
			table[pid] = node;

			// set the flag as true
			flag.store(true, std::memory_order_release);
			return;
		}

		//tries to sinstall the updated phy ptr
		inline bool install_node(const pid_t& pid, Node* expected, Node* update) {

			// atomically load the current value of node ptr
			std::atomic<Node *> new_value;

			// store the update value
			new_value.store(update, std::memory_order_relaxed);

			// try to atomically update the new node
			if(std::atomic_compare_exchange_weak_explicit(
					&table[pid], &expected, new_value,
					std::memory_order_relaxed, std::memory_order_release
			)) return true;

			// CAS failed, return
			return false;
		}
	};

	// key of mapping table
	typedef unsigned short pid_t;

	// lock free mapping table
	LockFreeTable mapping_table_;

	// pid generator for nodes
	std::atomic_ushort pid_gen_;

	// logical pointer to root
	pid_t root_;

	// comparator, assuming the default comparator is lt
	KeyComparator less_comparator_;

	// Logical pointer to head leaf page
	pid_t head_leaf_ptr_;

	// Logical pointer to the tail leaf page
	pid_t tail_leaf_ptr_;


	struct LeafNode : public Node {
		// node's key vector (all nodes have keys)
		std::vector<KeyType> keys;

		// node's key's values vector (only leaves have values)
		std::vector<ValueType> values;

		// logical pointer to previous leaf node
		pid_t prevleaf;

		//logical pointer to next leaf
		pid_t nextleaf;

		//leaf nodes are always level 0
		inline LeafNode(const pid_t prevleaf, const pid_t nextleaf)
		{
			// doubly linked list of leaves
			this->prevleaf  = prevleaf;
			this->nextleaf = nextleaf;

			type = NodeType::leaf;

			//intialize chain length
			this->chain_length = 1;

			// leaf node is always at the end of a delta chain
			next = nullptr;
		}

	};

	struct InnerNode : public Node {
		// node's key vector
		std::vector<KeyType> keys;

		// node's key's children vector
		std::vector<pid_t> children;

		inline InnerNode() {
			type = NodeType::inner;

			// inner node is always at the end of a delta chain
			next = nullptr;

			// intialize chain length
			this->chain_length = 1;
		}
	};

	// delta record should track its original node
	struct DeltaRecord : public Node {
		Node *origin;

	};

	// delta insert record
	struct DeltaInsert : public DeltaRecord {
		// insert key
		KeyType key;
		//insert value
		ValueType value;

		inline DeltaInsert(const KeyType &key, const ValueType &value, Node *origin) {
			this->key = key;
			this->value = value;
			this->origin = origin;
			type = NodeType::deltaInsert;
		}
	};

	struct DeltaDelete : public DeltaRecord {
		//delete key
		KeyType key;

		inline DeltaDelete(const KeyType &key, Node *origin) {
			this->key = key;
			this->origin = origin;
			type = NodeType::deltaDelete;
		}
	};

	struct DeltaSplit : public DeltaRecord {
		// split key for this record
		KeyType splitKey;

		// logical pointer to the new child record
		pid_t new_child;

		inline DeltaSplit(const KeyType &key, const pid_t new_child, Node *origin) {
			splitKey = key;
			// set the new child for split
			this->new_child = new_child;
			type = NodeType::deltaSplit;
			// set the origin node
			this->origin = origin;
		}
	};

	struct Separator : public DeltaRecord {
		// low and high to specify key range
		KeyType low;
		KeyType high;

		// child logical pointer
		pid_t child;

		// shortcut pointer to the new node
		pid_t new_node;

		inline Separator(const KeyType& low, const KeyType& high,
										 const pid_t child, const pid_t new_node,
										 Node *origin) {
			//low and high key ranges
			this->low = low;
			this->high = high;

			this->child = child;
			this->new_node = new_node;

			//pointer to the original node
			this->origin = origin;

			type = NodeType::separator;

		}
	};

	//used for range scans
	class RangeVector {
		//logical pointer to the current node
		pid_t currnode;

		//cursor position within currnode
		int currpos;

		//stores the records from the range scan
		std::vector<std::pair<KeyType,ValueType>> range_result;

		inline RangeVector
				(const pid_t currnode, int currpos) :
				currnode(currnode), currpos(currpos)
		{}
	};

	// Compares two keys and returns true if a <= b
	inline bool key_compare_lte(const KeyType &a, const KeyType &b) {
		return !less_comparator_(b,a);
	}

	// Compares two keys and returns true if a < b
	inline bool key_compare_lt(const KeyType &a, const KeyType &b) {
		return less_comparator_(a,b);
	}

	// Compares two keys and returns true if a = b
	inline bool key_compare_eq(const KeyType &a, const KeyType &b){
		return !less_comparator_(a,b) && !less_comparator_(b,a);
	}



public:

	//by default, start the pid generator at 1, 0 is NULL page
	inline BWTree() {
		pid_gen_ = NULL_PID+1;
		root_ = static_cast<pid_t>(pid_gen_++);

		//insert the chain into the mapping table
		mapping_table_.insert_new_pid(root_, new LeafNode(NULL_PID, NULL_PID));

		//update the leaf pointers
		head_leaf_ptr_ = root_;
		tail_leaf_ptr_ = root_;
	}

	//Available modes: Greater than equal to, Greater tham
	enum node_search_mode {
		GTE, GT
	};

	// Performs a binary search on a tree node to find the position of
	// the key nearest to the search key, depending on the mode
	inline int node_key_search(const Node*& node,  const KeyType& key,
														 const node_search_mode& mode);

};

}  // End index namespace
}  // End peloton namespace
