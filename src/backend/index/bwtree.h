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
		separator,
	};

	struct Node{

	protected:
		//type of this node
		NodeType type;

	public:
		//logical ptr to the next node
		pid_t next;
	};

	struct LockFreeTable {
		//NOTE: STL containers can support sing writer and concurrent readers
		std::unordered_map<pid_t, std::atomic<Node*>> table;

		//used to manage conccurent inserts on different keys
		std::atomic<bool> flag;

		//flag is intially available
		inline LockFreeTable() : flag(true)
		{}

		//lookup and return physical pointer corresponding to pid
		inline Node* get_phy_ptr(const pid_t& pid) {
			//return the node pointer
			return table[pid].load(std::memory_order_relaxed);
		}

		//Used for first time insertion of this pid into the map
		inline void insert_new_pid(const pid_t& pid, Node* node) {
			bool expected = true;
			while(true) {
				if(std::atomic_compare_exchange_weak_explicit(
						&flag, &expected, false, std::memory_order_release,
						std::memory_order_relaxed)){
					std::atomic<Node *> atomic_ptr;

					//only thread here, relaxed access
					atomic_ptr.store(node, std::memory_order_relaxed);

					//insert into the map
					table[pid] = node;

					//set the flag as true
					flag.store(true, std::memory_order_release);
					return;
				}
			}
		}

		//update the pid's pointer mapping
		inline void update_pid_val(const pid_t& pid, Node* update) {
			//atomically load the current value of node ptr
			std::atomic<Node *> value;
			std::atomic<Node *> new_value;

			//store the update value
			new_value.store(update, std::memory_order_relaxed);

			while(true) {
				//store the expected value and create a pointer
				Node *expected = table[pid].load(std::memory_order_relaxed);

				//return after successful update of the map with the new value
				if(std::atomic_compare_exchange_weak_explicit(
						&table[pid], &expected, new_value,
						std::memory_order_relaxed, std::memory_order_release
				)) {
					return;
				}
			}
		}
	};

	//key of mapping table
	typedef unsigned short pid_t;

	//lock free mapping table
	LockFreeTable mapping_table_;

	//pid generator for nodes
	std::atomic_ushort pid_gen_;

	//logical pointer to root
	pid_t root_;

	//comparator, assuming the default comparator is lt
	KeyComparator less_comparator_;

	//Logical pointer to head leaf page
	pid_t head_leaf_ptr_;

	//Logical pointer to the tail leaf page
	pid_t tail_leaf_ptr_;


	struct LeafNode : Node {
		//node's key vector (all nodes have keys)
		std::vector<KeyType> keys;

		//node's key's values vector (only leaves have values)
		std::vector<ValueType> values;

		//logical pointer to previous leaf node
		pid_t prevleaf;

		//logical pointer to next leaf
		pid_t nextleaf;

		//leaf nodes are always level 0
		static inline pid_t create(const pid_t& prev, const pid_t& next)
		{
			LeafNode *node = new LeafNode();
			node->prevleaf  = prev;
			node->nextleaf = next;
			node->type = NodeType::leaf;

		}

	};

	struct Inner

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

	struct InnerNode : public Node, public DeltaChainType {

		//wrapper for the children pid vector
		std::vector<LogicalPtr> children;

		//not a delta record, set delta chain type as false
		inline InnerNode(const unsigned short l)
				: Node(l), DeltaChainType(false)
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
		LogicalPtr ptr;

		//size of the delta chain
		int len;

		inline DeltaChain(DeltaChainType *head, Node *node, const LogicalPtr& ptr) :
				head(head), tree_node(node), ptr(ptr), len(1)
		{}

		inline DeltaChainIterator begin() const {
			return DeltaChainIterator(head);
		}

		inline DeltaChainIterator end() const {
			if(tree_node->isLeaf())
				return DeltaChainIterator(reinterpret_cast<LeafNode*>(tree_node);
			return DeltaChainIterator(reinterpret_cast<InnerNode*>(tree_node));
		}
	};

	class DeltaChainIterator {
	private:
		DeltaChainType *curr;

	public:
		inline DeltaChainIterator(DeltaChainType *start) : curr(start)
		{}

		//reference as a delta chain record
		inline DeltaRecord*& operator *() {
			return reinterpret_cast<DeltaRecord*>(curr);
		}

		//iterator increment
		inline DeltaChainIterator& operator ++() {
			curr = curr->next;
			return *this;
		}
	};

	//allocate an inner node, return its logical ptr
	inline LogicalPtr allocate_inner_node(const unsigned short level) {
		pid_t pid = static_cast<pid_t>(pid_gen_++);

		//construct inner node
		InnerNode *node = new InnerNode(level);

		//create a logical pointer to pid
		auto ptr = lpfactory_.get_logical_ptr(pid);

		//create the delta chain pointer
		DeltaChain *chain =
				new DeltaChain(node, node, ptr);

		//link the logical pointer to the physical pointer
		*ptr = chain;

		return ptr;
	}

	//allocate a leaf node, return its logical ptr
	inline pid_t allocate_leaf_node() {
		pid_t pid = static_cast<pid_t >(pid_gen_++);

		//construct the leaf node
		LeafNode *node = new LeafNode();

		//create a logical pointer
		auto ptr = lpfactory_.get_logical_ptr(pid);

		//construct the delta chain
		DeltaChain *chain = new DeltaChain(node, node, ptr);


		//map the logical pinter to its physical pointer
		*ptr = chain;

		//return the logical pointer
		return ptr;
	}

	//deletes each element of the inner node chain and the chain itself
	inline void delete_inner_node(const LogicalPtr& pid) {
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

	class LeafIterator {
	private:
		//logical pointer to the current node we are at
		LogicalPtr currnode;

		//index of currnode's key vector we are at
		unsigned int currslot;

	public:
		inline LeafIterator() : currnode(LogicalPtr(NULL_PID)), currslot(0)
		{}

		inline LeafIterator(LogicalPtr node, unsigned int slot)
				: currnode(node), currslot(slot)
		{}

		//constructor used for end() to assign to last slot
		inline LeafIterator(LogicalPtr node) :
				currnode(node)
		{
			//set slot as last position
			LeafNode *leafnode =
					reinterpret_cast<LeafNode*>((*node)->tree_node);
			currslot = leafnode->keys.size();
		}

		inline ValueType& operator *() {
			// get physical pointer of the tree node from the delta
			// chain and dereference
			LeafNode *node =
					reinterpret_cast<LeafNode*>((*currnode)->tree_node);
			return node->values[currslot];
		}

		inline const KeyType & key() const {
			// get physical pointer of the tree node from the delta
			// chain and dereference
			LeafNode *node =
					reinterpret_cast<LeafNode*>((*currnode)->tree_node);
			return node->keys[currslot];
		}

	};

	inline LeafIterator begin() {
		return LeafIterator(head_leaf_ptr_, 0);
	}

	inline LeafIterator end() {
		return LeafIterator(tail_leaf_ptr_);
	}

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
	inline BWTree() : pid_gen_(NULL_PID+1),
										lpfactory_(LogicalPtrFactory(mapping_table_)),
										root_(LogicalPtr(NULL_PID)),
										head_leaf_ptr_(root_),
										tail_leaf_ptr_(root_)
	{}

	//Available modes: Greater than equal to, Greater tham
	enum node_search_mode {
		GTE, GT
	};

	// Performs a binary search on a tree node to find the position of
	// the key nearest to the search key, depending on the mode
	inline int node_key_search(const DeltaChain*& chain,  const KeyType& key,
														 const node_search_mode& mode);

	LeafIterator tree_search(const KeyType& key, const node_search_mode& mode);

};

}  // End index namespace
}  // End peloton namespace
