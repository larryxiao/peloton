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

	//key of mapping table
	typedef unsigned short pid_t;

	//Mapping table type
	typedef std::unordered_map<pid_t, DeltaChain*> MappingTableType;

private:

	MappingTableType mapping_table_;

	//pid generator for nodes
	std::atomic_ushort pid_gen_;

	//Logical pointer wrapper for pid
	struct LogicalPtr{
		pid_t pid;
		MappingTableType mapping_table;

		inline LogicalPtr(const pid_t& pid, MappingTableType& mapping_table)
				: pid(pid), mapping_table(mapping_table)
		{}

		//used for Null logical ptr intialization
		inline LogicalPtr(const pid_t& pid) : pid(pid)
		{}

		//null logical ptr checker
		inline bool is_null(){
			return (pid == NULL_PID);
		}

		//assigns a null ptr
		inline void set_pid(const pid_t& new_pid){
			pid = new_pid;
		}

		//overload * operator to return physical pointer
		inline DeltaChain*& operator *() {
			return mapping_table[pid];
		}
	};

	struct LogicalPtrFactory {
		MappingTableType mapping_table;

		inline LogicalPtrFactory(MappingTableType& mapping_table) :
				MappingTableType(mapping_table)
		{}

		inline LogicalPtr get_logical_ptr(const pid_t& pid) {
			return LogicalPtr(pid, mapping_table);
		}

	};

	LogicalPtrFactory lpfactory_;


	//logical pointer to root
	LogicalPtr root_;

	//comparator, assuming the default comparator is lt
	KeyComparator less_comparator_;

	//Logical pointer to head leaf page
	LogicalPtr head_leaf_ptr_;

	//Logical pointer to the tail leaf page
	LogicalPtr tail_leaf_ptr_;

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

	struct InnerNode : public Node, public DeltaChainType {

		//wrapper for the children pid vector
		std::vector<LogicalPtr> children;

		//not a delta record, set delta chain type as false
		inline InnerNode(const unsigned short l)
				: Node(l), DeltaChainType(false)
		{}

	};

	struct LeafNode : public Node, public DeltaChainType {

		//logical pointer to previous leaf node
		LogicalPtr prevleaf;

		//logical pointer to next leaf
		LogicalPtr nextleaf;

		//node's key's values vector (only leaves have values)
		std::vector<ValueType> values;

		//leaf nodes are always level 0
		inline LeafNode() : Node(0), DeltaChainType(false),
												prevleaf(LogicalPtr(NULL_PID)),
												nextleaf(LogicalPtr(NULL_PID))
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
	inline LogicalPtr allocate_leaf_node() {
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
