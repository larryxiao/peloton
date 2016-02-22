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
#include "index_key.h"

//Null page id
#define NULL_PID 0

namespace peloton {
namespace index {

// Look up the stx btree interface for background.
// peloton/third_party/stx/btree.h
template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class BWTree {

private:
	// logical pointer type
	typedef unsigned int pid_t;

	//Delta and standard node types
	enum NodeType : std::int8_t {
		leaf,
		inner,
		indexDelta,
		deleteIndex,
		deltaSplitInner,
		mergeInner,
		deltaInsert,
		deltaDelete,
		deltaSplitLeaf,
		mergeLeaf,
		removeNode,
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

		//used for split/merge case detection
		int record_count;

		int level;

		virtual void set_next(Node *) {}

		inline bool is_leaf() {
			return (level == 0);
		}

		inline NodeType get_type() {
			return type;
		}

	};

	// TODO: performance issues?
	struct LockFreeTable {
		// static table block
		std::vector<std::atomic<Node*>> table;

		inline LockFreeTable() : table(4194304)
		{}

		// lookup and return physical pointer corresponding to pid
		inline Node* get_phy_ptr(const pid_t& pid) {
			// return the node pointer
			return table[pid].load(std::memory_order_acquire);
		}

		// Used for first time insertion of this pid into the map
		inline void insert_new_pid(const pid_t& pid, Node* node) {
			// insert into the map
            // std::atomic<T> isn't copy-constructible, nor copy-assignable.
			// only thread here, relaxed access
			table[pid].store(node, std::memory_order_relaxed);

			return;
		}

		//tries to install the updated phy ptr
		inline bool install_node(const pid_t& pid, Node* expected, Node* update) {

            // try to atomically update the new node
            return std::atomic_compare_exchange_weak_explicit(
                    &(table[pid]), &expected, update,
                    std::memory_order_release, std::memory_order_relaxed);
		}
	};

	// lock free mapping table
	LockFreeTable mapping_table_;

	// pid generator for nodes
	std::atomic_ushort pid_gen_;

	// logical pointer to root
	pid_t root_;

	// comparator, assuming the default comparator is lt
	KeyComparator less_comparator_;

	// equality checker
	KeyEqualityChecker eq_checker_;

	// Logical pointer to head leaf page
	pid_t head_leaf_ptr_;

	// Logical pointer to the tail leaf page
	pid_t tail_leaf_ptr_;

	// leaf consolidation threshold
	int consolidate_threshold_leaf_;

	// inner node consolidation threshold
	int consolidate_threshold_inner_;

	// threshold for triggering split
	int split_threshold_;

	// threshold for triggering merge
	int merge_threshold_;

	// A tree node has a vector of key templates
	// and corresponding value templates
	// and a side link
	template <typename K, typename V>
	struct TreeNode : public Node {
		// node's key vector
		std::vector<std::pair<K, V>> key_values;

		// self pid
		pid_t pid;

		// logical pointer to next leaf on the right
		pid_t sidelink;

	};

	// leaf node of the bw-tree, has KeyType and corresp.
	// ValueType of record
	struct LeafNode : public TreeNode<KeyType, ValueType> {
		//leaf nodes are always level 0
		inline LeafNode(const pid_t self, const pid_t nextleaf)
		{
			// doubly linked list of leaves
			this->sidelink = nextleaf;

			this->pid = self;

			this->type = NodeType::leaf;

			//initialize chain length
			this->chain_length = 1;

			// no records yet
			this->record_count = 0;

			// leaf node, level 0
			this->level = 0;

			// leaf node is always at the end of a delta chain
			this->next = nullptr;
		}

		void set_next(Node *next_node) {
			this->next = next_node;
			this->chain_length = next_node->chain_length + 1;
		}

	};

	// inner node of the bwtree, stores pairs of keys and the
	// child on their left pointer
	struct InnerNode : public TreeNode<KeyType, pid_t> {
		// pointer to the (n+1)th child, for n keys
		pid_t last_child;

		inline InnerNode(const pid_t self, int level,
										 const pid_t adj_node) {
			this->type = NodeType::inner;

			this->pid = self;

			// inner node is always at the end of a delta chain
			this->next = nullptr;

			// set the sidelink
			this->sidelink = adj_node;

			// intialize chain length
			this->chain_length = 1;

			// intialize the level
			this->level = level;

			// no records intially
			this->record_count = 0;
		}

		void set_next(Node *next_node) {
			this->next = next_node;
			this->chain_length = next_node->chain_length + 1;
		}
	};

	// IndexDelta record of the delta chain
	struct IndexDelta : public Node {
		// low and high to specify key range
		KeyType low;
		KeyType high;

		// child logical pointer
		pid_t child;

		// shortcut pointer to the new node
		pid_t new_node;

		inline IndexDelta(const KeyType& low, const KeyType& high,
										 const pid_t new_node) {

			// set the node's type
			this->type = NodeType::indexDelta;

			//low and high key ranges
			this->low = low;
			this->high = high;

			// add logical pointer to the provided new node
			this->new_node = new_node;
	}

		void set_next(Node *next_node) {
			// next node in the delta chain
			this->next = next_node;

			// chain has grown
			this->chain_length = next_node->chain_length + 1;

			// a new record is being added, increment
			this->record_count = next_node->record_count + 1;

			// level remains the same
			this->level = next_node->level;
		}

	};

	// DeleteIndex delta record for node merging
	struct DeleteIndex : public Node {
		// low and high to specify key range
		KeyType low;
		KeyType high;

		// shortcut pointer to the merged node
		pid_t merge_node;

		inline DeleteIndex(const KeyType& low, const KeyType& high,
											 const pid_t merge_node) {

			// set the node's type
			this->type = NodeType::deleteIndex;

			//low and high key ranges
			this->low = low;
			this->high = high;

			// update the node for merging
			this->merge_node = merge_node;
		}

		void set_next(Node *next_node) {
			// next node in the delta chain
			this->next = next_node;

			// chain has grown
			this->chain_length = next_node->chain_length + 1;

			// a record is being removed, decrement
			this->record_count = next_node->record_count - 1;

			// level remains the same
			this->level = next_node->level;
		}

	};

	// split record for inner node
	struct DeltaSplitInner : public Node {

	public:
		// split key for this node
		KeyType splitKey;

		// shortcut pointer to the new node
		pid_t new_node;


		inline DeltaSplitInner(const KeyType& splitKey, const pid_t new_node,
													 const int new_record_count) {
			// set the node's type
			this->type = NodeType::deltaSplitInner;

			// set the split key
			this->splitKey = splitKey;

			// set the new node
			this->new_node = new_node;

			// get the new record count from SplitPage
			this->record_count = new_record_count;
		}

		// update the next pointer and bookeeping
		void set_next(Node *next_node) {
			// next node in the delta chain
			this->next = next_node;

			// chain has grown
			this->chain_length = next_node->chain_length + 1;

			// level remains the same
			this->level = next_node->level;
		}

	};

	struct MergeInner : public Node {
		// split key for this node
		KeyType splitKey;

		// physical pointer to the node being delted
		InnerNode* deleting_node;

		inline MergeInner(const KeyType& splitKey, InnerNode* deleting_node,
											const int new_record_count) {
			// set the type
			this->type = NodeType::mergeInner;

			// set the split key
			this->splitKey = splitKey;

			// set the deleting node
			this->deleting_node = deleting_node;

			// update the record count from MergePage
			this->record_count = new_record_count;
		}

		// update the next pointer and bookeeping
		void set_next(Node *next_node) {
			// next node in the delta chain
			this->next = next_node;

			// chain has grown
			this->chain_length = next_node->chain_length + 1;

			// level remains the same
			this->level = next_node->level;
		}

	};

	// delta insert record
	struct DeltaInsert : public Node {
		// insert key
		KeyType key;

		//insert value
		ValueType value;

		inline DeltaInsert(const KeyType &key, const ValueType& value) {
			// set the type
			this->type = NodeType::deltaInsert;

			// set the key, value and base node
			this->key = key;
			this->value = value;
		}

		void set_next(Node *next_node) {
			this->next = next_node;

			// chain has grown
			this->chain_length = next_node->chain_length + 1;

			// a new record is being added, increment
			this->record_count = next_node->record_count + 1;

			// level remains the same
			this->level = next_node->level;
		}
	};

	struct DeltaDelete : public Node {
		//delete key
		KeyType key;

		inline DeltaDelete(const KeyType &key) {
			// set the base node
			this->type = NodeType::deltaDelete;

			// set the key and base node
			this->key = key;
		}

		// sets the next node in the delta chain
		void set_next(Node *next_node) {
			this->next = next_node;

			// chain has grown
			this->chain_length = next_node->chain_length + 1;

			// a record is being removed, decrement
			this->record_count = next_node->record_count - 1;

			// level remains the same
			this->level = next_node->level;
		}

	};

	struct DeltaSplitLeaf : public Node {
		// split key for this record
		KeyType splitKey;

		// logical pointer to the new child record
		pid_t new_child;

		inline DeltaSplitLeaf(const KeyType &key, const pid_t new_child,
													const int new_record_count) {
			// set the type
			this->type = NodeType::deltaSplitLeaf;

			// set the split key
			this->splitKey = key;

			// set the new child for split
			this->new_child = new_child;

			// set the updated record count from SplitPage
			this->record_count = new_record_count;
		}

		// sets the next node in the delta chain
		void set_next(Node *next_node) {
			this->next = next_node;

			// chain has grown
			this->chain_length = next_node->chain_length + 1;

			// level remains the same
			this->level = next_node->level;
		}
	};

	struct MergeLeaf : public Node {
		// split key for this node
		KeyType splitKey;

		// physical pointer to the node being delted
		LeafNode* deleting_node;

		inline MergeLeaf(const KeyType& splitKey, LeafNode* deleting_node,
											const int new_record_count) {
			// set the type
			this->type = NodeType::mergeLeaf;

			// set the split key
			this->splitKey = splitKey;

			// set the deleting node
			this->deleting_node = deleting_node;

			// update the record count from MergePage
			this->record_count = new_record_count;
		}

		// update the next pointer and bookeeping
		void set_next(Node *next_node) {
			// next node in the delta chain
			this->next = next_node;

			// chain has grown
			this->chain_length = next_node->chain_length + 1;

			// level remains the same
			this->level = next_node->level;
		}
	};

	// Remove node delta record for any node
	struct RemoveNode : public Node {

		inline RemoveNode() {
			// set the type
			this->type = NodeType::removeNode;
		}

		void set_next(Node *next_node) {
			// set the next node
			this->next = next_node;

			// the delta chain has grown
			this->chain_length = next_node->chain_length + 1;

			// record count remains the same
			this->record_count = next_node->record_count;

			// level remains the same
			this->level = next_node->level;
		}
	};

	// stores the result of the operation
	struct TreeOpResult {
		// status of the operation
		bool status;

		// value returned, if any
		ValueType value;

		// validity of the value
		bool is_valid_value;

		bool needs_split;

		bool needs_merge;

        inline TreeOpResult(__attribute__((__unused__)) 
                const bool status = false) : 
            is_valid_value(false),
            needs_split(false),
            needs_merge(false)
		{}

        inline TreeOpResult(const ValueType value) : status(true), value(value),
        needs_split(false),
        needs_merge(false)
        {}
	};

	inline TreeOpResult get_update_success_result() {
		return TreeOpResult(true);
	}

	inline TreeOpResult get_failed_result() {
		return TreeOpResult();
	}

	inline TreeOpResult make_search_result(ValueType value) {
		return TreeOpResult(value);
	}

	inline TreeOpResult make_search_fail_result() {
		return TreeOpResult(true);
	}

	// TODO: used for range scans
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

	//Available modes: Greater than equal to, Greater tham
	enum NodeSearchMode {
		GTE, GT
	};


	// Performs a binary search on a tree node to find the position of
	// the key nearest to the search key, depending on the mode.
	// Returns the position of the child to the left of nearest greater key
	// and -1 for failed search
	template <typename K, typename V>
	inline unsigned long node_key_search(const TreeNode<K,V> *node,  const KeyType& key);

	enum OperationType : int8_t{
		insert_op,
		delete_op,
		search_op,
	};

	union LeafOperation {
		// Search the leaf page PID for the given key
		TreeOpResult (BWTree::*search_leaf_page)(const pid_t pid, const KeyType& key);
		TreeOpResult (BWTree::*update_leaf_delta_chain)(const pid_t pid, const KeyType& key,
                const ValueType& value,
                const OperationType op_type);
	};


	// Does a tree operation on inner node (head of it's delta chain)
    // with leaf node operation passed as a function pointer
    TreeOpResult do_tree_operation(Node* head, const KeyType& key,
            const ValueType& value,
            const LeafOperation *leaf_operation,
            const OperationType op_type);

    // Wrapper for the above function that looks up pid from mapping table
    TreeOpResult do_tree_operation(const pid_t node_pid, const KeyType& key,
            const ValueType& value,
            const LeafOperation *leaf_operation,
            const OperationType op_type);


	// Search leaf page and return the found value, if exists. Try SMOs /
	// Consolidation, if necessary
	TreeOpResult search_leaf_page_phy(Node *head, const KeyType& key);

	// Wrapper for the above function
	TreeOpResult search_leaf_page(const pid_t pid, const KeyType& key);

	// Update the leaf delta chain during insert/delta. Try SMOs /
	// Consolidation, if necessary
    TreeOpResult update_leaf_delta_chain(const pid_t pid, const KeyType& key,
            const ValueType& value,
            const OperationType op_type);


	// consolidation skeleton, starting from the given physical pointer
	void consolidate(const Node * node);

	// Merge page operation for node underflows
	bool merge_page(pid_t pid_l, pid_t pid_r, pid_t pid_parent);

public:

	//BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>();
	//by default, start the pid generator at 1, 0 is NULL page
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker> (
            IndexMetadata *metadata)
        : less_comparator_(metadata),
          eq_checker_(metadata)
    {
        pid_gen_ = NULL_PID+1;
        root_ = static_cast<pid_t>(pid_gen_++);

        //insert the chain into the mapping table
        mapping_table_.insert_new_pid(root_, new LeafNode(root_, NULL_PID));

        //update the leaf pointers
        head_leaf_ptr_ = root_;
        tail_leaf_ptr_ = root_;

        // TODO: decide values
        consolidate_threshold_inner_ = 5;

        consolidate_threshold_leaf_ = 8;

        merge_threshold_ = 3;

        split_threshold_ = 100;
    }

	//bool Insert(__attribute__((unused)) KeyType key,
							//__attribute__((unused)) ValueType value);
	bool Insert(const KeyType &key, const ValueType& value);
	bool Search(const KeyType& key, ValueType **value);
	bool Delete(const KeyType &key);
	bool Cleanup();
};

}  // End index namespace
}  // End peloton namespace
