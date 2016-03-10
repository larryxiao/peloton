// g++ -g -std=c++11 -pthread epoch_manager.cpp

#include <vector>
#include <atomic>
#include <cstdint>
#include <thread>
#include <cassert>
#include <time.h>
#include <unistd.h>
#include <stdio.h>

typedef uint64_t epoch;
#define TABLESIZE 10000

// internal compiler error
// std::atomic<epoch_entry*> epoch_table[TABLESIZE] = {}; // GCC 4.8 bug

// epoch_table = new std::atomic<epoch_entry*>[1000](0);
// ../../src/backend/index/bwtree.h:270:21: error: parenthesized initializer in array new [-Werror=permissive]

class BWTree
{
  struct Node
  {
    int payload;
    struct Node *next;
    Node(int p, Node* n) {
      payload = p;
      next = n;
    }
  };

  class epoch_manager
  {
    struct delete_entry
    {
      Node* payload;
      struct delete_entry *next;
      delete_entry(Node *p, delete_entry* ent) {
        payload = p;
        next = ent;
      }
    };
    struct epoch_entry
    {
      uint32_t refcnt;
      epoch epoch_;
      std::atomic<delete_entry*> list; // start nodes of deltachain to GC
      // struct epoch_entry *next;
      epoch_entry(uint32_t r, epoch e, delete_entry* ent) {
        refcnt = r;
        epoch_ = e;
        list = ent;
      }
    };
    std::atomic<epoch_entry*> epoch_table[TABLESIZE] = {};
  private:
    epoch_entry* nullptrlvalue = nullptr;
    epoch *epoch_;
    // ticker *tk;
    struct timespec t = {0, 40*1000*1000}; // 40 ms

    void deleteChain(Node *node) {
      if (node == nullptr)
        return;
      Node * next = node->next;
      while (next != nullptr) {
        printf("delete node %d\n", node->payload);
        delete node;
        node = next;
        next = next->next;
      }
      printf("delete node %d\n", node->payload);
      delete node;
    }
    void GC(epoch_entry *entry) {
      printf("GC\n");
      delete_entry * node = entry->list;
      if (node == nullptr)
        return;
      delete_entry * next = node->next;
      while (next != nullptr) {
        printf("deletechain\n");
        deleteChain(node->payload);
        delete node;
        node = next;
        next = next->next;
      }
      deleteChain(node->payload);
      delete node;
      delete(entry);
    };
    /**
     * ticker increments epoch
     * and check epoch update, and run GC on previous epoch
     */
    void ticker() {
      for(;;) {
        nanosleep(&t, nullptr);
        epoch previous = *epoch_;
        (*epoch_)++;
        epoch_entry *entry = epoch_table[previous % TABLESIZE].load(std::memory_order_relaxed);
        if (entry != nullptr && entry->refcnt == 0) { 
          assert(entry->epoch_ == previous);
          bool ret = std::atomic_compare_exchange_weak_explicit(
                &epoch_table[previous % TABLESIZE], &entry, nullptrlvalue,
                std::memory_order_release, std::memory_order_relaxed);
          // someone else succeeds
          if (!ret)
            return;
          printf("start gc by ticker\n");
          std::thread thd(&epoch_manager::GC, this, entry);
          thd.detach();
        }
      }
    };
  public:
    epoch_manager() {
      epoch_ = new epoch(0UL);
      std::thread thd(&epoch_manager::ticker, this);
      thd.detach();
    };
    ~epoch_manager() {
      delete(epoch_);
    };
    // called by normal threads
    epoch enter() {
      printf("enter\n");
      epoch current = *epoch_;
      epoch_entry *entry = epoch_table[current % TABLESIZE].load(std::memory_order_relaxed);
      if (entry != nullptr) {
        assert(entry->epoch_ == current); // make sure no collision
        entry->refcnt++;
      } else {
        entry = new epoch_entry(1, current, nullptr);
        bool ret = std::atomic_compare_exchange_weak_explicit(
              &epoch_table[current % TABLESIZE], &nullptrlvalue, entry,
              std::memory_order_release, std::memory_order_relaxed);
        // someone else succeeds
        if (!ret) { 
          delete(entry);
          epoch_entry *entry = epoch_table[current % TABLESIZE].load(std::memory_order_relaxed);
          assert(entry->epoch_ == current); // make sure no collision
          entry->refcnt++;
        }
      }
      return current;
    };
    void exit(epoch e) {
      printf("exit\n");
      // when refcnt drop to zero, and epoch has progressed, spawn thread to do GC
      epoch current = *epoch_;
      epoch_entry *entry = epoch_table[e % TABLESIZE].load(std::memory_order_relaxed);
      assert(entry->epoch_ == e); // make sure no collision
      // TODO can have leaked entries because no thread join before epoch moves on
      printf("refcnt %d epoch %lu, current epoch %lu\n", entry->refcnt-1, entry->epoch_, current);
      if (--entry->refcnt == 0 && current > entry->epoch_) {
        bool ret = std::atomic_compare_exchange_weak_explicit(
              &epoch_table[e % TABLESIZE], &entry, nullptrlvalue,
              std::memory_order_release, std::memory_order_relaxed);
        // someone else succeeds
        if (!ret)
          return;
        printf("start gc\n");
        std::thread thd(&epoch_manager::GC, this, entry);
        thd.detach();
      }
    };
    // called by consolidate
    void addGCEntry(epoch e, Node *node) {
      printf("addGCEntry\n");
      epoch_entry *entry = epoch_table[e % TABLESIZE].load(std::memory_order_relaxed);
      assert(entry->epoch_ == e); // make sure no collision
      delete_entry *head, *nnew;
      nnew = new delete_entry(node, nullptr);
      // retry until succeed
      do {
        head = entry->list.load(std::memory_order_relaxed);
        nnew->next = head;
      } while (!std::atomic_compare_exchange_weak_explicit(
              &entry->list, &head, nnew,
              std::memory_order_release, std::memory_order_relaxed)
      );
    };
  };
public:
  BWTree();
  ~BWTree();

  /* data */
};

int main(int argc, char const *argv[])
{
  /* code */
  return 0;
}