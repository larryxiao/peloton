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

/**
 * thread to increment epoch
 */
class ticker
{
private:
  epoch *epoch_;
  struct timespec t = {0, 40*1000*1000}; // 40 ms
public:
  ticker(epoch *epoch_) {
    this->epoch_ = epoch_;
    std::thread thd(&ticker::tickerloop, this);
    thd.detach();
  };
  void tickerloop() {
    for(;;) {
      nanosleep(&t, nullptr);
      (*epoch_)++;
      // printf("ticks\n");
    }
  };
  ~ticker() {
  };
};

struct Node
{
  int payload;
  struct Node *next;
  Node(int p, Node* n) {
    payload = p;
    next = n;
  }
};

/**
 * Epoch Table
 * coarse granularity: Thread -> Epoch
 * TODO better granularity: Thread -> (Epoch, PID)
 * 0. handout pointer to epoch_entry, to save lookup, use linked list
 * need to handle remove epoch_entry from list, only one thread will delete
 * one epoch_entry, but many threads can trigger delete at the same time
 * 1. use fixed size array, handout epoch. just free the spot. 
 * possibility of collision, with long running thread
 * 2. use concurrent hashmap
 * 
 * Delete Entry: head of delta chain to be deleted
 * lock free linked list
 */
#define TABLESIZE 10000
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
  std::atomic<epoch_entry*> epoch_table[TABLESIZE];
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

int main(int argc, char const *argv[])
{
  // test ticker, 25 ticks per seconds
  if (false) {
    epoch *epoch_ = new epoch(0UL);
    ticker tk(epoch_);
    for (int i = 0; i < 10; ++i)
    {
      sleep(1);
      printf("%lu\n", *epoch_);
    }
    delete(epoch_);
  }
  printf("[TEST] epoch_manager\n");
  epoch_manager em;
  Node *n1 = new Node(1, nullptr);
  Node *ck11 = n1->next = new Node(11, nullptr);
  Node *ck12 = n1->next->next = new Node(12, nullptr);
  Node *ck13 = n1->next->next->next = nullptr;
  Node *n2 = new Node(2, nullptr);
  Node *ck21 = n2->next = new Node(21, nullptr);
  Node *ck22 = n2->next->next = nullptr;
  Node *n3 = new Node(3, nullptr);
  Node *ck31 = n3->next = new Node(31, nullptr);
  Node *ck32 = n3->next->next = new Node(32, nullptr);
  Node *ck33 = n3->next->next->next = nullptr;
  Node *n4 = new Node(4, nullptr);
  Node *ck41 = n4->next = new Node(41, nullptr);
  Node *ck42 = n4->next->next = nullptr;
  printf("[TEST] normal gc, 1 entry\n");
  epoch e1 = em.enter();
  em.addGCEntry(e1, n1);
  epoch e2 = em.enter();
  printf("e1 %lu e2 %lu\n", e1, e2);
  em.exit(e1);
  sleep(1);
  em.exit(e2);
  // manually check
  printf("[TEST] normal gc, 2 entry\n");
  epoch e3 = em.enter();
  epoch e4 = em.enter();
  printf("e3 %lu e4 %lu\n", e3, e4);
  em.addGCEntry(e3, n2);
  em.addGCEntry(e4, n3);
  em.exit(e3);
  sleep(1);
  em.exit(e4);
  // manually check
  printf("[TEST] leaked gc\n");
  epoch e5 = em.enter();
  printf("e5 %lu\n", e5);
  em.addGCEntry(e5, n4);
  em.exit(e5);
  sleep(1);
  // manually check
  return 0;
}