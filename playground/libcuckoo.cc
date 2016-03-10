#include <stdio.h>
#include <string>
#include <iostream>
#include <atomic>

#include "../src/cuckoohash_map.hh"
#include "../src/city_hasher.hh"

struct Node {
  int payload;
  Node * next;
  Node(int p) {
    payload = p;
    next = nullptr;
  }
};

int main() {
    cuckoohash_map<int, std::atomic<Node*>*, DefaultHasher<int> > Table;
    for (int i = 0; i < 100; i++) {
      std::atomic<Node*>* node = new std::atomic<Node*>();
      node->store(new Node(i), std::memory_order_relaxed);
      Table.insert(i, node);
    }

    for (int i = 0; i < 101; i++) {
        std::atomic<Node*>* out;

        if (Table.find(i, out)) {
            std::cout << i << "  " << out->load(std::memory_order_acquire)->payload << std::endl;
        } else {
            std::cout << i << "  NOT FOUND" << std::endl;
        }
    }
}
~