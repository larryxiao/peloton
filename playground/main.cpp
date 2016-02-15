/*
 * example of CAS delta updates
 * 
 * spawn threads to prepend to the list
 * check the number of nodes are correct
 * 
 * time ./a.out
 * 2000000
 * ./a.out  1.78s user 0.05s system 705% cpu 0.258 total
 */

#include <cstddef>
#include <iostream>
#include <thread>
#include <memory>
#include <atomic>
#include "DeltaChain.cpp"

using namespace std;

#define NUM_THREADS 20

typedef DeltaChain<int> Delta;

std::atomic<Delta *> head;

// worker thread attempts to prepend a node into delta chain
void worker(int id)
{
    for (int i = 0; i < 100000; ++i)
    {
        unique_ptr<int> p(new int(id * 1000 + i));
        Delta * new_node = new Delta(p.release());
        new_node->next = head.load(std::memory_order_relaxed);
        while(!std::atomic_compare_exchange_weak_explicit(
            &head,
            &new_node->next,
            new_node,
            std::memory_order_release,
            std::memory_order_relaxed)
           );
    }
}

int main(){
    // initialize the linkedlist
    head = NULL;

    // create many threads and use the linked list to prepend and delete at the same time
    thread threads[NUM_THREADS];
    int rc;
    int i;
    for(i=0; i < NUM_THREADS; i++ ){
        threads[i] = thread(worker, i);
    }
    for(i=0; i < NUM_THREADS; i++ ){
        threads[i].join();
    }
    // see if number of nodes created is correct
    int count = 0;
    Delta * itr = head.load(std::memory_order_relaxed);
    while (itr != NULL) {
        count++;
        itr = itr->next;
    }
    cout<<count;
    return 0;
}

// http://www.cplusplus.com/reference/thread/thread/
// http://en.cppreference.com/w/cpp/atomic/atomic_compare_exchange