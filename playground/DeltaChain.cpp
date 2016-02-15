/*
 * linked list for delta updates
 * 
 * (head) latest update -> older updates
 * when CAS fail
 * 
 * overhead of class, function invocation
 */

#include <cstddef>

template <typename T>
struct DeltaChain
{
   DeltaChain* next;
   T* delta;
   DeltaChain(T* delta) {
       next = NULL;
       delta = delta;
   }
   ~DeltaChain() {
       delete delta;
       delta = NULL;
       next = NULL;
   }
   // update
   DeltaChain * prepend(DeltaChain* node);
};