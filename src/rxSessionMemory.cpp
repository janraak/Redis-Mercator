
// #include "version.h"

// #include "tls.hpp"
// #include "rxSessionMemory.hpp"
#define NR_OF_SESSION_MEMORY_SLOTS_PERBLOCK 1024

struct _SessionMemory_Slot{
    const char *tag;
    void *alloc;
};

typedef struct _SessionMemory_
{
    struct _SessionMemory_ *prev;
    int free_slots;
    int claimed_slots;
    int released_slots;
    struct _SessionMemory_Slot slots[NR_OF_SESSION_MEMORY_SLOTS_PERBLOCK];
} _SessionMemory_;


struct _Session_Allocation{
    struct _SessionMemory_ *here;
    struct _SessionMemory_Slot *slot;
    void *ptr;
};

// void * allocate_SessionMemory_(void *){
//     struct _SessionMemory_ *control = (struct _SessionMemory_ *)rxMemAlloc(sizeof(_SessionMemory_));
//     control->free_slots = NR_OF_SESSION_MEMORY_SLOTS_PERBLOCK;
//     control->claimed_slots = 0;
//     control->released_slots = 0;
//     return control;
// }

// void *rxMemAllocSession(size_t size, const char *tag)
// {
//     return rxMemAlloc(size);
//     // auto *master = tls_get<_SessionMemory_ *>((const char*)"rxMemAlloc", allocate_SessionMemory_, NULL);
//     // auto *tail = master;
//     // auto *before_tail = master;
//     // while (tail&&(!tail->free_slots && !tail->released_slots))
//     // {
//     //     before_tail = tail;
//     //     tail = tail->prev;
//     // }
//     // if(!tail){
//     //     tail = (struct _SessionMemory_ *)allocate_SessionMemory_(NULL);
//     //     before_tail->prev = tail;
//     // }
//     // size_t slot = 0;
//     // if (tail->released_slots){
//     //     for (slot = 0; slot < NR_OF_SESSION_MEMORY_SLOTS_PERBLOCK; ++slot){
//     //         if(!tail->slots[slot].tag && !tail->slots[slot].alloc){
//     //             tail->released_slots--;
//     //             break;
//     //         }
//     //     }
//     // }else{
//     //     slot = tail->claimed_slots;
//     //     tail->free_slots--;
//     //     tail->claimed_slots++;
//     // }
//     // _Session_Allocation *allocation = (_Session_Allocation *)rxMemAlloc(size + sizeof(_Session_Allocation));
//     // tail->slots[slot].tag = tag;
//     // tail->slots[slot].alloc = allocation;

//     // allocation->here = tail;
//     // allocation->slot = &tail->slots[slot];
//     // allocation->ptr = allocation + sizeof(_Session_Allocation);

//     // return allocation->ptr;
// }

// void rxMemFreeSession(void *ptr){
//     // // Check if a session block is freed.
//     // auto *allocation = (_Session_Allocation *)(ptr - sizeof(_Session_Allocation));
//     // if(ptr == allocation->ptr){
//     //     allocation->here->released_slots++;
//     //     allocation->slot->tag = NULL;
//     //     allocation->slot->alloc = NULL;
//     //     rxMemFreeX(allocation->slot->alloc);
//     // }
//     // else
//     // {
//         rxMemFreeX(ptr);
//     // }
// }
