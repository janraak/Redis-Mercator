
#ifndef __RXINDEXINTERCEPTORS_H__
#define __RXINDEXINTERCEPTORS_H__
#define WRAPPER
#include "simpleQueue.hpp"

#ifdef __cplusplus
extern "C"
{
#endif

// #define REDISMODULE_EXPERIMENTAL_API
// #include "redismodule.h"



void installIndexerInterceptors();
void uninstallIndexerInterceptors();
void freeIndexingRequest(void *kfv);

#ifdef __cplusplus
}
#endif

typedef struct indexerThread
{
    SimpleQueue *key_indexing_request_queue;
    SimpleQueue *key_indexing_respone_queue;
    SimpleQueue *index_update_request_queue;
    SimpleQueue *index_update_respone_queue;
    SimpleQueue *index_rxRuleApply_request_queue;
    SimpleQueue *index_rxRuleApply_respone_queue;

    int field_index_tally;
    int rxbegin_tally;
    int rxadd_tally;
    int rxcommit_tally;
    int set_tally;
    int hset_tally;

    long long start_batch_ms;
    void *executor;

} indexerThread;
#endif