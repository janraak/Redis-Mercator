
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

typedef void indexerCallBack(void *priv);

typedef void *cmd_interceptions;

typedef void rxRedisCommandProc(client *c);

struct redisCommandInterceptRule
{
    char *name;
    rxRedisCommandProc *proc;
    int id; /* Command ID. This is a progressive ID starting from 0 that
               is assigned at runtime, and is used in order to check
               ACLs. A connection is able to execute a given command if
               the user associated to the connection has this command
               bit set in the bitmap of allowed commands. */
};

void *getIndexInterceptRules();
void *getTriggerInterceptRules();

void installIndexerInterceptors(indexerCallBack *pre_op, indexerCallBack *post_op);
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