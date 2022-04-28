
#ifndef __RXINDEXINTERCEPTORS_H__
#define __RXINDEXINTERCEPTORS_H__

#define REDISMODULE_EXPERIMENTAL_API
#include "redismodule.h"
#include "/usr/include/arm-linux-gnueabihf/bits/types/siginfo_t.h"
#include <sched.h>
#include <signal.h>

#include "adlist.h"
#include "dict.h"
#include "server.h"
#include <ctype.h>
#include <locale.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "simpleQueue.h"

void installIndexerInterceptors();
void uninstallIndexerInterceptors();
void freeIndexingRequest(sds *kfv);

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

    sds index_address;
    int index_port;

    sds database_id;
    long long start_batch_ms;
} indexerThread;
#endif