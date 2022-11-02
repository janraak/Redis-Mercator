/* rxIndexer -- A module for Redis/Mercator by Rocksoft
 *
 * This module implements a full text index on string and hash keyes.
 *
 * -----------------------------------------------------------------------------
 *
 * Copyright (c) 2021, Jan Raak <jan dotraak dot nl at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#define REDISMODULE_EXPERIMENTAL_API
#include "rxIndexer.hpp"

#ifdef __cplusplus
extern "C"
{
#endif
#include "../../deps/hiredis/hiredis.h"
#include "../../src/adlist.h"
#include "../../src/dict.h"
#include "../../src/rax.h"

#include "rxSuiteHelpers.h"

    struct redisCommand *lookupCommand(sds name);

#ifdef __cplusplus
}
#endif

#include "indexIntercepts.h"

#include "client-pool.hpp"
#include "rule.hpp"
#include "sjiboleth.h"
#include <string.h>
#include "rxWaitIndexing-multiplexer.hpp"

#define stringKey sdsnew("S")
#define hashKey sdsnew("H")
#define streamKey sdsnew("X")
#define noFieldName sdsnew("*")

#define indexer_set_thread_title(name) pthread_setname_np(pthread_self(), name)
indexerThread data_info = {};
indexerThread index_info = {};

#define BEGIN_COMMAND_INTERCEPTOR(fn)       \
    static int fn(void *stash, redisNodeInfo *index_config, SilNikParowy_Kontekst *executor) \
    {                                       \
        int argc = *((int *)stash);                       \
        void **argv = (void **)(stash + sizeof(void *));  \
        rxStashCommand(index_info.index_update_request_queue, "MULTI", 0);

#define END_COMMAND_INTERCEPTOR(fn)                                \
    rxStashCommand(index_info.index_update_request_queue, "EXEC", 0); \
    return 0;                                                      \
    }

CSjiboleth *json_parser = newJsonEngine();
CSjiboleth *text_parser = newTextEngine();
static int must_stop = 0;

BEGIN_COMMAND_INTERCEPTOR(indexingHandlerDelCommand)
for (int k = 1; k < argc; k++)
{
    rxUNUSED(index_config);
    rxUNUSED(executor);
    sds key = (sds)rxGetContainedObject(argv[k]);
    rxStashCommand(index_info.index_update_request_queue, "RXBEGIN", 1, key);
    index_info.rxbegin_tally++;
    rxStashCommand(index_info.index_update_request_queue, "RXCOMMIT", 1, key);
    index_info.rxcommit_tally++;
}
END_COMMAND_INTERCEPTOR(indexingHandlerDelCommand)

BEGIN_COMMAND_INTERCEPTOR(indexingHandlerXDelCommand)
    rxUNUSED(index_config);
    rxUNUSED(executor);
    sds okey = (sds)rxGetContainedObject(argv[1]);
    for (int k = 2; k < argc; k++)
    {
        sds id = (sds)rxGetContainedObject(argv[k]);
        sds key = sdscatprintf(sdsempty(), "%s#%s", okey, id);
        rxStashCommand(index_info.index_update_request_queue, "RXBEGIN", 1, key);
        index_info.rxbegin_tally++;
        rxStashCommand(index_info.index_update_request_queue, "RXCOMMIT", 1, key);
        index_info.rxcommit_tally++;
    }
END_COMMAND_INTERCEPTOR(indexingHandlerXDelCommand)

BEGIN_COMMAND_INTERCEPTOR(indexingHandlerXaddCommand)
    rxUNUSED(executor);
    sds okey = (sds)rxGetContainedObject(argv[1]);

    auto *collector = raxNew();
    auto *index_node = RedisClientPool<redisContext>::Acquire(index_config->host_reference);
    executor->Memoize("@@TEXT_PARSER@@", (void *)text_parser);
    executor->Memoize("@@collector@@", (void *)collector);


    ParsedExpression *parsed_text;
    int key_type = rxOBJ_STREAM;


    int j = 2;
        sds currentArg = (sds)rxGetContainedObject(argv[j]);
        if (strcmp(currentArg, "NOMKSTREAM") == 0)
        {
            j++;
        }
        if ((strcmp(currentArg, "MAXLEN") == 0) || (strcmp(currentArg, "MINID") == 0))
        {
            j++;
            currentArg = (sds)rxGetContainedObject(argv[j]);
            if ((strcmp(currentArg, "=") == 0) || (strcmp(currentArg, "~") == 0))
            {
                j++;
                currentArg = (sds)rxGetContainedObject(argv[j]);
            }
            j++;
            currentArg = (sds)rxGetContainedObject(argv[j + 1]);
            if (strcmp(currentArg, "LIMIT") == 0)
            {
                j = j + 2;
            }
        }
            j++;
    while (j < argc)
    {
        sds currentField = (sds)rxGetContainedObject(argv[j]);
        sds currentValue = (sds)rxGetContainedObject(argv[j + 1]);

        executor->Memoize("@@field@@", currentField);
        auto *parser = currentValue[0] == '{' ? json_parser : text_parser;
        parsed_text = (ParsedExpression *)parseQ(parser, currentValue);
        auto *sub = parsed_text;
        while (sub)
        {
            executor->Execute(sub);
            sub = sub->Next();
        }
        delete parsed_text;
        executor->Forget("@@field@@");

        j = j + 2;
    }

    TextDialect::FlushIndexables(collector, okey, key_type, index_node);
    RedisClientPool<redisContext>::Release(index_node);
    raxFree(collector);
    executor->Forget("@@TEXT_PARSER@@");
    executor->Forget("@@collector@@");
    executor->Reset();

    END_COMMAND_INTERCEPTOR(indexingHandlerXaddCommand)

#ifdef __cplusplus
extern "C"
{
#endif
    struct client *createAOFClient(void);
    static int indexingHandler(int tally, void *stash, redisNodeInfo *index_config, SilNikParowy_Kontekst *executor);
    void freeCompletedRedisRequests();
    static void *execIndexerThread(void *ptr);
    static void *execUpdateRedisThread(void *ptr);

    int rxrule_timer_handler(struct aeEventLoop *eventLoop, long long int id, void *clientData);

    int indexerInfo(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
    int indexerControl(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
    int reindex(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
    int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
    int RedisModule_OnUnload(RedisModuleCtx *ctx);

#ifdef __cplusplus
}
#endif

int rxrule_helper_interval = 100;
long long rxrule_helper_cron = -1;
struct client *rxrule_fakeClient = NULL;
struct redisCommand *rule_apply_cmd;
void *rxruleApply = NULL;
/* This is the rule.apply timer handler.
 * An apply entry is scheduled after an rxcommit to
 * update all business rules. */
int rxrule_timer_handler(struct aeEventLoop *, long long int , void *)
{
    redisNodeInfo *index_config = rxIndexNode();

    if(must_stop == 1)
        return -1;

    void *index_request = index_info.index_rxRuleApply_request_queue->Dequeue();
    while (index_request != NULL)
    {
        ExecuteRedisCommand(index_info.index_rxRuleApply_respone_queue, index_request, index_config->host_reference);
        index_request = index_info.index_rxRuleApply_request_queue->Dequeue();
    }

    return rxrule_helper_interval;
}

static int indexingHandler(int tally, void *stash, redisNodeInfo *index_config, SilNikParowy_Kontekst *executor)
{
    int argc = *((int *)stash);
    void **argv = (void **)(stash + sizeof(void *));

    sds command = (sds)rxGetContainedObject(argv[0]);
    sds key = (sds)rxGetContainedObject(argv[1]);
    int len = strlen(command);
    if (stringmatchlen(command, len, "DEL", 3, 1) == 1)
        return indexingHandlerDelCommand(stash, index_config, executor);
    else if (stringmatchlen(command, len, "XDEL", 4, 1) == 1)
        return indexingHandlerXDelCommand(stash, index_config, executor);
    else if (stringmatchlen(command, len, "XADD", 4, 1) == 1)
        return indexingHandlerXaddCommand(stash, index_config, executor);

    auto *collector = raxNew();
    auto *index_node = RedisClientPool<redisContext>::Acquire(index_config->host_reference);
    executor->Memoize("@@TEXT_PARSER@@", (void *)text_parser);
    executor->Memoize("@@collector@@", (void *)collector);
    executor->Memoize("@@INDEX@@", (void *)index_node);

    ParsedExpression *parsed_text;
    int key_type;

    if (argc == 3)
    {
        sds f = sdsnew("*");
        sds v = (sds)rxGetContainedObject(argv[2]);
        executor->Memoize("@@field@@", f);
        key_type = rxOBJ_STRING;
        auto *parser = v[0] == '{' ? json_parser : text_parser;
        parsed_text = (ParsedExpression *)parseQ(parser, v);
        auto *sub = parsed_text;
        while (sub)
        {
            sub->Show(v);
            SilNikParowy::Execute(sub, executor);
            sub = sub->Next();
        }
        delete parsed_text;
        executor->Forget("@@field@@");
        sdsfree(f);
    }
    else
    {
        key_type = rxOBJ_HASH;
        for(int j = 2; j < argc;  j += 2)
        {
            sds f = (sds)rxGetContainedObject(argv[j]);
            sds v = (sds)rxGetContainedObject(argv[j + 1]);

            rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(),"Indexing Hash field %s with %s", f, v));

            executor->Memoize("@@field@@", f);
            auto *parser = v[0] == '{' ? json_parser : text_parser;
            parsed_text = (ParsedExpression *)parseQ(parser, v);
            auto *sub = parsed_text;
            while (sub)
            {
                SilNikParowy::Execute(sub, executor);
                sub = sub->Next();
            }
            delete parsed_text;
            executor->Forget("@@field@@");
        }
    }
    if(!index_node)
        index_node = RedisClientPool<redisContext>::Acquire(index_config->host_reference);
    if(raxSize(collector) > 0)
        TextDialect::FlushIndexables(collector, key, key_type, index_node);
    RedisClientPool<redisContext>::Release(index_node);
    raxFree(collector);
    collector = NULL;
    executor->Reset();

    rxStashCommand(index_info.index_update_request_queue, "RXTRIGGER", 1, key);
    return C_OK;
}

void freeCompletedRedisRequests()
{
    if(index_info.index_update_respone_queue == NULL)
        return;
    void *index_request = NULL;
    index_request = index_info.index_update_respone_queue->Dequeue();
    while (index_request != NULL)
    {
        GET_ARGUMENTS_FROM_STASH(index_request);
        if (argc > 1)
        {
            sds key = (sds)rxGetContainedObject(argv[1]);
            rxStashCommand(index_info.index_rxRuleApply_request_queue, "RULE.APPLY", 1, key);
        }

        FreeStash(index_request);
        index_request = index_info.index_update_respone_queue->Dequeue();
    }
    if (index_info.index_rxRuleApply_respone_queue != NULL)
    {
        index_request = index_info.index_rxRuleApply_respone_queue->Dequeue();
        while (index_request != NULL)
        {
            // FreeStash(index_request);
            index_request = index_info.index_rxRuleApply_respone_queue->Dequeue();
    }
    }
}

int indexerInfo(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc == -10)
        return RedisModule_WrongArity(ctx);
    // RedisModule_DictSet(Keyspace,argv[1],argv[2]);
    // /* We need to keep a reference to the value stored at the key, otherwise
    //  * it would be freed when this callback returns. */
    REDISMODULE_NOT_USED(argv);
    sds response = sdsempty();
    response = sdscatfmt(response, "Number of SET commands intercepts:         %i\n", index_info.set_tally);
    response = sdscatfmt(response, "Number of HSET commands intercepts:        %i\n", index_info.hset_tally);
    response = sdscatfmt(response, "Number of indexing queued requests:        %i\n", index_info.key_indexing_request_queue->QueueLength());
    response = sdscatfmt(response, "Number of queued redis requests:           %i\n", index_info.index_update_request_queue->QueueLength());
    response = sdscatfmt(response, "Number of RXBEGIN commands:                %i\n", index_info.rxbegin_tally);
    response = sdscatfmt(response, "Number of RXADD commands:                  %i\n", index_info.rxadd_tally);
    response = sdscatfmt(response, "Number of RXCOMMIT commands:               %i\n", index_info.rxcommit_tally);
    response = sdscatfmt(response, "Number of completed requests:              %i\n", index_info.key_indexing_respone_queue->QueueLength());
    //response = sdscatfmt(response, "Number of key_indexing_enqueues:           %i\n", index_info.key_indexing_respone_queue->enqueue_fifo_tally);
    // response = sdscatfmt(response, "Number of key_indexing_dequeues:           %i\n", index_info.key_indexing_respone_queue->dequeue_fifo_tally);
    response = sdscatfmt(response, "Number of completed Redis requests:        %i\n", index_info.index_update_request_queue->QueueLength());
    // response = sdscatfmt(response, "Number of redis_enqueues:                  %i\n", index_info.index_update_request_queue->enqueue_fifo_tally);
    // response = sdscatfmt(response, "Number of redis_dequeues:                  %i\n", index_info.index_update_request_queue->dequeue_fifo_tally);
    // response = sdscatfmt(response, "Number of REDIS commands:                  %i\n", dictSize(server.commands));
    // response = sdscatfmt(response, "Number of REDIS commands:                  %i, intercepts: %i\n", dictSize(server.commands), sizeof(interceptorCommandTable) / sizeof(struct redisCommand));
    response = sdscatfmt(response, "Number of Field index comm                 %i\n", index_info.field_index_tally);

    return RedisModule_ReplyWithSimpleString(ctx, response);
}

static char indexer_state = 0;
long long reindex_helper_cron = -1;
int reindex_cron_slot_time = 100;
int reindex_cron_interval_time = 5;
dictIterator *reindex_iterator = NULL;
long long reindex_total_no_of_keys = 0;
long long reindex_processed_no_of_keys = 0;
long long reindex_skipped_no_of_keys = 0;
long long reindex_yielded_no_of_keys = 0;
long long reindex_qthrottleded_no_of_keys = 0;
long long reindex_throttleded_no_mem = 0;
long long reindex_no_of_slots = 0;
long long reindex_starttime = 0;
long long reindex_latency = 0;

int rx_wait_indexing_complete(struct RedisModuleCtx *ctx)
{
    auto *multiplexer = new RxWaitIndexingMultiplexer(index_info.key_indexing_request_queue, &reindex_iterator);
    multiplexer->Start(ctx);
    return REDISMODULE_OK;
}

int indexerControl(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc == 2)
    {
        size_t arg_len;
        sds arg = sdsnew((char *)RedisModule_StringPtrLen(argv[1], &arg_len));
        sdstoupper(arg);
        if (sdscmp(arg, sdsnew("WAIT")) == 0)
        {
            return rx_wait_indexing_complete(ctx);
        }
        else if (sdscmp(arg, sdsnew("ON")) == 0)
        {
            if (indexer_state)
                return RedisModule_ReplyWithSimpleString(ctx, "Indexer already active.");
            installIndexerInterceptors();
            indexer_state = 1;
        }
        else if (sdscmp(arg, sdsnew("OFF")) == 0)
        {
            if (!indexer_state)
                return RedisModule_ReplyWithSimpleString(ctx, "Indexer already inactive.");
            uninstallIndexerInterceptors();
            indexer_state = 0;
        }
        else
        {
            return RedisModule_ReplyWithSimpleString(ctx, "Invalid argument, must be ON or OFF.");
        }
    }
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/* This is the timer handler that is called by the main event loop. We schedule
 * this timer to be called when the nearest of our module timers will expire. */
int reindex_cron(struct aeEventLoop *, long long, void *clientData)
{
    if (must_stop == 1)
        return -1;
    redisNodeInfo *index_config = rxIndexNode();

    auto *engine = (SilNikParowy_Kontekst *)clientData;

    if (!reindex_iterator)
    {
        return -1;
    }

    unsigned long long mem = mem_avail();
    if (mem < 256 * 256)
    {
        // Yield when slot time over
        reindex_throttleded_no_mem++;
        return reindex_cron_interval_time * 25;
    }

    reindex_no_of_slots++;
    long long start = ustime();
    dictEntry *de;
    while ((de = dictNext(reindex_iterator)) != NULL)
    {
        reindex_processed_no_of_keys++;
        sds key = (sds)dictGetKey(de);
        if (*key == '^')
        {
            reindex_skipped_no_of_keys++;
            continue;
        }
        auto *collector = raxNew();
        engine->Memoize("@@collector@@", (void *)collector);

        ParsedExpression *parsed_text;
        int key_type = rxGetObjectType(dictGetVal(de));

        switch (rxGetObjectType(dictGetVal(de)))
        {
        case rxOBJ_STRING:
        {
            sds value = (sds)rxGetContainedObject(dictGetVal(de));
            sds f = sdsnew("*");
            engine->Memoize("@@field@@", f);
            parsed_text = (ParsedExpression *)parseQ((value[0] == '{') ? json_parser : text_parser, value);
            auto *sub = parsed_text;
            while (sub)
            {
                engine->Execute(sub);
                sub = sub->Next();
            }
            delete parsed_text;
            engine->Forget("@@field@@");
            sdsfree(f);
        }
        break;
        case rxOBJ_LIST:
            serverLog(LL_NOTICE, "ReIndexing unexpected type");
            break;
        case rxOBJ_HASH:
        {
            struct rxHashTypeIterator *hi = rxHashTypeInitIterator(dictGetVal(de));
            int j = 0;
            while (rxHashTypeNext(hi) != C_ERR)
            {
                sds field = rxHashTypeCurrentObjectNewSds(hi, rxOBJ_HASH_KEY);
                sds field_value = rxHashTypeCurrentObjectNewSds(hi, rxOBJ_HASH_VALUE);
                engine->Memoize("@@field@@", field);
                parsed_text = (ParsedExpression *)parseQ((field_value[0] == '{') ? json_parser : text_parser, field_value);
                auto *sub = parsed_text;
                while (sub)
                {
                    engine->Execute(sub);
                    sub = sub->Next();
                }
                j += 2;
                delete parsed_text;
                engine->Forget("@@field@@");
            }
            rxHashTypeReleaseIterator(hi);
        }
        break;
        default:
            serverLog(LL_NOTICE, "ReIndexing unexpected type");
            break;
        }
        auto *index_node = RedisClientPool<redisContext>::Acquire(index_config->host_reference);
        TextDialect::FlushIndexables(collector, key, key_type, index_node);
        RedisClientPool<redisContext>::Release(index_node);
        engine->Reset();
        raxFree(collector);
        engine->Forget("@@collector@@");

        rxStashCommand(index_info.index_update_request_queue, "RXTRIGGER", 1, key);
        if (ustime() - start >= reindex_cron_slot_time * 1000)
        {
            // Yield when slot time over
            reindex_yielded_no_of_keys++;
            return reindex_cron_interval_time;
        }
        if (index_info.index_update_request_queue->QueueLength() > 100000)
        {
            // Yield when slot time over
            reindex_qthrottleded_no_of_keys++;
            return reindex_cron_interval_time * 25;
        }
    }
    dictReleaseIterator(reindex_iterator);
    reindex_iterator = NULL;
    reindex_latency = ustime() - reindex_starttime;
    delete engine;
    // Done!
    serverLog(LL_NOTICE, "ReIndexing completed");
    // if (original_indexer_state){
    //     installIndexerInterceptors();
    //     indexer_state = 1;
    //     serverLog(LL_NOTICE, "Indexing restored");
    // }

    return -1;
}

int reindex(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    rxUNUSED(ctx);
    sds response = sdsempty();
    redisNodeInfo *index_config = rxIndexNode();

    if (argc == 2)
    {
        size_t arg_len;
        sds arg = sdsnew((char *)RedisModule_StringPtrLen(argv[1], &arg_len));
        sdstoupper(arg);
        if (sdscmp(arg, sdsnew("STATUS")) == 0)
        {
            response = sdscatfmt(response, "Indexer is:                 %s\n", indexer_state ? "Active" : "Not Active");
            response = sdscatfmt(response, "ReIndexing is:              %s\n", reindex_iterator ? "Active" : "Not Active");
            response = sdscatfmt(response, "ReIndexing stats:\r");
            response = sdscatfmt(response, "Total no of keys:           %i\n", reindex_total_no_of_keys);
            response = sdscatfmt(response, "Total no of keys processed: %i\n", reindex_processed_no_of_keys);
            response = sdscatfmt(response, "Total no of keys skipped:   %i\n", reindex_skipped_no_of_keys);
            response = sdscatfmt(response, "\n");
            response = sdscatfmt(response, "Total reindexing time:      %ims\n", reindex_latency / 1000);
            response = sdscatfmt(response, "\n");
            response = sdscatfmt(response, "Total no of timer slots:    %i\n", reindex_no_of_slots);
            response = sdscatfmt(response, "Total no of timer expires:  %i\n", reindex_yielded_no_of_keys);
            response = sdscatfmt(response, "Total no of queue throttles:%i\n", reindex_qthrottleded_no_of_keys);
            response = sdscatfmt(response, "Total no of no memory throttles:%i\n", reindex_throttleded_no_mem);
            return RedisModule_ReplyWithStringBuffer(ctx, response, sdslen(response));
        }
    }

    REDISMODULE_NOT_USED(argv);
    if (!indexer_state)
        return RedisModule_ReplyWithSimpleString(ctx, "Indexer NOT active.");

    if (reindex_iterator)
        return RedisModule_ReplyWithSimpleString(ctx, "ReIndexing already started.");

    rxStashCommand(index_info.index_update_request_queue, "FLUSHDB", 0);

    uninstallIndexerInterceptors();
    indexer_state = 0;

    reindex_iterator = rxGetDatabaseIterator(0);
    reindex_total_no_of_keys = rxDatabaseSize(0);
    reindex_processed_no_of_keys = 0;
    reindex_skipped_no_of_keys = 0;
    reindex_yielded_no_of_keys = 0;
    reindex_qthrottleded_no_of_keys = 0;
    reindex_throttleded_no_mem = 0;
    reindex_starttime = ustime();
    reindex_no_of_slots = 0;

    auto *engine = new SilNikParowy_Kontekst(index_config, NULL);
    engine->Memoize("@@TEXT_PARSER@@", (void *)text_parser);

    reindex_helper_cron = rxCreateTimeEvent(1, (aeTimeProc *)reindex_cron, engine, NULL);
    reindex_cron(NULL, 0, engine);

    // serverLog(LL_NOTICE, "ReIndexing started");

    return RedisModule_ReplyWithSimpleString(ctx, response);
}
SilNikParowy_Kontekst *executor = NULL;
static void *execIndexerThread(void *ptr)
{
    SimpleQueue *indexing_queue = (SimpleQueue *)ptr;
    indexer_set_thread_title("rxIndexer index keys");
    serverLog(LL_NOTICE, "execIndexerThread started pumping");
    indexing_queue->Started();

    redisNodeInfo *index_config = rxIndexNode();
     auto *c = (struct client *)RedisClientPool<struct client>::Acquire(index_config->host_reference);
    RedisClientPool<struct client>::Release(c);
   
    if(executor == NULL){
        if(index_config->executor == NULL)
            index_config->executor = new SilNikParowy_Kontekst(index_config);
        executor = (SilNikParowy_Kontekst *)index_config->executor;
    }

    long long start = ustime();
    int tally = 0;
    while (!indexing_queue->must_stop)
    {    
        if (ustime() - start >= reindex_cron_slot_time * 1000)
        {
            // Yield when slot time over
            sched_yield();
            start = ustime();
        }
        void *index_request = indexing_queue->Dequeue();
        while (index_request != NULL)
        {
            indexingHandler(tally, index_request, index_config, executor);
            index_info.key_indexing_respone_queue->Enqueue(index_request);
            index_request = indexing_queue->Dequeue();
        }
        freeCompletedRedisRequests();
        sched_yield();
    }
    rxrule_helper_interval = 0;
    indexing_queue->Stopped();
    return NULL;
}

static void *execUpdateRedisThread(void *ptr)
{
    SimpleQueue *redis_queue = (SimpleQueue *)ptr;
    indexer_set_thread_title("rxIndexer update index node");
    redis_queue->Started();
    redisNodeInfo *index_config = rxIndexNode();

    serverLog(LL_NOTICE, "\ncapture starting: %s\n", index_config->host_reference);
    redisContext *context;
    context = redisConnect(index_config->host, index_config->port);
    if (context == NULL || context->err)
    {
        if (context)
        {
            serverLog(LL_NOTICE, "Connection error: %s\n", context->errstr);
            redisFree(context);
        }
        else
        {
            serverLog(LL_NOTICE, "Connection error: can't allocate lzf context\n");
        }
        return NULL;
    }
    redisReply *rcc = (redisReply *)redisCommand(context, "PING");
    if (rcc)
    {

        serverLog(LL_NOTICE, "............ execUpdateRedisThread ... 050 .... %s", rcc->str);
        freeReplyObject(rcc);
    }
    else
        serverLog(LL_NOTICE, "............ execUpdateRedisThread ... 051 .... NO PING Reply");

    serverLog(LL_NOTICE, "execUpdateRedisThread started pumping");

    long long start = ustime();
    while (must_stop == 0)
    {
        if (ustime() - start >= reindex_cron_slot_time * 1000)
        {
            // Yield when slot time over
            sched_yield();
            start = ustime();
        }

        void *index_request = redis_queue->Dequeue();
        if (index_request != NULL)
        {
            index_info.start_batch_ms = mstime();
            while (index_request != NULL)
            {
                void **argv = (void **)(index_request + sizeof(void *));
                sds cmd = (sds)rxGetContainedObject(argv[0]);
                if (strncmp("RXTRIGGER", cmd, 9) != 0)
                {
                    // ExecuteRedisCommand(NULL, index_request, data_config->host_reference);
                }
                index_info.index_update_respone_queue->Enqueue(index_request);

                long long now = mstime();
                if ((now - index_info.start_batch_ms) > 100)
                {
                    // if(index_info.key_indexing_fifo->len == 0 && index_info.index_update_fifo->len == 0 && batch_size > 0){
                    break;
                }
                index_request = (sds *)redis_queue->Dequeue();
            }
        }
        else
            sched_yield();
    }
    redis_queue->Stopped();
    return NULL;
}

static void startIndexerThreads()
{
    indexerThread *t = &index_info;

    t->key_indexing_respone_queue = new SimpleQueue();
    t->index_update_respone_queue = new SimpleQueue();
    t->index_rxRuleApply_respone_queue = new SimpleQueue();
    t->index_rxRuleApply_request_queue = new SimpleQueue(t->index_rxRuleApply_respone_queue);
    t->key_indexing_request_queue = new SimpleQueue((void *)execIndexerThread, 1, t->key_indexing_respone_queue);
    t->index_update_request_queue = new SimpleQueue((void *)execUpdateRedisThread, 1, t->index_update_respone_queue);

    rxrule_helper_interval = 100;
    rxrule_helper_cron = rxCreateTimeEvent(1, (aeTimeProc *)rxrule_timer_handler, NULL, NULL);
    int i = rxrule_timer_handler(NULL, 0, NULL);
    serverLog(LL_NOTICE, "RX Rule cron started: %lld interval=%d", rxrule_helper_cron, i);
}

static void stopIndexerThreads()
{
    indexerThread *t = &index_info;
    must_stop = 1;

    rxDeleteTimeEvent(reindex_helper_cron);
    rxDeleteTimeEvent(rxrule_helper_cron);
    t->key_indexing_request_queue->Release();
    t->key_indexing_respone_queue->Release();
    t->index_update_request_queue->Release();
    t->index_update_respone_queue->Release();
    t->index_rxRuleApply_request_queue->Release();
    t->index_rxRuleApply_respone_queue->Release();

    serverLog(LL_NOTICE, "Indexer stopped");
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    initRxSuite();

    REDISMODULE_NOT_USED(argv);

    if (RedisModule_Init(ctx, "rxIndexer", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    rxRegisterConfig(ctx, argv, argc);

    if (RedisModule_CreateCommand(ctx, "rxIndexer.info",
                                  indexerInfo, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "rx.info",
                                  indexerInfo, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "rxIndex",
                                  indexerControl, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "rxReIndex",
                                  reindex, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    installIndexerInterceptors();
    indexer_state = 1;

    startIndexerThreads();

    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx)
{
    rxUNUSED(ctx);

    stopIndexerThreads();
    sdsfree(stringKey);
    sdsfree(noFieldName);

    uninstallIndexerInterceptors();
    indexer_state = 0;

    finalizeRxSuite();
    return REDISMODULE_OK;
}
