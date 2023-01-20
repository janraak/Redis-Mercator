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
    void raxRecursiveFree(rax *rax, raxNode *n, void (*free_callback)(void *));

#include "rxSuiteHelpers.h"

    struct redisCommand *lookupCommand(rxString name);

#ifdef __cplusplus
}
#endif

#include "indexIntercepts.h"

#include "client-pool.hpp"
#include "rule.hpp"
#include "rxWaitIndexing-multiplexer.hpp"
#include "sjiboleth.h"
#include <string.h>

static void startIndexerThreads();
static void stopIndexerThreads();

static void startIndexerThreads();
static void stopIndexerThreads();

#define stringKey rxStringNew("S")
#define hashKey rxStringNew("H")
#define streamKey rxStringNew("X")
#define noFieldName rxStringNew("*")

#define indexer_set_thread_title(name) pthread_setname_np(pthread_self(), name)
indexerThread data_info = {};
indexerThread index_info = {};

#define BEGIN_COMMAND_INTERCEPTOR(fn)                                                        \
    static int fn(void *stash, redisNodeInfo *index_config, SilNikParowy_Kontekst *executor) \
    {                                                                                        \
        int argc = *((int *)stash);                                                          \
        void **argv = (void **)(stash + sizeof(void *));                                     \
        rxStashCommand(index_info.index_update_request_queue, "MULTI", 0);

#define END_COMMAND_INTERCEPTOR(fn)                                   \
    rxStashCommand(index_info.index_update_request_queue, "EXEC", 0); \
    return 0;                                                         \
    }

CSjiboleth *json_parser = newJsonEngine();
CSjiboleth *text_parser = newTextEngine();
static int must_stop = 0;

BEGIN_COMMAND_INTERCEPTOR(indexingHandlerDelCommand)
for (int k = 1; k < argc; k++)
{
    rxUNUSED(index_config);
    rxUNUSED(executor);
    rxString key = (rxString)rxGetContainedObject(argv[k]);
    rxStashCommand(index_info.index_update_request_queue, "RXBEGIN", 1, key);
    index_info.rxbegin_tally++;
    rxStashCommand(index_info.index_update_request_queue, "RXCOMMIT", 1, key);
    index_info.rxcommit_tally++;
}
END_COMMAND_INTERCEPTOR(indexingHandlerDelCommand)

BEGIN_COMMAND_INTERCEPTOR(indexingHandlerXDelCommand)
rxUNUSED(index_config);
rxUNUSED(executor);
rxString okey = (rxString)rxGetContainedObject(argv[1]);
for (int k = 2; k < argc; k++)
{
    rxString id = (rxString)rxGetContainedObject(argv[k]);
    rxString key = rxStringFormat("%s#%s", okey, id);
    rxStashCommand(index_info.index_update_request_queue, "RXBEGIN", 1, key);
    index_info.rxbegin_tally++;
    rxStashCommand(index_info.index_update_request_queue, "RXCOMMIT", 1, key);
    index_info.rxcommit_tally++;
}
END_COMMAND_INTERCEPTOR(indexingHandlerXDelCommand)

void freeIndexableObject(void *o)
{
    rxMemFree(o);
}

void preFlushCallback(void *p);
void postFlushCallback(void *p);

BEGIN_COMMAND_INTERCEPTOR(indexingHandlerXaddCommand)
rxUNUSED(executor);
rxString okey = (rxString)rxGetContainedObject(argv[1]);
rxUNUSED(okey);

auto *collector = raxNew();
executor->Memoize("@@TEXT_PARSER@@", (void *)text_parser);
executor->Memoize("@@collector@@", (void *)collector);

ParsedExpression *parsed_text;
int key_type = rxOBJ_STREAM;
rxUNUSED(key_type);

int j = 2;
rxString currentArg = (rxString)rxGetContainedObject(argv[j]);
if (strcmp(currentArg, "NOMKSTREAM") == 0)
{
    j++;
}
if ((strcmp(currentArg, "MAXLEN") == 0) || (strcmp(currentArg, "MINID") == 0))
{
    j++;
    currentArg = (rxString)rxGetContainedObject(argv[j]);
    if ((strcmp(currentArg, "=") == 0) || (strcmp(currentArg, "~") == 0))
    {
        j++;
        currentArg = (rxString)rxGetContainedObject(argv[j]);
    }
    j++;
    currentArg = (rxString)rxGetContainedObject(argv[j + 1]);
    if (strcmp(currentArg, "LIMIT") == 0)
    {
        j = j + 2;
    }
}
j++;
while (j < argc)
{
    rxString currentField = (rxString)rxGetContainedObject(argv[j]);
    rxString currentValue = (rxString)rxGetContainedObject(argv[j + 1]);

    executor->Memoize("@@field@@", (void *)currentField);
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

// TextDialect::FlushIndexables(collector, okey, key_type, NULL, bool use_bracket);
// RedisClientPool<redisContext>::Release(NULL);
rxRaxFreeWithCallback(collector, freeIndexableObject);
executor->Forget("@@TEXT_PARSER@@");
executor->Forget("@@collector@@");
// TODO: executor->Reset();

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

int client_health_interval = 100;
long long client_health_cron = -1;
struct client *rxrule_fakeClient = NULL;
struct redisCommand *rule_apply_cmd;
void *rxruleApply = NULL;
/* This is the rule.apply timer handler.
 * An apply entry is scheduled after an rxcommit to
 * update all business rules. */
int rxrule_timer_handler(struct aeEventLoop *, long long int, void *)
{
    redisNodeInfo *index_config = rxIndexNode();

    if (must_stop == 1)
        return -1;

    void *index_request = index_info.index_rxRuleApply_request_queue->Dequeue();
    while (index_request != NULL)
    {
        // rxServerLogHexDump(rxLL_NOTICE, index_request, 128/*rxMemAllocSize(index_request)*/, "rxrule_timer_handler %s %p DEQUEUE", index_info.index_rxRuleApply_request_queue->name, index_request);

        ExecuteRedisCommand(index_info.index_rxRuleApply_respone_queue, index_request, index_config->host_reference);
        index_request = index_info.index_rxRuleApply_request_queue->Dequeue();
    }

    return client_health_interval;
}

static int indexingHandler(int, void *stash, redisNodeInfo *index_config, SilNikParowy_Kontekst *executor)
{
    int argc = *((int *)stash);
    void **argv = (void **)(stash + sizeof(void *));

    rxString command = (rxString)rxGetContainedObject(argv[0]);
    if (command == NULL)
    {
        // rxServerLogHexDump(rxLL_NOTICE, index_request, 128/*rxMemAllocSize(index_request)*/, "freeCompletedRedisRequests %s %p DEQUEUE", index_info.index_update_respone_queue->name, index_request);
        return C_ERR;
    }
    rxString key = (rxString)rxGetContainedObject(argv[1]);
    if (rxStringMatch(command, "DEL", MATCH_IGNORE_CASE))
        return indexingHandlerDelCommand(stash, index_config, executor);
    else if (rxStringMatch(command, "XDEL", MATCH_IGNORE_CASE))
        return indexingHandlerXDelCommand(stash, index_config, executor);
    else if (rxStringMatch(command, "XADD", MATCH_IGNORE_CASE))
        return indexingHandlerXaddCommand(stash, index_config, executor);

    auto *collector = raxNew();
    executor->Memoize("@@TEXT_PARSER@@", (void *)text_parser);
    executor->Memoize("@@collector@@", (void *)collector);

    ParsedExpression *parsed_text;
    int key_type;
    bool use_bracket = true;
    if (argc == 3)
    {
        rxString f = rxStringNew("*");
        rxString v = (rxString)rxGetContainedObject(argv[2]);
        executor->Memoize("@@field@@", (void *)f);
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
        rxStringFree(f);
    }
    else
    {
        key_type = rxOBJ_HASH;
        use_bracket = argc > 4;
        for (int j = 2; j < (argc-1); j += 2)
        {
            rxString f = (rxString)rxGetContainedObject(argv[j]);
            rxString v = (rxString)rxGetContainedObject(argv[j + 1]);

            rxServerLog(rxLL_DEBUG, "Indexing Hash field %s with %s", f, v);

            executor->Memoize("@@field@@", (void *)f);
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
        }
    }
    if (raxSize(collector) > 0)
        TextDialect::FlushIndexables(collector, key, key_type, index_info.index_update_request_queue, use_bracket);
    raxFree(collector);
    collector = NULL;
    // TODO: executor->Reset();

    rxStashCommand(index_info.index_update_request_queue, "RXTRIGGER", 1, key);
    return C_OK;
}

static char *freeCompletedRedisRequests_last_key = NULL;
void freeCompletedRedisRequests()
{
    if (index_info.index_update_respone_queue == NULL)
        return;
    void *index_request = NULL;
    index_request = index_info.index_update_respone_queue->Dequeue();
    while (index_request != NULL)
    {
        // rxServerLogHexDump(rxLL_NOTICE, index_request, 128/*rxMemAllocSize(index_request)*/, "freeCompletedRedisRequests %s %p DEQUEUE", index_info.index_update_respone_queue->name, index_request);

        GET_ARGUMENTS_FROM_STASH(index_request);
        rxUNUSED(argv);
        if (argc > 1)
        {
            rxString key = (rxString)rxGetContainedObject(argv[1]);
            rxStashCommand(index_info.index_rxRuleApply_request_queue, "RULE.APPLY", 1, key);
            // auto *sdsv = (void **)(index_request + (argc + 3) * sizeof(void *));
            // // TODO : refactor next
            // sdsv = sdsv + 16;
            // // rxString key = (rxString)rxGetContainedObject(argv[1]);
            // char *key = (char *)sdsv;
            // if (freeCompletedRedisRequests_last_key == NULL || strcmp(freeCompletedRedisRequests_last_key, key) != 0)
            // {
            //     auto *stash = rxStashCommand(index_info.index_rxRuleApply_request_queue, "RULE.APPLY", 1, key);
            //     ExecuteRedisCommand(NULL, stash, rxDataNode()->host_reference);
            //     freeCompletedRedisRequests_last_key = key;
            // }
        }

        FreeStash(index_request);
        index_request = index_info.index_update_respone_queue->Dequeue();
    }
    if (index_info.index_rxRuleApply_respone_queue != NULL)
    {
        index_request = index_info.index_rxRuleApply_respone_queue->Dequeue();
        while (index_request != NULL)
        {
            // rxServerLogHexDump(rxLL_NOTICE, index_request, 128/*rxMemAllocSize(index_request)*/, "freeCompletedRedisRequests %s %p DEQUEUE", index_info.index_rxRuleApply_respone_queue->name, index_request);
            // FreeStash(index_request);
            index_request = index_info.index_rxRuleApply_respone_queue->Dequeue();
        }
    }
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

int indexerInfo(RedisModuleCtx *ctx, RedisModuleString **, int)
{
    rxString response = rxStringFormat("Indexer is:                       %s\n", indexer_state ? "Active" : "Not Active");
    response = rxStringFormat("%sReIndexing is:                             %s\n", response, reindex_iterator ? "Active" : "Not Active");
    response = rxStringFormat("%sNumber of SET commands intercepts:         %d\n", response, index_info.set_tally);
    response = rxStringFormat("%sNumber of HSET commands intercepts:        %d\n", response, index_info.hset_tally);
    response = rxStringFormat("%sNumber of indexing queued requests:        %d\n", response, index_info.key_indexing_request_queue->QueueLength());
    response = rxStringFormat("%sNumber of queued redis requests:           %d\n", response, index_info.index_update_request_queue->QueueLength());
    response = rxStringFormat("%sNumber of RXBEGIN commands:                %d\n", response, index_info.rxbegin_tally);
    response = rxStringFormat("%sNumber of RXADD commands:                  %d\n", response, index_info.rxadd_tally);
    response = rxStringFormat("%sNumber of RXCOMMIT commands:               %d\n", response, index_info.rxcommit_tally);
    response = rxStringFormat("%sNumber of completed requests:              %d\n", response, index_info.key_indexing_respone_queue->QueueLength());
    response = rxStringFormat("%sNumber of key_indexing_enqueues:           %d\n", response, index_info.key_indexing_respone_queue->enqueue_fifo_tally.load());
    response = rxStringFormat("%sNumber of key_indexing_dequeues:           %d\n", response, index_info.key_indexing_respone_queue->dequeue_fifo_tally.load());
    response = rxStringFormat("%sNumber of completed Redis requests:        %d\n", response, index_info.index_update_respone_queue->QueueLength());
    response = rxStringFormat("%sNumber of redis_enqueues:                  %d\n", response, index_info.index_update_request_queue->enqueue_fifo_tally.load());
    response = rxStringFormat("%sNumber of redis_dequeues:                  %d\n", response, index_info.index_update_request_queue->dequeue_fifo_tally.load());
    // response = rxStringFormat("%sNumber of REDIS commands:                  %d\n", response, dictSize(server.commands));
    // response = rxStringFormat("%sNumber of REDIS commands:                  %d, intercepts: %d\n", response, dictSize(server.commands), sizeof(interceptorCommandTable) / sizeof(struct redisCommand));
    response = rxStringFormat("%sNumber of Field index comm                 %d\n", response, index_info.field_index_tally);

    return RedisModule_ReplyWithSimpleString(ctx, response);
}

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
        rxString arg = rxStringNew((char *)RedisModule_StringPtrLen(argv[1], &arg_len));
        rxStringToUpper(arg);
        if (rxStringMatch(arg, "WAIT", MATCH_IGNORE_CASE))
        {
            if (indexer_state == 0)
                rxServerLog(rxLL_NOTICE, "Indexing turned on!");

            return rx_wait_indexing_complete(ctx);
        }
        else if (rxStringMatch(arg, "ON", MATCH_IGNORE_CASE))
        {
            if (indexer_state)
                return RedisModule_ReplyWithSimpleString(ctx, "Indexer already active.");
            startIndexerThreads();
            indexer_state = 1;
            rxServerLog(rxLL_NOTICE, "Indexing turned on!");
        }
        else if (rxStringMatch(arg, "OFF", MATCH_IGNORE_CASE))
        {
            if (!indexer_state)
                return RedisModule_ReplyWithSimpleString(ctx, "Indexer already inactive.");
            stopIndexerThreads();
            indexer_state = 0;
            rxServerLog(rxLL_NOTICE, "Indexing turned off!");
        }
        else if (rxStringMatch(arg, "STATUS", MATCH_IGNORE_CASE))
        {
            return indexerInfo(ctx, argv, argc);
        }
        else
        {
            return RedisModule_ReplyWithSimpleString(ctx, "Invalid argument, must be ON or OFF.");
        }
    }
    else
        return indexerInfo(ctx, argv, argc);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/* This is the timer handler that is called by the main event loop. We schedule
 * this timer to be called when the nearest of our module timers will expire. */
int reindex_cron(struct aeEventLoop *, long long, void *clientData)
{
    if (must_stop == 1)
        return -1;

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
        rxString key = (rxString)dictGetKey(de);
        if (*key == '^')
        {
            reindex_skipped_no_of_keys++;
            continue;
        }
        auto *collector = raxNew();
        engine->Memoize("@@collector@@", (void *)collector);

        ParsedExpression *parsed_text;
        void *value_for_key = dictGetVal(de);
        int key_type = rxGetObjectType(value_for_key);

        switch (key_type)
        {
        case rxOBJ_STRING:
        {
            rxString value = (rxString)rxGetContainedObject(value_for_key);
            if (value != NULL)
            {
                rxString f = rxStringNew("*");
                engine->Memoize("@@field@@", (void *)f);
                parsed_text = (ParsedExpression *)parseQ((value[0] == '{') ? json_parser : text_parser, value);
                auto *sub = parsed_text;
                while (sub)
                {
                    engine->Execute(sub);
                    sub = sub->Next();
                }
                delete parsed_text;
                engine->Forget("@@field@@");
                rxStringFree(f);
            }
        }
        break;
        case rxOBJ_LIST:
            rxServerLog(LL_NOTICE, "ReIndexing unexpected type: rxOBJ_LIST");
            break;
        case rxOBJ_HASH:
        {
            struct rxHashTypeIterator *hi = rxHashTypeInitIterator(value_for_key);
            int j = 0;
            while (rxHashTypeNext(hi) != C_ERR)
            {
                rxString field = rxHashTypeCurrentObjectNewSds(hi, rxOBJ_HASH_KEY);
                rxString field_value = rxHashTypeCurrentObjectNewSds(hi, rxOBJ_HASH_VALUE);
                engine->Memoize("@@field@@", (void *)field);
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
            rxServerLog(LL_NOTICE, "ReIndexing unexpected type");
            break;
        }
        TextDialect::FlushIndexables(collector, key, key_type, index_info.index_update_request_queue, true);
        // TODO: engine->Reset();
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
    rxServerLog(LL_NOTICE, "ReIndexing completed");
    // if (original_indexer_state){
    //     installIndexerInterceptors();
    //     indexer_state = 1;
    //     rxServerLog(LL_NOTICE, "Indexing restored");
    // }

    // check database!
    reindex_iterator = rxGetDatabaseIterator(0);
    while ((de = dictNext(reindex_iterator)) != NULL)
    {
        rxString key = (rxString)dictGetKey(de);
        if (*key == '^')
        {
            continue;
        }
        void *value_for_key = dictGetVal(de);
        if (value_for_key == NULL)
        {
            rxServerLog(rxLL_NOTICE, "after reindex check %s null value", key);
        }
        void *check_value = rxFindKey(0, key);
        if (check_value == NULL)
        {
            rxServerLog(rxLL_NOTICE, "after reindex check %s no value from rxFindKey", key);
        }
        if (check_value != value_for_key)
        {
            rxServerLog(rxLL_NOTICE, "after reindex check %s %p mismatched values to %p from rxFindKey", key, value_for_key, check_value);
        }
    }
    dictReleaseIterator(reindex_iterator);
    reindex_iterator = NULL;

    return -1;
}

int reindex(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    rxUNUSED(ctx);
    rxString response = rxStringEmpty();
    redisNodeInfo *index_config = rxIndexNode();

    if (argc == 2)
    {
        size_t arg_len;
        rxString arg = rxStringNew((char *)RedisModule_StringPtrLen(argv[1], &arg_len));
        rxStringToUpper(arg);
        if (rxStringMatch(arg, "STATUS", 1))
        {
            response = rxStringFormat("%sIndexer is:                 %s\n", response, indexer_state ? "Active" : "Not Active");
            response = rxStringFormat("%sReIndexing is:              %s\n", response, reindex_iterator ? "Active" : "Not Active");
            response = rxStringFormat("%sReIndexing stats:\r");
            response = rxStringFormat("%sTotal no of keys:           %d\n", response, reindex_total_no_of_keys);
            response = rxStringFormat("%sTotal no of keys processed: %d\n", response, reindex_processed_no_of_keys);
            response = rxStringFormat("%sTotal no of keys skipped:   %d\n", response, reindex_skipped_no_of_keys);
            response = rxStringFormat("%s\n", response);
            response = rxStringFormat("%sTotal reindexing time:      %ims\n", response, reindex_latency / 1000);
            response = rxStringFormat("%s\n", response);
            response = rxStringFormat("%sTotal no of timer slots:    %d\n", response, reindex_no_of_slots);
            response = rxStringFormat("%sTotal no of timer expires:  %d\n", response, reindex_yielded_no_of_keys);
            response = rxStringFormat("%sTotal no of queue throttles:%d\n", response, reindex_qthrottleded_no_of_keys);
            response = rxStringFormat("%sTotal no of no memory throttles:%d\n", response, reindex_throttleded_no_mem);
            return RedisModule_ReplyWithStringBuffer(ctx, response, strlen(response));
        }
        return RedisModule_ReplyWithSimpleString(ctx, "Invalid option");
    }

    // if (!indexer_state)
    //     return RedisModule_ReplyWithSimpleString(ctx, "Indexer NOT active.");

    if (reindex_iterator)
        return RedisModule_ReplyWithSimpleString(ctx, "ReIndexing already started.");

    rxStashCommand(index_info.index_update_request_queue, "FLUSHDB", 0);

    // uninstallIndexerInterceptors();
    // indexer_state = 0;

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

    // rxServerLog(LL_NOTICE, "ReIndexing started");

    return RedisModule_ReplyWithSimpleString(ctx, response);
}

SilNikParowy_Kontekst *executor = NULL;
static void *execIndexerThread(void *ptr)
{
    SimpleQueue *indexing_queue = (SimpleQueue *)ptr;
    indexer_set_thread_title("rxIndexer index keys");
    rxServerLog(LL_NOTICE, "execIndexerThread started pumping");
    indexing_queue->Started();

    redisNodeInfo *index_config = rxIndexNode();
    auto *c = (struct client *)RedisClientPool<struct client>::Acquire(index_config->host_reference, "_FAKE", "execIndexerThread");
    RedisClientPool<struct client>::Release(c, "execIndexerThread");

    if (executor == NULL)
    {
        if (index_config->executor == NULL)
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
    client_health_interval = 0;
    indexing_queue->Stopped();
    return NULL;
}

static void *execUpdateRedisThread(void *ptr)
{
    SimpleQueue *redis_queue = (SimpleQueue *)ptr;
    indexer_set_thread_title("rxIndexer update index node");
    redis_queue->Started();
    redisNodeInfo *index_config = rxIndexNode();

    rxServerLog(LL_NOTICE, "\ncapture starting: %s\n", index_config->host_reference);
    redisContext *context;
    context = redisConnect(index_config->host, index_config->port);
    if (context == NULL || context->err)
    {
        if (context)
        {
            rxServerLog(LL_NOTICE, "Connection error: %s\n", context->errstr);
            redisFree(context);
        }
        else
        {
            rxServerLog(LL_NOTICE, "Connection error: can't allocate lzf context\n");
        }
        return NULL;
    }
    redisReply *rcc = (redisReply *)redisCommand(context, "PING");
    if (rcc)
    {

        rxServerLog(LL_NOTICE, "............ execUpdateRedisThread ... 050 .... %s", rcc->str);
        freeReplyObject(rcc);
    }
    else
        rxServerLog(LL_NOTICE, "............ execUpdateRedisThread ... 051 .... NO PING Reply");

    rxServerLog(LL_NOTICE, "execUpdateRedisThread started pumping");

    long long start = ustime();
    while (redis_queue->must_stop.load() == 0)
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
                // rxServerLogHexDump(rxLL_NOTICE, index_request, 128/*rxMemAllocSize(index_request)*/, "execUpdateRedisThread %s %p DEQUEUE", redis_queue->name, index_request);

                GET_ARGUMENTS_FROM_STASH(index_request);
                if (argc < 1)
                {
                    rxServerLog(rxLL_NOTICE, "Invalid request from queue %p argc=%d", index_request, argc);
                    FreeStash(index_request);
                    continue;
                }

                rxString cmd = (rxString)rxGetContainedObject(argv[0]);
                if (!rxStringMatch(cmd, "RXTRIGGER", MATCH_IGNORE_CASE))
                {
                    ExecuteRedisCommandRemote(NULL, index_request, index_config->host_reference);
                }
                index_info.index_update_respone_queue->Enqueue(index_request);

                long long now = mstime();
                if ((now - index_info.start_batch_ms) > 100)
                {
                    // if(index_info.key_indexing_fifo->len == 0 && index_info.index_update_fifo->len == 0 && batch_size > 0){
                    break;
                }
                index_request = (rxString *)redis_queue->Dequeue();
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
    must_stop = 0;
    indexerThread *t = &index_info;

    t->key_indexing_respone_queue = new SimpleQueue("STRESP");
    t->index_update_respone_queue = new SimpleQueue("IXRESP");
    t->index_rxRuleApply_respone_queue = new SimpleQueue("RLRESP");
    t->index_rxRuleApply_request_queue = new SimpleQueue("RLREQ", t->index_rxRuleApply_respone_queue);
    t->key_indexing_request_queue = new SimpleQueue("IXREQ", (void *)execIndexerThread, 1, t->key_indexing_respone_queue);
    t->index_update_request_queue = new SimpleQueue("STREQ", (void *)execUpdateRedisThread, 1, t->index_update_respone_queue);
    rxServerLog(LL_NOTICE, "Indexer started");

    client_health_interval = 100;
    client_health_cron = rxCreateTimeEvent(1, (aeTimeProc *)rxrule_timer_handler, NULL, NULL);
    int i = rxrule_timer_handler(NULL, 0, NULL);
    rxServerLog(LL_NOTICE, "RX Rule cron started: %lld interval=%d", client_health_cron, i);
    installIndexerInterceptors(&preFlushCallback, &postFlushCallback);
}

static void stopIndexerThreads()
{
    uninstallIndexerInterceptors();
    indexerThread *t = &index_info;
    must_stop = 1;

    rxDeleteTimeEvent(reindex_helper_cron);
    rxDeleteTimeEvent(client_health_cron);
    t->key_indexing_request_queue = SimpleQueue::Release(t->key_indexing_request_queue);
    t->key_indexing_respone_queue = SimpleQueue::Release(t->key_indexing_respone_queue);
    t->index_update_request_queue = SimpleQueue::Release(t->index_update_request_queue);
    t->index_update_respone_queue = SimpleQueue::Release(t->index_update_respone_queue);
    t->index_rxRuleApply_request_queue = SimpleQueue::Release(t->index_rxRuleApply_request_queue);
    t->index_rxRuleApply_respone_queue = SimpleQueue::Release(t->index_rxRuleApply_respone_queue);

    if (reindex_iterator)
    {
        rxServerLog(LL_NOTICE, "Will abort reindexer");
        dictReleaseIterator(reindex_iterator);
    }
    reindex_iterator = NULL;
    // Verify db0
    dictEntry *de;
    dictIterator *checker = rxGetDatabaseIterator(0);
    while ((de = dictNext(checker)) != NULL)
    {
        rxString key = (rxString)dictGetKey(de);
        void *value_for_key = dictGetVal(de);
        if (value_for_key == NULL)
        {
            rxServerLog(rxLL_NOTICE, "DB check %s == NULL", key);
        }
        else if (rxGetRefcount(value_for_key) < 1)
        {
            rxServerLog(rxLL_NOTICE, "DB check %s refcnt error", key);
        }
    }
    dictReleaseIterator(checker);

    rxServerLog(LL_NOTICE, "Indexer stopped");
}

void preFlushCallback(void *)
{
    stopIndexerThreads();
}

void postFlushCallback(void *)
{
    startIndexerThreads();
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    initRxSuite();

    char *libpath = getenv("LD_LIBRARY_PATH");
    if(libpath)
        rxServerLog(rxLL_NOTICE, "rxIndexer LD_LIBRARY_PATH[%d]=%s", strlen(libpath), libpath);
    else
        rxServerLog(rxLL_NOTICE, "rxIndexer NO LD_LIBRARY_PATH SET ");

    if (RedisModule_Init(ctx, "rxIndexer", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
    {
        rxServerLog(rxLL_NOTICE, "OnLoad rxIndexer. Init error!");
        return REDISMODULE_ERR;
    }

    rxRegisterConfig((void **)argv, argc);

    if (RedisModule_CreateCommand(ctx, "rxIndex",
                                  indexerControl, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "rxReIndex",
                                  reindex, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    installIndexerInterceptors(&preFlushCallback, &postFlushCallback);
    indexer_state = 1;

    startIndexerThreads();
    rxServerLog(rxLL_NOTICE, "OnLoad rxIndexer. Done!");

    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx)
{
    rxUNUSED(ctx);

    stopIndexerThreads();
    rxStringFree(stringKey);
    rxStringFree(noFieldName);

    uninstallIndexerInterceptors();
    indexer_state = 0;

    finalizeRxSuite();
    return REDISMODULE_OK;
}
