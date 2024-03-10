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
#include "tls.hpp"

#include "client-pool.hpp"
#include "rule.hpp"
#include "rxWaitIndexing-multiplexer.hpp"
#include "sjiboleth.h"
#include <string.h>

static void startIndexerThreads();
static void stopIndexerThreads();

#define stringKey rxStringNew("S")
#define hashKey rxStringNew("H")
#define streamKey rxStringNew("X")
#define noFieldName rxStringNew("*")

#define indexer_set_thread_title(name) pthread_setname_np(pthread_self(), name)
indexerThread data_info = {};
indexerThread index_info = {};

#define BEGIN_COMMAND_INTERCEPTOR(fn)                                            \
    static int fn(void *stash, redisNodeInfo *, SilNikParowy_Kontekst *executor) \
    {                                                                            \
        int argc = *((int *)stash);                                              \
        void **argv = (void **)(stash + sizeof(void *));                         \
        rxStashCommand(index_info.index_update_request_queue, "MULTI", 0);

#define END_COMMAND_INTERCEPTOR(fn)                                   \
    rxStashCommand(index_info.index_update_request_queue, "EXEC", 0); \
    return 0;                                                         \
    }

static int must_stop = 0;

// BEGIN_COMMAND_INTERCEPTOR(indexingHandlerDelCommand)
// for (int k = 1; k < argc; k++)
// {
//     rxUNUSED(executor);
//     rxString key = (rxString)rxGetContainedObject(argv[k]);
//     rxStashCommand(index_info.index_update_request_queue, "RXBEGIN", 1, key);
//     index_info.rxbegin_tally++;
//     rxStashCommand(index_info.index_update_request_queue, "RXCOMMIT", 1, key);
//     index_info.rxcommit_tally++;
// }
// END_COMMAND_INTERCEPTOR(indexingHandlerDelCommand)

// BEGIN_COMMAND_INTERCEPTOR(indexingHandlerXDelCommand)
// rxUNUSED(executor);
// rxString okey = (rxString)rxGetContainedObject(argv[1]);
// for (int k = 2; k < argc; k++)
// {
//     rxString id = (rxString)rxGetContainedObject(argv[k]);
//     rxString key = rxStringFormat("%s#%s", okey, id);
//     rxStashCommand(index_info.index_update_request_queue, "RXBEGIN", 1, key);
//     index_info.rxbegin_tally++;
//     rxStashCommand(index_info.index_update_request_queue, "RXCOMMIT", 1, key);
//     index_info.rxcommit_tally++;
// }
// END_COMMAND_INTERCEPTOR(indexingHandlerXDelCommand)

// void freeIndexableObject(void *o)
// {
//     rxMemFree(o);
// }

void preFlushCallback(void *p);
void postFlushCallback(void *p);

// BEGIN_COMMAND_INTERCEPTOR(indexingHandlerXaddCommand)
// rxUNUSED(executor);
// rxString okey = (rxString)rxGetContainedObject(argv[1]);

// auto *collector = raxNew();
// executor->Memoize("@@collector@@", (void *)collector);

// ParsedExpression *parsed_text;
// int key_type = rxOBJ_STREAM;

// bool use_bracket = true;

// int j = 2;
// rxString currentArg = (rxString)rxGetContainedObject(argv[j]);
// if (strcmp(currentArg, "NOMKSTREAM") == 0)
// {
//     j++;
// }
// if ((strcmp(currentArg, "MAXLEN") == 0) || (strcmp(currentArg, "MINID") == 0))
// {
//     j++;
//     currentArg = (rxString)rxGetContainedObject(argv[j]);
//     if ((strcmp(currentArg, "=") == 0) || (strcmp(currentArg, "~") == 0))
//     {
//         j++;
//         currentArg = (rxString)rxGetContainedObject(argv[j]);
//     }
//     j++;
//     currentArg = (rxString)rxGetContainedObject(argv[j + 1]);
//     if (strcmp(currentArg, "LIMIT") == 0)
//     {
//         j = j + 2;
//     }
// }
// j++;
// while (j < argc)
// {
//     rxString currentField = (rxString)rxGetContainedObject(argv[j]);
//     rxString currentValue = (rxString)rxGetContainedObject(argv[j + 1]);

//     executor->Memoize("@@field@@", (void *)currentField);
//     auto *parser = currentValue[0] == '{' ? Sjiboleth::Get("JsonDialect") : Sjiboleth::Get("TextDialect");
//     parsed_text = (ParsedExpression *)parseQ(parser, currentValue);
//     auto *sub = parsed_text;
//     while (sub)
//     {
//         executor->Execute(sub);
//         sub = sub->Next();
//     }
//     delete parsed_text;
//     executor->Forget("@@field@@");

//     j = j + 2;
// }

// if (raxSize(collector) > 0)
//     TextDialect::FlushIndexables(collector, okey, key_type, index_info.index_update_request_queue, use_bracket);
// raxFree(collector);
// collector = NULL;

// rxUNUSED(okey);

// END_COMMAND_INTERCEPTOR(indexingHandlerXaddCommand)

#ifdef __cplusplus
extern "C"
{
#endif
    struct client *createAOFClient(void);
    // static int indexingHandler(int tally, void *stash, redisNodeInfo *index_config, SilNikParowy_Kontekst *executor);
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

void freeCompletedRedisRequests()
{
    if (index_info.index_update_respone_queue == NULL)
        return;
    void *index_request = NULL;
    index_request = index_info.index_update_respone_queue->Dequeue();
    while (index_request != NULL)
    {
        GET_ARGUMENTS_FROM_STASH(index_request);
        rxUNUSED(argv);

        FreeStash(index_request);
        index_request = index_info.index_update_respone_queue->Dequeue();
    }
    if (index_info.index_rxRuleApply_respone_queue != NULL)
    {
        index_request = index_info.index_rxRuleApply_respone_queue->Dequeue();
        while (index_request != NULL)
        {
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
    auto config = (rxSuiteShared *)initRxSuite();
    rxString response = rxStringFormat("Indexer is:                       %s\012", indexer_state ? "Active" : "Not Active");
    response = rxStringFormat("%sReIndexing is:                             %s\012", response, reindex_iterator ? "Active" : "Not Active");
    response = rxStringFormat("%sNumber of SET commands intercepts:         %d\012", response, index_info.set_tally);
    response = rxStringFormat("%sNumber of HSET commands intercepts:        %d\012", response, index_info.hset_tally);
    response = rxStringFormat("%sNumber of enqueued objects to index:       %d\012", response, config->n_enq_touchedKeys);
    response = rxStringFormat("%sNumber of enqueue key to index spins:      %d\012", response, config->n_touchedKey_spins);
    response = rxStringFormat("%sTotal ms for enqueue key to index spins:   %d\012", response, config->us_touchedKey_spins);
    response = rxStringFormat("%sAverage ms for enqueue key spins:          %0.3f\012", response, config->us_touchedKey_spins * 1.0 / config->n_touchedKey_spins);
    response = rxStringFormat("%sNumber of indexed keys:                    %d\012", response, config->n_indexed);
    response = rxStringFormat("%sTotal ms for indexed keys:                 %d\012", response, config->us_indexed);
    response = rxStringFormat("%sAverage ms per indexed key:                %0.3f\012", response, config->us_indexed * 1.0 / config->n_indexed);
    response = rxStringFormat("%sNumber of dequeued objects to index:       %d\012", response, config->n_deq_touchedKeys);
    response = rxStringFormat("%sNumber of pending objects to index:        %d\012", response, PendingObjectForIndexing());
    response = rxStringFormat("%sNumber of enqueued key triggered:          %d\012", response, config->n_enq_triggeredKeys);
    response = rxStringFormat("%sNumber of dequeued key triggered:          %d\012", response, config->n_deq_triggeredKeys);
    response = rxStringFormat("%sNumber of enqueue triggered key spins:     %d\012", response, config->n_triggeredKey_spins);
    response = rxStringFormat("%sTotal ms for enqueue triggered key spins:  %d\012", response, config->us_triggeredKey_spins);
    response = rxStringFormat("%sAverage ms for enqueue triggered key spins:%0.3f\012", response, config->us_triggeredKey_spins * 1.0 / config->us_triggeredKey_spins);
    response = rxStringFormat("%sNumber of pending keys triggered:          %d\012", response, PendingKeysTriggered());
    response = rxStringFormat("%sNumber of processed triggered keys:        %d\012", response, config->n_processingTriggered);
    response = rxStringFormat("%sTotal ms for processing triggered keys:    %d\012", response, config->us_indexed);
    response = rxStringFormat("%sAverage ms per triggered key:              %0.3f\012", response, config->us_processingTriggered * 1.0 / config->n_processingTriggered);
    // response = rxStringFormat("%sNumber of RXBEGIN commands:                %d\012", response, index_info.rxbegin_tally);
    // response = rxStringFormat("%sNumber of RXADD commands:                  %d\012", response, index_info.rxadd_tally);
    // response = rxStringFormat("%sNumber of RXCOMMIT commands:               %d\012", response, index_info.rxcommit_tally);
    response = rxStringFormat("%savg us hset                                %d\012", response, config->us_hset / config->n_hset);
    response = rxStringFormat("%savg us hset  key block                     %d\012", response, config->us_hset_key_block / config->n_hset);
    response = rxStringFormat("%savg us hset  original                      %d\012", response, config->us_hset_std / config->n_hset);
    response = rxStringFormat("%sn_enqueue_pool1                            %d\012", response, config->n_enqueue_pool1);
    response = rxStringFormat("%sn_enqueue_pool2                            %d\012", response, config->n_enqueue_pool2);
    response = rxStringFormat("%savg us_enqueue_outside                     %d\012", response, config->us_enqueue_outside / config->n_enqueue_pool1);
    response = rxStringFormat("%savg us_enqueue_inside                      %d\012", response, config->us_enqueue_inside / config->n_enqueue_pool1);
    response = rxStringFormat("%savg us_enqueue_pool1                       %d\012", response, config->us_enqueue_pool1 / config->n_enqueue_pool1);
    response = rxStringFormat("%savg us_enqueue_pool2                       %d\012", response, config->n_enqueue_pool2 ? config->us_enqueue_pool2 / config->n_enqueue_pool2 : 0);
    response = rxStringFormat("%skey lookup using lookup                    %d\012", response, config->n_lookupByLookup);
    if (config->n_lookupByLookup)
        response = rxStringFormat("%savg lookup using lookup                    %d\012", response, config->us_lookupByLookup / config->n_lookupByLookup);
    response = rxStringFormat("%skey lookup using dict                      %d\012", response, config->n_lookupByDict);
    if (config->n_lookupByDict)
        response = rxStringFormat("%savg lookup using dict                      %d\012", response, config->us_lookupByDict / config->n_lookupByDict);
    response = rxStringFormat("%skey lookup using scan                    %d\012", response, config->n_lookupByScan);
    if (config->n_lookupByScan)
        response = rxStringFormat("%savg lookup using scan                    %d\012", response, config->us_lookupByScan / config->n_lookupByScan);
    response = rxStringFormat("%skey lookup Not Found                           %d\012", response, config->n_lookupNotFound);
    if (config->n_lookupNotFound)
        response = rxStringFormat("%savg lookup Not Found                       %d\012", response, config->us_lookupNotFound / config->n_lookupNotFound);

    return RedisModule_ReplyWithStringBuffer(ctx, response, strlen(response));
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

void Index_Object(rxString key, void *value_for_key, SilNikParowy_Kontekst *engine)
{
    rax *collector = raxNew();
    engine->Memoize("@@collector@@", (void *)collector);

    index_info.object_index_dequeued_tally++;

    ParsedExpression *parsed_text;
    int key_type = rxGetObjectType(value_for_key);
    // rxHashFieldAndValues *mob = NULL;
    switch (key_type)
    {
    case rxOBJ_STRING:
    {
        rxString value = (rxString)rxGetContainedObject(value_for_key);
        if (value != NULL)
        {
            rxString f = rxStringNew("*");
            engine->Memoize("@@field@@", (void *)f);
            parsed_text = (ParsedExpression *)parseQ((value[0] == '{') ? Sjiboleth::Get("JsonDialect") : Sjiboleth::Get("TextDialect"), value);
            auto *sub = parsed_text;
            while (sub)
            {
                sub->PrependExecutionMode();
                engine->Execute(sub);
                sub = sub->Next();
            }
            delete parsed_text;
            engine->Forget("@@field@@");
            rxStringFree(f);
        }
    }
        goto done;
    case rxOBJ_LIST:
        rxServerLog(LL_NOTICE, "ReIndexing unexpected type: rxOBJ_LIST");
        break;
    case rxOBJ_HASH:
        if (rxStringCharCount(key, ':') >= 2)
            goto done;
        rxHashTraverse(
            key,
            value_for_key, [](const char *f, const char *v, void *p)
            {
                    // if(rxStringMatch("type", f, 1))
                    //     return;
                    auto engine = (SilNikParowy_Kontekst *)p;
                    ParsedExpression *parsed_text;
                    engine->Memoize("@@field@@", (void *)f);
                    auto leftQuote = strchr(v, '\'');
                    auto end = strchr(v, 0x00) - 1;
                    if (!(*v == '\'' && *end == '\''))
                        while (leftQuote)
                        {
                            // Some names, or sentences may start with a quote
                            // auto rightQuote = strchr(leftQuote + 1, '\'');
                            // if(rightQuote && rightQuote != end)
                            //     rightQuote = NULL;

                            // if (leftQuote && !rightQuote)
                            *((char *)leftQuote) = ' ';
                            leftQuote = strchr(v, '\'');
                        }
                    parsed_text = (ParsedExpression *)parseQ((v[0] == '{') ? Sjiboleth::Get("JsonDialect") : Sjiboleth::Get("TextDialect"), v);
                    auto *sub = parsed_text;
                    while (sub)
                    {
                        sub->PrependExecutionMode();
                        engine->Execute(sub);
                        sub = sub->Next();
                    }
                    delete parsed_text;
                    engine->Forget("@@field@@"); },
            engine);
        goto done;
    default:
        rxServerLog(LL_NOTICE, "ReIndexing unexpected type: %d", key_type);
        break;
    }
done:
    TextDialect::FlushIndexables(collector, key, key_type, index_info.index_update_request_queue, true);
    // if(mob)
    //     rxFreeHashFields(mob);
    engine->Forget("@@collector@@");
    raxFree(collector);
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
        void *value_for_key = dictGetVal(de);
        Index_Object(key, value_for_key, engine);
        // TODO: engine->Reset();

        // rxStashCommand(index_info.index_update_request_queue, "RXTRIGGER", 1, key);
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
            response = rxStringFormat("%sIndexer is:                 %s\r\n", response, indexer_state ? "Active" : "Not Active");
            response = rxStringFormat("%sReIndexing is:              %s\r\n", response, reindex_iterator ? "Active" : "Not Active");
            response = rxStringFormat("%sReIndexing stats:\r\n");
            response = rxStringFormat("%sTotal no of keys:           %d\r\n", response, reindex_total_no_of_keys);
            response = rxStringFormat("%sTotal no of keys processed: %d\r\n", response, reindex_processed_no_of_keys);
            response = rxStringFormat("%sTotal no of keys skipped:   %d\r\n", response, reindex_skipped_no_of_keys);
            response = rxStringFormat("%s\r\n", response);
            response = rxStringFormat("%sTotal reindexing time:      %ims\r\n", response, reindex_latency / 1000);
            response = rxStringFormat("%s\r\n", response);
            response = rxStringFormat("%sTotal no of timer slots:    %d\r\n", response, reindex_no_of_slots);
            response = rxStringFormat("%sTotal no of timer expires:  %d\r\n", response, reindex_yielded_no_of_keys);
            response = rxStringFormat("%sTotal no of queue throttles:%d\r\n", response, reindex_qthrottleded_no_of_keys);
            response = rxStringFormat("%sTotal no of no memory throttles:%d\r\n", response, reindex_throttleded_no_mem);
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

    reindex_helper_cron = rxCreateTimeEvent(1, (aeTimeProc *)reindex_cron, engine, NULL);
    reindex_cron(NULL, 0, engine);

    // rxServerLog(LL_NOTICE, "ReIndexing started");

    return RedisModule_ReplyWithSimpleString(ctx, response);
}

void *Allocate_Global_Context(void *config)
{
    return (void *)new SilNikParowy_Kontekst((redisNodeInfo *)config);
}

int object_indexing_duration = 100;
int object_indexing_interval = 100;

static void *execIndexerThread(void *ptr)
{
    CommencedIndexingHandler();
    // // return;
    // SimpleQueue *indexing_queue = (SimpleQueue *)ptr;
    // indexer_set_thread_title("rxIndexer index keys");
    // rxServerLog(LL_NOTICE, "execIndexerThread started pumping");
    // indexing_queue->Started();

    redisNodeInfo *index_config = rxIndexNode();

    auto executor = new SilNikParowy_Kontekst(index_config, NULL);

    // while (!indexing_queue->must_stop)
    // {
    // WaitForIndexRequest();
    snapshot_request *key = NULL;
    while ((key = getIndexableKey()))
    {
        void *obj = rxFindKey(0, key->k);
        Index_Object(key->k, obj, executor);
        key->completed = ustime();
        freeIndexedKey(key);
    }
    //     sched_yield();
    // }

    // indexing_queue->Stopped();
    delete executor;
    return NULL;
}

static void *execUpdateRedisThread(void *ptr)
{
    SimpleQueue *redis_queue = (SimpleQueue *)ptr;
    indexer_set_thread_title("rxIndexer update index node");
    redis_queue->Started();
    redisNodeInfo *index_config = rxIndexNode();

    rxServerLog(LL_NOTICE, "\ncapture starting: %s\r\n", index_config->host_reference);
    auto *context = RedisClientPool<redisContext>::Acquire(index_config->host_reference, "_CLIENT", "execUpdateRedisThread");
    if (context != NULL)
    {
        redisReply *rcc = (redisReply *)redisCommand(context, "PING");
        if (rcc)
            freeReplyObject(rcc);
        else
            rxServerLog(LL_NOTICE, "............ execUpdateRedisThread ... 051 .... NO PING Reply");
        RedisClientPool<redisContext>::Release(context, "execUpdateRedisThread");
    }

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

    SetIndexingHandler(execIndexerThread);
    t->key_indexing_respone_queue = new SimpleQueue("STRESP");
    t->index_update_respone_queue = new SimpleQueue("IXRESP");
    // t->key_indexing_request_queue = new SimpleQueue("IXREQ", (void *)execIndexerThread, 4, t->key_indexing_respone_queue);
    // t->index_update_request_queue = new SimpleQueue("STREQ", (void *)execUpdateRedisThread, 1, t->index_update_respone_queue);
    rxServerLog(LL_NOTICE, "Indexer started");
    SetIndex_info(t);

    installIndexerInterceptors(&preFlushCallback, &postFlushCallback);
}

static void stopIndexerThreads()
{
    uninstallIndexerInterceptors();
    indexerThread *t = &index_info;
    must_stop = 1;

    rxDeleteTimeEvent(reindex_helper_cron);
    t->key_indexing_request_queue = SimpleQueue::Release(t->key_indexing_request_queue);
    t->key_indexing_respone_queue = SimpleQueue::Release(t->key_indexing_respone_queue);
    t->index_update_request_queue = SimpleQueue::Release(t->index_update_request_queue);
    t->index_update_respone_queue = SimpleQueue::Release(t->index_update_respone_queue);

    if (reindex_iterator)
    {
        rxServerLog(LL_NOTICE, "Will abort reindexer");
        dictReleaseIterator(reindex_iterator);
    }
    reindex_iterator = NULL;

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
    if (libpath)
        rxServerLog(rxLL_NOTICE, RXINDEX_FORMAT_001, strlen(libpath), libpath);
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
    Sjiboleth::Get("QueryDialect");
    Sjiboleth::Get("GremlinDialect");
    Sjiboleth::Get("JsonDialect");
    Sjiboleth::Get("TextDialect");

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

long long indexer_stats_cron_id = -1;
#define ms_5MINUTES (2 * 60 * 1000)
#define ms_1MINUTE (1 * 60 * 1000)

typedef struct indexer_stats_entry
{
    const char *label;
    long long tally;
    long long sum;
    const char *suffix;
} indexer_stats_entry;

long long indexer_stats__last_stats = 0;
int indexer_stats_cron(struct aeEventLoop *, long long, void *)
{
    if ((mstime() - indexer_stats__last_stats) < ms_5MINUTES)
        return ms_1MINUTE;
    indexer_stats__last_stats = mstime();
    auto config = (rxSuiteShared *)initRxSuite();

    indexer_stats_entry entries[] = {
        {/*00*/ "Number of SET commands intercepts", index_info.set_tally, 0, NULL},
        {/*01*/ "Number of HSET commands intercepts", index_info.hset_tally, 0, "\n"},
        {/*02*/ "Number of enqueued objects to index", config->n_enq_touchedKeys, config->us_enq_touchedKeys, NULL},
        {/*02*/ "Number of re-enqueued objects to index", config->n_renq_touchedKeys, 0, NULL},
        {/*04*/ "Enqueue key spins", config->n_touchedKey_spins, config->us_touchedKey_spins, NULL},
        {/*06*/ "Indexed keys", config->n_indexed, config->us_indexed, NULL},
        {/*09*/ "Number of dequeued objects to index", config->n_deq_touchedKeys, 0, NULL},
        {/*09*/ "Indexing completed objects", config->n_done_touchedKeys, config->n_indexed > 0 ? config->us_indexed * 1.0 / config->n_indexed : 0, NULL},
        {/*10*/ "Pending objects to index", PendingObjectForIndexing(), 0, NULL},
        {/*11*/ "Enqueued keys triggered", config->n_enq_triggeredKeys, config->us_enq_triggeredKeys, NULL},
        {/*12*/ "Dequeued keys triggered", config->n_deq_triggeredKeys, 0, NULL},
        {/*12*/ "Keys trigger executions", config->n_done_triggeredKeys, config->n_processingTriggered > 0 ? config->us_processingTriggered / config->n_processingTriggered : 0, NULL},
        {/*13*/ "Enqueue triggered spins", config->n_triggeredKey_spins, config->n_triggeredKey_spins > 0 ? config->us_triggeredKey_spins * 1.0 / config->n_triggeredKey_spins : 0, NULL},
        {/*16*/ "Number of pending keys triggered", PendingKeysTriggered(), 0, NULL},
        {/*17*/ "Processed key triggers", config->n_processingTriggered, config->n_processingTriggered > 0 ? config->us_processingTriggered / config->n_processingTriggered : 0, NULL},
        {/*20*/ "avg us hset", config->n_hset, config->us_hset, NULL},
        {/*21*/ "avg us hset  key block", config->n_hset, config->us_hset_key_block, NULL},
        {/*22*/ "avg us hset  original", config->n_hset, config->us_hset_std, NULL},
        {/*23*/ "queue (1)", config->n_enqueue_pool1, config->us_enqueue_pool1, NULL},
        {/*23*/ "queue (1) outside", config->n_enqueue_pool1, config->us_enqueue_outside, NULL},
        {/*23*/ "queue (1) inside", config->n_enqueue_pool1, config->us_enqueue_inside, NULL},
        {/*24*/ "in progress (2)", config->n_enqueue_pool2, config->us_enqueue_pool2, NULL},
        {/*29*/ "key lookup using lookup", config->n_lookupByLookup, config->us_lookupByLookup, NULL},
        {/*31*/ "key lookup using dict", config->n_lookupByDict, config->us_lookupByDict, NULL},
        {/*33*/ "key lookup using scan", config->n_lookupByScan, config->us_lookupByScan, NULL},
        {/*33*/ "hash snapshots", config->n__hash_snapshots, config->us_hash_snapshots, NULL},
        {/*33*/ "hash snapshot request", config->n_hash_snapshot_request, config->us_hash_snapshot_request, NULL},
        {/*33*/ "hash snapshot completions", config->n_hash_snapshot_response, config->us_hash_snapshot_response, NULL},
        {/*33*/ "hash snapshot request", config->n_hash_snapshot_release, config->us_hash_snapshot_release, NULL},
        {/*33*/ "hash snapshot cron leaps", config->n_snapshots_cron, config->us_snapshots_cron, NULL},
        {/*33*/ "hash snapshot cron idle", config->n_snapshots_cron, config->us_snapshots_cron_yield, NULL},
        {/*33*/ "hash snapshot cron keys", config->n_snapshots_cron_requests, config->us_snapshots_cron_requests, NULL},
        {/*33*/ "hash snapshot cron key releases", config->n_snapshots_cron, config->us_snapshots_cron_releases, NULL},
        {/*33*/ "hash snapshot cron keys per leap", config->n_snapshots_cron_requests, config->us_snapshots_cron_requests, NULL},
        {/*33*/ "hash snapshot cron releases per leap", config->n_snapshots_cron_releases, config->us_snapshots_cron_requests, NULL}};

    rxServerLog(rxLL_NOTICE, "Indexer Statistics");
    rxServerLog(rxLL_NOTICE, "%48s %10s %10s", "Counter", "Count", "Avg us");
    for (size_t n = 0; n < sizeof(entries) / sizeof(indexer_stats_entry); ++n)
    {

        if (entries[n].tally != 0)
        {
            double avg = entries[n].sum * 1.0 / entries[n].tally;
            rxServerLog(rxLL_NOTICE, "%48s %10lld %12.2f%s", entries[n].label, entries[n].tally, avg, entries[n].suffix ? entries[n].suffix : "");
        }
    }

    return ms_1MINUTE;
}

void *rxIndexerStartup()
{
    indexer_stats_cron_id = rxCreateTimeEvent(1, (aeTimeProc *)indexer_stats_cron, NULL, NULL);
    rxServerLog(rxLL_NOTICE, "Indexer stats cron started on => %lld", indexer_stats_cron_id);
    indexer_stats_cron(NULL, 0, NULL);
    return (void *)&rxIndexerStartup;
}

void *startUp = rxIndexerStartup();
