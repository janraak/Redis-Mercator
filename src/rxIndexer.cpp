/* rxIndexer -- An example of modules dictionary API
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

    struct redisCommand *lookupCommand(sds name);

#ifdef __cplusplus
}
#endif

#include "indexIntercepts.h"

#include "client-pool.hpp"
#include "rule.hpp"
#include "sjiboleth.h"
#include <string.h>
#include "rxWaitIndexing-duplexer.hpp"

#define stringKey sdsnew("S")
#define hashKey sdsnew("H")
#define streamKey sdsnew("X")
#define noFieldName sdsnew("*")

#define indexer_set_thread_title(name) pthread_setname_np(pthread_self(), name)
indexerThread index_info = {};

#define BEGIN_COMMAND_INTERCEPTOR(fn)       \
    static int fn(sds *kfv, int argc)       \
    {                                       \
        sds *redis_request = new sds[2];    \
        redis_request[0] = sdsnew("MULTI"); \
        redis_request[1] = NULL;            \
        index_info.index_update_request_queue->Enqueue(redis_request);

#define END_COMMAND_INTERCEPTOR(fn)                                \
    redis_request = new sds[2];                                    \
    redis_request[0] = sdsnew("EXEC");                             \
    redis_request[1] = NULL;                                       \
    index_info.index_update_request_queue->Enqueue(redis_request); \
    return 0;                                                      \
    }

CSjiboleth *json_parser = newJsonEngine();
CSjiboleth *text_parser = newTextEngine();
static int must_stop = 0;

BEGIN_COMMAND_INTERCEPTOR(indexingHandlerDelCommand)
for (int k = 1; k < argc; k++)
{
    sds key = kfv[k];
    redis_request = new sds[2];
    redis_request[0] = sdsempty();
    redis_request[0] = sdsnew("RXBEGIN ");
    redis_request[0] = sdscat(redis_request[0], key);
    redis_request[1] = NULL;
    index_info.rxbegin_tally++;
    index_info.index_update_request_queue->Enqueue(redis_request);
    redis_request = new sds[2];
    redis_request[0] = sdsempty();
    redis_request[0] = sdsnew("RXCOMMIT ");
    redis_request[0] = sdscat(redis_request[0], key);
    redis_request[1] = NULL;
    index_info.rxcommit_tally++;
    index_info.index_update_request_queue->Enqueue(redis_request);
}
END_COMMAND_INTERCEPTOR(indexingHandlerDelCommand)

BEGIN_COMMAND_INTERCEPTOR(indexingHandlerXDelCommand)
    sds okey = kfv[1];
    for (int k = 2; k < argc; k++)
    {
        sds id = kfv[k];
        sds key = sdscatprintf(sdsempty(), "%s#%s", okey, id);
        sds *redis_request = new sds[2];
        redis_request[0] = sdsempty();
        redis_request[0] = sdscatprintf(redis_request[0], "RXBEGIN %s", key);
        redis_request[1] = NULL;
        index_info.rxbegin_tally++;
        index_info.index_update_request_queue->Enqueue(redis_request);
        redis_request = new sds[2];
        redis_request[0] = sdsempty();
        redis_request[0] = sdscatprintf(redis_request[0], "RXCOMMIT %s", key);
        redis_request[1] = NULL;
        index_info.rxcommit_tally++;
        index_info.index_update_request_queue->Enqueue(redis_request);
    }
END_COMMAND_INTERCEPTOR(indexingHandlerXDelCommand)

BEGIN_COMMAND_INTERCEPTOR(indexingHandlerXaddCommand)
    sds okey = kfv[1];
    // sds key = sdscatprintf(sdsempty(), "%s#%s", okey, id);
    // redis_request = new sds[2];
    // redis_request[0] = sdsempty();
    // redis_request[0] = sdscatprintf(redis_request[0], "RXBEGIN %s", key);
    // redis_request[1] = NULL;
    // index_info.rxbegin_tally++;
    // index_info.index_update_request_queue->Enqueue(redis_request);

    auto *collector = raxNew();
    auto *index_node = RedisClientPool<redisContext>::Acquire(index_info.index_address, index_info.index_port);
    auto *e = new SilNikParowy_Kontekst((char *)index_info.index_address, index_info.index_port, NULL);
    e->Memoize("@@SilNikParowy_Kontekst@@", (void *)e);
    e->Memoize("@@TEXT_PARSER@@", (void *)text_parser);
    e->Memoize("@@collector@@", (void *)collector);

    ParsedExpression *parsed_text;
    sds key_type = streamKey;


    int j = 2;
        sds currentArg = kfv[j];
        if (strcmp(currentArg, "NOMKSTREAM") == 0)
        {
            j++;
        }
        if ((strcmp(currentArg, "MAXLEN") == 0) || (strcmp(currentArg, "MINID") == 0))
        {
            j++;
            currentArg = kfv[j];
            if ((strcmp(currentArg, "=") == 0) || (strcmp(currentArg, "~") == 0))
            {
                j++;
                currentArg = kfv[j];
            }
            j++;
            currentArg = kfv[j + 1];
            if (strcmp(currentArg, "LIMIT") == 0)
            {
                j = j + 2;
            }
        }
            j++;
    while (j < argc)
    {
        currentArg = kfv[j];

        e->Memoize("@@field@@", kfv[j]);
        parsed_text = (ParsedExpression *)parseQ((kfv[j + 1][0] == '{') ? json_parser : text_parser, kfv[j + 1]);
        auto *sub = parsed_text;
        while (sub)
        {
            e->Execute(sub);
            sub = sub->Next();
        }
        delete parsed_text;
        e->Forget("@@field@@");

        j = j + 2;
    }

    TextDialect::FlushIndexables(collector, okey, key_type, index_node);
    RedisClientPool<redisContext>::Release(index_node);
    raxFree(collector);
    e->Forget("@@SilNikParowy_Kontekst@@");
    e->Forget("@@TEXT_PARSER@@");
    e->Forget("@@collector@@");
    e->Reset();

    END_COMMAND_INTERCEPTOR(indexingHandlerXaddCommand)

#ifdef __cplusplus
extern "C"
{
#endif
    struct client *createAOFClient(void);
    static int indexingHandler(sds *kfv);
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
int rxrule_timer_handler(struct aeEventLoop *eventLoop, long long int id, void *clientData)
{
    rxUNUSED(eventLoop);
    rxUNUSED(id);
    rxUNUSED(clientData);

    if(must_stop == 1)
        return -1;

    if (rxrule_fakeClient == NULL)
    {
        rxrule_fakeClient = createAOFClient();
        sds ruleapply = sdsnew("RULE.APPLY");
        rxruleApply = rxCreateStringObject(ruleapply, 10);
        rule_apply_cmd = lookupCommand(ruleapply);
        sdsfree(ruleapply);
    }

    void *argv[2];
    rxAllocateClientArgs(rxrule_fakeClient, (void **)&argv, 2);
    argv[0] = rxruleApply;

    sds *index_request = index_info.index_rxRuleApply_request_queue->Dequeue();
    while (index_request != NULL)
    {
        int segments = 0;
        sds *parts = sdssplitlen(*index_request, sdslen(*index_request), " ", 1, &segments);
        argv[1] = rxCreateStringObject(parts[1], sdslen(parts[1]));
        if (!rule_apply_cmd)
            rule_apply_cmd = lookupCommand((sds)rxGetContainedObject(rxruleApply));
        rxClientExecute(rxrule_fakeClient, rule_apply_cmd);

        rxFreeStringObject(argv[1]);
        // sdsfreesplitres(parts, segments);
        index_info.index_rxRuleApply_respone_queue->Enqueue(index_request);

        index_request = index_info.index_rxRuleApply_request_queue->Dequeue();
    }

    return rxrule_helper_interval;
}

static int indexingHandler(sds *kfv)
{
    int argc = 0;
    for (; kfv[argc]; argc++)
    {
    }

    sds command = kfv[0];
    sds key = kfv[1];
    sdstoupper(command);
    if (strcmp(command, "DEL") == 0)
        return indexingHandlerDelCommand(kfv, argc);
    else if (strcmp(command, "XDEL") == 0)
        return indexingHandlerXDelCommand(kfv, argc);
    else if (strcmp(command, "XADD") == 0)
        return indexingHandlerXaddCommand(kfv, argc);

    auto *collector = raxNew();
    auto *index_node = RedisClientPool<redisContext>::Acquire(index_info.index_address, index_info.index_port);
    auto *e = new SilNikParowy_Kontekst((char *)index_info.index_address, index_info.index_port, NULL);
    e->Memoize("@@SilNikParowy_Kontekst@@", (void *)e);
    e->Memoize("@@TEXT_PARSER@@", (void *)text_parser);
    e->Memoize("@@collector@@", (void *)collector);

    ParsedExpression *parsed_text;
    sds key_type;

    if (argc == 3)
    {
        sds f = sdsnew("*");
        e->Memoize("@@field@@", f);
        key_type = stringKey;
        parsed_text = (ParsedExpression *)parseQ((kfv[2][0] == '{') ? json_parser : text_parser, kfv[2]);
        auto *sub = parsed_text;
        while (sub)
        {
            e->Execute(sub);
            sub = sub->Next();
        }
        delete parsed_text;
        e->Forget("@@field@@");
        sdsfree(f);
    }
    else
    {
        key_type = hashKey;
        int j = 2;
        while (kfv[j])
        {
            e->Memoize("@@field@@", kfv[j]);
            parsed_text = (ParsedExpression *)parseQ((kfv[2][0] == '{') ? json_parser : text_parser, kfv[j + 1]);
            auto *sub = parsed_text;
            while (sub)
            {
                e->Execute(sub);
                sub = sub->Next();
            }
            j += 2;
            delete parsed_text;
            e->Forget("@@field@@");
        }
    }
    TextDialect::FlushIndexables(collector, key, key_type, index_node);
    RedisClientPool<redisContext>::Release(index_node);
    raxFree(collector);
    e->Forget("@@SilNikParowy_Kontekst@@");
    e->Forget("@@TEXT_PARSER@@");
    e->Forget("@@collector@@");
    e->Reset();

    sds *redis_request = new sds[2];
    redis_request[0] = sdsempty();
    redis_request[0] = sdscatprintf(redis_request[0], "RXTRIGGER %s", key);
    redis_request[1] = NULL;
    index_info.rxcommit_tally++;
    index_info.index_update_request_queue->Enqueue(redis_request);
    return C_OK;
}

void freeCompletedRedisRequests()
{
    if(index_info.index_update_respone_queue == NULL)
        return;
    sds *index_request = NULL;
    index_request = index_info.index_update_respone_queue->Dequeue();
    while (index_request != NULL)
    {
        if (index_request[0] != NULL)
        {
            if (strncmp("RXTRIGGER", index_request[0], 9) == 0)
            {
                index_info.index_rxRuleApply_request_queue->Enqueue(index_request);
            }
            else
            {
                freeIndexingRequest(index_request);
            }
        }
        index_request = index_info.index_update_respone_queue->Dequeue();
    }
    if(index_info.index_rxRuleApply_respone_queue != NULL){
        index_request = index_info.index_rxRuleApply_respone_queue->Dequeue();
        while (index_request != NULL)
        {
            freeIndexingRequest(index_request);
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
    // response = sdscatfmt(response, "Number of key_indexing_enqueues:           %i\n", index_info.key_indexing_respone_queue->enqueue_fifo_tally);
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
    auto *duplexer = new RxWaitIndexingDuplexer(index_info.key_indexing_request_queue, &reindex_iterator);
    duplexer->Start(ctx);
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
int reindex_cron(struct aeEventLoop *eventLoop, long long id, void *clientData)
{
    if (must_stop == 1)
        return -1;

    rxUNUSED(eventLoop);
    rxUNUSED(id);
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
        sds key_type = sdsempty();

        switch (rxGetObjectType(dictGetVal(de)))
        {
        case rxOBJ_STRING:
        {
            sds value = (sds)rxGetContainedObject(dictGetVal(de));
            key_type = stringKey;
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
                // printf("K: %s F:%s V:%s\n", key, field, field_value);
                auto *sub = parsed_text;
                while (sub)
                {
                    // printf("k: %s f:%s v:%s\n", key, field, sub->ToString());
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
        auto *index_node = RedisClientPool<redisContext>::Acquire(index_info.index_address, index_info.index_port);
        TextDialect::FlushIndexables(collector, key, key_type, index_node);
        RedisClientPool<redisContext>::Release(index_node);
        engine->Reset();
        raxFree(collector);
        engine->Forget("@@collector@@");

        sds *redis_request = new sds[2];
        redis_request[0] = sdsempty();
        redis_request[0] = sdscatprintf(redis_request[0], "RXTRIGGER %s", key);
        redis_request[1] = NULL;
        index_info.rxcommit_tally++;
        index_info.index_update_request_queue->Enqueue(redis_request);

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

    sds *flushdb_request = new sds[2];
    flushdb_request[0] = sdsempty();
    flushdb_request[0] = sdsnew("FLUSHDB");
    flushdb_request[1] = NULL;
    index_info.index_update_request_queue->Enqueue(flushdb_request);

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

    auto *engine = new SilNikParowy_Kontekst((char *)index_info.index_address, index_info.index_port, NULL);
    engine->Memoize("@@SilNikParowy_Kontekst@@", (void *)engine);
    engine->Memoize("@@TEXT_PARSER@@", (void *)text_parser);

    reindex_helper_cron = rxCreateTimeEvent(1, (aeTimeProc *)reindex_cron, engine, NULL);
    reindex_cron(NULL, 0, engine);

    // serverLog(LL_NOTICE, "ReIndexing started");

    return RedisModule_ReplyWithSimpleString(ctx, response);
}

static void *execIndexerThread(void *ptr)
{
    SimpleQueue *indexing_queue = (SimpleQueue *)ptr;
    indexer_set_thread_title("rxIndexer index keys");
    serverLog(LL_NOTICE, "execIndexerThread started pumping");
    indexing_queue->Started();

    long long start = ustime();
    while (!indexing_queue->must_stop)
    {
        if (ustime() - start >= reindex_cron_slot_time * 1000)
        {
            // Yield when slot time over
            sched_yield();
            start = ustime();
        }
        sds *index_request = indexing_queue->Dequeue();
        while (index_request != NULL)
        {
            indexingHandler(index_request);
            // /**/ printf("x%x #2 key_indexing_request_queue to key_indexing_respone_queue\n", (POINTER)index_request);
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

    // while (!canDequeue(index_info.index_update_request_queue))
    // {
    //     sched_yield();
    // }
    // struct timeval timeout = {30, 500000}; // 1.5 seconds
    serverLog(LL_NOTICE, "\ncapture starting: %s %d\n", index_info.index_address, index_info.index_port);
    redisContext *context;
    context = redisConnect(index_info.index_address, index_info.index_port);
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

        sds *index_request = redis_queue->Dequeue();
        if (index_request != NULL)
        {
            int cmd_size = 0;
            index_info.start_batch_ms = mstime();
            while (index_request != NULL)
            {
                if (strncmp("RXTRIGGER ", index_request[0], 10) != 0)
                {
                    sds cmd = sdsnew(index_request[0]);
                    for (int j = 1; index_request[j]; ++j)
                    {
                        cmd = sdscatfmt(cmd, " \"%s\"", index_request[j]);
                    }
                    ++cmd_size;
                    redisReply *rcc = (redisReply *)redisCommand(context, cmd);
                    if (context->err)
                    {
                        serverLog(LL_NOTICE, "Error: %s on %s", context->errstr, cmd);
                        context->err = 0;
                    }
                    if (rcc)
                    {
                        // serverLog(LL_NOTICE, "Error: %s on rxadd", rcc->str);
                        freeReplyObject(rcc);
                    }
                    sdsfree(cmd);
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
    index_info.index_address = (char *)"127.0.0.1";
    index_info.index_port = 6389;

    REDISMODULE_NOT_USED(argv);

    if (RedisModule_Init(ctx, "rxIndexer", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    // claimParsers();

    if (argc > 0)
    {
        const char *s = RedisModule_StringPtrLen(argv[0], NULL);
        index_info.index_address = sdsdup(sdsnew(s));
    }
    if (argc > 1)
    {
        const char *s = RedisModule_StringPtrLen(argv[1], NULL);
        index_info.index_port = atoi(s);
    }
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
