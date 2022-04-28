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
#include "rxIndexer.h"
#include "../../deps/hiredis/hiredis.h"
#include "indexIntercepts.h"
#include "parser.h"
#include "queryEngine.h"
#include "sentenceProcessor.h"
#include <string.h>

#define stringKey sdsnew("S")
#define hashKey sdsnew("H")
#define streamKey sdsnew("X")
#define noFieldName sdsnew("*")

static Parser *json_parser = NULL;
static Parser *sentence_parser = NULL;

indexerThread index_info = {};

static void indexingHandlerDelCommand(sds *kfv, int argc)
{
    for (int k = 1; k < argc; k++)
    {
        sds key = kfv[k];
        sds *redis_request = zmalloc(sizeof(sds) * 2);
        redis_request[0] = sdsempty();
        redis_request[0] = sdscatprintf(redis_request[0], "RXBEGIN %s", key);
        redis_request[1] = NULL;
        index_info.rxbegin_tally++;
        enqueueSimpleQueue(index_info.index_update_request_queue, redis_request);
        redis_request = zmalloc(sizeof(sds) * 2);
        redis_request[0] = sdsempty();
        redis_request[0] = sdscatprintf(redis_request[0], "RXCOMMIT %s", key);
        redis_request[1] = NULL;
        index_info.rxcommit_tally++;
        enqueueSimpleQueue(index_info.index_update_request_queue, redis_request);
    }
}

static void indexingHandlerXDelCommand(sds *kfv, int argc)
{
    sds okey = kfv[1];
    for (int k = 2; k < argc; k++)
    {
        sds id = kfv[k];
        sds key = sdscatprintf(sdsempty(), "%s#%s", okey, id);
        sds *redis_request = zmalloc(sizeof(sds) * 2);
        redis_request[0] = sdsempty();
        redis_request[0] = sdscatprintf(redis_request[0], "RXBEGIN %s", key);
        redis_request[1] = NULL;
        index_info.rxbegin_tally++;
        enqueueSimpleQueue(index_info.index_update_request_queue, redis_request);
        redis_request = zmalloc(sizeof(sds) * 2);
        redis_request[0] = sdsempty();
        redis_request[0] = sdscatprintf(redis_request[0], "RXCOMMIT %s", key);
        redis_request[1] = NULL;
        index_info.rxcommit_tally++;
        enqueueSimpleQueue(index_info.index_update_request_queue, redis_request);
    }
}

static void indexingHandlerXaddCommand(sds *kfv, int argc)
{
    sds okey = kfv[1];
    sds id = kfv[2];
    sds key = sdscatprintf(sdsempty(), "%s#%s", okey, id);
    sds *redis_request = zmalloc(sizeof(sds) * 2);
    redis_request[0] = sdsempty();
    redis_request[0] = sdscatprintf(redis_request[0], "RXBEGIN %s", key);
    redis_request[1] = NULL;
    index_info.rxbegin_tally++;
    enqueueSimpleQueue(index_info.index_update_request_queue, redis_request);

    int j = 3;
    while (j < argc)
    {
        sds currentArg = kfv[j];
        sdstoupper(currentArg);
        if (strcmp(currentArg, "NOMKSTREAM") == 0)
        {
            j++;
            continue;
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
            continue;
        }

        if (kfv[j + 1][0] == '{')
        {
            parseJson(json_parser, (const char *)kfv[j + 1]);
            Token *t1 = NULL, *t2 = NULL, *t3 = NULL;
            listIter *li = listGetIterator(json_parser->rpn, AL_START_HEAD);
            listNode *ln;
            while ((ln = listNext(li)))
            {
                t1 = t2;
                t2 = t3;
                t3 = ln->value;
                while (strcmp(t3->token, ",") == 0)
                {
                    ln = listNext(li);
                    if (!ln)
                        break;
                    t3 = ln->value;
                }
                if (!t3)
                    break;
                if (strcmp(t3->token, ":") == 0)
                {
                    tokenizeValue(key, streamKey, sdsnew(t1->token), sdsnew(t2->token));
                }
            }
            listReleaseIterator(li);
            resetParser(json_parser);
        }
        else
        {
            tokenizeValue(key, streamKey, kfv[j], kfv[j + 1]);
        }
        j = j + 2;
    }

    redis_request = zmalloc(sizeof(sds) * 2);
    redis_request[0] = sdsempty();
    redis_request[0] = sdscatprintf(redis_request[0], "RXCOMMIT %s", key);
    redis_request[1] = NULL;
    index_info.rxcommit_tally++;
    enqueueSimpleQueue(index_info.index_update_request_queue, redis_request);
}

extern struct client *createAOFClient(void);

int rxrule_helper_interval = 100;
long long rxrule_helper_cron = -1;
struct client *rxrule_fakeClient = NULL;
struct redisCommand *rule_apply_cmd;
robj *rxruleApply = NULL;
/* This is the rule.apply timer handler.
 * An apply entry is scheduled after an rxcommit to
 * update all business rules. */
int rxrule_timer_handler(struct aeEventLoop *eventLoop, long long int id, void *clientData) {
    rxUNUSED(eventLoop);
    rxUNUSED(id);
    rxUNUSED(clientData);

    if(!rxrule_fakeClient){
        rxrule_fakeClient =  createAOFClient();
        rxruleApply = createStringObject("RULE.APPLY", 10);
        rule_apply_cmd = lookupCommand(rxruleApply->ptr);
    }

    robj *argv[2];
    rxrule_fakeClient->argc = 2;
    rxrule_fakeClient->argv = (robj **)&argv;
    argv[0] = rxruleApply;

    while (canDequeueSimpleQueue(index_info.index_rxRuleApply_request_queue))
    {
        sds *index_request = dequeueSimpleQueue(index_info.index_rxRuleApply_request_queue);
        int segments = 0;
        sds *parts = sdssplitlen(*index_request, sdslen(*index_request), " ", 1, &segments);
        argv[1] = createStringObject(parts[1], sdslen(parts[1]));
        if(!rule_apply_cmd)
                rule_apply_cmd = lookupCommand(rxruleApply->ptr);
        rxrule_fakeClient->cmd = rule_apply_cmd;
        rule_apply_cmd->proc(rxrule_fakeClient);
        freeStringObject(argv[1]);
        sdsfreesplitres(parts, segments);
        enqueueSimpleQueue(index_info.index_rxRuleApply_respone_queue, index_request);
    }

    return rxrule_helper_interval;
}

static void indexingHandler(sds *kfv)
{
    int argc = 0;
    for (; kfv[argc]; argc++)
        ;

    sds command = kfv[0];
    sds key = kfv[1];
    sdstoupper(command);
    if (strcmp(command, "DEL") == 0)
    {
        indexingHandlerDelCommand(kfv, argc);
        return;
    }
    else if (strcmp(command, "XDEL") == 0)
    {
        indexingHandlerXDelCommand(kfv, argc);
        return;
    }
    else if (strcmp(command, "XADD") == 0)
    {
        indexingHandlerXaddCommand(kfv, argc);
        return;
    }

    sds *redis_request = zmalloc(sizeof(sds) * 2);
    redis_request[0] = sdsempty();
    redis_request[0] = sdscatprintf(redis_request[0], "RXBEGIN %s", key);
    redis_request[1] = NULL;
    index_info.rxbegin_tally++;
    enqueueSimpleQueue(index_info.index_update_request_queue, redis_request);

    if (argc == 3)
    {
        if (kfv[2][0] == '{')
        {
            parseJson(json_parser, (const char *)kfv[2]);
            Token *t1 = NULL, *t2 = NULL, *t3 = NULL;
            listIter *li = listGetIterator(json_parser->rpn, AL_START_HEAD);
            listNode *ln;
            while ((ln = listNext(li)))
            {
                t1 = t2;
                t2 = t3;
                t3 = ln->value;
                while (strcmp(t3->token, ",") == 0)
                {
                    ln = listNext(li);
                    if (!ln)
                        break;
                    t3 = ln->value;
                }
                if (!t3)
                    break;
                if (strcmp(t3->token, ":") == 0)
                {
                    tokenizeValue(key, stringKey, sdsnew(t1->token), sdsnew(t2->token));
                }
            }
            listReleaseIterator(li);
            resetParser(json_parser);
        }
        else
            tokenizeValue(key, stringKey, noFieldName, kfv[2]);
    }
    else
    {
        int j = 2;
        while (kfv[j])
        {
            tokenizeValue(key, hashKey, kfv[j], kfv[j + 1]);
            j += 2;
        }
    }

    redis_request = zmalloc(sizeof(sds) * 2);
    redis_request[0] = sdsempty();
    redis_request[0] = sdscatprintf(redis_request[0], "RXCOMMIT %s", key);
    redis_request[1] = NULL;
    index_info.rxcommit_tally++;
    enqueueSimpleQueue(index_info.index_update_request_queue, redis_request);
}

void freeCompletedRedisRequests()
{
    while (canDequeueSimpleQueue(index_info.index_update_respone_queue))
    {
        sds *index_request = dequeueSimpleQueue(index_info.index_update_respone_queue);
        if(strncmp("RXCOMMIT", *index_request, 8) == 0){
            enqueueSimpleQueue(index_info.index_rxRuleApply_request_queue, index_request);
        }else{
            freeIndexingRequest(index_request);
        }
    }
    while (canDequeueSimpleQueue(index_info.index_rxRuleApply_respone_queue))
    {
        sds *index_request = dequeueSimpleQueue(index_info.index_rxRuleApply_respone_queue);
        freeIndexingRequest(index_request);
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
    response = sdscatfmt(response, "Number of indexing queued requests:        %i\n", index_info.key_indexing_request_queue->fifo->len);
    response = sdscatfmt(response, "Number of queued redis requests:           %i\n", index_info.index_update_request_queue->fifo->len);
    response = sdscatfmt(response, "Number of RXBEGIN commands:                %i\n", index_info.rxbegin_tally);
    response = sdscatfmt(response, "Number of RXADD commands:                  %i\n", index_info.rxadd_tally);
    response = sdscatfmt(response, "Number of RXCOMMIT commands:               %i\n", index_info.rxcommit_tally);
    response = sdscatfmt(response, "Number of completed requests:              %i\n", index_info.key_indexing_respone_queue->fifo->len);
    response = sdscatfmt(response, "Number of key_indexing_enqueues:           %i\n", index_info.key_indexing_respone_queue->enqueue_fifo_tally);
    response = sdscatfmt(response, "Number of key_indexing_dequeues:           %i\n", index_info.key_indexing_respone_queue->dequeue_fifo_tally);
    response = sdscatfmt(response, "Number of completed Redis requests:        %i\n", index_info.index_update_request_queue->fifo->len);
    response = sdscatfmt(response, "Number of redis_enqueues:                  %i\n", index_info.index_update_request_queue->enqueue_fifo_tally);
    response = sdscatfmt(response, "Number of redis_dequeues:                  %i\n", index_info.index_update_request_queue->dequeue_fifo_tally);
    response = sdscatfmt(response, "Number of REDIS commands:                  %i\n", dictSize(server.commands));
    // response = sdscatfmt(response, "Number of REDIS commands:                  %i, intercepts: %i\n", dictSize(server.commands), sizeof(interceptorCommandTable) / sizeof(struct redisCommand));
    response = sdscatfmt(response, "Number of Field index comm                 %i\n", index_info.field_index_tally);

    return RedisModule_ReplyWithSimpleString(ctx, response);
}

static char indexer_state = 0;
long long reindex_helper_cron = -1;

int indexerControl(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc == 2)
    {
        size_t arg_len;
        sds arg = sdsnew((char *)RedisModule_StringPtrLen(argv[1], &arg_len));
        sdstoupper(arg);
        if (sdscmp(arg, sdsnew("ON")) == 0)
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

int reindex_cron_slot_time = 20;
int reindex_cron_interval_time = 20;
dictIterator *reindex_iterator = NULL;
long long reindex_total_no_of_keys = 0;
long long reindex_processed_no_of_keys = 0;
long long reindex_skipped_no_of_keys = 0;
long long reindex_yielded_no_of_keys = 0;
long long reindex_qthrottleded_no_of_keys = 0;
long long reindex_no_of_slots = 0;
long long reindex_starttime = 0;
long long reindex_latency = 0;
/* This is the timer handler that is called by the main event loop. We schedule
 * this timer to be called when the nearest of our module timers will expire. */
int reindex_cron(struct aeEventLoop *eventLoop, long long id, void *clientData)
{
    rxUNUSED(eventLoop);
    rxUNUSED(id);
    rxUNUSED(clientData);

    if (!reindex_iterator)
    {
        return -1;
    }

    reindex_no_of_slots++;
    long long start = ustime();
    dictEntry *de;
    while ((de = dictNext(reindex_iterator)) != NULL)
    {
        reindex_processed_no_of_keys++;
        sds key = dictGetKey(de);
        if (*key == '^')
        {
            reindex_skipped_no_of_keys++;
            continue;
        }
        robj *value = dictGetVal(de);
        switch (value->type)
        {
        case OBJ_STRING:
            tokenizeValue(key, stringKey, noFieldName, value->ptr);
            break;
        case OBJ_LIST:
            serverLog(LL_NOTICE, "ReIndexing unexpected type");
            break;
        case OBJ_HASH:
        {
            hashTypeIterator *hi = hashTypeInitIterator(value);
            while (hashTypeNext(hi) != C_ERR)
            {
                sds field = hashTypeCurrentObjectNewSds(hi, OBJ_HASH_KEY);
                sds field_value = hashTypeCurrentObjectNewSds(hi, OBJ_HASH_VALUE);
                tokenizeValue(key, hashKey, field, field_value);
            }
            hashTypeReleaseIterator(hi);
        }
        break;
        default:
            serverLog(LL_NOTICE, "ReIndexing unexpected type");
            break;
        }
        if (ustime() - start >= reindex_cron_slot_time * 1000)
        {
            // Yield when slot time over
            reindex_yielded_no_of_keys++;
            return reindex_cron_interval_time;
        }
        if (dequeueSimpleQueueLength(index_info.index_update_request_queue) > 100000)
        {
            // Yield when slot time over
            reindex_qthrottleded_no_of_keys++;
            return reindex_cron_interval_time * 25;
        }
    }
    dictReleaseIterator(reindex_iterator);
    reindex_iterator = NULL;
    reindex_latency = ustime() - reindex_starttime;
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
            response = sdscatfmt(response, "ReIndexing stats:\n");
            response = sdscatfmt(response, "Total no of keys:           %i\n", reindex_total_no_of_keys);
            response = sdscatfmt(response, "Total no of keys processed: %i\n", reindex_processed_no_of_keys);
            response = sdscatfmt(response, "Total no of keys skipped:   %i\n", reindex_skipped_no_of_keys);
            response = sdscatfmt(response, "\n");
            response = sdscatfmt(response, "Total reindexing time:      %ims\n", reindex_latency / 1000);
            response = sdscatfmt(response, "\n");
            response = sdscatfmt(response, "Total no of timer slots:    %i\n", reindex_no_of_slots);
            response = sdscatfmt(response, "Total no of timer expires:  %i\n", reindex_yielded_no_of_keys);
            response = sdscatfmt(response, "Total no of queue throttles:%i\n", reindex_qthrottleded_no_of_keys);
            return RedisModule_ReplyWithSimpleString(ctx, response);
        }
    }

    REDISMODULE_NOT_USED(argv);
    if (!indexer_state)
        return RedisModule_ReplyWithSimpleString(ctx, "Indexer NOT active.");

    if (reindex_iterator)
        return RedisModule_ReplyWithSimpleString(ctx, "ReIndexing already started.");

    sds *flushdb_request = zmalloc(sizeof(sds) * 2);
    flushdb_request[0] = sdsempty();
    flushdb_request[0] = sdsnew("FLUSHDB");
    flushdb_request[1] = NULL;
    enqueueSimpleQueue(index_info.index_update_request_queue, flushdb_request);

    // original_indexer_state = indexer_state;
    // uninstallIndexerInterceptors();
    // indexer_state = 0;

    reindex_iterator = dictGetSafeIterator(server.db[0].dict);
    reindex_total_no_of_keys = dictSize(server.db[0].dict);
    reindex_processed_no_of_keys = 0;
    reindex_skipped_no_of_keys = 0;
    reindex_yielded_no_of_keys = 0;
    reindex_qthrottleded_no_of_keys = 0;
    reindex_starttime = ustime();
    reindex_no_of_slots = 0;
    reindex_helper_cron = aeCreateTimeEvent(server.el, 1, (aeTimeProc *)reindex_cron, NULL, NULL);
    reindex_cron(NULL, 0, NULL);

    serverLog(LL_NOTICE, "ReIndexing started");

    return RedisModule_ReplyWithSimpleString(ctx, response);
}

static int must_stop = 0;
static void *execIndexerThread(void *ptr)
{
    rxUNUSED(ptr);
    serverLog(LL_NOTICE, "execIndexerThread started pumping");
    while (must_stop == 0)
    {
        while (canDequeueSimpleQueue(index_info.key_indexing_request_queue)) //(index_info.key_indexing_fifo->len > 0)
        {
            // if (index_info.index_update_fifo->len > 10000)
            //     break;

            sds *index_request = dequeueSimpleQueue(index_info.key_indexing_request_queue);
            if (index_request != NULL)
            {
                indexingHandler(index_request);
                // /**/ printf("x%x #2 key_indexing_request_queue to key_indexing_respone_queue\n", (POINTER)index_request);
                enqueueSimpleQueue(index_info.key_indexing_respone_queue, index_request);
            }
        }
        freeCompletedRedisRequests();
        sched_yield();
    }
    rxrule_helper_interval = 0;
    return NULL;
}

static void *execUpdateRedisThread(void *ptr)
{
    rxUNUSED(ptr);

    while (!canDequeueSimpleQueue(index_info.index_update_request_queue))
    {
        sched_yield();
    }
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
    redisReply *rcc = redisCommand(context, "PING");
    if (rcc)
    {

        serverLog(LL_NOTICE, "............ execUpdateRedisThread ... 050 .... %s", rcc->str);
        freeReplyObject(rcc);
    }
    else
        serverLog(LL_NOTICE, "............ execUpdateRedisThread ... 051 .... NO PING Reply");

    serverLog(LL_NOTICE, "execUpdateRedisThread started pumping");

    while (must_stop == 0)
    {
        if (canDequeueSimpleQueue(index_info.index_update_request_queue))
        {
            // redisCommand(context, "MULTI");
            int batch_size = 1;
            int cmd_size = 0;
            index_info.start_batch_ms = mstime();
            while ((canDequeueSimpleQueue(index_info.index_update_request_queue)) && batch_size-- > 0)
            {
                sds *index_request = dequeueSimpleQueue(index_info.index_update_request_queue);
                sds cmd = sdsnew(index_request[0]);
                for (int j = 1; index_request[j]; ++j)
                {
                    cmd = sdscatfmt(cmd, " \"%s\"", index_request[j]);
                }
                ++cmd_size;
                redisReply *rcc = redisCommand(context, cmd);
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
                enqueueSimpleQueue(index_info.index_update_respone_queue, index_request);
                long long now = mstime();
                if ((now - index_info.start_batch_ms) > 100)
                {
                    // if(index_info.key_indexing_fifo->len == 0 && index_info.index_update_fifo->len == 0 && batch_size > 0){
                    break;
                }
            }
            // if(cmd_size == 0){
            //     sleep(125);
            // }

            // rcc = redisCommand(context, "EXEC");
            // if (rcc)
            // {
            //     if (rcc->len > 0)
            //         serverLog(LL_NOTICE, "Error: %s on EXEC", rcc->str);
            //     freeReplyObject(rcc);
            // }
            // serverLog(LL_NOTICE, "Dequeue: %i indexing requests: %li, redis requests: %li RXADD commands: %i", index_info.set_tally, index_info.key_indexing_fifo->len, index_info.index_update_fifo->len, index_info.rxadd_tally);
        }
        else
            sched_yield();
    }
    return NULL;
}

static void startIndexerThreads()
{
    indexerThread *t = &index_info;

    t->key_indexing_request_queue = newSimpleQueueAsync(execIndexerThread, 4);
    t->key_indexing_respone_queue = newSimpleQueue();
    t->index_update_request_queue = newSimpleQueueAsync(execUpdateRedisThread, 1);
    t->index_update_respone_queue = newSimpleQueue();
    t->index_rxRuleApply_request_queue = newSimpleQueue(); 
    t->index_rxRuleApply_respone_queue = newSimpleQueue();

    rxrule_helper_interval = 100;
    rxrule_helper_cron = aeCreateTimeEvent(server.el, 1, (aeTimeProc *)rxrule_timer_handler, NULL, NULL);
    int i = rxrule_timer_handler(NULL, 0, NULL);
    serverLog(LL_NOTICE, "RX Rule cron started: %lld interval=%d", rxrule_helper_cron, i);
}

static void stopIndexerThreads()
{
    indexerThread *t = &index_info;
    must_stop = 1;

    releaseSimpleQueue(t->key_indexing_request_queue);
    releaseSimpleQueue(t->key_indexing_respone_queue);
    releaseSimpleQueue(t->index_update_request_queue);
    releaseSimpleQueue(t->index_update_respone_queue);
    releaseSimpleQueue(t->index_rxRuleApply_request_queue);
    releaseSimpleQueue(t->index_rxRuleApply_respone_queue);

    serverLog(LL_NOTICE, "Indexer stopped");
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    initRxSuite();
    index_info.index_address = "127.0.0.1";
    index_info.index_port = 6389;

    REDISMODULE_NOT_USED(argv);

    if (RedisModule_Init(ctx, "rxIndexer", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    claimParsers();

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

    if (!json_parser)
        json_parser = newParser("json");
    if (!sentence_parser)
        sentence_parser = newParser("text");

    startIndexerThreads();

    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx)
{
    rxUNUSED(ctx);
    if (!json_parser)
        freeParser(json_parser);
    if (!sentence_parser)
        freeParser(sentence_parser);

    stopIndexerThreads();
    sdsfree(stringKey);
    sdsfree(noFieldName);

    uninstallIndexerInterceptors();
    indexer_state = 0;

    releaseParsers();
    finalizeRxSuite();
    return REDISMODULE_OK;
}

char *toLowerCase(char *pString)
{
    char *start = (char *)pString;
    while (*pString)
    {
        *pString = tolower(*pString);
        ++pString;
    }
    return start;
}

char *strdup(const char *s);

void tallyValues(int caller, dict *tokens, sds tokenValue)
{
    rxUNUSED(caller);
    sds key = tokenValue;
    toLowerCase(tokenValue);
    dictEntry *token = dictFind(tokens, key);
    if (token)
    {
        long *tally = (long *)(long *)token->v.val;
        (*tally)++;
    }
    else
    {
        long *tally = (long *)zmalloc(sizeof(long));
        dictAdd(tokens, (void *)sdsdup(key), tally);
        *tally = 1L;
    }
    sdsfree(key);
}

void updateIndex(sds objectKey, char *keyType, sds fieldName, sds tokenValue, double confidence)
{
    toLowerCase(fieldName);
    toLowerCase(tokenValue);

    sds *redis_request = zmalloc(sizeof(sds) * 2);
    redis_request[0] = sdsempty();
    redis_request[0] = sdscatprintf(redis_request[0], "RXADD %s %s %s %s %f 0", objectKey, keyType, fieldName, tokenValue, confidence /*, index_info.database_id ?? "0"*/);
    redis_request[1] = NULL;
    index_info.rxadd_tally++;
    setDirty();
    enqueueSimpleQueue(index_info.index_update_request_queue, redis_request);
}

void tokenizeValue(sds objectKey, sds keyType, sds fieldName, sds fieldValue)
{
    sds special_field = sdsnew("~~~key~~~");
    if (sdscmp(fieldName, special_field) != 0)
    {
        tokenizeValue(objectKey, keyType, special_field, objectKey);
    }
    sdsfree(special_field);
    rxUNUSED(keyType);
    index_info.field_index_tally++;

    if (!fieldValue || sdslen(fieldValue) <= 0)
        return;
    dict *tokens = dictCreate(&tokenDictType, (void *)NULL);
    int valueTally = 0;

    parseSentence(sentence_parser, fieldValue);
    int sentenceStart = 0;
    int sentenceEnd = 0;
    while ((sentenceEnd = getSentenceEnd(sentence_parser, sentenceStart)) > 0)
    {
        int subSentenceStart = 0;
        int subSentenceEnd = 0;
        while ((subSentenceEnd = getSubSentenceEnd(sentence_parser, subSentenceStart)) > 0)
        {
            int qualifierStart = subSentenceStart;
            int qualifierEnd = getClassifierEnd(sentence_parser, qualifierStart);
            if (qualifierEnd > qualifierStart)
            {
                sds qualifier = sdsempty();
                char *sep = "";
                for (int j = qualifierStart; j < qualifierEnd; j++)
                {
                    const char *token = getSentenceToken(sentence_parser, j);
                    qualifier = sdscatfmt(qualifier, "%s%s", sep, token);
                    sep = " ";
                }
                int listStart = qualifierEnd + 1;
                int listEnd = getListEnd(sentence_parser, listStart);
                if (listEnd > listStart)
                {
                    float listCount = (listStart + listEnd + 1) / 2;
                    for (int j = listStart; j < listEnd; j += 2)
                    {
                        const char *token = getSentenceToken(sentence_parser, j);
                        updateIndex(objectKey, keyType, qualifier, sdsnew(token), 1.0 / listCount);
                    }
                    subSentenceEnd = listEnd;
                }
                else
                {
                    float listCount = (subSentenceEnd - subSentenceStart);
                    for (int j = subSentenceStart; j < subSentenceEnd; j++)
                    {
                        const char *token = getSentenceToken(sentence_parser, j);
                        updateIndex(objectKey, keyType, qualifier, sdsnew(token), 1.0 / listCount);
                        valueTally++;
                    }
                }
            }
            else
            {
                int listStart = subSentenceStart;
                int listEnd = getListEnd(sentence_parser, listStart);
                if (listEnd > listStart)
                {
                    float listCount = (subSentenceEnd - subSentenceStart);
                    for (int j = subSentenceStart; j < subSentenceEnd; j++)
                    {
                        const char *token = getSentenceToken(sentence_parser, j);
                        updateIndex(objectKey, keyType, fieldName, sdsnew(token), 1.0 / listCount);
                        valueTally++;
                    }
                }
                else
                {
                    for (int j = subSentenceStart; j < subSentenceEnd; j++)
                    {
                        const char *token = getSentenceToken(sentence_parser, j);
                        tallyValues(101, tokens, (char *)token);
                        valueTally++;
                    }
                }
            }
            subSentenceStart = subSentenceEnd + 1;
        }
        sentenceStart = subSentenceStart;
    }
    resetParser(sentence_parser);
    dictIterator *iter = dictGetIterator(tokens);
    dictEntry *token = dictNext(iter);
    while (token)
    {
        long *tally = (long *)(long *)token->v.val;
        updateIndex(objectKey, keyType, fieldName, (char *)token->key, (*tally * 1.0) / valueTally);
        zfree(tally);
        token->v.val = NULL;
        token = dictNext(iter);
    }
    dictReleaseIterator(iter);
    dictRelease(tokens);
}
