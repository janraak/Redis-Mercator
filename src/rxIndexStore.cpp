/* rxIndexStore -- An example of modules dictionary API
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
#include "rxIndexStore.hpp"
/*
 * TODO:
 * 1) Add A/V and V/A indices (rax trees) for rxIndexStore Performance improvements.
 *    a) A rxFetch %value% can be optimized using a raxTree.
 *    b) A rxFetch %value%* can be optimized using a raxTree.
 *    c) A rxFetch *%value%[*] will require a database scan
 *    d) A rxFetch %value% %field% can be optimized using a raxTree.
 *    e) A rxFetch %value%* %field% can be optimized using a raxTree.
 *    f) A rxFetch *%value%[*]  %field% will require a database scan
 * 
 */
#ifdef __cplusplus
extern "C"
{
#endif
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
#ifdef __cplusplus
}
#endif

/*
 * This module execute rx query commands
 */
#include "../../deps/hiredis/hiredis.h"
#include "sdsWrapper.h"

#include "rxFetch-multiplexer.hpp"
#include "rxDescribe-multiplexer.hpp"
#include "rxIndex.hpp"

static Mercator_Index *mercator_index;
void initComparisonsStatic();
// extern int keyIsExpired(redisDb *db, robj *key);

  FILE *rxadd_log;

/*
 * RXFETCH implementation
 *
 * 1) Block the client
 * 2) start CRON to handle RXFETCH
 *    a) Execute for %slot_size% milliseconds
 *    b) Pause   for %defer% milliseconds
 * 
 *    This will allow multiple RXFETCH (and other supported commands 
 *    to be executed in partial and will enhance thruput).
 * 
 * 3) Unblock the client while done.
 * 
 * 
 */

/*
 * Fetch members for value or value/field
 * 
 * Values may be in different fields if so the union is returned.
 * A value range could be specified if so the union is returned.
 * 
 * 
 */
int rx_fetch(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    int dbId = RedisModule_GetSelectedDb(ctx);
    rxString v;
    rxString f;
    rxString op;
    RxFetchMultiplexer *multiplexer;
    switch (argc)
    {
    case VALUE_ONLY:
        v = (const char *)rxGetContainedObject(argv[1]);
        // f = rxStringNew("*");
        multiplexer = new RxFetchMultiplexer(argc, dbId, v);
        // rxStringFree(f);
        break;
    case FIELD_AND_VALUE_ONLY:
        v = (const char *)rxGetContainedObject(argv[1]);
        f = (const char *)rxGetContainedObject(argv[2]);
        multiplexer = new RxFetchMultiplexer(argc, dbId, v, f);
        break;
    case FIELD_OP_VALUE:{
        v = (const char *)rxGetContainedObject(argv[1]);
        f = (const char *)rxGetContainedObject(argv[2]);
        op = (const char *)rxGetContainedObject(argv[3]);
        rxComparisonProc compare  = rxFindComparisonProc(op);
        if(compare == NULL)
            return RedisModule_ReplyWithError(ctx, "Invalid operator command! Syntax: rxFetch %value% [%field%] [ = | == | > | < | <= | >= | != ]");
        multiplexer = new RxFetchMultiplexer(argc, dbId, v, f, compare);
        }
        break;
    default:
        return RedisModule_ReplyWithError(ctx, "Invalid command! Syntax: rxFetch %value% [%field%]");
    }
    multiplexer->Start(ctx);
    return REDISMODULE_OK;
}

int rx_describe(struct RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    int dbId = RedisModule_GetSelectedDb(ctx);
    rxString v;
    rxString f;
    RxDescribeMultiplexer *multiplexer;
    switch (argc)
    {
    case VALUE_ONLY:
        v = (const char *)rxGetContainedObject(argv[1]);
        f = rxStringNew("*");
        multiplexer = new RxDescribeMultiplexer(dbId, v, f);
        rxStringFree(f);
        break;
    case FIELD_AND_VALUE_ONLY:
        v = (const char *)rxGetContainedObject(argv[1]);
        f = (const char *)rxGetContainedObject(argv[2]);
        multiplexer = new RxDescribeMultiplexer(dbId, v, f);
        break;
    default:
        return RedisModule_ReplyWithError(ctx, "Invalid command! Syntax: rxDescribe %value% [%field%]");
    }
    multiplexer->Start(ctx);
    return REDISMODULE_OK;
}

int rxadd_cntr = 0;
int rx_add(struct RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
        // "RXADD %s %s %s %s %d", objectKey, keyType, fieldName, tokenValue, confidence

    if(argc <= 5)
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    rxString objectKey = (const char *)rxGetContainedObject(argv[1]);
    rxString keyType = (const char *)rxGetContainedObject(argv[2]);
    rxString fieldName = (const char *)rxGetContainedObject(argv[3]);
    rxString tokenValue = (const char *)rxGetContainedObject(argv[4]);
    rxString confidence = (const char *)rxGetContainedObject(argv[5]);
    int dbId = RedisModule_GetSelectedDb(ctx);

    rxString valueIndexkey = rxStringFormat("_zx_:%s:%s", tokenValue, fieldName);
    // TODO: Forlater
    // rxString vakey = rxStringFormat("%s:%s", tokenValue, fieldName);
    // rxString avkey = rxStringFormat("%s:%s", fieldName, tokenValue);
    rxString objectReference = rxStringFormat("%s\t%s", objectKey, keyType);
    rxString objectIndexkey = rxStringFormat("_ox_:%s", objectKey);

    rxAddSortedSetMember(valueIndexkey, dbId, atof(confidence), objectReference);
    rxAddSetMember(objectIndexkey, dbId, valueIndexkey);

    rxStringFree(valueIndexkey);
    rxStringFree(objectReference);
    
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}
int rx_del(struct RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
        // "RXADD %s %s %s %s %d", objectKey, keyType, fieldName, tokenValue, confidence

    if(argc <= 4)
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    rxString objectKey = (const char *)rxGetContainedObject(argv[1]);
    rxString keyType = (const char *)rxGetContainedObject(argv[2]);
    rxString fieldName = (const char *)rxGetContainedObject(argv[3]);
    rxString tokenValue = (const char *)rxGetContainedObject(argv[4]);
    int dbId = RedisModule_GetSelectedDb(ctx);

    rxString valueIndexkey = rxStringFormat("_zx_:%s:%s", tokenValue, fieldName);
    // TODO: Forlater
    // rxString vakey = rxStringFormat("%s:%s", tokenValue, fieldName);
    // rxString avkey = rxStringFormat("%s:%s", fieldName, tokenValue);
    rxString objectReference = rxStringFormat("%s\t%s", objectKey, keyType);
    rxString objectIndexkey = rxStringFormat("_ox_:%s", objectKey);

    rxDeleteSortedSetMember(valueIndexkey, dbId, objectReference);
    rxDeleteSetMember(objectIndexkey, dbId, valueIndexkey);

    rxStringFree(valueIndexkey);
    rxStringFree(objectReference);
    
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int rx_begin_key(struct RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    rxUNUSED(argc);
    int dbId = RedisModule_GetSelectedDb(ctx);
    rxString key = (const char *)rxGetContainedObject(argv[1]);
    mercator_index->Open_Key(key, dbId);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int rx_commit_key(struct RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    rxUNUSED(argc);
    int dbId = RedisModule_GetSelectedDb(ctx);
    rxString key = (const char *)rxGetContainedObject(argv[1]);
    mercator_index->Commit_Key(key, dbId);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int rx_rollback_key(struct RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    rxUNUSED(argc);
    int dbId = RedisModule_GetSelectedDb(ctx);
    rxString key = (const char *)rxGetContainedObject(argv[1]);
    mercator_index->Rollback_Key(key, dbId);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **, int)
{
    rxServerLog(rxLL_NOTICE, "OnLoad rxIndexStore.");

    if (RedisModule_Init(ctx, "rxIndexStore", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR){
        rxServerLog(rxLL_NOTICE, "OnLoad rxIndexStore. Init error!");
        return REDISMODULE_ERR;
    }

    rxInitComparisonsProcs();
    rxServerLog(rxLL_NOTICE, "OnLoad rxIndexStore. Comparers initialized");

    if (RedisModule_CreateCommand(ctx, "rxFetch",
                                  rx_fetch, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "rxAdd",
                                  rx_add, "write", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;


    if (RedisModule_CreateCommand(ctx, "rxDel",
                                  rx_del, "write", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "rxBegin",
                                  rx_begin_key, "write", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "rxCommit",
                                  rx_commit_key, "write", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "rxRollback",
                                  rx_rollback_key, "write", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "rxDescribe",
                                  rx_describe, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    rxServerLog(rxLL_NOTICE, "OnLoad rxIndexStore. Commands added");
    rxadd_log = fopen ("/tmp/rxadd.log", "w");
    rxServerLog(rxLL_NOTICE, "OnLoad rxIndexStore. Logfile prepared");
    mercator_index = new Mercator_Index();
    rxServerLog(rxLL_NOTICE, "OnLoad rxIndexStore. Done");
    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx)
{
    delete mercator_index;
    mercator_index = NULL;
    fclose(rxadd_log);
    rxadd_log = NULL;
    REDISMODULE_NOT_USED(ctx);
    return REDISMODULE_OK;
}
