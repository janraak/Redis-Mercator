/* rxFetch -- An example of modules dictionary API
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
#include "rxFetch.hpp"
/*
 * TODO:
 * 1) Add A/V and V/A indices (rax trees) for rxFetch Performance improvements.
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
// #include "parser.h"
#include "../../deps/hiredis/hiredis.h"
// #include <queryEngine.h>

#include "rxFetch-duplexer.hpp"
#include "rxDescribe-duplexer.hpp"
#include "rxIndex.hpp"

static rax *ComparisonsMap = NULL;
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
    sds v;
    sds f;
    sds op;
    RxFetchDuplexer *duplexer;
    switch (argc)
    {
    case VALUE_ONLY:
        v = (char *)rxGetContainedObject(argv[1]);
        // f = sdsnew("*");
        duplexer = new RxFetchDuplexer(argc, dbId, v);
        // sdsfree(f);
        break;
    case FIELD_AND_VALUE_ONLY:
        v = (char *)rxGetContainedObject(argv[1]);
        f = (char *)rxGetContainedObject(argv[2]);
        duplexer = new RxFetchDuplexer(argc, dbId, v, f);
        break;
    case FIELD_OP_VALUE:{
        v = (char *)rxGetContainedObject(argv[1]);
        f = (char *)rxGetContainedObject(argv[2]);
        op = (char *)rxGetContainedObject(argv[3]);
        comparisonProc *compare  = (comparisonProc *)raxFind(ComparisonsMap, (UCHAR *)op, sdslen(op));
        if(compare == raxNotFound)
            return RedisModule_ReplyWithError(ctx, "Invalid operator command! Syntax: rxFetch %value% [%field%] [ = | == | > | < | <= | >= | != ]");
        duplexer = new RxFetchDuplexer(argc, dbId, v, f, compare);
        }
        break;
    default:
        return RedisModule_ReplyWithError(ctx, "Invalid command! Syntax: rxFetch %value% [%field%]");
    }
    duplexer->Start(ctx);
    return REDISMODULE_OK;
}

int rx_describe(struct RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    int dbId = RedisModule_GetSelectedDb(ctx);
    sds v;
    sds f;
    RxDescribeDuplexer *duplexer;
    switch (argc)
    {
    case VALUE_ONLY:
        v = (char *)rxGetContainedObject(argv[1]);
        f = sdsnew("*");
        duplexer = new RxDescribeDuplexer(dbId, v, f);
        sdsfree(f);
        break;
    case FIELD_AND_VALUE_ONLY:
        v = (char *)rxGetContainedObject(argv[1]);
        f = (char *)rxGetContainedObject(argv[2]);
        duplexer = new RxDescribeDuplexer(dbId, v, f);
        break;
    default:
        return RedisModule_ReplyWithError(ctx, "Invalid command! Syntax: rxDescribe %value% [%field%]");
    }
    duplexer->Start(ctx);
    return REDISMODULE_OK;
}

int rxadd_cntr = 0;
int rx_add(struct RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
        // "RXADD %s %s %s %s %d", objectKey, keyType, fieldName, tokenValue, confidence

    if(argc <= 5)
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    sds objectKey = (char *)rxGetContainedObject(argv[1]);
    sds keyType = (char *)rxGetContainedObject(argv[2]);
    sds fieldName = (char *)rxGetContainedObject(argv[3]);
    sds tokenValue = (char *)rxGetContainedObject(argv[4]);
    sds confidence = (char *)rxGetContainedObject(argv[5]);
    int dbId = RedisModule_GetSelectedDb(ctx);

    sds valueIndexkey = sdscatfmt(sdsempty(), "_zx_:%s:%s", tokenValue, fieldName);
    // TODO: Forlater
    // sds vakey = sdscatfmt(sdsempty(), "%s:%s", tokenValue, fieldName);
    // sds avkey = sdscatfmt(sdsempty(), "%s:%s", fieldName, tokenValue);
    sds objectReference = sdscatprintf(sdsempty(), "%s\t%s", objectKey, keyType);
    sds objectIndexkey = sdscatfmt(sdsempty(), "_ox_:%s", objectKey);

    rxAddSortedSetMember(valueIndexkey, dbId, atof(confidence), objectReference);
    rxAddSetMember(objectIndexkey, dbId, valueIndexkey);

    sdsfree(valueIndexkey);
    sdsfree(objectReference);
    
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int rx_begin_key(struct RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    rxUNUSED(argc);
    int dbId = RedisModule_GetSelectedDb(ctx);
    sds key = (char *)rxGetContainedObject(argv[1]);
    mercator_index->Open_Key(key, dbId);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int rx_commit_key(struct RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    rxUNUSED(argc);
    int dbId = RedisModule_GetSelectedDb(ctx);
    sds key = (char *)rxGetContainedObject(argv[1]);
    mercator_index->Commit_Key(key, dbId);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int rx_rollback_key(struct RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    rxUNUSED(argc);
    int dbId = RedisModule_GetSelectedDb(ctx);
    sds key = (char *)rxGetContainedObject(argv[1]);
    mercator_index->Rollback_Key(key, dbId);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    // serverLog(LL_NOTICE, "Loading rxFetch");
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "rxFetch", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "rxFetch",
                                  rx_fetch, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "rxAdd",
                                  rx_add, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "rxBegin",
                                  rx_begin_key, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "rxCommit",
                                  rx_commit_key, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "rxRollback",
                                  rx_rollback_key, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "rxDescribe",
                                  rx_describe, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    rxadd_log = fopen ("/tmp/rxadd.log", "w");
    mercator_index = new Mercator_Index();
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

int compareEquals(char *l, int ll, char *r){
    if(isdigit(*l)){
        double v = atof(l);
        double t = atof(r);
        return v == t;
    }else
        return strncmp(l, r, ll) == 0;
 }

int compareGreaterEquals(char *l, int ll, char *r){
    if(isdigit(*l)){
        double v = atof(l);
        double t = atof(r);
        return v >= t;
    }else
        return strncmp(l, r, ll) >= 0;

 }

int compareGreater(char *l, int ll, char *r){
    if(isdigit(*l)){
        double v = atof(l);
        double t = atof(r);
        return v > t;
    }else
        return strncmp(l, r, ll) > 0;

 }

int compareLessEquals(char *l, int ll, char *r){
    if(isdigit(*l)){
        double v = atof(l);
        double t = atof(r);
        return v <= t;
    }else
        return strncmp(l, r, ll) <= 0;

 }

int compareLess(char *l, int ll, char *r){
    if(isdigit(*l)){
        double v = atof(l);
        double t = atof(r);
        return v < t;
    }else
        return strncmp(l, r, ll) < 0;

 }

int compareNotEquals(char *l, int ll, char *r){
    if(isdigit(*l)){
        double v = atof(l);
        double t = atof(r);
        return v != t;
    }else
        return strncmp(l, r, ll) != 0;

 }

void initComparisonsStatic()
{
	ComparisonsMap = raxNew();
    void *old;
    raxTryInsert(ComparisonsMap, (UCHAR *)"=", 1, (void *)compareEquals, &old);
    raxTryInsert(ComparisonsMap, (UCHAR *)"==", 2, (void *)compareEquals, &old);
	raxTryInsert(ComparisonsMap, (UCHAR *)">=", 2, (void *)compareGreaterEquals, &old);
	raxTryInsert(ComparisonsMap, (UCHAR *)"<=", 2, (void *)compareLessEquals, &old);
	raxTryInsert(ComparisonsMap, (UCHAR *)">", 2, (void *)compareGreater, &old);
	raxTryInsert(ComparisonsMap, (UCHAR *)"<", 2, (void *)compareLess, &old);
	raxTryInsert(ComparisonsMap, (UCHAR *)"!=", 2, (void *)compareNotEquals, &old);
}
