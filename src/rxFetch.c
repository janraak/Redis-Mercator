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
#include "redismodule.h"
#include <ctype.h>
#include <stdio.h>
#include <string.h>
#include "token.h"
/*
 * This module execute rx query commands
 */
#include "parser.h"
#include "../../deps/hiredis/hiredis.h"
#include <string.h>
#include <queryEngine.h>

static dict *ComparisonsMap = NULL;
void initComparisonsStatic();
typedef int comparisonProc(char *l, int ll, char *r);

extern int keyIsExpired(redisDb *db, robj *key);

extern dictType tokenDictType;

  FILE *rxadd_log;


/* Modified keysCommand */
list *findMatchingKeys(redisDb *db, sds pattern) {
    sdstolower(pattern);
    dictIterator *di;
    dictEntry *de;
    int plen = sdslen(pattern);
    unsigned long numkeys = 0;
    REDISMODULE_NOT_USED(numkeys);
    list *matchingKeys = listCreate();
    di = dictGetSafeIterator(db->dict);
    while((de = dictNext(di)) != NULL) {
        sds key = dictGetKey(de);
        if (stringmatchlen(pattern,plen,key,sdslen(key),0)) {
            // if (!keyIsExpired(db,keyobj)) {
                listAddNodeTail(matchingKeys, key);
                numkeys++;
            // }
        }
    }
    dictReleaseIterator(di);
    return matchingKeys;
}

/* Modified keysCommand */
list *findMatchingKeysWithCondition(redisDb *db, sds pattern, char *op, char *v) {
    sdstolower(pattern);
    if(!ComparisonsMap) initComparisonsStatic();
    dictIterator *di;
    dictEntry *de;
    int plen = sdslen(pattern);
    unsigned long numkeys = 0;
    REDISMODULE_NOT_USED(numkeys);
    list *matchingKeys = listCreate();
    di = dictGetSafeIterator(db->dict);
    dictEntry *o = dictFind(ComparisonsMap, (void *)op);
    comparisonProc *compare  = (comparisonProc *)o->v.val;
    while((de = dictNext(di)) != NULL) {
        sds key = dictGetKey(de);
        if (stringmatchlen(pattern,plen,key,sdslen(key),0)) {
            char * left_colon = strchr(key, ':');
            char * right_colon = strchr(left_colon + 1, ':');
            if( compare(left_colon +1, right_colon - left_colon - 1, v) ){
            // if (!keyIsExpired(db,keyobj)) {
                listAddNodeTail(matchingKeys, key);
                numkeys++;
            // }
            }
        }
    }
    dictReleaseIterator(di);
    return matchingKeys;
}

/*
 * Fetch members for value or value/field
 * 
 * Values may be in different fields if so the union is returned.
 * A value range could be specified if so the union is returned.
 * 
 * 
 */
int executeFetchCommand(struct RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    int dbId = RedisModule_GetSelectedDb(ctx);
    redisDb *db = server.db + dbId;
    sds pattern = sdsnew("_zx_:");
            robj *v;
            robj *f;
            robj *op;
    list *matchingKeys;
    switch(argc){
        case 2:
            v = (robj *)argv[1];
            pattern = sdscatfmt(pattern, "%s*", (const char *)v->ptr);
            matchingKeys = findMatchingKeys(db, pattern);
            break;
        case 3:
            v = (robj *)argv[1];
            f = (robj *)argv[2];
            pattern = sdscatfmt(pattern, "%s:%s", ( char *)v->ptr, (const char *)f->ptr);
            matchingKeys = findMatchingKeys(db, pattern);
            break;
        case 4:
            v = (robj *)argv[1];
            f = (robj *)argv[2];
            op = (robj *)argv[3];
            pattern = sdscatfmt(pattern, "*:%s", (const char *)f->ptr);
            matchingKeys = findMatchingKeysWithCondition(db, pattern, (char *)op->ptr, (char *)v->ptr);
            break;
        default:
            return RedisModule_ReplyWithError(ctx, "Invalid command! Syntax: rxFetch %value% [%field%]");
    }

    sds zset_name = sdsempty();
    sds sets_for_union = sdsempty();
    listIter *li = listGetIterator(matchingKeys, AL_START_HEAD);
    listNode *ln;
    int no_of_values = 0;
    robj  **keys = RedisModule_Alloc(sizeof(robj  *) * matchingKeys->len);
    while((ln = listNext(li))){
        sds k = (sds)ln->value;
        *(keys + no_of_values) = createStringObject(k,sdslen(k));
        sets_for_union = sdscatfmt(sets_for_union, " %s", k);
        char * left_colon = strchr(k, ':');
        char * right_colon = strchr(left_colon + 1, ':');
        sds value = sdsnewlen(left_colon + 1, right_colon - left_colon - 1);
        zset_name = sdscatfmt(zset_name, "_%s", value);
        ++no_of_values;
    }
    listReleaseIterator(li);
    listRelease(matchingKeys);
    sds no_of_sets = sdscatfmt(sdsempty(), "%i", no_of_values); 
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "ZUNIONSTORE", "ccvcc", zset_name, no_of_sets, keys,  no_of_values, "AGGREGATE", "SUM");
    if (reply)
    {
        sds tally = sdscatfmt(sdsempty(), "%i", RedisModule_CallReplyInteger(reply));
        // size_t len;
        // char *key_count = reply->proto;
        //(char *)RedisModule_CallReplyStringPtr(reply, &len);
        RedisModule_FreeCallReply(reply);
        reply = RedisModule_Call(ctx, "ZREVRANGE", "ccc", zset_name, "0", tally);
    }
    // if(reply){
    //     RedisModule_FreeCallReply(reply);
    // }
    // reply = RedisModule_Call(ctx, "SUNION", "v", keys,  no_of_values);
    RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN);
    size_t items = 0;
    if(reply){
        items = RedisModule_CallReplyLength(reply);
        for (size_t j = 0; j < items; j++) {
            RedisModuleCallReply *ele = RedisModule_CallReplyArrayElement(reply,j);
            RedisModule_ReplyWithCallReply(ctx,ele);
        }
        RedisModule_FreeCallReply(reply);
    }
    RedisModule_ReplySetArrayLength(ctx, items);

    sdsfree(sets_for_union);
    RedisModule_Free(keys);
    return 0;
}
int rxadd_cntr = 0;
int executeIndexAddCommand(struct RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
        // "RXADD %s %s %s %s %d", objectKey, keyType, fieldName, tokenValue, confidence

        // for (int j = 0; j < argc; j++)
        // {
        //     const char *s = RedisModule_StringPtrLen(argv[j], NULL);
        //     serverLog(LL_NOTICE, "RXADD %d: %s", j, s);
        // }

    if(argc <= 5)
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    robj *objectKey = (robj *)argv[1];
    robj *keyType = (robj *)argv[2];
    robj *fieldName = (robj *)argv[4];
    robj *tokenValue = (robj *)argv[4];
    robj *confidence = (robj *)argv[5];
    robj *db_id = NULL;
    if(argc >= 6)
        db_id = (robj *)argv[6];

    // fprintf(rxadd_log, "%d: %s %s %s %s %s", ++rxadd_cntr, (const char *)db_id->ptr, (const char *)objectKey->ptr, (const char *)keyType->ptr, (const char *)fieldName->ptr, (const char *)tokenValue->ptr);
    // fflush(rxadd_log);
    RedisModuleCallReply *reply;
    if (db_id)
    {
        reply = RedisModule_Call(ctx, "SELECT", "c", (const char *)db_id->ptr);
        if(reply)
            RedisModule_FreeCallReply(reply);
    }
    sds valueIndexkey = sdsnew("_zx_:");
    valueIndexkey = sdscatfmt(valueIndexkey, "%s:%s", (const char *)tokenValue->ptr, (const char *)fieldName->ptr);

    sds fieldNameIndexkey = sdsnew("_zx_:");
    fieldNameIndexkey = sdscatfmt(fieldNameIndexkey, "%s:_kw_", (const char *)fieldName->ptr);

    sds objectReference = sdsnew((const char *)objectKey->ptr);
    objectReference = sdscatprintf(objectReference, "\t%s", (const char *)keyType->ptr);

    reply = RedisModule_Call(ctx, "ZADD", "ccc", valueIndexkey, (const char *)confidence->ptr, objectReference);
    if(reply)
        RedisModule_FreeCallReply(reply);

    // reply = RedisModule_Call(ctx, "ZADD", "ccc", fieldNameIndexkey, (const char *)confidence->ptr, valueIndexkey);
    // if(reply)
    //     RedisModule_FreeCallReply(reply);


    sds objectIndexkey = sdsnew("_ox_:");
    objectIndexkey = sdscatfmt(objectIndexkey, "%s", (const char *)objectKey->ptr);
    reply = RedisModule_Call(ctx, "SADD", "cc", objectIndexkey, valueIndexkey);
    if(reply)
        RedisModule_FreeCallReply(reply);

    sdsfree(valueIndexkey);
    sdsfree(fieldNameIndexkey);
    sdsfree(objectReference);
    
    // fprintf(rxadd_log, ".\n");
    // fflush(rxadd_log);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    initRxSuite();
    // serverLog(LL_NOTICE, "Loading rxFetch");
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "rxFetch", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "rxFetch",
                                  executeFetchCommand, "readonly", 1, 1, 0) == REDISMODULE_ERR)

        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "rxAdd",
                                  executeIndexAddCommand, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    rxadd_log = fopen ("/tmp/rxadd.log", "w");

    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx)
{
    fclose(rxadd_log);
    rxadd_log = NULL;
    REDISMODULE_NOT_USED(ctx);
    finalizeRxSuite();
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
	ComparisonsMap = dictCreate(&tokenDictType, (void *)NULL);
	dictAdd(ComparisonsMap, (void *)sdsnew("=="), &compareEquals);
	dictAdd(ComparisonsMap, (void *)sdsnew(">="), &compareGreaterEquals);
	dictAdd(ComparisonsMap, (void *)sdsnew("<="), &compareLessEquals);
	dictAdd(ComparisonsMap, (void *)sdsnew(">"), &compareGreater);
	dictAdd(ComparisonsMap, (void *)sdsnew("<"), &compareLess);
	dictAdd(ComparisonsMap, (void *)sdsnew("!="), &compareNotEquals);
}
