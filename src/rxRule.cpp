
/* rxRule  -- Execute Rule expressions against
 *            Modified Redis keys
 */
#define REDISMODULE_EXPERIMENTAL_API
#include "rxRule.hpp"
#include <string>

#include <fcntl.h>
#include <iostream>

using std::string;
#include "rule.hpp"
#include "sjiboleth.h"
#include "client-pool.hpp"

// #include "rxIndex-silnikparowy.hpp"

#ifdef __cplusplus
extern "C"
{
#endif
#include "rax.h"
#include "rxSuiteHelpers.h"
#include "util.h"
#include <sys/stat.h>

    int rxRuleSet(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
    int rxApply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
    int rxRuleSet(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
    int rxRuleList(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
    int rxRuleDel(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
    int rxRuleGet(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

    // int test(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

    int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

    int RedisModule_OnUnload(RedisModuleCtx *ctx);

#ifdef __cplusplus
}
#endif
// extern dictType tokenDictType;

#include "rxFetch-duplexer.hpp"

// const char *EMPTY_STRING = "";

const char *AS_ARG = "AS";
const char *DEBUG_ARG = "DEBUG";
const char *RESET_ARG = "RESET";
const char *CLEAR_ARG = "RESET";
const char *JSON_PREFX = "j:";
const char *JSON_PREFX_ALT = "j.";
const char *TXT_PREFX = "t:";
const char *TXT_PREFX_ALT = "t.";

const char *REDIS_CMD_SADD = "SADD";
const char *REDIS_CMD_SMEMBERS = "SMEMBERS";

const char *REDIS_CMD_HSET = "HSET";
const char *REDIS_CMD_HGETALL = "HGETALL";
const char *REDIS_CMD_GET = "GET";
const char *OK = "OK";

const char *REDIS_CMD_SCRIPT = "SCRIPT";
const char *REDIS_CMD_SCRIPT_LOAD_ARG = "LOAD";
const char *REDIS_CMD_EVALSHA = "EVALSHA";

const char *RX_QUERY = "RXQUERY";
const char *RX_GET = "RXGET";
const char *RXCACHE = "RXCACHE";
const char *RX_LOADSCRIPT = "RXLOADSCRIPT";
const char *RX_EVALSHA = "RXEVALSHA";
const char *RX_HELP = "RXHELP";

IndexerInfo index_info = {sdsempty(), 6379, 0, 0};
IndexerInfo data_info = {sdsempty(), 6379, 0, 0};

/*
    Define an active business rule.

    An active business rule:
    * A named rxQuery expression.
    * Has a key set being monitored and updated.
    * Can be used in rxQuery Expressions.
    * Does not need computing when executing rxQuery expressions.
    * [Are persisted (embedded in backup?). ]
    * [Are cluster wide].
    * [Replication]?
 */
int rxRuleSet(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    size_t len;
    const char *ruleName = RedisModule_StringPtrLen(argv[1], &len);
    sds sep = sdsnew("");
    sds query = sdsnew("");
    for (int j = 2; j < argc; ++j)
    {
        char *q = (char *)RedisModule_StringPtrLen(argv[j], &len);
        query = sdscatfmt(query, "%s%s", sep, q);
        sep = sdsnew(" ");
    }
    BusinessRule *br = new BusinessRule(ruleName, query, index_info);
    sdsfree(sep);
    sdsfree(query);
    sds response = sdscatfmt(sdsempty(), "Rule for: %s Expression: Expression appears to be ", ruleName);
    switch (br->isvalid)
    {
    case true:
        response = sdscat(response, "correct!");
        BusinessRule::Retain(br);
        break;
    default:
        response = sdscat(response, "incorrect!");
        BusinessRule::Retain(br);
        // br = NULL;
        break;
    }
    RedisModule_ReplyWithSimpleString(ctx, response);
    sdsfree(response);
    return REDISMODULE_OK;
}

int rxRuleList(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    rxUNUSED(argv);
    rxUNUSED(argc);
    return BusinessRule::WriteList(ctx);
}

int rxRuleDel(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    rxUNUSED(argv);
    rxUNUSED(argc);
    rxUNUSED(argv);
    rxUNUSED(argc);
    size_t len;
    const char *ruleName = RedisModule_StringPtrLen(argv[1], &len);
    if(strcmp("*", ruleName) == 0)
        BusinessRule::ForgetAll();
    else {
        auto *br = BusinessRule::Find((sds)ruleName);
        if( br!= NULL){
            BusinessRule::Forget(br);
        }
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int rxRuleGet(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    rxUNUSED(argv);
    rxUNUSED(argc);
    RedisModule_ReplyWithSimpleString(ctx, "Command not yet implemented!");
    return REDISMODULE_OK;
}

int rxApply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    rxUNUSED(ctx);
    rxUNUSED(argc);
    sds key = (char *)rxGetContainedObject(argv[1]);
    rxServerLogRaw(rxLL_WARNING, 
        sdscatprintf(sdsempty(), "Applying all rules to: %s\n", key));
    sds response = BusinessRule::ApplyAll(key);
    RedisModule_ReplyWithSimpleString(ctx, response);
    rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(), "Applied all rules to: %s\n", key));
    sdsfree(response);
    return REDISMODULE_OK;
}

// void executeTest(sds key, CSjiboleth *g, const char *cmd, RedisModuleCtx *ctx, list *errors)
// {
//     rxUNUSED(errors);
//     auto *parser = (Sjiboleth *)g;
//     auto text_parser = newTextEngine();

//     auto *t = parser->Parse(cmd);
//     auto *index_node = RedisClientPool<redisContext>::Acquire("192.168.1.182", 6379);
//     auto *e = new SilNikParowy_Kontekst((char *)"192.168.1.182", 6379, ctx);
//     auto *collector = raxNew();
//     e->Memoize("@@SilNikParowy_Kontekst@@", (void *)e);
//     e->Memoize("@@TEXT_PARSER@@", (void *)text_parser);
//     e->Memoize("@@collector@@", (void *)collector);
//     t->Show(cmd);
//     auto *tt = t;
//     int no_sub_expr = 0;
//     while (tt != NULL)
//     {
//         no_sub_expr++;
//         tt = tt->Next();
//     }
//     if(no_sub_expr > 1)
//         RedisModule_ReplyWithArray(ctx, no_sub_expr);
//     while (t != NULL)
//     {
//         // rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(), "%s\n", t->ToString()));
//         if (parsedWithErrors(t))
//         {
//             writeParsedErrors(t, ctx);
//             return;
//         }
//         rax *r = e->Execute(t);
//         if (r)
//         {
//             WriteResults(r, ctx, 0, NULL);
//             if (e->CanDeleteResult())
//                 FreeResults(r);
//         }
//         else
//             RedisModule_ReplyWithSimpleString(ctx, "No results!");
//         t = t->Next();
//     }
//     TextDialect::FlushIndexables(collector, key, (char *)"S", index_node);
//     e->Reset();
//     RedisClientPool<redisContext>::Release(index_node);
//     raxFree(collector);
//     e->Forget("@@SilNikParowy_Kontekst@@");
//     e->Forget("@@TEXT_PARSER@@");
//     e->Forget("@@collector@@");
//     releaseQuery(t);
//     releaseParser(text_parser);
// }

// int executeQueryCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
// {
//     sds cmd = (char *)rxGetContainedObject(argv[0]);
//     sds key = (char *)rxGetContainedObject(argv[1]);
//     const char *target_setname = NULL;
//     sdstoupper(cmd);
//     // int fetch_rows = strcmp(RX_GET, cmd) == 0 ? 1 : 0;
//     sds query = sdsempty();
//     int dialect_skippy = 0;
//     size_t arg_len;
//     sds sep = sdsnew("");
//     int show_parser_debug_info = 0;
//     for (int j = 2; j < argc; ++j)
//     {
//         char *q = (char *)RedisModule_StringPtrLen(argv[j], &arg_len);
//         if (stringmatchlen(q, strlen(AS_ARG), AS_ARG, strlen(AS_ARG), 1) && strlen(q) == strlen(AS_ARG))
//         {
//             ++j;
//             q = (char *)RedisModule_StringPtrLen(argv[j], &arg_len);
//             target_setname = q;
//         }
//         else if (stringmatchlen(q, strlen(q), DEBUG_ARG, strlen(DEBUG_ARG), 1))
//         {
//             show_parser_debug_info = 1;
//         }
//         else if (stringmatchlen(q, strlen(RESET_ARG), RESET_ARG, strlen(RESET_ARG), 1) && strlen(q) == strlen(RESET_ARG))
//         {
//             // clearCache();
//         }
//         else
//         {
//             query = sdscatfmt(query, "%s%s", sep, q);
//             sep = sdsnew(" ");
//         }
//     }
//     sdsfree(sep);

//     rxUNUSED(target_setname);
//     rxUNUSED(show_parser_debug_info);

//     CSjiboleth *parser;
//     if (
//         stringmatchlen(query, 2, JSON_PREFX, strlen(JSON_PREFX), 1))
//     {
//         parser = newJsonEngine();
//         dialect_skippy = strlen(JSON_PREFX);
//     }
//     else if (
//         stringmatchlen(query, 2, TXT_PREFX, strlen(TXT_PREFX), 1))
//     {
//         parser = newTextEngine();
//         dialect_skippy = strlen(TXT_PREFX);
//     }
//     else
//         parser = newQueryEngine();

//     list *errors = listCreate();
    
//     executeTest(key, parser, (const char *)query + dialect_skippy, ctx, errors);
//     listRelease(errors);
//     releaseParser(parser);
//     return REDISMODULE_OK;
// }


/* This function must be present on each R
edis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    initRxSuite();

    if (RedisModule_Init(ctx, "rxRule", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
   
    index_info.database_no = 0;
    index_info.is_local = 0;
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
     if (argc > 2)
    {
        const char *s = RedisModule_StringPtrLen(argv[2], NULL);
        data_info.index_address = sdsdup(sdsnew(s));
    }
    if (argc > 3)
    {
        const char *s = RedisModule_StringPtrLen(argv[3], NULL);
        data_info.index_port = atoi(s);
    }
    index_info.is_local = sdscmp(index_info.index_address, data_info.index_address) == 0
                        && index_info.index_port == data_info.index_port;

    rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(), "\nrxRule loaded, is local:%d index: %s:%d data: %s:%d \n\n",
    index_info.is_local,
    index_info.index_address, index_info.index_port,
    data_info.index_address, data_info.index_port));
    if (RedisModule_CreateCommand(ctx, "RULE.SET",
                                  rxRuleSet, EMPTY_STRING, 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "RULEADD",
                                  rxRuleSet, EMPTY_STRING, 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "RULE.APPLY",
                                  rxApply, EMPTY_STRING, 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "RULE.LIST",
                                  rxRuleList, EMPTY_STRING, 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "RULE.DEL",
                                  rxRuleDel, EMPTY_STRING, 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "RULE.GET",
                                  rxRuleGet, EMPTY_STRING, 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    // if (RedisModule_CreateCommand(ctx, "test.json",
    //                               executeQueryCommand, EMPTY_STRING, 1, 1, 0) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;

    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx)
{
    rxUNUSED(ctx);
    finalizeRxSuite();
    return REDISMODULE_OK;
}
