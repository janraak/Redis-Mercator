
/* rxRule  -- Execute Rule expressions against
 *            Modified Redis keys
 */
#define REDISMODULE_EXPERIMENTAL_API
#include "rxRule.hpp"
#include <string>

#include <fcntl.h>
#include <iostream>
#define WRAPPER
#include "simpleQueue.hpp"

using std::string;
#include "client-pool.hpp"
#include "rule.hpp"
#include "sjiboleth.h"

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

#include "rxFetch-multiplexer.hpp"

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
    BusinessRule *br = new BusinessRule(ruleName, query);
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
    size_t len;
    if (argc <= 1)
    {
        RedisModule_ReplyWithSimpleString(ctx, "No rule name and not *");
        return REDISMODULE_OK;
    }
    const char *ruleName = RedisModule_StringPtrLen(argv[1], &len);
    if (strcmp("*", ruleName) == 0)
        BusinessRule::ForgetAll();
    else
    {
        auto *br = BusinessRule::Find((sds)ruleName);
        if (br != NULL)
        {
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


/* This function must be present on each R
edis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    initRxSuite();

    if (RedisModule_Init(ctx, "rxRule", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    rxRegisterConfig(ctx, argv, argc);
    redisNodeInfo *index_config = rxIndexNode();
    redisNodeInfo *data_config = rxDataNode();

    rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(), "\nrxRule loaded, is local:%d index: %s data: %s \n\n",
                                              index_config->is_local,
                                              index_config->host_reference,
                                              data_config->host_reference));
    if (RedisModule_CreateCommand(ctx, "RULE.SET",
                                  rxRuleSet, EMPTY_STRING, 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "RULEADD",
                                  rxRuleSet, EMPTY_STRING, 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "RULE.APPLY",
                                  rxApply, EMPTY_STRING, 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "RXTRIGGER",
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
