
/* rxRule  -- Execute Rule expressions against
 *            Modified Redis keys
 */
#define REDISMODULE_EXPERIMENTAL_API
#include "rxRule.hpp"
#include <string>
#include <list>

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
#include "graphstack.hpp"

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

int rule_execution_duration = 100;
int rule_execution_interval = 100;

static void *execRulesThread(void *ptr)
{
    CommencedTriggerHandler();
    redisNodeInfo *data_config = rxDataNode();

    FaBlok *triggers = FaBlok::New("TriggerSet", KeysetDescriptor_TYPE_GREMLIN_EDGE_SET);
    std::list<snapshot_request *> snapshots;
    // while (true)
    {
        // WaitForKeyTriggered();
        SilNikParowy_Kontekst *stack = new SilNikParowy_Kontekst(data_config, (RedisModuleCtx *)ptr);
        long long start = ustime();
        snapshot_request *key = NULL;
        while ((key = getTriggeredKey()) != NULL)
        {
            snapshots.push_front(key);
            // index_info.key_triggered_dequeued_tally++;
            // rxServerLog(rxLL_NOTICE, "getTriggeredKey()=>%p %s", key, key->k);
            triggers->InsertKey(key->k, NULL);
            // BusinessRule::ApplyAll(key, stack);
            // freeTriggerProcessedKey(key);
            long long cycle = ustime() - start;
            if (cycle > 250000)
            {
                break;
            }
        }
        if (raxSize(triggers->keyset) > 0)
        {
            BusinessRule::ApplyAll(triggers, stack);
        }
        while (snapshots.size())
        {
            auto req = snapshots.front();
            snapshots.remove(req);
            freeTriggerProcessedKey(req);
        }
        // TODO: key set maintenance !!
        // if (raxSize(triggers->keyset) > 0)
        // {
        //     BusinessRule::ApplyAll(triggers, stack);

        //     raxIterator setIterator;
        //     raxStart(&setIterator, triggers->keyset);
        //     raxSeek(&setIterator, "^", NULL, 0);
        //     while (raxSize(triggers->keyset) > 0)
        //     {
        //         freeTriggerProcessedKey(setIterator.key, setIterator.key_len);
        //         raxRemove(triggers->keyset, setIterator.key, setIterator.key_len, NULL);
        //         raxSeek(&setIterator, "^", NULL, 0);
        //     }
        //     raxStop(&setIterator);
        //     triggers->size = 0;
        // }
        delete stack;
        // sched_yield();
        }
    FaBlok::Delete(triggers);
    return NULL;
}

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
    rxString sep[6] = {rxStringNew(""), rxStringNew(""), rxStringNew("")};
    rxString query[6] = {rxStringNew(""), rxStringNew(""), rxStringNew("")};
    int phrase = 0;
    bool isFinal = false;
    for (int j = 2; j < argc; ++j)
    {
        char *q = (char *)RedisModule_StringPtrLen(argv[j], &len);
        if (rxStringMatch("FINAL", q, 1))
        {
            isFinal = true;
            continue;
        }
        else         if (rxStringMatch("IF", q, 1))
        {
            phrase = 1;
            continue;
        }
        else if (rxStringMatch("THEN", q, 1))
        {
            phrase = 0;
            continue;
        }
        else if (rxStringMatch("ELSE", q, 1))
        {
            phrase = 2;
            continue;
        }
        else if (rxStringMatch("IFNOT", q, 1))
        {
            phrase = 3;
            continue;
        }
        else if (rxStringMatch("APPLY", q, 1))
        {
            phrase = phrase == 0 ? 4 :5;
            continue;
        }
        query[phrase] = rxStringFormat("%s%s%s", query[phrase], sep[phrase], q);
        sep[phrase] = rxStringNew(" ");
    }
    BusinessRule *br = new BusinessRule(ruleName, isFinal, query[1], query[0], query[4], query[2], query[5]);
    for (size_t n = 0; n < sizeof(sep) / sizeof(rxString); ++n)
    {
        rxStringFree(sep[n]);
        rxStringFree(query[n]);
    }
    rxString response = rxStringFormat("Rule for: %s Expression: Expression appears to be ", ruleName);
    switch (br->isvalid)
    {
    case true:
        response = rxStringFormat("%s%s", response, "correct!");
        BusinessRule::Retain(br);
        break;
    default:
        response = rxStringFormat("%s%s", response, "incorrect!");
        BusinessRule::Retain(br);
        // br = NULL;
        break;
    }
    RedisModule_ReplyWithSimpleString(ctx, response);
    rxStringFree(response);
    rxAlsoPropagate(0, (void **)argv, argc, -1);

    return REDISMODULE_OK;
}

int rxRuleList(RedisModuleCtx *ctx, RedisModuleString **, int)
{
    return BusinessRule::WriteList(ctx);
}

int rxRuleResetCounters(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    rxAlsoPropagate(0, (void **)argv, argc, -1);
    return BusinessRule::ResetCounters(ctx);
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
        auto *br = BusinessRule::Find((rxString)ruleName);
        if (br != NULL)
        {
            BusinessRule::Forget(br);
        }
    }
    rxAlsoPropagate(0, (void **)argv, argc, -1);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int rxRuleGet(RedisModuleCtx *ctx, RedisModuleString **, int)
{
    RedisModule_ReplyWithSimpleString(ctx, "Command not yet implemented!");
    return REDISMODULE_OK;
}

int rxRuleWait(RedisModuleCtx *ctx, RedisModuleString **, int)
{
    RedisModule_ReplyWithSimpleString(ctx, "Command not yet implemented!");
    return REDISMODULE_OK;
}

extern RedisModuleCtx *loadCtx;

// int rxApply(RedisModuleCtx *, RedisModuleString **argv, int )
// {
//     rxString key = (char *)rxGetContainedObject(argv[1]);
//     /*index_info.object_index_enqueued_tally += */KeyTriggered(key);
//     return REDISMODULE_OK;
// }

pthread_t rule_threads[1];

/* This function must be present on each R
edis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    initRxSuite();

    if (RedisModule_Init(ctx, "rxRule", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
    {
        rxServerLog(rxLL_NOTICE, "OnLoad rxRule. Init error!");
        return REDISMODULE_ERR;
    }

    rxRegisterConfig((void **)argv, argc);
    redisNodeInfo *index_config = rxIndexNode();
    redisNodeInfo *data_config = rxDataNode();

    rxServerLog(rxLL_WARNING, "\nrxRule loaded, is local:%d index: %s data: %s \n\n",
                index_config->is_local,
                index_config->host_reference,
                data_config->host_reference);
    if (RedisModule_CreateCommand(ctx, "RULE.SET",
                                  rxRuleSet, "admin write", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "RULEADD",
                                  rxRuleSet, "admin write", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    // if (RedisModule_CreateCommand(ctx, "RULE.APPLY",
    //                               rxApply, "write", 0, 0, 0) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;
    // if (RedisModule_CreateCommand(ctx, "RXTRIGGER",
    //                               rxApply, "write", 0, 0, 0) == REDISMODULE_ERR)
    //     return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "RULE.LIST",
                                  rxRuleList, "admin readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "RULE.RESET",
                                  rxRuleResetCounters, "admin readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "RULE.DEL",
                                  rxRuleDel, "admin write", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "RULE.GET",
                                  rxRuleGet, "admin readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "RULE.WAIT",
                                  rxRuleWait, "admin readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    // rule_execution_cron_id = rxCreateTimeEvent(1, (aeTimeProc *)rule_execution_cron, NULL, NULL);
    // rule_execution_cron(NULL, 0, NULL);

    SetTriggerHandler(execRulesThread);
    // for (size_t n = 0; n < (sizeof(rule_threads) / sizeof(pthread_t)); n++)
    // {
 
    //     if (pthread_create(&rule_threads[n], NULL, execRulesThread, ctx))
    //     {
    //         printf("FATAL: Failed to start indexer thread\n");
    //     }
    //     else
    //         pthread_setname_np(rule_threads[n], "RULES");
    // }

    rxServerLog(rxLL_NOTICE, "OnLoad rxRule. Done!");
    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx)
{
    rxUNUSED(ctx);
    finalizeRxSuite();
    return REDISMODULE_OK;
}
