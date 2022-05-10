
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

const char *EMPTY_STRING = "";

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
    rxUNUSED(argv);
    rxUNUSED(argc);
    RedisModule_ReplyWithSimpleString(ctx, "Command not yet implemented!");
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
    sds response = BusinessRule::ApplyAll(key);
    RedisModule_ReplyWithSimpleString(ctx, response);
    sdsfree(response);
    return REDISMODULE_OK;
}

void executeTest(CSjiboleth *g, const char *cmd, RedisModuleCtx *ctx, list *errors)
{
    rxUNUSED(errors);
    auto *parser = (Sjiboleth *)g;
    auto *t = parser->Parse(cmd);
    t->Show(cmd);
    if (parsedWithErrors(t))
    {
        writeParsedErrors(t, ctx);
        return;
    }
    auto *e = new SilNikParowy_Kontekst((char *)"192.168.1.182", 6379, ctx);
    rax *r = e->Execute(t);
    if (r)
    {
        WriteResults(r, ctx, 0, NULL);
        if (e->CanDeleteResult())
            FreeResults(r);
    }
    else
        RedisModule_ReplyWithSimpleString(ctx, "No results!");
    e->Reset();
    releaseQuery(t);
}

SJIBOLETH_HANDLER(IndexerJsonComma)
rxUNUSED(t);

rxUNUSED(stack);
ERROR("Operation not yet implemented");
END_SJIBOLETH_HANDLER(IndexerJsonComma)

SJIBOLETH_HANDLER(IndexerJsonAttributeValue)
rxUNUSED(t);

rxUNUSED(stack);
FaBlok *value = stack->Pop();
FaBlok *field = stack->Pop();
printf("%s = %s\n", field->AsSds(), value->AsSds());
END_SJIBOLETH_HANDLER(IndexerJsonAttributeValue)

SJIBOLETH_HANDLER(IndexerTextDash)
printf("%s\n", t->TokenAsSds());
stack->DumpStack();
END_SJIBOLETH_HANDLER()
SJIBOLETH_HANDLER(IndexerTextDot)
printf("%s\n", t->TokenAsSds());
stack->DumpStack();
END_SJIBOLETH_HANDLER()
SJIBOLETH_HANDLER(IndexerTextAt)
printf("%s\n", t->TokenAsSds());
stack->DumpStack();
END_SJIBOLETH_HANDLER()
SJIBOLETH_HANDLER(IndexerTextComma)
printf("%s\n", t->TokenAsSds());
stack->DumpStack();
END_SJIBOLETH_HANDLER()
SJIBOLETH_HANDLER(IndexerTextSemiColon)
printf("%s\n", t->TokenAsSds());
stack->DumpStack();
END_SJIBOLETH_HANDLER()
SJIBOLETH_HANDLER(IndexerTextColon)
printf("%s\n", t->TokenAsSds());
stack->DumpStack();
END_SJIBOLETH_HANDLER()
SJIBOLETH_HANDLER(IndexerTextTab)
printf("%s\n", t->TokenAsSds());
stack->DumpStack();
END_SJIBOLETH_HANDLER()
SJIBOLETH_HANDLER(IndexerTextNL)
printf("%s\n", t->TokenAsSds());
stack->DumpStack();
END_SJIBOLETH_HANDLER()
SJIBOLETH_HANDLER(IndexerTextQuote)
printf("%s\n", t->TokenAsSds());
stack->DumpStack();
END_SJIBOLETH_HANDLER()
SJIBOLETH_HANDLER(IndexerTextApostrophe)
printf("%s\n", t->TokenAsSds());
stack->DumpStack();
END_SJIBOLETH_HANDLER()

int executeQueryCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    sds cmd = (char *)rxGetContainedObject(argv[0]);
    const char *target_setname = NULL;
    sdstoupper(cmd);
    // int fetch_rows = strcmp(RX_GET, cmd) == 0 ? 1 : 0;
    sds query = sdsempty();
    int dialect_skippy = 0;
    size_t arg_len;
    sds sep = sdsnew("");
    int show_parser_debug_info = 0;
    for (int j = 1; j < argc; ++j)
    {
        char *q = (char *)RedisModule_StringPtrLen(argv[j], &arg_len);
        if (stringmatchlen(q, strlen(AS_ARG), AS_ARG, strlen(AS_ARG), 1) && strlen(q) == strlen(AS_ARG))
        {
            ++j;
            q = (char *)RedisModule_StringPtrLen(argv[j], &arg_len);
            target_setname = q;
        }
        else if (stringmatchlen(q, strlen(q), DEBUG_ARG, strlen(DEBUG_ARG), 1))
        {
            show_parser_debug_info = 1;
        }
        else if (stringmatchlen(q, strlen(RESET_ARG), RESET_ARG, strlen(RESET_ARG), 1) && strlen(q) == strlen(RESET_ARG))
        {
            // clearCache();
        }
        else
        {
            query = sdscatfmt(query, "%s%s", sep, q);
            sep = sdsnew(" ");
        }
    }
    sdsfree(sep);

    rxUNUSED(target_setname);
    rxUNUSED(show_parser_debug_info);

    CSjiboleth *parser;
    if (
        stringmatchlen(query, 2, JSON_PREFX, strlen(JSON_PREFX), 1))
    {
        parser = newJsonEngine();
        dialect_skippy = strlen(JSON_PREFX);
        ((Sjiboleth *)parser)->RegisterSyntax(",", 5, 0, 0, IndexerJsonComma);
        ((Sjiboleth *)parser)->RegisterSyntax(":", 30, 0, 0, IndexerJsonAttributeValue);
    }
    else if (
        stringmatchlen(query, 2, TXT_PREFX, strlen(TXT_PREFX), 1))
    {
        parser = newTextEngine();
        dialect_skippy = strlen(TXT_PREFX);
        ((Sjiboleth *)parser)->RegisterSyntax("=", 5, 0, 0, NULL);
        ((Sjiboleth *)parser)->RegisterSyntax("-", 5, 0, 0, IndexerTextDash);
        ((Sjiboleth *)parser)->RegisterSyntax(".", 5, 0, 0, IndexerTextDot);
        ((Sjiboleth *)parser)->RegisterSyntax("@", 5, 0, 0, IndexerTextAt);
        ((Sjiboleth *)parser)->RegisterSyntax(",", 5, 0, 0, IndexerTextComma);
        ((Sjiboleth *)parser)->RegisterSyntax(";", 5, 0, 0, IndexerTextSemiColon);
        ((Sjiboleth *)parser)->RegisterSyntax(":", 5, 0, 0, IndexerTextColon);
        ((Sjiboleth *)parser)->RegisterSyntax("\t", 5, 0, 0, IndexerTextTab);
        ((Sjiboleth *)parser)->RegisterSyntax("\n", 5, 0, 0, IndexerTextNL);
        ((Sjiboleth *)parser)->RegisterSyntax("`", 5, 0, 0, IndexerTextQuote);
        ((Sjiboleth *)parser)->RegisterSyntax("'", 5, 0, 0, IndexerTextApostrophe);
    }
    else
        parser = newQueryEngine();

    list *errors = listCreate();
    executeTest(parser, (const char *)query + dialect_skippy, ctx, errors);
    listRelease(errors);
    releaseParser(parser);
    return REDISMODULE_OK;
}

/* This function must be present on each R
edis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{

    rxUNUSED(argv);
    rxUNUSED(argc);
    initRxSuite();

    // CSjiboleth *gd = newGremlinEngine();
    // executeQ(gd);
    // rxUNUSED(gd);

    if (RedisModule_Init(ctx, "rxRule", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

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
    if (RedisModule_CreateCommand(ctx, "test.json",
                                  executeQueryCommand, EMPTY_STRING, 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx)
{
    rxUNUSED(ctx);
    finalizeRxSuite();
    return REDISMODULE_OK;
}
