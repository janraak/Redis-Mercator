
/* rxQuery -- Execute SET and/or Graph queries
 *            against a Redis Database
 */
#define REDISMODULE_EXPERIMENTAL_API
#include "rxQuery.hpp"
#include <string>

using std::string;
#include "sjiboleth-fablok.hpp"
#include "sjiboleth.h"

#include "../../deps/hiredis/hiredis.h"
#ifdef __cplusplus
extern "C"
{
#endif
#include "sdsWrapper.h"
#include <string.h>
#include <sys/stat.h>

#include "rxSuiteHelpers.h"

    int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

    int RedisModule_OnUnload(RedisModuleCtx *ctx);

#ifdef __cplusplus
}
#endif
#include "passthru-multiplexer.hpp"
#include "rxQuery-multiplexer.hpp"

// const char *AS_ARG = "AS";
// const char *DEBUG_ARG = "DEBUG";
// const char *RESET_ARG = "RESET";
// const char *CLEAR_ARG = "RESET";
// const char *GREMLIN_PREFX = "g:";
// const char *GREMLIN_PREFIX_ALT = "g.";
// const char *GREMLIN_DIALECT = "gremlin";
// const char *QUERY_DIALECT = "query";

// const char *REDIS_CMD_SADD = "SADD";
const char *QREDIS_CMD_SMEMBERS = "SMEMBERS";

// const char *REDIS_CMD_HSET = "HSET";
// const char *REDIS_CMD_HGETALL = "HGETALL";
const char *REDIS_CMD_GET = "GET";
// const char *OK = "OK";
const char *REDIS_CMD_SCRIPT = "SCRIPT";
const char *REDIS_CMD_SCRIPT_LOAD_ARG = "LOAD";
const char *REDIS_CMD_EVALSHA = "EVALSHA";

const char *RX_QUERY = "RXQUERY";
const char *RX_QUERY_ASYNC = "RXQUERY.ASYNC";
const char *RX_GET = "RXGET";
const char *RXCACHE = "RXCACHE";
const char *RX_LOADSCRIPT = "RXLOADSCRIPT";
const char *RX_EVALSHA = "RXEVALSHA";
const char *RX_HELP = "RXHELP";

const char *HELP_STRING = "RX Query Commands:\n"
                          "\n"
                          "RXQUERY [DEBUG] [RESET] [AS <setname>] <query parts>\n"
                          "RXQUERY [DEBUG] [RESET] [AS <setname>] g:<gremlin like query>\n"
                          "\n"
                          "RXGET [DEBUG] [RESET] [AS <setname>] <query parts>\n"
                          "RXGET [DEBUG] [RESET] [AS <setname>] g:<gremlin like query>\n"
                          "\n"
                          "RXINDEX [ON | OFF]\n"
                          "\n"
                          "RXREINDEX\n"
                          "\n"
                          "RXCACHE [RESET]\n"
                          "\n"
                          "RXLOADSCRIPT <path>\n"
                          "\n"
                          "RXEVALSHA <set name> <script sha1>\n"
                          "\n"
                          "Graph Query functions:\n"
                          "\n"
                          "G. | G:\n"
                          "Invoke Gremlin like query engine\n"
                          "\n"
                          "V()\n"
                          "V(<iri (key)>)\n"
                          "V(<vertice type>)\n"
                          "Get one or more vertices\n"
                          "\n"
                          "E()\n"
                          "E(<iri (key)>)\n"
                          "V(<vertice type>)\n"
                          "Get one or more edges\n"
                          "\n"
                          "MATCH()\n"
                          "\n"
                          "IN()\n"
                          "\n"
                          "OUT()\n"
                          "\n"
                          "INOUT()\n"
                          "\n"
                          "HAS()\n"
                          "\n"
                          "ADDVERTEX()\n"
                          "\n"
                          "ADDEDGE()\n"
                          "\n"
                          "FROM()\n"
                          "\n"
                          "TO()\n"
                          "\n"
                          "PROPERTY()\n"
                          "\n";

#define HASHTYPE 'H'
#define STRINGTYPE 'S'
#include "adlist.h"

// rxString hashToJson(robj *o, rxString json)
// {
//     hashTypeIterator *hi = hashTypeInitIterator(o);
//     rxString fieldsep = rxStringEmpty();
//     while (hashTypeNext(hi) != C_ERR)
//     {
//         rxString field = hashTypeCurrentObjectNewSds(hi, OBJ_HASH_KEY);
//         rxString value = hashTypeCurrentObjectNewSds(hi, OBJ_HASH_VALUE);
//         json = rxStringFormat("%s%s%s\"%s\":\"%s\"", json, fieldsep, field, value);
//         fieldsep = rxStringNew(", ");
//     }
//     hashTypeReleaseIterator(hi);
//     json = rxStringFormat("%s%s", json, "}");
//     return json;
// }

// rxString asJson(dict *keyset)
// {
//     rxString json = rxStringNew("[");
//     dictIterator *iter = dictGetSafeIterator(keyset);
//     dictEntry *match;
//     rxString objsep = rxStringEmpty();
//     while ((match = dictNext(iter)))
//     {
//         robj *o = dictGetVal(match);
//         json = rxStringFormat("%s%s{ \"key\":\"%s\", \"value\" : {", json, objsep, (char *)match->key);
//         if (o->type == rxOBJ_TRIPLET)
//         {
//             Graph_Triplet *t = (Graph_Triplet *)o->ptr; // cn->value;
//             if (t)
//             {
//                 // if (t->subject)
//                 // {
//                 //     json = hashToJson(t->subject, json);
//                 //     json = rxStringFormat("%s%s", json, ", ");
//                 // }
//                 json = rxStringFormat("%s\"subject\":\"%s\"", json, t->subject_key);
//                 json = rxStringFormat("%s, \"objects\":[", json);
//                 listIter *eli = listGetIterator(t->edges, 0);
//                 listNode *elnarglen;
//                 rxString sep = rxStringEmpty();
//                 while ((eln = listNext(eli)))
//                 {
//                     Graph_Triplet_Edge *e = (Graph_Triplet_Edge *)eln->value;
//                     json = rxStringFormat("%s%s{\"object\":\"%s\", \"path\":[", json, sep, e->object_key);
//                     // if (e->object)
//                     //     json = hashToJson(e->object, json);
//                     listIter *li = listGetIterator(e->path, 0);
//                     listNode *ln;
//                     rxString pathsep = rxStringEmpty();
//                     while ((ln = listNext(li)))
//                     {
//                         rxString k = (rxString)ln->value;
//                         json = rxStringFormat("%s%s\"%s\"", json, pathsep, k);
//                         pathsep = rxStringNew(", ");
//                     }
//                     listReleaseIterator(li);
//                     json = rxStringFormat("%s%s", json, "] }");
//                     sep = rxStringNew(", ");
//                 }
//                 json = rxStringFormat("%s%s", json, "]");
//                 listReleaseIterator(eli);
//                 json = rxStringFormat("%s%s", json, "}");
//             }
//         }
//         else if (o->type == OBJ_HASH)
//         {
//             json = hashToJson(o, json);
//         }
//         json = rxStringFormat("%s%s", json, "}");
//         objsep = rxStringNew(", ");
//     }

//     json = rxStringFormat("%s%s", json, "]");
//     return json;
// }

void executeQueryCommand(Sjiboleth *parser, const char *cmd, int fetch_rows, RedisModuleCtx *ctx, list *errors, bool ranked, double ranked_lower_bound, double ranked_upper_bound)
{
    rxUNUSED(errors);
    rxUNUSED(fetch_rows);

    redisNodeInfo *index_config = rxIndexNode();
    redisNodeInfo *data_config = rxDataNode();
    auto *t = parser->Parse(cmd);
    if (parsedWithErrors(t))
    {
        writeParsedErrors(t, ctx);
        return;
    }
    auto *e = (SilNikParowy_Kontekst *)data_config->executor;
    if(e){
        if (e->fieldSelector)
        {
            delete e->fieldSelector;
            e->fieldSelector = NULL;
        }
        if (e->sortSelector)
        {
            delete e->sortSelector;
            e->sortSelector = NULL;
        }
    }
    if (data_config->executor == NULL)
    {
        e = new SilNikParowy_Kontekst(index_config, ctx);
        data_config->executor = e;
        index_config->executor = e;
    }
    rax *r = e->Execute(t);
    if (r)
    {
        WriteResults(r, ctx, fetch_rows, NULL, ranked, ranked_lower_bound, ranked_upper_bound, e->fieldSelector, e->sortSelector);
        if (e->CanDeleteResult())
            FreeResults(r);
    }
    else
        RedisModule_ReplyWithSimpleString(ctx, "No results!");
    e->Reset();
    releaseQuery(t);
}

int executeParseCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    rxString query = rxStringEmpty();
    int dialect_skippy = 0;
    Sjiboleth *parser = NULL;;
    size_t arg_len;
    char sep[2] = {0x00, 0x00};
    for (int j = 1; j < argc; ++j)
    {
        char *q = (char *)RedisModule_StringPtrLen(argv[j], &arg_len);
        if (rxStringMatchLen(q, 2, GREMLIN_PREFX, strlen(GREMLIN_PREFX), 1))
        {
            dialect_skippy = 2;
            parser = GremlinDialect::Get("GremlinDialect");
        }
        else if (rxStringMatchLen(q, 2, "j:", 2, 1))
        {
            parser = JsonDialect::Get("JsonDialect");
        dialect_skippy = 2;
        }
        else if (rxStringMatchLen(q, 2, "t:", 2, 1))
        {
            dialect_skippy = 2;
        parser = TextDialect::Get("TextDialect");
        }
            query = rxStringFormat("%s%s%s", query, sep, q);
            sep[0] = ' ';
    }
    if (parser== NULL)
        parser = QueryDialect::Get("QueryDialect");

    list *errors = listCreate();
    auto *t = parser->Parse(query + dialect_skippy);
    if (parsedWithErrors(t))
    {
        writeParsedErrors(t, ctx);
        return REDISMODULE_ERR;
    }
    t->Write(ctx);    
    listRelease(errors);
    releaseParser(parser);
    rxStringFree(query);
    return REDISMODULE_OK;
}

int executeQueryCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    rxString cmd = (char *)rxGetContainedObject(argv[0]);
    const char *target_setname = NULL;
    rxStringToUpper(cmd);
    int fetch_rows = strcmp(RX_GET, cmd) == 0 ? 1 : 0;
    rxString query = rxStringEmpty();
    bool ranked = false;
    double ranked_lower_bound = -1;//std::numeric_limits<double>::min();
    double ranked_upper_bound = std::numeric_limits<double>::max();
    int dialect_skippy = 0;
    size_t arg_len;
    char sep[2] = {0x00, 0x00};
    for (int j = 1; j < argc; ++j)
    {
        char *q = (char *)RedisModule_StringPtrLen(argv[j], &arg_len);
        if (rxStringMatch(q, AS_ARG, 1) && strlen(q) == strlen(AS_ARG))
        {
            ++j;
            q = (char *)RedisModule_StringPtrLen(argv[j], &arg_len);
            target_setname = q;
        }
        else if (rxStringMatch(q, RESET_ARG, MATCH_IGNORE_CASE))
        {
            FaBlok::ClearCache();
        }
        else if (rxStringMatch(q, RANKED_ARG, MATCH_IGNORE_CASE))
        {
            ranked = true;
        }
        else if (rxStringMatch(q, OVER_ARG, MATCH_IGNORE_CASE))
        {
            ++j;
            q = (char *)RedisModule_StringPtrLen(argv[j], &arg_len);
            ranked_lower_bound = atof(q);
            ranked_upper_bound = std::numeric_limits<double>::max();
        }
        else if (rxStringMatch(q, BELOW_ARG, MATCH_IGNORE_CASE))
        {
            ++j;
            q = (char *)RedisModule_StringPtrLen(argv[j], &arg_len);
            ranked_lower_bound = std::numeric_limits<double>::min();
            ranked_upper_bound = atof(q);
        }
        else
        {
            query = rxStringFormat("%s%s%s", query, sep, q);
            sep[0] = ' ';
        }
    }
    rxUNUSED(target_setname);
    Sjiboleth *parser;
    try
    {
        if (
            rxStringMatchLen(query, 2, GREMLIN_PREFX, strlen(GREMLIN_PREFX), 1) ||
            rxStringMatchLen(query, 2, GREMLIN_PREFIX_ALT, strlen(GREMLIN_PREFIX_ALT), 1))
        {
            parser = GremlinDialect::Get("GremlinDialect");
            dialect_skippy = strlen(GREMLIN_PREFX);
            // ranked = false;
        }
        else
            parser = QueryDialect::Get("QueryDialect");

        list *errors = listCreate();
        executeQueryCommand(parser, (const char *)query + dialect_skippy, fetch_rows, ctx, errors, ranked, ranked_lower_bound, ranked_upper_bound);
        listRelease(errors);
        releaseParser(parser);
        rxStringFree(query);
        return REDISMODULE_OK;
    }
    catch (void *)
    {
        RedisModule_ReplyWithSimpleString(ctx, "Pointer exception!");
        return C_ERR;
    }
}

int executeQueryAsyncCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    // redisNodeInfo *index_config = rxIndexNode();
    redisNodeInfo *data_config = rxDataNode();
    rxString cmd = (char *)rxGetContainedObject(argv[0]);
    auto *multiplexer = new RxQueryMultiplexer(rxStashCommand2(NULL, cmd, 1, argc, (void **)argv), data_config);
    multiplexer->Start(ctx);
    return REDISMODULE_OK;
}

int passthru(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    auto *multiplexer = new PassthruMultiplexer(argv, argc);
    multiplexer->Start(ctx);
    return REDISMODULE_OK;
}

int executeEvalShaCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc != 3)
        return RedisModule_ReplyWithSimpleString(ctx, HELP_STRING);
    size_t arg_len;
    const char *setname = RedisModule_StringPtrLen(argv[1], &arg_len);
    const char *sha1 = RedisModule_StringPtrLen(argv[2], &arg_len);
    RedisModuleCallReply *r = RedisModule_Call(ctx,
                                               QREDIS_CMD_SMEMBERS, "c",
                                               setname);
    if (r && RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ARRAY)
    {
        size_t reply_len = RedisModule_CallReplyLength(r);
        if (reply_len)
        {
            size_t j = 0;
            for (; j < reply_len; ++j)
            {
                RedisModuleCallReply *subreply = RedisModule_CallReplyArrayElement(r, j);
                if (subreply)
                {
                    size_t len;
                    const char *member_key = RedisModule_CallReplyStringPtr(subreply, &len);
                    RedisModuleCallReply *lua_reply = RedisModule_Call(ctx,
                                                                       REDIS_CMD_EVALSHA, "ccc",
                                                                       sha1,
                                                                       "1",
                                                                       member_key);
                    if (lua_reply)
                    {
                        int reply_type = RedisModule_CallReplyType(lua_reply);
                        if (reply_type == REDISMODULE_REPLY_ERROR)
                        {
                            return RedisModule_ReplyWithCallReply(ctx, lua_reply);
                        }
                        RedisModule_FreeCallReply(lua_reply);
                    }
                    RedisModule_FreeCallReply(subreply);
                }
                else
                    break;
                ++j;
            }
            char msg[512];
            snprintf((char *)&msg, sizeof(msg), "script %s called with %ld members from %s", sha1, j, setname);
            RedisModule_ReplyWithSimpleString(ctx, msg);
        }
        else
            RedisModule_ReplyWithSimpleString(ctx, "no members");
        RedisModule_FreeCallReply(r);
    }
    else
        RedisModule_ReplyWithSimpleString(ctx, "key not found");

    return REDIS_OK;
}

int executeLoadScriptCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc != 2)
        return RedisModule_ReplyWithSimpleString(ctx, HELP_STRING);
    size_t arg_len;
    const char *path = RedisModule_StringPtrLen(argv[1], &arg_len);
    struct stat sb;
    char *script_text;

    FILE *input_file = fopen(path, "r");
    if (input_file == NULL)
    {
        return RedisModule_ReplyWithSimpleString(ctx, "File not found");
    }

    stat(path, &sb);
    script_text = (char *)rxMemAllocSession(sb.st_size, "LoadScriptCommand");
    fread(script_text, sb.st_size, 1, input_file);
    fclose(input_file);

    RedisModuleCallReply *r = RedisModule_Call(ctx,
                                               REDIS_CMD_SCRIPT, "cc",
                                               REDIS_CMD_SCRIPT_LOAD_ARG,
                                               script_text);
    if (r)
    {
        RedisModule_ReplyWithCallReply(ctx, r);
        RedisModule_FreeCallReply(r);
    }
    else
        RedisModule_ReplyWithSimpleString(ctx, "Script not loaded");

    rxMemFree(script_text);

    return REDIS_OK;
}

int executeCacheCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    rxString response = FaBlok::GetCacheReport();
    if (argc > 1)
    {
        rxString cmd = (char *)rxGetContainedObject(argv[0]);
        rxStringToUpper(cmd);
        if (strcmp(CLEAR_ARG, cmd) == 0)
            FaBlok::ClearCache();
    }
    return RedisModule_ReplyWithSimpleString(ctx, response);
}

int executeHelpCommand(RedisModuleCtx *ctx, RedisModuleString **, int)
{
    return RedisModule_ReplyWithSimpleString(ctx, HELP_STRING);
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (RedisModule_Init(ctx, RX_QUERY, 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
    {
        rxServerLog(rxLL_NOTICE, "OnLoad rxQuery. Init error!");
        return REDISMODULE_ERR;
    }
    initRxSuite();
    rxRegisterConfig((void **)argv, argc);

    if (RedisModule_CreateCommand(ctx, RX_QUERY,
                                  executeQueryCommand, "readonly write", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, RX_GET,
                                  executeQueryCommand, "readonly write", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, RXCACHE,
                                  executeCacheCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, RX_LOADSCRIPT,
                                  executeLoadScriptCommand, "write", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, RX_EVALSHA,
                                  executeEvalShaCommand, "write", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, RX_HELP,
                                  executeHelpCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
   if (RedisModule_CreateCommand(ctx, "rxParse",
                                  executeParseCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"rxFetch",
                                  passthru, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "rxDescribe",
                                  passthru, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    redisNodeInfo *index_config = rxIndexNode();
    auto *c = (struct client *)RedisClientPool<struct client>::Acquire(index_config->host_reference, "_FAKE", RX_QUERY);
    RedisClientPool<struct client>::Release(c, RX_QUERY);
    redisNodeInfo *data_config = rxDataNode();
    c = (struct client *)RedisClientPool<struct client>::Acquire(data_config->host_reference, "_CLIENT", RX_QUERY);
    RedisClientPool<struct client>::Release(c, RX_QUERY);
    rxServerLog(rxLL_NOTICE, "OnLoad rxQuery. Done!");
    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx)
{
    rxUNUSED(ctx);
    finalizeRxSuite();
    return REDISMODULE_OK;
}
