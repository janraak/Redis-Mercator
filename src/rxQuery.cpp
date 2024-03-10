
/* rxQuery -- Execute SET and/or Graph queries
 *            against a Redis Database
 */
#define REDISMODULE_EXPERIMENTAL_API
#include "rxQuery.hpp"
#include <string>

using std::string;
#include "sjiboleth-fablok.hpp"
#include "sjiboleth.hpp"
#include "sjiboleth.h"

#include "../../deps/hiredis/hiredis.h"
#ifdef __cplusplus
extern "C"
{
#endif
// #include "rxSessionMemory.hpp"
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

#include "tls.hpp"

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
#include "../../src/adlist.h"

class QueryAsync : public Multiplexer
{
public:
    rax *r;
    SilNikParowy_Kontekst *kontekst;
    int fetch_rows;
    bool ranked = false;
    double ranked_lower_bound;
    double ranked_upper_bound;

    QueryAsync()
        : Multiplexer()
    {
    }

    QueryAsync(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
        : Multiplexer(argv, argc)
    {
        this->ctx = ctx;
        this->r = NULL;
        redisNodeInfo *index_config = rxIndexNode();
        redisNodeInfo *data_config = rxDataNode();
            auto kontekst = new SilNikParowy_Kontekst(index_config, ctx);
        // if (data_config->executor == NULL)
        // {
        //     auto kontekst = new SilNikParowy_Kontekst(index_config, ctx);
        //     data_config->executor = kontekst;
        //     index_config->executor = kontekst;
        // }

            this->kontekst = kontekst;             //(SilNikParowy_Kontekst *)data_config->executor;
            this->fetch_rows = 0;
            this->ranked = false;
            this->ranked_lower_bound = -1; // std::numeric_limits<double>::min();
            this->ranked_upper_bound = std::numeric_limits<double>::max();
    }

    int Write(RedisModuleCtx *ctx)
    {
        if (this->r)
        {
            WriteResults(this->r, ctx, this->fetch_rows, NULL, this->ranked, this->ranked_lower_bound, this->ranked_upper_bound, this->kontekst->fieldSelector, this->kontekst->sortSelector);
            if (kontekst->CanDeleteResult())
                FreeResults(this->r);
        }
        else
            RedisModule_ReplyWithSimpleString(ctx, "No results!");
        kontekst->Reset();
        return -1;
    }

    int Done()
    {
        return -1;
    }

    int Execute()
    {
        return -1;
    };

    void *StopThread()
    {
        this->state = Multiplexer::done;
    //int rc = 0; //pthread_exit(&rc);
        return NULL;
    }
};

// static void *_allocateKontekst(void *data_config){
//     return new SilNikParowy_Kontekst(data_config, data_config);
// }

short executeQueryCommand(QueryAsync *multiplexer, Sjiboleth *parser, const char *cmd, RedisModuleCtx *ctx, list *errors)
{
    rxUNUSED(errors);

    auto *t = parser->Parse(cmd);
    short read_or_write = t->read_or_write;
    
    // t->show(NULL);
    if (parsedWithErrors(t))
    {
        writeParsedErrors(t, ctx);
        return read_or_write;
    }
    if (multiplexer->kontekst)
    {
        auto kontekst = multiplexer->kontekst;
        if (kontekst->fieldSelector)
        {
            delete kontekst->fieldSelector;
            kontekst->fieldSelector = NULL;
        }
        if (kontekst->sortSelector)
        {
            delete kontekst->sortSelector;
            kontekst->sortSelector = NULL;
        }
    }
    auto kontekst = multiplexer->kontekst;
    // tls_get<SilNikParowy_Kontekst *>((const char *)"SilNikParowy_Kontekst", _allocateKontekst, data_config);
    kontekst->serviceConfig = rxIndexNode();
    multiplexer->r = kontekst->Execute(t);
    releaseQuery(t);
    return read_or_write;
}

int executeParseCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    rxString query = rxStringEmpty();
    int dialect_skippy = 0;
    Sjiboleth *parser = NULL;
    ;
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
    int l = strlen(query);
    if ((query[0] == '\"' && query[l - 1] == '\"') || (query[0] == '\'' && query[l - 1] == '\''))
    {
        ((char *)query)[0] = ' ';
        ((char *)query)[l - 1] = ' ';
        ++query;
    }

    if (parser == NULL)
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
};

int executeQueryCommand(QueryAsync *multiplexer, RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    loadCtx = ctx;
    if (rxGetMemoryUsedPercentage() > 95.0)
    {
        RedisModule_ReplyWithSimpleString(ctx, "Short on memory! Query command not executed.");
        return C_ERR;
    }

    rxString cmd = (char *)rxGetContainedObject(argv[0]);
    const char *target_setname = NULL;
    rxStringToUpper(cmd);
    multiplexer->fetch_rows = strcmp(RX_GET, cmd) == 0 ? 1 : strcmp("Q", cmd) == 0 ? 1 : strcmp("G", cmd) == 0 ? 1 : 0;
    rxString query = rxStringEmpty();
    rxString sv_query;
    multiplexer->ranked = false;
    multiplexer->ranked_lower_bound = -1; // std::numeric_limits<double>::min();
    multiplexer->ranked_upper_bound = std::numeric_limits<double>::max();
    int dialect_skippy = 0;
    size_t arg_len;
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
            multiplexer->ranked = true;
        }
        else if (rxStringMatch(q, OVER_ARG, MATCH_IGNORE_CASE))
        {
            ++j;
            q = (char *)RedisModule_StringPtrLen(argv[j], &arg_len);
            multiplexer->ranked_lower_bound = atof(q);
            multiplexer->ranked_upper_bound = std::numeric_limits<double>::max();
        }
        else if (rxStringMatch(q, BELOW_ARG, MATCH_IGNORE_CASE))
        {
            ++j;
            q = (char *)RedisModule_StringPtrLen(argv[j], &arg_len);
            multiplexer->ranked_lower_bound = std::numeric_limits<double>::min();
            multiplexer->ranked_upper_bound = atof(q);
        }
        else
        {
            query = rxStringFormat("%s %s", query, q);
        }
    }
    rxUNUSED(target_setname);
    Sjiboleth *parser;
    sv_query = query;
    try
    {
        while (*query == ' ')
        {
            ++query;
        }
        int l = strlen(query);
        char *tail = (char *)query + (l - 1);
        while (*tail == ' ')
        {
            *tail = 0x00;
            --l;
            --tail;
        }
        if ((query[0] == '\"' && query[l - 1] == '\"') || (query[0] == '\'' && query[l - 1] == '\''))
        {
            ((char *)query)[0] = ' ';
            ((char *)query)[l - 1] = 0x00;
            ++query;
        }

        const char *g = strstr(query, "g:");
        if (g == NULL)
            g = strstr(query, "G:");
        if (strcmp("G", cmd) == 0)
        {
            parser = GremlinDialect::Get("GremlinDialect");
        }
        else if (g != NULL || strcmp("G", cmd) == 0)
        {
            parser = GremlinDialect::Get("GremlinDialect");
            query = g + strlen(GREMLIN_PREFX);
        }
        else
        {
            parser = QueryDialect::Get("QueryDialect");
        }
        list *errors = listCreate();
        short read_or_write = executeQueryCommand(multiplexer, parser, (const char *)query + dialect_skippy, ctx, errors);
        // Propage WRITE queries to replicas
        if(read_or_write != Q_READONLY)
            rxAlsoPropagate(0, (void**)argv, argc, -1);
        listRelease(errors);
        releaseParser(parser);
        rxStringFree(sv_query);
        return REDISMODULE_OK;
    }
    catch (void *)
    {
        RedisModule_ReplyWithSimpleString(ctx, "Pointer exception!");
        return C_ERR;
    }
}

void *QueryAsync_Go(void *privData)
{
    auto *multiplexer = (QueryAsync *)privData;
    multiplexer->state = Multiplexer::running;

    executeQueryCommand(multiplexer, multiplexer->ctx, multiplexer->argv, multiplexer->argc);
    return multiplexer->StopThread();
}

int executeQueryAsyncCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    auto *multiplexer = new QueryAsync(ctx, argv, argc);
    multiplexer->Async(ctx, QueryAsync_Go);

    return C_OK;
}

int passthru(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    auto *multiplexer = new PassthruMultiplexer(argv, argc);
    multiplexer->Async(ctx, PassthruAsync_Go);
    return REDISMODULE_OK;
}

int addTypeTree(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc < 2)
        return RedisModule_ReplyWithSimpleString(ctx, HELP_STRING);
    size_t arg_len;
    char type_prefix = '#';

    int n = 2;
    const char *super_type = RedisModule_StringPtrLen(argv[1], &arg_len);
    if(rxStringMatch("VERTEX", super_type, 1)){
        type_prefix = '#';
        super_type = RedisModule_StringPtrLen(argv[n], &arg_len);
        ++n;
    }else if(rxStringMatch("EDGE", super_type, 1)){
        type_prefix = '~';
        super_type = RedisModule_StringPtrLen(argv[n], &arg_len);
        ++n;
    }
    rxString typetree = rxStringFormat("%c%s", type_prefix, super_type);
    while (n < argc)
    {
        const char *sub_type = RedisModule_StringPtrLen(argv[n], &arg_len);
        rxString typetree2 = rxStringFormat("%c%s", type_prefix, sub_type);
        RedisModuleCallReply *r = RedisModule_Call(ctx,
                                                   "SADD", "cc",
                                                   typetree, typetree2);

        rxStringFree((typetree2));
        if (r)
            RedisModule_FreeCallReply(r);

        ++n;
    }
    RedisModule_ReplyWithSimpleString(ctx, typetree);
    rxStringFree((typetree));
    rxAlsoPropagate(0, (void**)argv, argc, -1);
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
    script_text = (char *)rxMemAlloc(sb.st_size + 1);
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

    rxAlsoPropagate(0, (void**)argv, argc, -1);

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
                                  executeQueryAsyncCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, RX_GET,
                                  executeQueryAsyncCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "Q",
                                  executeQueryAsyncCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "G",
                                  executeQueryAsyncCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "GA",
                                  executeQueryAsyncCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR)
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
    if (RedisModule_CreateCommand(ctx, "rxFetch",
                                  passthru, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "rxDescribe",
                                  passthru, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "rxTypeTree",
                                  addTypeTree, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    redisNodeInfo *index_config = rxIndexNode();
    auto *c = (struct client *)RedisClientPool<struct client>::Acquire(index_config->host_reference, "_FAKE", RX_QUERY);
    RedisClientPool<struct client>::Release(c, RX_QUERY);
    redisNodeInfo *data_config = rxDataNode();
    c = (struct client *)RedisClientPool<struct client>::Acquire(data_config->host_reference, "_CLIENT", RX_QUERY);
    RedisClientPool<struct client>::Release(c, RX_QUERY);
    rxServerLog(rxLL_NOTICE, "OnLoad rxQuery. Done!");

    Sjiboleth::Get("QueryDialect");
    Sjiboleth::Get("GremlinDialect");
    Sjiboleth::Get("JsonDialect");
    Sjiboleth::Get("TextDialect");

    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx)
{
    rxUNUSED(ctx);
    finalizeRxSuite();
    return REDISMODULE_OK;
}
