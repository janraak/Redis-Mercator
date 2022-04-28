// TODO:
//      1) Add default operator to module load.
//      2) Add clear option to rxcache

/* rxIndexer -- An example of modules dictionary API
 *
 */
#define REDISMODULE_EXPERIMENTAL_API
#include "redismodule.h"

/*
 * This module execute rx query commands
 */
#include "../../deps/hiredis/hiredis.h"
#include "../../src/dict.h"
#include "parser.h"
#include <queryEngine.h>
#include <string.h>
#include <sys/stat.h>
#include "util.h"

const char *AS_ARG = "AS";
const char *DEBUG_ARG = "DEBUG";
const char *RESET_ARG = "RESET";
const char *CLEAR_ARG = "RESET";
const char *GREMLIN_PREFX = "g:";
const char *GREMLIN_PREFIX_ALT = "g.";
const char *GREMLIN_DIALECT = "gremlin";
const char *QUERY_DIALECT = "query";

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

typedef struct indexerThreadRX_LOADSCRIPT
{
    sds index_address;
    int index_port;
    sds default_query_operator;
    int database_no;
} IndexerInfo;

IndexerInfo index_info = {0};

sds hashToJson(robj *o, sds json)
{
    hashTypeIterator *hi = hashTypeInitIterator(o);
    sds fieldsep = sdsempty();
    while (hashTypeNext(hi) != C_ERR)
    {
        sds field = hashTypeCurrentObjectNewSds(hi, OBJ_HASH_KEY);
        sds value = hashTypeCurrentObjectNewSds(hi, OBJ_HASH_VALUE);
        json = sdscatprintf(json, "%s\"%s\":\"%s\"", fieldsep, field, value);
        fieldsep = sdsnew(", ");
    }
    hashTypeReleaseIterator(hi);
    json = sdscat(json, "}");
    return json;
}

sds asJson(dict *keyset)
{
    sds json = sdsnew("[");
    dictIterator *iter = dictGetSafeIterator(keyset);
    dictEntry *match;
    sds objsep = sdsempty();
    while ((match = dictNext(iter)))
    {
        robj *o = dictGetVal(match);
        json = sdscatprintf(json, "%s{ \"key\":\"%s\", \"value\" : {", objsep, (char *)match->key);
        if (o->type == OBJ_TRIPLET)
        {
            Graph_Triplet *t = (Graph_Triplet *)o->ptr; // cn->value;
            if (t)
            {
                // if (t->subject)
                // {
                //     json = hashToJson(t->subject, json);
                //     json = sdscat(json, ", ");
                // }
                json = sdscatprintf(json, "\"subject\":\"%s\"", t->subject_key);
                json = sdscatprintf(json, ", \"objects\":[");
                listIter *eli = listGetIterator(t->edges, 0);
                listNode *eln;
                sds sep = sdsempty();
                while ((eln = listNext(eli)))
                {
                    Graph_Triplet_Edge *e = (Graph_Triplet_Edge *)eln->value;
                    json = sdscatprintf(json, "%s{\"object\":\"%s\", \"path\":[", sep, e->object_key);
                    // if (e->object)
                    //     json = hashToJson(e->object, json);
                    listIter *li = listGetIterator(e->path, 0);
                    listNode *ln;
                    sds pathsep = sdsempty();
                    while ((ln = listNext(li)))
                    {
                        sds k = (sds)ln->value;
                        json = sdscatprintf(json, "%s\"%s\"", pathsep, k);
                        pathsep = sdsnew(", ");
                    }
                    listReleaseIterator(li);
                    json = sdscat(json, "] }");
                    sep = sdsnew(", ");
                }
                json = sdscat(json, "]");
                listReleaseIterator(eli);
                json = sdscat(json, "}");
            }
        }
        else if (o->type == OBJ_HASH)
        {
            json = hashToJson(o, json);
        }
        json = sdscat(json, "}");
        objsep = sdsnew(", ");
    }

    json = sdscat(json, "]");
    return json;
}

void RespondWithHash(RedisModuleCtx *ctx, sds key, robj *o)
{
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithStringBuffer(ctx, (char *)key, sdslen(key));
    RedisModule_ReplyWithArray(ctx, 2 * hashTypeLength(o));
    hashTypeIterator *hi;
    hi = hashTypeInitIterator(o);
    while (hashTypeNext(hi) != C_ERR)
    {
        sds field = hashTypeCurrentObjectNewSds(hi, OBJ_HASH_KEY);
        sds value = hashTypeCurrentObjectNewSds(hi, OBJ_HASH_VALUE);
        RedisModule_ReplyWithStringBuffer(ctx, (char *)field, sdslen(field));
        RedisModule_ReplyWithStringBuffer(ctx, (char *)value, sdslen(value));
    }
    hashTypeReleaseIterator(hi);
}

void cleanupTriplets(dict *keyset)
{
    dictIterator *di = dictGetSafeIterator(keyset);
    dictEntry *de;
    while ((de = dictNext(di)))
    {
        sds k = dictGetKey(de);
        robj *o = dictGetVal(de);
        if (o->type == OBJ_TRIPLET)
        {
            dictDelete(keyset, k);
            Graph_Triplet *t = (Graph_Triplet *)o->ptr;
            deleteTripletResult(t);
            zfree(o);
            //sdsfree(k);
        }
    }
    dictReleaseIterator(di);
}

int executeQueryCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    robj *q = (robj *)argv[0];
    sds cmd = sdsnew(q->ptr);
    const char *target_setname = NULL;
    sdstoupper(cmd);
    int fetch_rows = strcmp(RX_GET, cmd) == 0 ? 1 : 0;
    sds query = sdsempty();
    const char *dialect = QUERY_DIALECT;
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
            clearCache();
        }
        else
        {
            query = sdscatfmt(query, "%s%s", sep, q);
            sep = sdsnew(" ");
        }
    }
    sdsfree(sep);
    if (
        stringmatchlen(query, 2, GREMLIN_PREFX, strlen(GREMLIN_PREFX), 1) ||
        stringmatchlen(query, 2, GREMLIN_PREFIX_ALT, strlen(GREMLIN_PREFIX_ALT), 1))
    {
        dialect = GREMLIN_DIALECT;
        dialect_skippy = strlen(GREMLIN_PREFX);
    }

    Parser *parser = newParser(dialect);
    setDefaultOperator(parser, index_info.default_query_operator);
    setShowDebugInfo(parser, show_parser_debug_info);

    parseQuery(parser, (const char *)query + dialect_skippy);
    // if(dialect_skippy)
    //     optimizeQuery(parser);
    sdsfree(query);
    if (connectIndex(parser, ctx, index_info.index_address, index_info.index_port) == C_ERR)
    {
        RedisModule_ReplyWithSimpleString(ctx, "Cannot connect to index");
        return C_ERR;
    }
    dict *keyset = executeQuery(parser);
    // emitResults(ctx, fetch_rows, keyset);
    size_t check_items = dictSize(keyset);
    size_t items = 0;
    // sds json = asJson(keyset);
    // printf("%s\n", json);
    // sdsfree(json);
    if (listLength(parser->errors))
    {
        sds errors = sdsempty();
        while (listLength(parser->errors) > 0)
        {
            listNode *node = listIndex(parser->errors, 0);
            if (node)
            {
                sds e = node->value;
                listDelNode(parser->errors, node);
                errors = sdscatfmt(errors, "\n%s", e);
            }
            RedisModule_ReplyWithSimpleString(ctx, errors);
            return REDIS_ERR;
        }
    }
    else if (keyset)
    {
        RedisModule_ReplyWithArray(ctx, check_items);
        dictIterator *iter = dictGetSafeIterator(keyset);
        dictEntry *match;
        RedisModuleCallReply *reply = NULL;
        while ((match = dictNext(iter)))
        {
            robj *o = dictGetVal(match);
            if (target_setname)
            {
                RedisModuleCallReply *r = RedisModule_Call(ctx,
                                                           REDIS_CMD_SADD, "cc",
                                                           target_setname,
                                                           (char *)match->key);
                if (r)
                    RedisModule_FreeCallReply(r);
            }
            if (fetch_rows == 0)
            {
                if (o->type == OBJ_TRIPLET)
                {
                        Graph_Triplet *t = (Graph_Triplet *)o->ptr; // cn->value;
                        if (t)
                        {
                            RedisModule_ReplyWithArray(ctx, 4);
                            RedisModule_ReplyWithStringBuffer(ctx, "subject", 7);
                            RedisModule_ReplyWithStringBuffer(ctx, (char *)t->subject_key, strlen((char *)t->subject_key));
                            RedisModule_ReplyWithStringBuffer(ctx, "objects", 7);
                            if (listLength(t->edges) > 1)
                                RedisModule_ReplyWithArray(ctx, listLength(t->edges));

                            listIter *eli = listGetIterator(t->edges, 0);
                            listNode *eln;
                            while ((eln = listNext(eli)))
                            {
                                RedisModule_ReplyWithArray(ctx, 4);
                                Graph_Triplet_Edge *e = (Graph_Triplet_Edge *)eln->value;

                                RedisModule_ReplyWithStringBuffer(ctx, "object", 6);
                                RedisModule_ReplyWithStringBuffer(ctx, (char *)e->object_key, strlen((char *)e->object_key));
                                RedisModule_ReplyWithStringBuffer(ctx, "path", 4);
                                RedisModule_ReplyWithArray(ctx, listLength(e->path));
                                listIter *li = listGetIterator(e->path, 0);
                                listNode *ln;
                                while ((ln = listNext(li)))
                                {
                                    sds k = (sds)ln->value;
                                    RedisModule_ReplyWithStringBuffer(ctx, (char *)k, sdslen(k));
                                }
                                listReleaseIterator(li);
                            }
                    }
                }
                else
                    RedisModule_ReplyWithStringBuffer(ctx, (char *)match->key, strlen((char *)match->key));
            }
            else
            {
                if (o->type == OBJ_TRIPLET)
                {
                    Graph_Triplet *t = (Graph_Triplet *)o->ptr; // cn->value;
                    RedisModule_ReplyWithArray(ctx, 3);
                    RespondWithHash(ctx, t->subject_key, (robj *)t->subject);
                    if (listLength(t->edges) > 0)
                        RedisModule_ReplyWithArray(ctx, listLength(t->edges));

                    listIter *eli = listGetIterator(t->edges, 0);
                    listNode *eln;
                    while ((eln = listNext(eli)))
                    {
                        Graph_Triplet_Edge *e = (Graph_Triplet_Edge *)eln->value;
                        RespondWithHash(ctx, e->object_key, (robj *)e->object);
                        RedisModule_ReplyWithArray(ctx, listLength(e->path));
                        listIter *li = listGetIterator(e->path, 0);
                        listNode *ln;
                        while ((ln = listNext(li)))
                        {
                            sds k = (sds)ln->value;
                            RedisModule_ReplyWithStringBuffer(ctx, (char *)k, sdslen(k));
                        }
                    }
                }
                else
                {
                    RedisModule_ReplyWithArray(ctx, 2);
                    RedisModule_ReplyWithStringBuffer(ctx, (char *)match->key, strlen((char *)match->key));
                    char *index_entry = (char *)(char *)match->v.val;
                    char *first_tab = strchr(index_entry, '\t');
                    char key_type = first_tab ? *(first_tab + 1) : HASHTYPE;
                    sds e;
                    switch (key_type)
                    {
                    case HASHTYPE:
                        reply = RedisModule_Call(ctx, REDIS_CMD_HGETALL, "c", (char *)match->key);
                        break;
                    case STRINGTYPE:
                        reply = RedisModule_Call(ctx, REDIS_CMD_GET, "c", (char *)match->key);
                        break;
                    default:
                        e = sdscatfmt(sdsnewlen(&key_type, 1), ": Unsupported key type found: %s!", match->key);
                        RedisModule_ReplyWithStringBuffer(ctx, e, sdslen(e));
                        // return RedisModule_ReplyWithError(ctx, "Unsupported key type found!");
                    }
                    RedisModule_ReplyWithCallReply(ctx, reply);
                    if (reply)
                        RedisModule_FreeCallReply(reply);
                    // RedisModule_ReplySetArrayLength(ctx, 2);
                    items++;
                }
            }
            items++;
        }
        dictReleaseIterator(iter);
        cleanupTriplets(keyset);
    }
    // RedisModule_ReplySetArrayLength(ctx, items);
    deleteAllTempKeysetDescriptors();
    freeParser(parser);
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
                                               REDIS_CMD_SMEMBERS, "c",
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
            snprintf((char *)&msg, sizeof(msg), "script %s called with %d members from %s", sha1, j, setname);
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
    script_text = zmalloc(sb.st_size);
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

    zfree(script_text);

    return REDIS_OK;
}

int executeCacheCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    sds response = getCacheReport();
    if (argc > 1)
    {
        robj *q = (robj *)argv[1];
        sds cmd = sdsnew(q->ptr);
        sdstoupper(cmd);
        if (strcmp(CLEAR_ARG, cmd) == 0)
            clearCache();
    }
    return RedisModule_ReplyWithSimpleString(ctx, response);
}

int executeHelpCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    rxUNUSED(argv);
    rxUNUSED(argc);
    return RedisModule_ReplyWithSimpleString(ctx, HELP_STRING);
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (RedisModule_Init(ctx, RX_QUERY, 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    initRxSuite();
    claimParsers();
    const char *INDEX_SERVER_ADDRESS = "127.0.0.1";
    int INDEX_SERVER_PORT = 6379;
    const char *DEFAULT_QUERY_OPERATOR = "&";

    size_t arg_len;
    if (argc >= 1)
    {
        INDEX_SERVER_ADDRESS = RedisModule_StringPtrLen(argv[0], &arg_len);
        if (argc >= 2)
        {
            const char *port = RedisModule_StringPtrLen(argv[1], &arg_len);
            INDEX_SERVER_PORT = atoi(port);
            if (argc >= 3)
            {
                DEFAULT_QUERY_OPERATOR = RedisModule_StringPtrLen(argv[2], &arg_len);
            }
        }
    }

    index_info.index_address = sdsdup(sdsnew(INDEX_SERVER_ADDRESS));
    index_info.index_port = INDEX_SERVER_PORT;
    index_info.default_query_operator = sdsdup(sdsnew(DEFAULT_QUERY_OPERATOR));
    index_info.database_no = 0;

    if (RedisModule_CreateCommand(ctx, RX_QUERY,
                                  executeQueryCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, RX_GET,
                                  executeQueryCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, RXCACHE,
                                  executeCacheCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, RX_LOADSCRIPT,
                                  executeLoadScriptCommand, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, RX_EVALSHA,
                                  executeEvalShaCommand, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, RX_HELP,
                                  executeHelpCommand, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx)
{
    REDISMODULE_NOT_USED(ctx);
    clearCache();
    deleteAllKeysetDescriptors();
    releaseParsers();
    finalizeRxSuite();
    return REDISMODULE_OK;
}
