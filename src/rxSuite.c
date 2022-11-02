#include "rxSuite.h"

#ifdef __cplusplus
extern "C"
{
#endif
#include "server.h"
#ifdef __cplusplus
}
#endif

#define rxUNUSED(x) (void)(x)

uint64_t tokenSdsHash(const void *key)
{
    return dictGenHashFunction((unsigned char *)key, sdslen((char *)key));
}

int tokenSdsKeyCompare(void *privdata, const void *key1,
                       const void *key2)
{
    int l1, l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2)
        return 0;
    return memcmp(key1, key2, l1) == 0;
}

void tokenSdsDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    sdsfree(val);
}

void *tokenSdsDup(void *privdata, const void *key)
{
    DICT_NOTUSED(privdata);
    return sdsdup((char *const)key);
}

/* Db->dict, keys are sds strings, vals are Redis objects. */
dictType tokenDictTypeDefinition = {
    tokenSdsHash,       /* hash function */
    tokenSdsDup,        /* key dup */
    NULL,               /* val dup */
    tokenSdsKeyCompare, /* key compare */
    tokenSdsDestructor, /* key destructor */
    NULL                /* val destructor */
};

void initRxSuite()
{
    redisDb *db = &server.db[0];
    dict *d = db->dict;
    if (!d->privdata)
    {
        rxSuiteShared *shared = zmalloc(sizeof(rxSuiteShared));
        shared->OperationMap = NULL;
        shared->KeysetCache = NULL;
        shared->parserClaimCount = 0;
        shared->tokenDictType = &tokenDictTypeDefinition;
        d->privdata = shared;

        shared->indexNode.host_reference = sdsnew("127.0.0.1:6379");
        shared->indexNode.host = sdsnew("127.0.0.1");
        shared->indexNode.port = 6379;
        shared->indexNode.database_id = 0;
        shared->indexNode.is_local = 0;
        shared->indexNode.executor = NULL;

        shared->dataNode.host_reference = sdsnew("127.0.0.1:6379");
        shared->dataNode.host = sdsnew("127.0.0.1");
        shared->dataNode.port = 6379;
        shared->dataNode.database_id = 0;
        shared->dataNode.is_local = 0;
        shared->dataNode.executor = NULL;
        shared->defaultQueryOperator = sdsnew("&");
    }
}

rxSuiteShared *getRxSuite()
{
    redisDb *db = &server.db[0];
    dict *d = db->dict;
    if (d->privdata == NULL)
        initRxSuite();
    return d->privdata;
}

void finalizeRxSuite()
{
    redisDb *db = &server.db[0];
    dict *d = db->dict;
    if (!d->privdata)
    {
        zfree(d->privdata);
    }
}

int sdscharcount(char *s, char c)
{
    int tally = 0;
    char *t = s;
    while (*t)
    {
        if (*t == toupper(c))
            ++tally;
        ++t;
    }
    return tally;
}

redisNodeInfo *rxIndexNode()
{
    rxSuiteShared *config = getRxSuite();
    return &config->indexNode;
}

redisNodeInfo *rxDataNode()
{
    rxSuiteShared *config = getRxSuite();
    return &config->dataNode;
}

static void extractArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int j, redisNodeInfo *node)
{
    rxUNUSED(ctx);
    const char *s = RedisModule_StringPtrLen(argv[j], NULL);
    node->host = sdsnew(s);
    s = RedisModule_StringPtrLen(argv[j + 1], NULL);
    node->port = atoi(s);
    s = RedisModule_StringPtrLen(argv[j + 2], NULL);
    node->database_id = atoi(s);
}

void rxRegisterConfig(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    rxSuiteShared *config = getRxSuite();
    for (int j = 0; j < argc; ++j)
    {
        const char *arg = RedisModule_StringPtrLen(argv[j], NULL);
        int argl = strlen(arg);
        if (stringmatchlen(arg, argl, "INDEX", 5, 1) == 1)
        {
            extractArgs(ctx, argv, j + 1, &config->indexNode);
            j += 3;
        }
        else if (stringmatchlen(arg, argl, "DATA", 4, 1) == 1)
        {
            extractArgs(ctx, argv, j + 1, &config->dataNode);
            j += 3;
        }
        else if (stringmatchlen(arg, argl, "DEFAULT_OPERATOR", 16, 1) == 1)
        {
            ++j;
            const char *s = RedisModule_StringPtrLen(argv[j], NULL);
            config->defaultQueryOperator = sdsnew(s);
        }
    }
    config->indexNode.is_local = sdscmp(config->indexNode.host, config->dataNode.host) == 0 
            && config->indexNode.port == config->dataNode.port;
    config->indexNode.host_reference = sdscatprintf(sdsempty(), "%s:%d", config->indexNode.host, config->indexNode.port);
    config->dataNode.is_local = config->indexNode.is_local;
    config->dataNode.host_reference = sdscatprintf(sdsempty(), "%s:%d", config->dataNode.host, config->dataNode.port);
}


/* Apply tolower() to every character of the string 's'. */
void strtolower(const char *cs) {
    char *s = (char *)cs;
    int len = strlen(s), j;
    for (j = 0; j < len; j++) s[j] = tolower(s[j]);
}

/* Apply toupper() to every character of the string 's'. */
void strtoupper(const char *cs) {
    char *s = (char *)cs;
    int len = strlen(s), j;
    for (j = 0; j < len; j++) s[j] = toupper(s[j]);
}
