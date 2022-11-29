#include "rxSuite.h"

#ifdef __cplusplus
extern "C"
{
#endif
#include "server.h"
#define REDISMODULE_EXPERIMENTAL_API
#include "../../src/redismodule.h"

#ifdef __cplusplus
}
#endif

#define rxUNUSED(x) (void)(x)

// uint64_t tokenSdsHash(const void *key)
// {
//     return dictGenHashFunction((unsigned char *)key, sdslen((char *)key));
// }

// int tokenSdsKeyCompare(void *privdata, const void *key1,
//                        const void *key2)
// {
//     int l1, l2;
//     DICT_NOTUSED(privdata);

//     l1 = sdslen((sds)key1);
//     l2 = sdslen((sds)key2);
//     if (l1 != l2)
//         return 0;
//     return memcmp(key1, key2, l1) == 0;
// }

// void tokenSdsDestructor(void *privdata, void *val)
// {
//     DICT_NOTUSED(privdata);

//     sdsfree(val);
// }

// void *tokenSdsDup(void *privdata, const void *key)
// {
//     DICT_NOTUSED(privdata);
//     return sdsdup((char *const)key);
// }

// /* Db->dict, keys are sds strings, vals are Redis objects. */
// dictType tokenDictTypeDefinition = {
//     tokenSdsHash,       /* hash function */
//     tokenSdsDup,        /* key dup */
//     NULL,               /* val dup */
//     tokenSdsKeyCompare, /* key compare */
//     tokenSdsDestructor, /* key destructor */
//     NULL                /* val destructor */
// };

void *rxMercatorShared = NULL;

void *initRxSuite()
{
    if (!rxMercatorShared)
    {
        rxSuiteShared *shared = zmalloc(sizeof(rxSuiteShared));
        memset(shared, 0x00, sizeof(rxSuiteShared));
        shared->parserClaimCount = 0;
        rxMercatorShared = shared;

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

        shared->controllerNode.host_reference = sdsnew("127.0.0.1:6379");
        shared->controllerNode.host = sdsnew("127.0.0.1");
        shared->controllerNode.port = 6379;
        shared->controllerNode.database_id = 0;
        shared->controllerNode.is_local = 0;
        shared->controllerNode.executor = NULL;


    }
    return rxMercatorShared;
}

rxSuiteShared *getRxSuite()
{
    if (rxMercatorShared == NULL)
        initRxSuite();
    return rxMercatorShared;
}

void finalizeRxSuite()
{
    if (!rxMercatorShared)
    {
        rxMemFree(rxMercatorShared);
    }
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

redisNodeInfo *rxControllerNode()
{
    rxSuiteShared *config = getRxSuite();
    return &config->controllerNode;
}

static void extractArgs(RedisModuleString **oargv, int j, redisNodeInfo *node)
{
    RedisModuleString **argv = (RedisModuleString **)oargv;
    const char *s = (const char *)argv[j]->ptr;
    node->host = sdsnew(s);
    s = (const char *)argv[j + 1]->ptr;
    node->port = atoi(s);
    s = (const char *)argv[j + 2]->ptr;
    node->database_id = atoi(s);
}

void rxRegisterConfig(void **oargv, int argc)
{
    RedisModuleString **argv = (RedisModuleString **)oargv;
    rxSuiteShared *config = initRxSuite();
    for (int j = 0; j < argc; ++j)
    {
        const char *arg = (const char *)argv[j]->ptr;
        int argl = strlen(arg);
        if (stringmatchlen(arg, argl, "INDEX", 5, 1) == 1)
        {
            extractArgs(argv, j + 1, &config->indexNode);
            j += 3;
        }
        else if (stringmatchlen(arg, argl, "DATA", 4, 1) == 1)
        {
            extractArgs(argv, j + 1, &config->dataNode);
            j += 3;
        }
        else if (stringmatchlen(arg, argl, "CONTROLLER", 4, 1) == 1)
        {
            extractArgs(argv, j + 1, &config->controllerNode);
            j += 3;
        }
        else if (stringmatchlen(arg, argl, "DEFAULT_OPERATOR", 16, 1) == 1)
        {
            ++j;
            const char *s = (const char *)argv[j]->ptr;
            config->defaultQueryOperator = sdsnew(s);
        }
        else if (stringmatchlen(arg, argl, "CDN", 16, 1) == 1)
        {
            ++j;
            const char *s = (const char *)argv[j]->ptr;
            config->cdnRootUrl = sdsnew(s);
        }
        else if (stringmatchlen(arg, argl, "START-SCRIPT", 16, 1) == 1)
        {
            ++j;
            const char *s = (const char *)argv[j]->ptr;
            config->startScript = sdsnew(s);
        }
        else if (stringmatchlen(arg, argl, "INSTALL-SCRIPT", 16, 1) == 1)
        {
            ++j;
            const char *s = (const char *)argv[j]->ptr;
            config->installScript = sdsnew(s);
        }
    }
    config->indexNode.is_local = strcmp(config->indexNode.host, config->dataNode.host) == 0 && config->indexNode.port == config->dataNode.port;
    config->indexNode.host_reference = sdscatprintf(sdsempty(), "%s:%d", config->indexNode.host, config->indexNode.port);
    config->dataNode.is_local = config->indexNode.is_local;
    config->dataNode.host_reference = sdscatprintf(sdsempty(), "%s:%d", config->dataNode.host, config->dataNode.port);
    if (config->cdnRootUrl == NULL)
        config->cdnRootUrl = sdsnew("https://roxoft.dev/assets");
    if (config->startScript == NULL)
        config->startScript = sdsnew("__start_redis.sh");
    if (config->installScript == NULL)
        config->installScript = sdsnew("__install_rxmercator.sh");
}

char *rxGetExecutable()
{
    return server.executable;
}
