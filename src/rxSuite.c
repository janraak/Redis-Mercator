#include "rxSuite.h"

#ifdef __cplusplus
extern "C"
{
#endif
#include "../../src/server.h"

#ifdef __cplusplus
}
#endif

#define rxUNUSED(x) (void)(x)

typedef const char *rxRedisModuleString;
static rxSuiteShared rxMercatorConfig;

extern void *rxMercatorShared = NULL;

void *initRxSuite()
{
    if (!rxMercatorShared)
    {
        rxSuiteShared *shared = &rxMercatorConfig;
        //zmalloc(sizeof(rxSuiteShared));
        memset(shared, 0x00, sizeof(rxSuiteShared));
        shared->parserClaimCount = 0;        
        shared->debugMessages = 0;        
        rxMercatorShared = shared;
        shared->triggeredKeys = raxNew();
        mtx_init(&shared->triggeredKeysLock, mtx_plain);
        shared->indexableKeys = raxNew();
        mtx_init(&shared->indexableKeysLock, mtx_plain);

        char default_address[48];
        snprintf(default_address, sizeof(default_address), "127.0.0.1:%d", server.port);

        shared->indexNode.host_reference = sdsnew(default_address);
        shared->indexNode.host = sdsnew("127.0.0.1");
        shared->indexNode.port = server.port;
        shared->indexNode.database_id = 0;
        shared->indexNode.is_local = 0;
        shared->indexNode.executor = NULL;

        shared->dataNode.host_reference = sdsnew(default_address);
        shared->dataNode.host = sdsnew("127.0.0.1");
        shared->dataNode.port = server.port;
        shared->dataNode.database_id = 0;
        shared->dataNode.is_local = 0;
        shared->dataNode.executor = NULL;
        shared->defaultQueryOperator = sdsnew("&");

        shared->controllerNode.host_reference = sdsnew(default_address);
        shared->controllerNode.host = sdsnew("127.0.0.1");
        shared->controllerNode.port = server.port;
        shared->controllerNode.database_id = 0;
        shared->controllerNode.is_local = 0;
        shared->controllerNode.executor = NULL;
        rxMercatorShared = shared;
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

void *GetIndex_info()
{
    rxSuiteShared *config = getRxSuite();
    return config->index_info;
}

void SetIndex_info(void *indexer)
{

    rxSuiteShared *config = getRxSuite();
    config->index_info = indexer;
}

extern const char*rxGetContainedString(void *o);
    
static void extractArgs(rxRedisModuleString **oargv, int j, redisNodeInfo *node)
{
    const char *s = (const char *)rxGetContainedString(oargv[j]);
    node->host = sdsnew(s);
    s = rxGetContainedString(oargv[j + 1]);
    node->port = atoi(s);
    s = rxGetContainedString(oargv[j + 2]);
    node->database_id = atoi(s);
}

void rxRegisterConfig(void **oargv, int argc)
{
    rxRedisModuleString **argv = (rxRedisModuleString **)oargv;
    rxSuiteShared *config = initRxSuite();
    for (int j = 0; j < argc; ++j)
    {
        const char *arg = rxGetContainedString(argv[j]);
        if (stringmatch(arg, "INDEX", 1) == 1 && (j + 3) <= argc)
        {
            extractArgs(argv, j + 1, &config->indexNode);
            j += 3;
        }
        else if (stringmatch(arg, "DATA", 1) == 1 && (j + 3) <= argc)
        {
            extractArgs(argv, j + 1, &config->dataNode);
            j += 3;
        }
        else if (stringmatch(arg, "CONTROLLER", 1) == 1 && (j + 3) <= argc)
        {
            extractArgs(argv, j + 1, &config->controllerNode);
            j += 3;
        }
        else if (stringmatch(arg, "DEFAULT_OPERATOR", 1) == 1 && (j + 3) <= argc)
        {
            ++j;
            const char *s = rxGetContainedString(argv[j]);
            config->defaultQueryOperator = sdsnew(s);
        }
        else if (stringmatch(arg, "INDEX_SCORING", 1) == 1)
        {
            ++j;
            const char *s = rxGetContainedString(argv[j]);
            config->indexScoring = (stringmatch(s, "unweighted", 1)) ? UnweightedIndexScoring : WeightedIndexScoring;
        }
        else if (stringmatch(arg, "DEBUG", 1) == 1)
        {
            config->debugMessages = 16924;
        }
        else if (stringmatch(arg, "CDN", 1) == 1)
        {
            ++j;
            const char *s = rxGetContainedString(argv[j]);
            config->cdnRootUrl = sdsnew(s);
        }
        else if (stringmatch(arg, "START-SCRIPT", 1) == 1)
        {
            ++j;
            const char *s = rxGetContainedString(argv[j]);
            config->startScript = sdsnew(s);
        }
        else if (stringmatch(arg, "INSTALL-SCRIPT", 1) == 1)
        {
            ++j;
            const char *s = rxGetContainedString(argv[j]);
            config->installScript = sdsnew(s);
        }
        else if (stringmatch(arg, "DATA-DIR", 1) == 1)
        {
            ++j;
            const char *s = rxGetContainedString(argv[j]);
            config->wget_root = sdsnew(s);
        }
    }
    config->indexNode.is_local = strcmp(config->indexNode.host, config->dataNode.host) == 0 && config->indexNode.port == config->dataNode.port;
    config->indexNode.host_reference = sdscatprintf(sdsempty(), "%s:%d", config->indexNode.host, config->indexNode.port);
    config->dataNode.is_local = config->indexNode.is_local;
    config->dataNode.host_reference = sdscatprintf(sdsempty(), "%s:%d", config->dataNode.host, config->dataNode.port);
    if (argc == 0)
    {
        config->dataNode.is_local = -1000;
        config->indexNode.is_local = -1000;
    }
    if (config->cdnRootUrl == NULL)
        config->cdnRootUrl = sdsnew("https://roxoft.dev/assets");
    if (config->startScript == NULL)
        config->startScript = sdsnew("__start_redis.sh");
    if (config->installScript == NULL)
        config->installScript = sdsnew("__install_rxmercator.sh");
}

enum indexScoringMethod rxGetIndexScoringMethod()
{
    rxSuiteShared *config = initRxSuite();
    return config->indexScoring;
}

void rxSetIndexScoringMethod(enum indexScoringMethod scoringMethod)
{
    rxSuiteShared *config = initRxSuite();
    config->indexScoring = scoringMethod;
}

void rxSetIndexScoringMethodFromString(const char *s)
{
    rxSuiteShared *config = initRxSuite();
    config->indexScoring = (stringmatch(s, "unweighted", 1)) ? UnweightedIndexScoring : WeightedIndexScoring;
}

char *rxGetExecutable()
{
    return server.executable;
}

void rxRegisterCronCommandQueue(CSimpleQueue *queue)
{
    rxSuiteShared *config = initRxSuite();
    config->cron_command_request_queue = queue;
}

CSimpleQueue *rxGetCronCommandQueue()
{
    rxSuiteShared *config = initRxSuite();
    return config->cron_command_request_queue;
}

void rxSetDataRoot(const char *s)
{
    rxSuiteShared *config = initRxSuite();
    config->wget_root = sdsnew(s);
}

static int actionForKey(rax *pool, mtx_t *lock, const char *k)
{
    int nAdded = 0;
    mtx_lock(lock);
    {
        void *memo = raxFind(pool, (UCHAR *)k, strlen(k));
        if (memo == raxNotFound)
        {
            raxInsert(pool, (UCHAR *)k, strlen(k), NULL, NULL);
            nAdded = 1;
        }
    }
    mtx_unlock(lock);
    return nAdded;
}

static const char *getActionKey(rax *pool, mtx_t *lock)
{
    const char *k = NULL;
    mtx_lock(lock);
    {
        raxIterator ki;
        raxStart(&ki, pool);
        raxSeek(&ki, "^", NULL, 0);
        if(raxNext(&ki))
        {
            k = rxStringNewLen(ki.key, ki.key_len);
            raxRemove(pool, ki.key, ki.key_len, NULL);
        }
        raxStop(&ki);
    }
    mtx_unlock(lock);
    return k;
}

int KeyTriggered(const char *k)
{
    rxSuiteShared *config = initRxSuite();
    return actionForKey(config->triggeredKeys, &config->triggeredKeysLock, k);
}

int KeyTouched(const char *k)
{
    rxSuiteShared *config = initRxSuite();
    return actionForKey(config->indexableKeys, &config->indexableKeysLock, k);
}


const char *getTriggeredKey()
{
    rxSuiteShared *config = initRxSuite();
    return getActionKey(config->triggeredKeys, &config->triggeredKeysLock);
}

const char *getIndexableKey()
{
    rxSuiteShared *config = initRxSuite();
    return getActionKey(config->indexableKeys, &config->indexableKeysLock);
}
