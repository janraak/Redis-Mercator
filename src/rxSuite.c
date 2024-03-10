#include "rxSuite.h"
#include "pthread.h"
#include <stdatomic.h>

#ifdef __cplusplus
extern "C"
{
#endif
#include "../../src/server.h"

#ifdef __cplusplus
}
#endif

#define rxUNUSED(x) (void)(x)

extern int rxIsRehashingDatabase(int db);

typedef const char *rxRedisModuleString;

// long long n_enq_triggeredKeys;
// long long n_deq_triggeredKeys;
// long long us_enq_triggeredKeys;
// long long n_done_triggeredKeys;
// long long n_triggeredKeys;
// long long n_triggeredKey_spins;
// long long us_triggeredKey_spins;
// long long n_processingTriggered;
// long long us_processingTriggered;
// long long n_enq_touchedKeys;
// long long us_enq_touchedKeys;
// long long n_renq_touchedKeys;
// long long n_deq_touchedKeys;
// long long n_done_touchedKeys;
// long long n_touchedKeys;
// long long n_touchedKey_spins;
// long long us_touchedKey_spins;
// long long n_indexed;
// long long us_indexed;
// long long n_hset;
// long long us_hset;
// long long us_hset_key_block;
// long long us_hset_std;
// long long us_hset_enqueue;

// long long us_enqueue_outside;
// long long us_enqueue_inside;
// long long us_enqueue_pool1;
// long long us_enqueue_pool2;
// long long n_enqueue_pool1;
// long long n_enqueue_pool2;

// long long n_lookupByLookup;
// long long us_lookupByLookup;
// long long n_lookupByDict;
// long long us_lookupByDict;
// long long n_lookupByScan;
// long long us_lookupByScan;
// long long n_lookupNotFound;
// long long us_lookupNotFound;

// rxCounter enq_triggeredKeys = {"enq_triggeredKeys", 0, 0, 0, 0};
// rxCounter deq_triggeredKeys = {"deq_triggeredKeys", 0, 0, 0, 0};
// rxCounter enq_triggeredKeys = {"", 0, 0, 0, 0};
// rxCounter enq_triggeredKeys = {"", 0, 0, 0, 0};
// rxCounter enq_triggeredKeys = {"", 0, 0, 0, 0};
// rxCounter enq_triggeredKeys = {"", 0, 0, 0, 0};
// rxCounter enq_triggeredKeys = {"", 0, 0, 0, 0};
// rxCounter enq_triggeredKeys = {"", 0, 0, 0, 0};
// rxCounter enq_triggeredKeys = {"", 0, 0, 0, 0};
// rxCounter enq_triggeredKeys = {"", 0, 0, 0, 0};
// rxCounter enq_triggeredKeys = {"", 0, 0, 0, 0};
// rxCounter enq_triggeredKeys = {"", 0, 0, 0, 0};
// rxCounter enq_triggeredKeys = {"", 0, 0, 0, 0};
// rxCounter enq_triggeredKeys = {"", 0, 0, 0, 0};
// rxCounter enq_triggeredKeys = {"", 0, 0, 0, 0};

void *initRxSuite()
{

    if (!server.rxMercator)
    {
        rxSuiteShared *shared = zmalloc(sizeof(rxSuiteShared));
        memset(shared, 0x00, sizeof(rxSuiteShared));

        shared->parserClaimCount = 0;
        shared->debugMessages = 0;
        server.rxMercator = shared;

        shared->IndexingHandler = NULL;
        shared->nIndexingRequests = 0;
        shared->TriggerHandler = NULL;
        shared->nTriggerRequests = 0;
        shared->ThPoolSz = 0;

        shared->thpool = thpool_init(THREADPOOL_SIZE);
        shared->ThPoolSz = THREADPOOL_SIZE;

        shared->triggeredKeys = raxNew();
        shared->processingTriggeredKeys = raxNew();
        mtx_init(&shared->triggeredKeysLock, mtx_plain);
        shared->indexableKeys = raxNew();
        shared->indexingKeys = raxNew();
        mtx_init(&shared->indexableKeysLock, mtx_plain);
        sem_init(&shared->signpost_indexing, 0, 0);
        sem_init(&shared->signpost_triggering, 0, 0);
        shared->n_enq_triggeredKeys = 0;
        shared->us_enq_triggeredKeys = 0;
        shared->n_deq_triggeredKeys = 0;
        shared->n_done_triggeredKeys = 0;
        shared->n_triggeredKeys = 0;
        shared->n_triggeredKey_spins = 0;
        shared->us_triggeredKey_spins = 0;
        shared->n_processingTriggered = 0;
        shared->us_processingTriggered = 0;
        shared->n_enq_touchedKeys = 0;
        shared->n_renq_touchedKeys = 0;
        shared->us_enq_touchedKeys = 0;
        shared->n_deq_touchedKeys = 0;
        shared->n_done_touchedKeys = 0;
        shared->n_touchedKeys = 0;
        shared->n_touchedKey_spins = 0;
        shared->us_touchedKey_spins = 0;
        shared->n_indexed = 0;
        shared->us_indexed = 0;
        shared->n_hset = 0;
        shared->us_hset = 0;
        shared->us_hset_key_block = 0;
        shared->us_hset_std = 0;
        shared->us_hset_enqueue = 0;
        shared->n_enqueue_pool1 = 0;
        shared->n_enqueue_pool2 = 0;
        shared->us_enqueue_outside = 0;
        shared->us_enqueue_inside = 0;
        shared->us_enqueue_pool1 = 0;
        shared->us_enqueue_pool2 = 0;

        shared->n_lookupByLookup = 0;
        shared->n_lookupByDict = 0;
        shared->n_lookupByScan = 0;
        shared->us_lookupByLookup = 0;
        shared->us_lookupByDict = 0;
        shared->us_lookupByScan = 0;
        shared->n_lookupNotFound = 0;
        shared->us_lookupNotFound = 0;

        shared->snapShotRequests = raxNew();
        shared->snapshotResponses = raxNew();
        shared->snapshotReleases = raxNew();

        shared->n_snapshots_cron = 0;
        shared->us_snapshots_cron = 0;
        shared->n_snapshots_cron_requests = 0;
        shared->us_snapshots_cron_requests = 0;
        shared->n_snapshots_cron_releases = 0;
        shared->us_snapshots_cron_releases = 0;

        shared->n__hash_snapshots = 0;
        shared->us_hash_snapshots = 0;

        shared->n_hash_snapshot_request = 0;
        shared->us_hash_snapshot_request = 0;
        shared->n_hash_snapshot_response = 0;
        shared->us_hash_snapshot_response = 0;
        shared->n_hash_snapshot_release = 0;
        shared->us_hash_snapshot_release = 0;

        shared->n__set_snapshots = 0;
        shared->us_set_snapshots = 0;

        shared->n_set_snapshot_request = 0;
        shared->us_set_snapshot_request = 0;
        shared->n_set_snapshot_response = 0;
        shared->us_set_snapshot_response = 0;
        shared->n_set_snapshot_release = 0;
        shared->us_set_snapshot_release = 0;
        shared->us_snapshots_cron_yield = 0;
        mtx_init(&shared->SnapshotLock, mtx_plain);

        shared->sjeboleth_master_registry = NULL;

        char default_address[48];
        snprintf(default_address, sizeof(default_address), "127.0.0.1:%d", server.port);

        shared->indexNode.host_reference = sdsnew(default_address);
        shared->indexNode.host = sdsnew("127.0.0.1");
        shared->indexNode.port = server.port;
        shared->indexNode.database_id = 0;
        shared->indexNode.is_local = 0;
        // shared->indexNode.executor = NULL;

        shared->dataNode.host_reference = sdsnew(default_address);
        shared->dataNode.host = sdsnew("127.0.0.1");
        shared->dataNode.port = server.port;
        shared->dataNode.database_id = 0;
        shared->dataNode.is_local = 0;
        // shared->dataNode.executor = NULL;
        shared->defaultQueryOperator = sdsnew("&");

        shared->controllerNode.host_reference = sdsnew(default_address);
        shared->controllerNode.host = sdsnew("127.0.0.1");
        shared->controllerNode.port = server.port;
        shared->controllerNode.database_id = 0;
        shared->controllerNode.is_local = 0;
        // shared->controllerNode.executor = NULL;
        server.rxMercator = shared;
    }
    return server.rxMercator;
}

rxSuiteShared *getRxSuite()
{
    if (server.rxMercator == NULL)
        initRxSuite();
    return server.rxMercator;
}

void finalizeRxSuite()
{
    if (!server.rxMercator)
    {
        rxMemFree(server.rxMercator);
    }
}

void SetIndexingHandler(runner handler)
{
    rxSuiteShared *shared = initRxSuite();
    shared->IndexingHandler = handler;
}

void CommencedIndexingHandler()
{
    rxSuiteShared *shared = initRxSuite();
    shared->nIndexingRequests--;
}
void SetTriggerHandler(runner handler)
{
    rxSuiteShared *shared = initRxSuite();
    shared->TriggerHandler = handler;
}
void CommencedTriggerHandler()
{
    rxSuiteShared *shared = initRxSuite();
    shared->nTriggerRequests--;
}

rax *Sjiboleth_Master_Registry(timeProc cron)
{
    rxSuiteShared *config = getRxSuite();
    if (!config->sjeboleth_master_registry)
    {
        config->sjeboleth_master_registry = raxNew();
        if(cron){
            aeCreateTimeEvent(server.el, 0, cron, config->sjeboleth_master_registry, NULL);            
        }
    }
    return config->sjeboleth_master_registry;
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

extern const char *rxGetContainedString(void *o);

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

static int addActionForKey(rax *pool, mtx_t *lock, snapshot_request *k)
{
    rxSuiteShared *shared = initRxSuite();

    int nAdded = 0;
    long long start = ustime();
    // if (lock)
    mtx_lock(lock);
    long long stop1 = ustime();
    void *memo = raxFind(pool, (UCHAR *)k->k, k->k_len);
    if (memo == raxNotFound)
    {
        raxInsert(pool, (UCHAR *)k->k, k->k_len, k, NULL);
        nAdded = 1;
    }
    long long stop2 = ustime();
    long long stop3 = ustime();
    // if (lock)
    mtx_unlock(lock);
    long long stop4 = ustime();

    shared->n_enqueue_pool1++;
    shared->us_enqueue_outside += stop4 - start;
    shared->us_enqueue_inside += stop3 - stop1;
    shared->us_enqueue_pool1 += stop2 - stop1;

    if (nAdded == 0)
        shared->n_renq_touchedKeys++;

    // rxServerLog(rxLL_NOTICE, "#addActionForKey# 9999 inqueue:%d tally:%d pool:%p type:%d len:%d %s", raxSize(pool), nAdded, pool, k->type, k->k_len, k->k);
    return nAdded;
}
#define INDEXING_NO_DELAY_ms 0   /* = 0 ms = 0s*/
#define INDEXING_DELAY_ms 500000 /* = 500 ms = 0.5s*/
static snapshot_request *getActionKey(rax *pool, mtx_t *lock, long long *queue_time, long long delay)
{
    if (rxIsRehashingDatabase(0))
        return NULL;

    snapshot_request *k = NULL;
    mtx_lock(lock);
    {
        raxIterator ki;
        raxStart(&ki, pool);
        raxSeek(&ki, "^", NULL, 0);
        while (raxNext(&ki))
        {
            long long now = ustime();
            if ((now - (((snapshot_request *)ki.data)->enqueued)) > delay)
            {
                raxRemove(pool, (UCHAR *)ki.key, ki.key_len, (void *)&k);
                k->dequeued = ustime();
                if (queue_time)
                    *queue_time = *queue_time + k->dequeued - k->enqueued;

                break;
            }
        }
        raxStop(&ki);
    }
    mtx_unlock(lock);
    return k;
}

static snapshot_request *getActionData(rax *pool, mtx_t *lock)
{
    if (rxIsRehashingDatabase(0))
        return NULL;

    snapshot_request *d = NULL;
    mtx_lock(lock);
    {
        raxIterator ki;
        raxStart(&ki, pool);
        raxSeek(&ki, "^", NULL, 0);
        if (raxNext(&ki))
        {
            raxRemove(pool, ki.key, ki.key_len, (void *)&d);
        }
        raxStop(&ki);
    }
    mtx_unlock(lock);
    return d;
}

static size_t getPending(rax *pool, mtx_t *lock)
{
    size_t tally = 0;
    mtx_lock(lock);
    {
        tally = raxSize(pool);
    }
    mtx_unlock(lock);
    return tally;
}

snapshot_request *SnapShotRequestNew(const char *k, short type)
{
    size_t k_len = strlen(k);
    size_t cob_size = sizeof(snapshot_request) + k_len + 1;
    snapshot_request *req = rxMemAlloc(cob_size);
    memset(req, 0x00, cob_size);
    req->type = type;
    req->k_len = k_len;
    req->k = (const char *)((void *)req) + sizeof(snapshot_request);
    strncpy((char *)req->k, k, k_len + 1);
    req->enqueued = ustime();
    return req;
}

#define mstimeMAX_SPIN_ms 500000 /* = 50 ms = 0.05sec */

static void SchedulePendingWork(rxSuiteShared *config)
{
    if (raxSize(config->indexableKeys) > config->ThPoolSz)
        // for (int n = 0; n < config->ThPoolSz; ++n)
            addTaskToPool(config->IndexingHandler, config);
    if (raxSize(config->triggeredKeys) > config->ThPoolSz)
        addTaskToPool(config->TriggerHandler, config);
}

int KeyTouched(const char *k)
{
    rxSuiteShared *config = initRxSuite();
    // rxServerLog(rxLL_NOTICE, "#KeyTouched# 0000 I:%p T:%p", config->indexableKeys, config->triggeredKeys);
    snapshot_request *reqI = SnapShotRequestNew(k, -1);
    snapshot_request *reqT = SnapShotRequestNew(k, -2);

    config->n_touchedKeys++;
    int tallyI = addActionForKey(config->indexableKeys, &config->indexableKeysLock, reqI);
    if (tallyI && config->IndexingHandler && config->nIndexingRequests < (config->ThPoolSz - config->nIndexingRequests))
    {
        addTaskToPool(config->IndexingHandler, config);
        config->nIndexingRequests++;
    }
    int tallyT = addActionForKey(config->triggeredKeys, &config->indexableKeysLock, reqT);
    if (tallyI && config->TriggerHandler && config->nTriggerRequests < (config->ThPoolSz - config->nTriggerRequests))
    {
        addTaskToPool(config->TriggerHandler, config);
        config->nTriggerRequests++;
    }
    config->n_enq_touchedKeys += tallyI;
    config->n_enq_triggeredKeys += tallyT;
    // sem_post(&config->signpost_indexing);
    // sem_post(&config->signpost_triggering);
    return tallyI + tallyT;
}

int PendingKeysTriggered()
{
    rxSuiteShared *config = initRxSuite();
    SchedulePendingWork(config);
    return getPending(config->triggeredKeys, &config->indexableKeysLock);
}

int PendingObjectForIndexing()
{
    rxSuiteShared *config = initRxSuite();
    SchedulePendingWork(config);
    return getPending(config->indexableKeys, &config->indexableKeysLock);
}

/*
    For a key being indexed or triggered, wait for a completion
*/
int WaitForIndexAndTriggerCompletionKeyTouched(const char *k)
{
    return 0;
    rxSuiteShared *config = initRxSuite();
    long long start = ustime();
    int spin = 0;
    while ((ustime() - start) < mstimeMAX_SPIN_ms)
    {
        // mtx_lock(&config->indexableKeysLock);
        mtx_lock(&config->indexableKeysLock);
        void *memo1 = raxSize(config->indexingKeys) == 0 ? raxNotFound : raxFind(config->indexingKeys, (UCHAR *)k, strlen(k));
        void *memo2 = raxSize(config->processingTriggeredKeys) == 0 ? raxNotFound : raxFind(config->processingTriggeredKeys, (UCHAR *)k, strlen(k));
        mtx_unlock(&config->indexableKeysLock);
        // mtx_unlock(&config->indexableKeysLock);
        if (memo1 == raxNotFound && memo2 == raxNotFound)
        {
            break;
        }
        spin++;
        sched_yield();
    }
    if (spin)
    {
        config->us_touchedKey_spins += (ustime() - start);
        config->n_touchedKey_spins += spin++;
        config->n_triggeredKey_spins += spin++;
    }

    return spin;
}

snapshot_request *getTriggeredKey()
{
    rxSuiteShared *config = initRxSuite();
    snapshot_request *k = getActionKey(config->triggeredKeys, &config->indexableKeysLock, &config->us_enq_triggeredKeys, INDEXING_DELAY_ms);
    if (k)
    {
        // rxServerLog(rxLL_NOTICE, "#getTriggeredKey# 1000 pool:%p type:%d len:%d %s", config->processingTriggeredKeys, k->type, k->k_len, k->k);
        addActionForKey(config->processingTriggeredKeys, &config->indexableKeysLock, k);
        config->n_deq_triggeredKeys++;
    }else if(raxSize(config->triggeredKeys)){
        SchedulePendingWork(config);
    }
    return k;
}

snapshot_request *getIndexableKey()
{
    rxSuiteShared *config = initRxSuite();
    snapshot_request *k = getActionKey(config->indexableKeys, &config->indexableKeysLock, &config->us_enq_touchedKeys, INDEXING_DELAY_ms);
    if (k)
    {
        // rxServerLog(rxLL_NOTICE, "#getIndexableKey# 1000 pool:%p type:%d len:%d %s", config->processingTriggeredKeys, k->type, k->k_len, k->k);
        addActionForKey(config->indexingKeys, &config->indexableKeysLock, k);
        k->dequeued = ustime();
        config->n_deq_touchedKeys++;
    } else if(raxSize(config->indexingKeys)){
        SchedulePendingWork(config);
    }
    return k;
}

void freeIndexedKey(snapshot_request *k)
{
    rxSuiteShared *config = initRxSuite();
    mtx_lock(&config->indexableKeysLock);
    snapshot_request *start = NULL;
    raxRemove(config->indexingKeys, (UCHAR *)k->k, k->k_len, (void *)&start);
    k->completed = ustime();
    config->us_indexed += k->completed - k->enqueued;
    config->n_done_touchedKeys += 1;
    mtx_unlock(&config->indexableKeysLock);
}

void freeTriggerProcessedKey(snapshot_request *req)
{
    rxSuiteShared *config = initRxSuite();
    mtx_lock(&config->indexableKeysLock);
    snapshot_request *start = NULL;
    raxRemove(config->processingTriggeredKeys, req->k, req->k_len, (void *)&start);
    // rxServerLog(rxLL_NOTICE, "freeTriggerProcessedKey()=>%p %s %p", req, req->k, start);
    req->completed = ustime();
    config->us_processingTriggered += req->completed - req->enqueued;
    config->n_done_triggeredKeys += 1;
    mtx_unlock(&config->indexableKeysLock);
}

void tallyHset(long long start, long long stop1, long long stop2, long long stop3)
{
    rxSuiteShared *shared = initRxSuite();
    shared->n_hset++;
    shared->us_hset += stop3 - start;
    shared->us_hset_key_block += stop1 - start;
    shared->us_hset_std += stop2 - stop1;
    shared->us_hset_enqueue += stop3 - stop2;
}

void logLookupStats(long long start, long long stop1, long long stop2, long long stop3, long long stop4)
{
    rxSuiteShared *shared = initRxSuite();
    if (stop4)
    {
        shared->n_lookupNotFound++;
        shared->us_lookupNotFound += stop4 - start;
    }
    else if (stop3)
    {
        shared->n_lookupByScan++;
        shared->us_lookupByScan += stop3 - start;
    }
    else if (stop2)
    {
        shared->n_lookupByDict++;
        shared->us_lookupByDict += stop2 - start;
    }
    else
    {
        shared->n_lookupByLookup++;
        shared->us_lookupByLookup += stop1 - start;
    }
}

int RequestHashSnapshot(const char *k)
{
    rxSuiteShared *config = initRxSuite();
    snapshot_request *req = SnapShotRequestNew(k, OBJ_HASH);
    int tally = addActionForKey(config->snapShotRequests, &config->SnapshotLock, req);
    config->n_hash_snapshot_request += tally;
    config->us_hash_snapshot_request += ustime() - req->enqueued;
    // serverLog(LL_NOTICE, "..RequestHashSnapshot %s => %d", k, tally);
    SchedulePendingWork(config);
    return tally;
}

int ResponseHashSnapshot(snapshot_request *k)
{
    k->completed = ustime();
    rxSuiteShared *config = initRxSuite();
    int tally = addActionForKey(config->snapshotResponses, &config->SnapshotLock, k);
    config->n_hash_snapshot_response += tally;
    config->us_hash_snapshot_response += k->completed - k->dequeued;
    // serverLog(LL_NOTICE, "..ResponseHashSnapshot %s %p => %d", k, s, tally);
    return tally;
}

int ReleaseSnapshot(snapshot_request *k)
{
    k->released = ustime();
    rxSuiteShared *config = initRxSuite();
    k->completed = ustime();
    int tally = addActionForKey(config->snapshotReleases, &config->SnapshotLock, k);
    config->n_hash_snapshot_release += tally;
    config->us_hash_snapshot_release += k->released - k->completed;
    // serverLog(LL_NOTICE, "..ReleaseSnapshot %s %p => %d", k, s, tally);
    return tally;
}

snapshot_request *nextHashSnapshot(snapshot_request **kk)
{
    /* The return value is corrupted, hence an out parameter */
    rxSuiteShared *config = initRxSuite();
    snapshot_request *k = getActionKey(config->snapShotRequests, &config->SnapshotLock, NULL, INDEXING_NO_DELAY_ms);
    *kk = k;
    if (k)
    {
        k->dequeued = ustime();
    }
    // serverLog(LL_NOTICE, "..nextHashSnapshot %s ", k);
    return k;
}

rxHashFieldAndValues *getCompletedHashSnapshot(const char *k, rxHashFieldAndValues **d)
{
    rxSuiteShared *config = initRxSuite();

    snapshot_request *memo = raxNotFound;

    while (memo == raxNotFound)
    {
        mtx_lock(&config->SnapshotLock);
        raxRemove(config->snapshotResponses, (UCHAR *)k, strlen(k), (void *)&memo);
        mtx_unlock(&config->SnapshotLock);
        if (memo != raxNotFound)
        {
            *d = (rxHashFieldAndValues *)memo->obj;
            return *d;
        }
        sched_yield();
    }

    // serverLog(LL_NOTICE, "..getCompletedHashSnapshot %s => NULL", k);
    return NULL;
}

snapshot_request *nextReleasedSnapshot(snapshot_request **d)
{
    rxSuiteShared *config = initRxSuite();
    snapshot_request *request = getActionData(config->snapshotReleases, &config->SnapshotLock);
    *d = request;
    return *d;
}

void logHashSnapshotStats(long long start, long long stop)
{
    long long lag = stop - start;
    rxSuiteShared *shared = initRxSuite();
    shared->n__hash_snapshots++;
    shared->us_hash_snapshots += lag;
}

long long logHashSnapshotCronStats(long long start, long long stopRequests, long long nRequests, long long stopReleases, long long nReleases, long long idle)
{
    long long lag1 = stopRequests - start;
    long long lag2 = stopReleases - stopRequests;
    long long lag3 = stopReleases - start;
    rxSuiteShared *shared = initRxSuite();

    shared->n_snapshots_cron++;
    shared->us_snapshots_cron += lag3;
    shared->n_snapshots_cron_requests += nRequests;
    shared->us_snapshots_cron_requests += lag1;
    shared->n_snapshots_cron_releases += nReleases;
    shared->us_snapshots_cron_releases += lag2;
    shared->us_snapshots_cron_yield += idle;
    return ustime();
}

int WaitForIndexRequest()
{
    rxSuiteShared *shared = initRxSuite();
    sem_wait(&shared->signpost_indexing);
    return 1;
}
int WaitForKeyTriggered()
{
    rxSuiteShared *shared = initRxSuite();
    sem_wait(&shared->signpost_triggering);
    return 1;
};


//TODO: Optimize: return response when key not exists
int RequestSetSnapshot(const char *k)
{
    rxSuiteShared *config = initRxSuite();
    long long start = ustime();
    snapshot_request *req = SnapShotRequestNew(k, OBJ_SET);
    int tally = addActionForKey(config->snapShotRequests, &config->SnapshotLock, req);
    config->n_set_snapshot_request += tally;
    config->us_set_snapshot_request += ustime() - start;
    // serverLog(LL_NOTICE, "..RequestSetSnapshot %s => %d", k, tally);
    SchedulePendingWork(config);
    return tally;
}

int ResponseSetSnapshot(snapshot_request *k)
{
    k->completed = ustime();
    rxSuiteShared *config = initRxSuite();
    int tally = addActionForKey(config->snapshotResponses, &config->SnapshotLock, k);
    config->n_set_snapshot_response += tally;
    config->us_set_snapshot_response += k->completed - k->dequeued;
    // serverLog(LL_NOTICE, "..ResponseSetSnapshot %s %p => %d", k, s, tally);
    return tally;
}

snapshot_request *nextSetSnapshot(snapshot_request **kk)
{
    /* The return value is corrupted, hence an out parameter */
    rxSuiteShared *config = initRxSuite();
    long long time;
    snapshot_request *k = getActionKey(config->snapShotRequests, &config->SnapshotLock, &time, INDEXING_NO_DELAY_ms);
    *kk = k;
    // serverLog(LL_NOTICE, "..nextSetSnapshot %s ", k);
    return k;
}

rxSetMembers *getCompletedSetSnapshot(const char *k, rxSetMembers **d)
{
    rxSuiteShared *config = initRxSuite();

    snapshot_request *memo = raxNotFound;

    while (memo == raxNotFound)
    {
        mtx_lock(&config->SnapshotLock);
        raxRemove(config->snapshotResponses, (UCHAR *)k, strlen(k), (void *)&memo);
        mtx_unlock(&config->SnapshotLock);
        if (memo != raxNotFound)
        {
            *d = (rxSetMembers *)memo->obj;
            return *d;
        }
        sched_yield();
    }

    // serverLog(LL_NOTICE, "..getCompletedSetSnapshot %s => NULL", k);
    return NULL;
}

void addTaskToPool(runner handler, void *payload)
{
    rxSuiteShared *config = initRxSuite();
    thpool_add_work(config->thpool, handler, payload);
}