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

extern int rxIsRehashingDatabase(int db);

typedef const char *rxRedisModuleString;
static rxSuiteShared rxMercatorConfig;

extern void *rxMercatorShared;

void *initRxSuite()
{
    if (!rxMercatorShared)
    {
        rxSuiteShared *shared = &rxMercatorConfig;
        // zmalloc(sizeof(rxSuiteShared));
        memset(shared, 0x00, sizeof(rxSuiteShared));
        shared->parserClaimCount = 0;
        shared->debugMessages = 0;
        rxMercatorShared = shared;
        shared->triggeredKeys = raxNew();
        shared->processingTriggeredKeys = raxNew();
        mtx_init(&shared->triggeredKeysLock, mtx_plain);
        shared->indexableKeys = raxNew();
        shared->indexingKeys = raxNew();
        mtx_init(&shared->indexableKeysLock, mtx_plain);
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

        shared->requestHashSnapshotKeys = raxNew();
        shared->responseHashSnapshotKeys = raxNew();
        shared->releaseHashSnapshotKeys = raxNew();

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
        mtx_init(&shared->HashSnapshotLock, mtx_plain);

        shared->sjeboleth_master_registry = NULL;

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

rax *Sjiboleth_Master_Registry()
{
    rxSuiteShared *config = getRxSuite();
    if (!config->sjeboleth_master_registry)
    {
        config->sjeboleth_master_registry = raxNew();
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

static int actionForKey(rax *pool, rax *poolT, mtx_t *lock, const char *k, void *d)
{
    rxSuiteShared *shared = initRxSuite();

    int nAdded = 0;
    long long start = ustime();
    // if (lock)
    mtx_lock(lock);
    long long stop1 = ustime();
    void *memo = raxFind(pool, (UCHAR *)k, strlen(k));
    if (memo == raxNotFound)
    {
        long long start = mstime();
        raxInsert(pool, (UCHAR *)k, strlen(k), d ? d : (void *)start, NULL);
        nAdded = 1;
    }
    long long stop2 = ustime();
    if (poolT)
    {
        shared->n_enqueue_pool2++;
        memo = raxFind(poolT, (UCHAR *)k, strlen(k));
        if (memo == raxNotFound)
        {
            long long start = mstime();
            raxInsert(poolT, (UCHAR *)k, strlen(k), d ? d : (void *)start, NULL);
            nAdded = 1;
        }
    }
    long long stop3 = ustime();
    // if (lock)
    mtx_unlock(lock);
    long long stop4 = ustime();

    shared->n_enqueue_pool1++;
    shared->us_enqueue_outside += stop4 - start;
    shared->us_enqueue_inside += stop3 - stop1;
    shared->us_enqueue_pool1 += stop2 - stop1;
    if (poolT)
        shared->us_enqueue_pool2 += stop3 - stop2;

    return nAdded;
}

#define INDEXING_DELAY_ms 2500 /* = 500 ms = 0.5s*/
static const char *getActionKey(rax *pool, mtx_t *lock, long long *queue_time)
{
    if (rxIsRehashingDatabase(0))
        return NULL;

    const char *k = NULL;
    mtx_lock(lock);
    {
        raxIterator ki;
        raxStart(&ki, pool);
        raxSeek(&ki, "^", NULL, 0);
        while (raxNext(&ki))
        {
            long long now = mstime();
            if ((now - ((long long)ki.data)) > INDEXING_DELAY_ms)
            {
                k = rxStringNewLen((const char *)ki.key, ki.key_len);
                long long start;
                raxRemove(pool, ki.key, ki.key_len, (void *)&start);
                if (queue_time)
                    *queue_time = *queue_time + now - start;

                break;
            }
        }
        raxStop(&ki);
    }
    mtx_unlock(lock);
    return k;
}

static void *getActionData(rax *pool, mtx_t *lock)
{
    if (rxIsRehashingDatabase(0))
        return NULL;

    void *d = NULL;
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

#define mstimeMAX_SPIN_ms 50 /* = 50 ms = 0.05sec */

int KeyTouched(const char *k)
{
    rxSuiteShared *config = initRxSuite();
    config->n_touchedKeys++;
    // long long start = mstime();
    // int spin = 0;
    // while ((mstime() - start) < mstimeMAX_SPIN_ms)
    // {
    //     // mtx_lock(&config->triggeredKeysLock);
    //     // mtx_lock(&config->indexableKeysLock);
    //     void *memo1 = raxSize(config->indexingKeys) == 0 ? raxNotFound : raxFind(config->indexingKeys, (UCHAR *)k, strlen(k));
    //     void *memo2 = raxSize(config->processingTriggeredKeys) == 0 ? raxNotFound : raxFind(config->processingTriggeredKeys, (UCHAR *)k, strlen(k));
    //     // mtx_unlock(&config->indexableKeysLock);
    //     // mtx_unlock(&config->triggeredKeysLock);
    //     if (memo1 == raxNotFound && memo2 == raxNotFound)
    //     {
    //         break;
    //     }
    //     spin++;
    //     sched_yield();
    // }
    // if (spin)
    // {
    //     config->us_touchedKey_spins += mstime() - start;
    //     config->n_touchedKey_spins += spin;
    // }
    int tally = actionForKey(config->indexableKeys, config->triggeredKeys, &config->indexableKeysLock, k, NULL);
    config->n_enq_touchedKeys += tally;
    return tally;
}

int PendingKeysTriggered()
{
    rxSuiteShared *config = initRxSuite();
    return getPending(config->triggeredKeys, &config->indexableKeysLock);
}

int PendingObjectForIndexing()
{
    rxSuiteShared *config = initRxSuite();
    return getPending(config->indexableKeys, &config->indexableKeysLock);
}

int WaitForIndexAndTriggerCompletionKeyTouched(const char *k)
{
    rxSuiteShared *config = initRxSuite();
    long long start = mstime();
    int spin = 0;
    while ((mstime() - start) < mstimeMAX_SPIN_ms)
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
        config->us_touchedKey_spins += (mstime() - start);
        config->n_touchedKey_spins += spin++;
        config->n_triggeredKey_spins += spin++;
    }

    return spin;
}

const char *getTriggeredKey()
{
    rxSuiteShared *config = initRxSuite();
    const char *k = getActionKey(config->triggeredKeys, &config->indexableKeysLock, &config->us_enq_triggeredKeys);
    if (k)
    {
        actionForKey(config->processingTriggeredKeys, NULL, &config->indexableKeysLock, k, NULL);
        config->n_deq_triggeredKeys++;
    }
    return k;
}

const char *getIndexableKey()
{
    rxSuiteShared *config = initRxSuite();
    const char *k = getActionKey(config->indexableKeys, &config->indexableKeysLock, &config->us_enq_touchedKeys);
    if (k)
    {
        actionForKey(config->indexingKeys, NULL, &config->indexableKeysLock, k, NULL);
        config->n_deq_touchedKeys++;
    }
    return k;
}

void freeIndexedKey(const char *k)
{
    rxSuiteShared *config = initRxSuite();
    mtx_lock(&config->indexableKeysLock);
    long long start = mstime();
    raxRemove(config->indexingKeys, (UCHAR *)k, strlen(k), (void *)&start);
    config->us_indexed += mstime() - start;
    config->n_done_touchedKeys += 1;
    mtx_unlock(&config->indexableKeysLock);
}

void freeTriggerProcessedKey(const char *k)
{
    rxSuiteShared *config = initRxSuite();
    mtx_lock(&config->indexableKeysLock);
    long long start;
    raxRemove(config->processingTriggeredKeys, (UCHAR *)k, strlen(k), (void *)&start);
    config->us_processingTriggered += mstime() - start;
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

int RequestSnapshotKey(const char *k)
{
    rxSuiteShared *config = initRxSuite();
    long long start = mstime();
    int tally = actionForKey(config->requestHashSnapshotKeys, NULL, &config->HashSnapshotLock, k, NULL);
    config->n_hash_snapshot_request += tally;
    config->us_hash_snapshot_request += mstime() - start;
    // serverLog(LL_NOTICE, "..RequestSnapshotKey %s => %d", k, tally);
    return tally;
}

int ResponseSnapshotKey(const char *k, rxHashFieldAndValues *s)
{
    if (s->signature != 20697227649722765)
    {
        serverLog(LL_NOTICE, "#ResponseSnapshotKey# corrupted rxHashFieldAndValues %p signature %lld != 20697227649722765", s, s->signature);
    }

    rxSuiteShared *config = initRxSuite();
    long long start = mstime();
    int tally = actionForKey(config->responseHashSnapshotKeys, NULL, &config->HashSnapshotLock, k, s);
    config->n_hash_snapshot_response += tally;
    config->us_hash_snapshot_response += mstime() - start;
    // serverLog(LL_NOTICE, "..ResponseSnapshotKey %s %p => %d", k, s, tally);
    return tally;
}

int ReleaseSnapshotKey(const char *k, rxHashFieldAndValues *s)
{
    if (s->signature != 20697227649722765)
    {
        serverLog(LL_NOTICE, "#ReleaseSnapshotKey# corrupted rxHashFieldAndValues %p signature %lld != 20697227649722765", s, s->signature);
    }
    rxSuiteShared *config = initRxSuite();
    long long start = mstime();
    int tally = actionForKey(config->releaseHashSnapshotKeys, NULL, &config->HashSnapshotLock, k, s);
    config->n_hash_snapshot_release += tally;
    config->us_hash_snapshot_release += mstime() - start;
    // serverLog(LL_NOTICE, "..ReleaseSnapshotKey %s %p => %d", k, s, tally);
    return tally;
}

const char *nextSnapshotKey(const char **kk)
{
    /* The return value is corrupted, hence an out parameter */
    rxSuiteShared *config = initRxSuite();
    long long time;
    const char *k = getActionKey(config->requestHashSnapshotKeys, &config->HashSnapshotLock, &time);
    *kk = k;
    // serverLog(LL_NOTICE, "..nextSnapshotKey %s ", k);
    return k;
}

rxHashFieldAndValues *getCompletedSnapshotKey(const char *k, rxHashFieldAndValues **d)
{
    rxSuiteShared *config = initRxSuite();

    void *memo = raxNotFound;

    while (memo == raxNotFound)
    {
        mtx_lock(&config->HashSnapshotLock);
        size_t k_len = strlen(k);
        raxRemove(config->responseHashSnapshotKeys, (UCHAR *)k, k_len, (void *)&memo);
        // memo = raxFind(config->responseHashSnapshotKeys, (UCHAR *)k, k_len);
        mtx_unlock(&config->HashSnapshotLock);
        if (memo != raxNotFound)
        {
            *d = memo;
            // serverLog(LL_NOTICE, "..getCompletedSnapshotKey %s => %p", k, *d);
            if (*d && (*d)->signature != 20697227649722765)
            {
                serverLog(LL_NOTICE, "#getCompletedSnapshotKey# corrupted rxHashFieldAndValues %p signature %lld != 20697227649722765", (*d), (*d)->signature);
            }
            // mtx_lock(&config->HashSnapshotLock);
            // raxRemove(config->responseHashSnapshotKeys, (UCHAR *)k, k_len, (void *)&memo);
            // mtx_unlock(&config->HashSnapshotLock);
            return *d;
        }
        sched_yield();
    }

    // serverLog(LL_NOTICE, "..getCompletedSnapshotKey %s => NULL", k);
    return NULL;
}

rxHashFieldAndValues *nextReleaseSnapshotKey(rxHashFieldAndValues **d)
{
    rxSuiteShared *config = initRxSuite();
    *d = getActionData(config->releaseHashSnapshotKeys, &config->HashSnapshotLock);
    // serverLog(LL_NOTICE, "..nextReleaseSnapshotKey %p", *d);
    if (*d && (*d)->signature != 20697227649722765)
    {
        serverLog(LL_NOTICE, "#nextReleaseSnapshotKey# corrupted rxHashFieldAndValues %p signature %lld != 20697227649722765", (*d), (*d)->signature);
    }
    return *d;
}

void logHashSnapshotStats(long long start, long long stop)
{
    long long lag = stop - start;
    rxSuiteShared *shared = initRxSuite();
    shared->n__hash_snapshots++;
    shared->us_hash_snapshots += lag;
}

void logHashSnapshotCronStats(long long start, long long stopRequests, long long nRequests, long long stopReleases, long long nReleases)
{
    long long lag1 = stopRequests - start;
    long long lag2 = stopReleases - stopRequests;
    long long lag3 = stopReleases - start;
    rxSuiteShared *shared = initRxSuite();

    shared->n_snapshots_cron++;
    shared->us_snapshots_cron += lag3;
    shared->n_snapshots_cron_requests = nRequests;
    shared->us_snapshots_cron_requests += lag1;
    shared->n_snapshots_cron_releases = nReleases;
    shared->us_snapshots_cron_releases += lag2;
}
