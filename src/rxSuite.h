
#ifndef __RXSUITE_H__
#define __RXSUITE_H__
#include <stddef.h>
#include <threads.h>

#ifdef __cplusplus
extern "C"
{

#include "../../src/version.h"

#ifndef C_OK
#define C_OK 0
#define C_ERR -1
#endif

#include "rxliterals.h"
#endif
#include "../../src/rax.h"
#include <ctype.h>
#include <sched.h>
#include <signal.h>

#include "../../deps/hiredis/hiredis.h"
#include "sdsWrapper.h"

#define rxUNUSED(x) (void)(x)

#define POINTER unsigned int
#define UCHAR unsigned char

    typedef void CSimpleQueue;

    // #ifndef RXSUITE_SIMPLE
    typedef struct
    {
        const char *host_reference;
        const char *host;
        int port;
        int database_id;
        int is_local;
        void *executor;
    } redisNodeInfo;

    enum indexScoringMethod
    {
        WeightedIndexScoring,
        UnweightedIndexScoring
    };

    typedef struct
    {
        // dict *OperationMap;
        // dict *KeysetCache;
        int parserClaimCount;
        int debugMessages;
        redisNodeInfo indexNode;
        redisNodeInfo dataNode;
        redisNodeInfo controllerNode;
        void *index_info;
        enum indexScoringMethod indexScoring;
        const char *defaultQueryOperator;
        const char *cdnRootUrl;
        const char *startScript;
        const char *installScript;
        const char *ssh_identity;
        const char *wget_root;
        CSimpleQueue *cron_command_request_queue;
        rax *sjeboleth_master_registry;
        rax *triggeredKeys;
        rax *processingTriggeredKeys;
        mtx_t triggeredKeysLock;
        rax *indexableKeys;
        rax *indexingKeys;
        mtx_t indexableKeysLock;
        long long n_enq_triggeredKeys;
        long long n_deq_triggeredKeys;
        long long us_enq_triggeredKeys;
        long long n_done_triggeredKeys;
        long long n_triggeredKeys;
        long long n_triggeredKey_spins;
        long long us_triggeredKey_spins;
        long long n_processingTriggered;
        long long us_processingTriggered;
        long long n_enq_touchedKeys;
        long long us_enq_touchedKeys;
        long long n_deq_touchedKeys;
        long long n_done_touchedKeys;
        long long n_touchedKeys;
        long long n_touchedKey_spins;
        long long us_touchedKey_spins;
        long long n_indexed;
        long long us_indexed;
        long long n_hset;
        long long us_hset;
        long long us_hset_key_block;
        long long us_hset_std;
        long long us_hset_enqueue;

        long long us_enqueue_outside;
        long long us_enqueue_inside;
        long long us_enqueue_pool1;
        long long us_enqueue_pool2;
        long long n_enqueue_pool1;
        long long n_enqueue_pool2;

        long long n_lookupByLookup;
        long long us_lookupByLookup;
        long long n_lookupByDict;
        long long us_lookupByDict;
        long long n_lookupByScan;
        long long us_lookupByScan;
        long long n_lookupNotFound;
        long long us_lookupNotFound;

        /* Hash snapshot items */
        rax *requestHashSnapshotKeys;
        rax *responseHashSnapshotKeys;
        rax *releaseHashSnapshotKeys;
        mtx_t HashSnapshotLock;
        long long n_snapshots_cron;
        long long us_snapshots_cron;
        long long n_snapshots_cron_requests;
        long long us_snapshots_cron_requests;
        long long n_snapshots_cron_releases;
        long long us_snapshots_cron_releases;
        long long n__hash_snapshots;
        long long us_hash_snapshots;
        long long n_hash_snapshot_request;
        long long us_hash_snapshot_request;
        long long n_hash_snapshot_response;
        long long us_hash_snapshot_response;
        long long n_hash_snapshot_release;
        long long us_hash_snapshot_release;

    } rxSuiteShared;

    void *initRxSuite();
    rxSuiteShared *getRxSuite();
    redisNodeInfo *rxIndexNode();
    redisNodeInfo *rxDataNode();
    redisNodeInfo *rxControllerNode();

    void *GetIndex_info();
    void SetIndex_info(void *indexer);

    enum indexScoringMethod rxGetIndexScoringMethod();
    void rxSetIndexScoringMethod(enum indexScoringMethod scoringMethod);
    void rxSetIndexScoringMethodFromString(const char *s);
    void rxSetDataRoot(const char *s);
    void rxRegisterConfig(void **argv, int argc);
    void rxRegisterCronCommandQueue(CSimpleQueue *queue);
    CSimpleQueue *rxGetCronCommandQueue();

    // int KeyTriggered(const char *k);
    const char *getTriggeredKey();
    int KeyTouched(const char *k);
    const char *getIndexableKey();
    void freeIndexedKey(const char *k);
    void freeTriggerProcessedKey(const char *k);

    int PendingKeysTriggered();
    int PendingObjectForIndexing();
    int WaitForIndexAndTriggerCompletionKeyTouched(const char *k);
    void tallyHset(long long start, long long stop1, long long stop2, long long stop3);
    void logLookupStats(long long start, long long stop1, long long stop2, long long stop3, long long stop4);

    typedef struct rxFieldValuePair
    {
        const char *field;
        const char *value;
    } rxFieldValuePair;
    typedef struct rxHashFieldAndValues
    {
        long long signature;
        long long created_at;
        size_t member_count;         // Number of members in this block
        size_t member_index;         // Number of members in this block
        rxFieldValuePair *members[]; // Pointer to the members
        // The members are embedding in the allocated block;
    } rxHashFieldAndValues;

    int RequestSnapshotKey(const char *k);
    int ResponseSnapshotKey(const char *k, rxHashFieldAndValues *s);
    int ReleaseSnapshotKey(const char *k, rxHashFieldAndValues *s);
    const char *nextSnapshotKey(const char **kk);
    rxHashFieldAndValues *getCompletedSnapshotKey(const char *k, rxHashFieldAndValues **d);
    rxHashFieldAndValues *nextReleaseSnapshotKey(rxHashFieldAndValues **d);
    void logHashSnapshotStats(long long start, long long stop);
    void logHashSnapshotCronStats(long long start, long long stopRequests, long long nRequests, long long stopReleases, long long nReleases);
    char *rxGetExecutable();
    void finalizeRxSuite();

    rax *Sjiboleth_Master_Registry();

// #endif
#if REDIS_VERSION_NUM < 0x00050000
    int raxTryInsert(rax *rax, unsigned char *s, size_t len, void *data, void **old);
#endif

#ifdef __cplusplus
}
#endif

#endif