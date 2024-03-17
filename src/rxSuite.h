
#ifndef __RXSUITE_H__
#define __RXSUITE_H__
#include <semaphore.h>
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

#include "thpool.h"
#define THREADPOOL_SIZE 48

#include "../../deps/hiredis/hiredis.h"
#include "sdsWrapper.h"

#define rxUNUSED(x) (void)(x)

#define POINTER unsigned int
#define UCHAR unsigned char

    typedef int timeProc(void *eventLoop, long long id, void *clientData);
    // typedef int (*timeProc)(void *, long long int, void*)
    typedef void eventFinalizerProc(void *eventLoop, void *clientData);

    typedef void *(*runner)(void *);

    typedef void CSimpleQueue;

    // #ifndef RXSUITE_SIMPLE
    typedef struct
    {
        const char *host_reference;
        const char *host;
        int port;
        int database_id;
        int is_local;
        // void *executor;
    } redisNodeInfo;

    enum indexScoringMethod
    {
        WeightedIndexScoring,
        UnweightedIndexScoring
    };

    typedef struct rxCounter
    {
        const char *name;
        unsigned long long n_in;   // enqueues
        unsigned long long n_out;  // dequeues
        unsigned long long us_in;  // us(dequeue) -/- us(enqueue)
        unsigned long long us_out; // us(done) -/- us(enqueue)
        unsigned long long n_min;
        unsigned long long n_max;
        unsigned long long us_min;
        unsigned long long us_max;
    } rxCounter;

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
        sem_t signpost_indexing;
        sem_t signpost_triggering;

        rax *sjeboleth_master_registry;
        rax *FaBlok_registry;
        mtx_t FaBlok_registryLock;

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
        long long n_renq_touchedKeys;
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
        rax *snapShotRequests;
        rax *snapshotResponses;
        rax *snapshotReleases;
        mtx_t SnapshotLock;
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
        long long n__set_snapshots;
        long long us_set_snapshots;
        long long n_set_snapshot_request;
        long long us_set_snapshot_request;
        long long n_set_snapshot_response;
        long long us_set_snapshot_response;
        long long n_set_snapshot_release;
        long long us_set_snapshot_release;
        long long us_snapshots_cron_yield;

        threadpool thpool;
        runner IndexingHandler;
        int nIndexingRequests;
        runner TriggerHandler;
        int nTriggerRequests;
        int ThPoolSz;

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

    typedef struct rxSetMembers
    {
        size_t member_count;   // Number of members in this block
        size_t member_index;   // Number of members in this block
        const char *members[]; // Pointer to the members
        // The members are embedding in the allocated block;
    } rxSetMembers;

    typedef struct snapshot_request
    {
        size_t k_len;
        const char *k;
        short type;
        long long enqueued;
        long long dequeued;
        long long completed;
        long long released;
        void *obj;
    } snapshot_request;

    // int KeyTriggered(const char *k);
    snapshot_request *getTriggeredKey();
    int KeyTouched(const char *k);
    snapshot_request *getIndexableKey();
    void freeIndexedKey(snapshot_request *k);
    void freeTriggerProcessedKey(snapshot_request *req);

    int PendingKeysTriggered();
    int PendingObjectForIndexing();
    int WaitForIndexAndTriggerCompletionKeyTouched(const char *k);
    void tallyHset(long long start, long long stop1, long long stop2, long long stop3);
    void logLookupStats(long long start, long long stop1, long long stop2, long long stop3, long long stop4);

    int RequestHashSnapshot(const char *k);
    int ResponseHashSnapshot(snapshot_request *k);
    int ReleaseSnapshot(snapshot_request *k);
    snapshot_request *nextHashSnapshot(snapshot_request **kk);
    rxHashFieldAndValues *getCompletedHashSnapshot(const char *k, rxHashFieldAndValues **d);
    snapshot_request *nextReleasedSnapshot(snapshot_request **d);

    int RequestSetSnapshot(const char *k);
    int ResponseSetSnapshot(snapshot_request *k);
    int ReleaseSnapshot(snapshot_request *k);
    snapshot_request *nextSetSnapshot(snapshot_request **kk);
    rxSetMembers *getCompletedSetSnapshot(const char *k, rxSetMembers **d);

    int WaitForIndexRequest();
    int WaitForKeyTriggered();
    void logHashSnapshotStats(long long start, long long stop);
    long long logHashSnapshotCronStats(long long start, long long stopRequests, long long nRequests, long long stopReleases, long long nReleases, long long idle);
    char *rxGetExecutable();
    void finalizeRxSuite();

    void addTaskToPool(runner handler, void *payload);
    void SetIndexingHandler(runner handler);
    void CommencedIndexingHandler();
    void SetTriggerHandler(runner handler);
    void CommencedTriggerHandler();

    rax *Sjiboleth_Master_Registry(timeProc cron);
    rax *FaBlok_Get_Registry();

    void *FaBlok_Get(const char *n);
    void FaBlok_Set(const char *n, void *b);
    void FaBlok_Delete(const char *n);

// #endif
#if REDIS_VERSION_NUM < 0x00050000
    int raxTryInsert(rax *rax, unsigned char *s, size_t len, void *data, void **old);
#endif

#ifdef __cplusplus
}
#endif

#endif