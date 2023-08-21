#ifndef __RXSUITEHELPERS_H__
#define __RXSUITEHELPERS_H__

#undef _GNU_SOURCE
#undef _DEFAULT_SOURCE
#undef _LARGEFILE_SOURCE

#ifdef __cplusplus
extern "C"
{
#endif

#if REDIS_VERSION_NUM >= 0x00070200
// With redis 7.2.0 fmacro.h may cause compilation errors
#define _REDIS_FMACRO_H 
#endif
#include "ae.h"
// 
#include "rax.h"
#include "../../src/dict.h"
#include "sdsWrapper.h"

#ifdef __cplusplus
}
#endif

typedef void interceptorProc(void *c);

typedef struct
{
    char *name;
    interceptorProc *proc;
    interceptorProc *redis_proc;
    int id; /* Command ID. */
    long long no_of_intercepts;
    long long no_of_success_intercepts;
    long long no_of_failure_intercepts;
    long long no_of_fallback_intercepts;
} interceptRule;

#ifdef __cplusplus
extern "C"
{
#endif

    typedef struct redisCommandredis Command;

    typedef int timeProc(void *eventLoop, long long id, void *clientData);
    typedef void eventFinalizerProc(void *eventLoop, void *clientData);

    /*
     * Install one or more command interceptors.
     *
     * An interceptor replaces a the handler for a Redis command. The interceptor can call the original handler as needed.
     */
    void installInterceptors(interceptRule *commandTable, int no_of_commands, timeProc *cron_proc);

    /*
     * Restore the intercepted commands to the original handlers.
     */
    void uninstallInterceptors(interceptRule *commandTable, int no_of_commands);

    // #ifdef RX_INDEXING
    // redisCommandProc **rxInstallIndexerInterceptors(struct redisCommand *interceptorCommandTable, unsigned int interceptorCommandTable_size);
    // redisCommandProc **rxUninstallIndexerInterceptors(struct redisCommand *interceptorCommandTable, unsigned int interceptorCommandTable_size, redisCommandProc **standard_command_procs);
    // #endif

    void *rxFindKey(int dbNo, const char *key);
    void *rxFindSetKey(int dbNo, const char *key);
    void *rxFindHashKey(int dbNo, const char *key);
    void *rxScanKeys(int dbNo, void **diO, char **key);
    void *rxScanSetMembers(void *obj, void **siO, char **member, int64_t *member_len);
    rxString rxRandomSetMember(void *set);
    dictIterator *rxGetDatabaseIterator(int dbNo);
    long long rxDatabaseSize(int dbNo);

    void rxAllocateClientArgs(void *cO, void **argV, int argC);
    void rxClientExecute(void *cO, void *pO);

    int rxGetServerPort();

    int rxIsAddrBound(char *addr, int port);
#define MATCH_IS_FALSE 0
#define MATCH_IS_TRUE 1

#define POINTER unsigned int
#define AS_POINTER(p) ((POINTER)p)

    struct rxHashTypeIterator;

    int rxMatchHasValue(void *oO, const char *field, rxString pattern, int plen);

// Wrappers
#define rxOBJ_STRING 0       /* String object. */
#define rxOBJ_LIST 1         /* List object. */
#define rxOBJ_SET 2          /* Set object. */
#define rxOBJ_ZSET 3         /* Sorted set object. */
#define rxOBJ_HASH 4         /* Hash object. */
#define rxOBJ_STREAM 6       /* Hash object. */
#define rxOBJ_INDEX_ENTRY 12 /* Rax object. */
#define rxOBJ_RAX 13         /* RX Index Entry object. */
#define rxOBJ_TRIPLET 14     /* RX triplet object. */
#define rxOBJ_NULL 15     /* RX triplet object. */

    struct client *rxCreateAOFClient(void);
    struct redisCommand *rxLookupCommand(rxString name);

    void *rxSetStringObject(void *o, void *ptr);
    int rxSizeofRobj();

    short rxGetObjectType(void *o);
    void *rxCreateObject(int type, void *ptr);
    void *rxInitObject(void *oO, int type, void *ptr);
    void *rxCreateStringObject(const char *ptr, size_t len);
    void *rxCreateHashObject(void);
    int rxHashTypeGetValue(void *o, const char *field, unsigned char **vstr, POINTER *vlen, long long *vll);
    rxString rxGetHashField(void *o, const char *field);
    rxString rxGetHashField2(void *oO, const char *field);
     long long rxGetHashFieldAsLong(void *o, const char *field);
     double rxGetHashFieldAsDouble(void *o, const char *field);
     int rxHashTypeSet(void *o, const char *field, const char *value, int flags);
     int rxHashTypeDelete(void *o, const char *f);
     int rxHashTypeExists(void *o, const char *f);
     rxString rxHashAsJson(const char *key, void *o);
     struct rxHashTypeIterator *rxHashTypeInitIterator(void *subject);
     void rxHashTypeReleaseIterator(struct rxHashTypeIterator *hi);

     int rxHashTypeNext(struct rxHashTypeIterator *hi);

#define rxOBJ_HASH_KEY 1
#define rxOBJ_HASH_VALUE 2

    rxString rxHashTypeCurrentObjectNewSds(struct rxHashTypeIterator *hi, int what);

    void rxFreeStringObject(void *o);
    void rxFreeHashObject(void *o);
    void rxFreeObject(void *o);
    void *rxGetContainedObject(void *o);
    void *rxSetContainedObject(void *oO, void *ptr);
    int rxGetRefcount(void *o);
    rax *rxSetToRax(void *obj);

    long long rxCreateTimeEvent(long long milliseconds,
                                aeTimeProc *proc, void *clientData,
                                aeEventFinalizerProc *finalizerProc);
    void rxDeleteTimeEvent(long long id);

    void rxModulePanic(char *msg);

    void rxHarvestSortedSetMembers(void *zobj, rax *bucket);
    double rxAddSortedSetMember(const char *key, int dbNo, double score, rxString member);
    int rxAddSetMember(const char *key, int dbNo, rxString member);
    double rxDeleteSortedSetMember(const char *key, int dbNo, rxString member);
    int rxDeleteSetMember(const char *key, int dbNo, rxString member);

    void *rxRemoveKeyRetainValue(int dbNo, const char *key);
    void *rxRestoreKeyRetainValue(int dbNo, const char *key, void *obj);
    void *rxCommitKeyRetainValue(int dbNo, const char *key, void *old_2te);

    unsigned long long mem_avail();

    typedef double (*rxComputeProc)(const char *l, int ll, const char *r);
    typedef int (*rxComparisonProc)(const char *l, int ll, const char *r);
    typedef int (*rxComparisonProc2)(const char *l, int ll, const char *r, const char *h);
    void rxInitComparisonsProcs();
    rxComputeProc rxFindComputationProc(const char *op);
    rxComparisonProc rxFindComparisonProc(const char *op);

    // rxMercator overrides for embededded rax object
    void rxRaxFreeWithCallback(rax *rax, void (*free_callback)(void *));
    void rxRaxFree(rax *rax);

    typedef struct rxClientInfo
    {
        // Memory Info
        size_t used_memory;
        // size_t used_memory_rss;
        size_t used_memory_peak;
        // size_t used_memory_peak_perc;
        // size_t used_memory_overhead;
        // size_t used_memory_startup;
        // size_t used_memory_dataset;
        // size_t used_memory_dataset_perc;
        size_t maxmemory;
        size_t server_memory_available;
        // Client Info
        size_t connected_clients;
        size_t cluster_connections;
        // size_t maxclients;
        // size_t client_recent_max_input_buffer;
        // size_t client_recent_max_output_buffer;
        size_t blocked_clients;
        size_t tracking_clients;
        size_t clients_in_timeout_table;
        size_t total_keys;
        size_t bytes_per_key;
        // double dataset_perc;
        // double peak_perc;
        // double total_frag;
    } rxClientInfo;

    rxClientInfo rxGetClientInfo();
    rxClientInfo rxGetClientInfoForHealthCheck();
    double rxGetMemoryUsedPercentage();

#ifdef __cplusplus
}
#endif

// END OF Wrappers
#endif
