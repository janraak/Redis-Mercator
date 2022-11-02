#ifndef __AMZRUSTHELPER_H__
#define __AMZRUSTHELPER_H__

#undef _GNU_SOURCE
#undef _DEFAULT_SOURCE
#undef _LARGEFILE_SOURCE


#ifdef __cplusplus
extern "C"
{
#endif

#include "ae.h"
#include "rax.h"
#include "../../src/dict.h"
#include "../../src/sds.h"

#ifdef __cplusplus
}
#endif


typedef void interceptorProc(void *c);

typedef struct  {
    char *name;
    interceptorProc *proc;
    interceptorProc *redis_proc;
    int id;     /* Command ID. */
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

    void *rxFindKey(int dbNo, sds key);
    void *rxFindSetKey(int dbNo, sds key);
    void *rxFindHashKey(int dbNo, sds key);
    void *rxScanKeys(int dbNo, void **diO, char **key);
    void *rxScanSetMembers(void *obj, void **siO, char **member, int64_t *member_len);
    sds rxRandomSetMember(void *set);
    dictIterator *rxGetDatabaseIterator(int dbNo);
    long long rxDatabaseSize(int dbNo);
    
    void rxAllocateClientArgs(void *cO, void ** argV, int argC);
    void rxClientExecute(void *cO, void *pO);

    int rxIsAddrBound(char *addr, int port);
#define MATCH_IS_FALSE 0
#define MATCH_IS_TRUE 1


#define POINTER unsigned int
#define AS_POINTER(p) ((POINTER)p)

  struct rxHashTypeIterator;

int rxMatchHasValue(void *oO, sds field, sds pattern, int plen);

// Wrappers
#define rxOBJ_STRING 0    /* String object. */
#define rxOBJ_LIST 1      /* List object. */
#define rxOBJ_SET 2       /* Set object. */
#define rxOBJ_ZSET 3      /* Sorted set object. */
#define rxOBJ_HASH 4      /* Hash object. */
#define rxOBJ_TRIPLET 15  /* RX triplet object. */


    struct client *rxCreateAOFClient(void);
    struct redisCommand *rxLookupCommand(sds name);

    sds rxSdsAttachlen(struct sdshdr32 *s_preamble, const void *init, size_t initlen, size_t *total_size);
    void *rxSetStringObject(void *o, void *ptr);
    int rxSizeofRobj();

    short rxGetObjectType(void *o);
    void *rxCreateObject(int type, void *ptr);
    void *rxCreateStringObject(const char *ptr, size_t len);
    void *rxCreateHashObject(void);
    int rxHashTypeGetValue(void *o, sds field, unsigned char **vstr, POINTER *vlen, long long *vll);
    sds rxGetHashField(void *o, sds field);
    int rxHashTypeSet(void *o, sds field, sds value, int flags);
    sds rxHashAsJson(sds key, void *o);
    struct rxHashTypeIterator *rxHashTypeInitIterator(void *subject);
    void rxHashTypeReleaseIterator(struct rxHashTypeIterator *hi);

    int rxHashTypeNext(struct rxHashTypeIterator *hi);

#define rxOBJ_HASH_KEY 1
#define rxOBJ_HASH_VALUE 2

sds rxHashTypeCurrentObjectNewSds(struct rxHashTypeIterator *hi, int what);

void rxFreeStringObject(void *o);
void rxFreeHashObject(void *o);
void rxFreeObject(void *o);
void *rxGetContainedObject(void *o);
rax *rxSetToRax(void *obj);

long long rxCreateTimeEvent(long long milliseconds,
        aeTimeProc *proc, void *clientData,
        aeEventFinalizerProc *finalizerProc);
void rxDeleteTimeEvent(long long id);

void rxModulePanic(char *msg);
#define rxLL_DEBUG 0
#define rxLL_VERBOSE 1
#define rxLL_NOTICE 2
#define rxLL_WARNING 3

void rxServerLogRaw(int level, sds msg);
void rxHarvestSortedSetMembers(void *zobj, rax *bucket);
double rxAddSortedSetMember(sds key, int dbNo, double score, sds member);
int rxAddSetMember(sds key, int dbNo, sds member);
double rxDeleteSortedSetMember(sds key, int dbNo, sds member);
int rxDeleteSetMember(sds key, int dbNo, sds member);

void *rxRemoveKeyRetainValue(int dbNo, sds key);
void *rxRestoreKeyRetainValue(int dbNo, sds key, void *obj);
void *rxCommitKeyRetainValue(int dbNo, sds key, void *old_2te);

unsigned long long mem_avail();

typedef int rxComparisonProc(char *l, int ll, char *r);
typedef int rxComparisonProc2(char *l, int ll, char *r, char *h);
void rxInitComparisonsProcs();
rxComparisonProc *rxFindComparisonProc(char *op);

#ifdef __cplusplus
}
#endif

// END OF Wrappers
#endif
 