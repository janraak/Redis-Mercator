
#ifdef __cplusplus
extern "C"
{
#endif
#include "version.h"

#include <ctype.h>
#include <sched.h>
#include <signal.h>

#include "../../src/rax.h"
#include "../../src/sds.h"
#include "../../src/server.h"
#include "../../src/redismodule.h"
    extern struct client *createAOFClient(void);
    void raxRecursiveFree(rax *rax, raxNode *n, void (*free_callback)(void *));
    unsigned long getClusterConnectionsCount(void);
    robj *hashTypeDup(robj *o);
    int hashTypeSet(robj *o, sds field, sds value, int flags);
    unsigned long hashTypeLength(const robj *o);

#include <stdlib.h>
#include <string.h>

#include "rxSuiteHelpers.h"
#ifdef __cplusplus
}
#endif

#include <math.h>

#define rxUNUSED(x) (void)(x)

extern rxString hashTypeGetFromHashTable(robj *o, rxString field);
extern int hashTypeGetValue(robj *o, rxString field, unsigned char **vstr, POINTER *vlen, long long *vll);
#if REDIS_VERSION_NUM >= 0x00070000
#define HASTYPEGETFROMPACKED hashTypeGetFromListpack
#define HASHTYPE_PACKED OBJ_ENCODING_LISTPACK
#else
#define HASTYPEGETFROMPACKED hashTypeGetFromZiplist
#define HASHTYPE_PACKED OBJ_ENCODING_ZIPLIST
#endif
extern int HASTYPEGETFROMPACKED(robj *o, rxString field,
                                unsigned char **vstr,
                                unsigned int *vlen,
                                long long *vll);

zskiplistNode *zslGetElementByRank(zskiplist *zsl, unsigned long rank);

void installInterceptors(interceptRule *commandTable, int no_of_commands, timeProc *cron_proc)
{
    rxUNUSED(cron_proc);
    rxUNUSED(cron_proc);
    for (int j = 0; j < no_of_commands; ++j)
    {
        struct redisCommand *cmd = lookupCommandByCString(commandTable[j].name);
        if (cmd)
        {
#if REDIS_VERSION_NUM > 0x00060000
                commandTable[j].id = cmd->id;
#endif
#if REDIS_VERSION_NUM > 0x00060000
                commandTable[j].id = cmd->id;
#endif
            commandTable[j].redis_proc = (interceptorProc *)cmd->proc;
            cmd->proc = (redisCommandProc *)commandTable[j].proc;
            commandTable[j].no_of_intercepts = 0;
            commandTable[j].no_of_success_intercepts = 0;
            commandTable[j].no_of_failure_intercepts = 0;
            commandTable[j].no_of_fallback_intercepts = 0;
        }
    }
}

void uninstallInterceptors(interceptRule *commandTable, int no_of_commands)
{
    // Restore original command processors
    for (int j = 0; j < no_of_commands; ++j)
    {
        struct redisCommand *cmd = lookupCommandByCString(commandTable[j].name);
        if (cmd)
        {
            cmd->proc = (redisCommandProc *)commandTable[j].redis_proc;
            commandTable[j].redis_proc = NULL;
        }
    }
}

static rxString rxFindByScan(int dbNo, const char *regex)
{
    void *iter = NULL;
    rxString key;
    void *obj;

    while ((obj = rxScanKeys(dbNo, &iter, (char **)&key)) != NULL)
    {
        if (rxStringMatch(key, regex, MATCH_IGNORE_CASE))
            return key;
    }
    return NULL;
}

void *rxFindKey(int dbNo, const char *key)
{
    if (dbNo < 0 || dbNo >= server.dbnum)
        serverPanic("findKey: Invalid database");
    redisDb *db = (&server.db[dbNo]);
    if (!db)
        serverPanic("findKey: No REDIS DB!");
    if (!key)
        serverPanic("findKey: No key to search!");
    // robj k = {OBJ_STRING, OBJ_ENCODING_RAW, key, OBJ_SHARED_REFCOUNT};
    robj *ko = createStringObject(key, strlen(key));
    robj *kv = lookupKeyRead(db, ko);
    freeStringObject(ko);
    if (kv)
    {
        return kv;
    }
    sds k = sdsnew(key);
    dictEntry *de = dictFind(db->dict, k);
    sdsfree(k);
    if (de)
    {
        return dictGetVal(de);
    }
    else
    {
        rxString altK = rxFindByScan(dbNo, key);
        if (altK)
        {
            de = dictFind(db->dict, altK);
            if (de)
            {
                return dictGetVal(de);
            }
            return NULL;
        }
    }
    return NULL;
}
void *rxFindSetKey(int dbNo, const char *key)
{
    robj *o = rxFindKey(dbNo, key);
    if (o == NULL || o->type != OBJ_SET)
        return NULL;
    return o;
}

void *rxFindSortedSetKey(int dbNo, const char *key)
{
    robj *o = rxFindKey(dbNo, key);
    if (o == NULL || o->type != OBJ_ZSET)
        return NULL;
    return o;
}

void *rxFindHashKey(int dbNo, const char *key)
{
    robj *o = rxFindKey(dbNo, key);
    if (!o)
        return NULL;
    if (o->type == OBJ_HASH)
        return o;
    return NULL;
}

void *rxScanKeys(int dbNo, void **diO, char **key)
{
    dictIterator **di = (dictIterator **)diO;
    if (*diO == NULL)
    {
        redisDb *db = (&server.db[dbNo]);
        *di = dictGetSafeIterator(db->dict);
    }
    dictEntry *de;
    if ((de = dictNext(*di)) == NULL)
    {
        dictReleaseIterator(*di);
        *di = NULL;
        return NULL;
    }
    *key = dictGetKey(de);
    robj *value = dictGetVal(de);
    return value;
}

dictIterator *rxGetDatabaseIterator(int dbNo)
{
    redisDb *db = (&server.db[dbNo]);
    if (!db)
        serverPanic("findKey: No REDIS DB!");
    return dictGetSafeIterator(db->dict);
}

long long rxDatabaseSize(int dbNo)
{
    redisDb *db = (&server.db[dbNo]);
    if (!db)
        serverPanic("findKey: No REDIS DB!");
    return dictSize(db->dict);
}

void *rxScanSetMembers(void *obj, void **siO, char **member, int64_t *member_len)
{
    setTypeIterator **si = (setTypeIterator **)siO;
    robj *r = (robj *)obj;
    switch (r->encoding)
    {
    case OBJ_ENCODING_HT:
    case OBJ_ENCODING_INTSET:
#if REDIS_VERSION_NUM >= 0x00070200
    case OBJ_ENCODING_LISTPACK:
#endif
        break;
    default:
        rxServerLog(rxLL_NOTICE, "Invalid set encoding on %p %d", r, r->encoding);
        *si = NULL;
        return NULL;
    }
    if (*si == NULL)
    {
        *si = setTypeInitIterator(obj);
    }
#if REDIS_VERSION_NUM < 0x00070200
    if (setTypeNext(*si, member, member_len) == -1)
    {
        setTypeReleaseIterator(*si);
        *si = NULL;
        return NULL;
    }
#else
    int64_t llele;
    if (setTypeNext(*si, member, member_len, &llele) == -1)
    {
        setTypeReleaseIterator(*si);
        *si = NULL;
        return NULL;
    }
#endif
    int l = *member_len;
    if (l == -123456789)
        l = strlen(*member);
    char *m = rxMemAlloc(1 + l);
    memcpy(m, *member, l);
    m[l] = 0x00;
    *member = m;
    return *member;
}

rxString rxRandomSetMember(void *set)
{
    if (!set)
        return NULL;
    rxString ele;
    int64_t llele;
    int encoding;
#if REDIS_VERSION_NUM < 0x00070200
    encoding = setTypeRandomElement(set, (sds *)&ele, &llele);
    if (encoding == OBJ_ENCODING_INTSET)
    {
        ele = rxStringFormat("%lld", llele);
    }
#else
    size_t sz;
    char *e;
    encoding = setTypeRandomElement(set, &e, &sz, &llele);
    if (e == NULL)
        ele = rxStringFormat("%lld", llele);
    else
        ele = rxStringNewLen(e, sz);
#endif
    return ele;
}

rax *rxSetToRax(void *obj)
{
    rax *r = raxNew();
    rxSetMembers *mob = rxHarvestSetmembers(obj);
    char **p = (char **)&mob->members;
    for (size_t n = 0; n < mob->member_count; ++n)
    {
        char *member = *p;
        raxInsert(r, (unsigned char *)member, strlen(member), NULL, NULL);
        ++p;
    }
    rxFreeSetmembers(mob);
    return r;
}

int rxMatchHasValue(void *oO, const char *f, rxString pattern, int plen)
{
    robj *o = (robj *)oO;
    if (!o || o->type != OBJ_HASH)
        return MATCH_IS_FALSE;

    rxString field = rxStringNew(f);

    if (o->encoding == HASHTYPE_PACKED)
    {
        unsigned char *vstr = NULL;
        POINTER vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        int ret = HASTYPEGETFROMPACKED(o, field, &vstr, &vlen, &vll);
        if (ret < 0)
        {
            rxStringFree(field);
            return MATCH_IS_FALSE;
        }
        if (pattern == NULL)
        {
            rxStringFree(field);
            return MATCH_IS_TRUE;
        }
        if (vstr == NULL)
        {
            rxStringFree(field);
            return MATCH_IS_FALSE;
        }

        // rxServerLog(rxLL_NOTICE, " %s ", k);
        if (ret == 0 && stringmatchlen(pattern, plen, (const char *)vstr, vlen, 1))
        {
            rxStringFree(field);
            return MATCH_IS_TRUE;
        }
    }
    else if (o->encoding == OBJ_ENCODING_HT)
    {
        unsigned char *vstr;
        POINTER vlen;
        long long vll;
        hashTypeGetValue(o, field, &vstr, &vlen, &vll);
        // rxServerLog(rxLL_NOTICE, " %s ", vstr);
        if (vstr == NULL)
        {
            rxStringFree(field);
            return MATCH_IS_FALSE;
        }
        if (stringmatchlen(pattern, plen, (const char *)vstr, vlen, 1))
        {
            rxStringFree(field);
            return MATCH_IS_TRUE;
        }
    }
    rxStringFree(field);
    return MATCH_IS_FALSE;
}

int rxHashTypeSet(void *o, const char *f, const char *v, int flags)
{
    if (((robj *)o)->type != rxOBJ_HASH)
        return -1;
    rxString field = rxStringNew(f);
    rxString value = rxStringNew(v);
    int retval = hashTypeSet((robj *)o, (sds)field, (sds)value, flags);
    rxStringFree(field);
    rxStringFree(value);
    return retval;
}

int rxHashTypeDelete(void *o, const char *f)
{
    if (((robj *)o)->type != rxOBJ_HASH)
        return -1;
    rxString field = rxStringNew(f);
    int retval = hashTypeDelete((robj *)o, (sds)field);
    rxStringFree(field);
    return retval;
}

int rxHashTypeExists(void *o, const char *f)
{
    if (((robj *)o)->type != rxOBJ_HASH)
        return -1;
    rxString field = rxStringNew(f);
    int retval = hashTypeExists((robj *)o, (sds)field);
    rxStringFree(field);
    return retval;
}

extern const char *ValidateString(const char *s, size_t l){
    char *p = s;
    size_t pl = 0;
    while((*p >= 0x20 && *p <= 0x7f) || (*p >= 0xa0 && *p <= 0xff)){
        ++p;
        ++pl;
    }
    if(*p != 0x00){
        *p = 0x00;
    }
    if(l != pl){
        printf("Unexpected string: [%ld == %ld] %s", pl, l, s);
    }
    return s;
}

rxString rxGetHashField(void *oO, const char *f)
{
    rxString field = rxStringNew(f);
    robj *o = (robj *)oO;
    int ret;

    if (o == NULL)
    {
        rxStringFree(field);
        return rxStringEmpty();
    }
    if (((robj *)o)->type != rxOBJ_HASH)
    {
        rxServerLog(rxLL_WARNING, "%s is not a hash key", f);
        return NULL;
    }
    if (!hashTypeExists(o, (sds)field))
        return NULL;

    if (o->encoding == HASHTYPE_PACKED)
    {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        ret = HASTYPEGETFROMPACKED(o, field, &vstr, &vlen, &vll);
        if (ret >= 0)
        {
            if (vstr)
            {
                rxStringFree(field);
                return ValidateString(rxStringNewLen((const char *)vstr, vlen), vlen);
            }
            else
            {
                rxStringFree(field);
                return rxStringFormat("%lld", vll);
            }
        }
    }
    else if (o->encoding == OBJ_ENCODING_HT)
    {
        rxString value = hashTypeGetFromHashTable(o, field);
        if (value != NULL)
        {
            rxStringFree(field);
            return ValidateString(rxStringDup(value), strlen(value));
        }
    }
    else
    {
        serverPanic("Unknown hash encoding");
    }
    rxStringFree(field);
    return NULL;
}

rxString rxGetHashField2(void *oO, const char *field)
{
    rxString f = rxStringNew(field);
    rxString v = rxGetHashField(oO, f);
    rxStringFree(f);
    return v;
}

long long rxGetHashFieldAsLong(void *o, const char *field)
{
    rxString v = rxGetHashField(o, field);
    return v ? atol(v) : 0;
}
double rxGetHashFieldAsDouble(void *o, const char *field)
{
    rxString v = rxGetHashField(o, field);
    return v ? atof(v) : 0;
}

rxString rxHashAsJson(const char *key, void *o)
{
    rxString json = rxStringFormat("{\"key\": \"%s\", \"value\":{", key);

    hashTypeIterator *hi = hashTypeInitIterator((robj *)o);
    char sep[2];
    sep[0] = 0x00;
    sep[1] = 0x00;
    while (hashTypeNext(hi) != C_ERR)
    {
        rxString f = hashTypeCurrentObjectNewSds(hi, rxOBJ_HASH_KEY);
        rxString v = hashTypeCurrentObjectNewSds(hi, rxOBJ_HASH_VALUE);
        rxString njson = rxStringFormat("%s%s\"%s\":\"%s\"", json, sep, f, v);
        sep[0] = ',';
        rxStringFree(f);
        rxStringFree(v);
        rxStringFree(json);
        json = njson;
    }
    hashTypeReleaseIterator(hi);
    rxString njson = rxStringFormat("%s}", json);
    rxStringFree(json);
    return njson;
}

// Embeded robj setter

void *rxSetStringObject(void *oO, void *ptr)
{
    robj *o = (robj *)oO;
    o->type = OBJ_STRING;
    o->encoding = OBJ_ENCODING_RAW;
    o->ptr = ptr;
    o->refcount = 1;
    o->lru = 0;
    return o;
}

int rxSizeofRobj()
{
    return sizeof(robj);
}

// Wrappers
short rxGetObjectType(void *o)
{
    if (!o)
        return rxOBJ_NULL;
    return ((robj *)o)->type;
}

int rxGetRefcount(void *oO)
{
    robj *o = (robj *)oO;
    return o->refcount;
}

void *rxCreateObject(int type, void *ptr)
{
    return createObject(type, ptr);
}

void *rxSetContainedObject(void *oO, void *ptr)
{
    robj *o = (robj *)oO;
    o->ptr = ptr;
    return o;
}

void *rxInitObject(void *oO, int type, void *ptr)
{
    robj *o = (robj *)oO;
    o->type = type;
    o->encoding = OBJ_ENCODING_RAW;
    o->ptr = ptr;
    o->refcount = OBJ_SHARED_REFCOUNT;
    return o;
}
void *rxCreateStringObject(const char *ptr, size_t len)
{
    return createStringObject(ptr, len);
}
void *rxCreateHashObject()
{
    return createHashObject();
}
void rxFreeStringObject(void *o)
{
    freeStringObject(o);
}

void rxFreeHashObject(void *o)
{
    freeHashObject(o);
}

void rxFreeObject(void *o)
{
    rxMemFree(o);
}

void *rxGetContainedObject(void *o)
{
    if (!o)
        return NULL;
    robj *ro = (robj *)o;
    return ro->ptr;
}

const char*rxGetContainedString(void *o)
{
    if (!o)
        return NULL;
    robj *ro = (robj *)o;
    return (const char*)ro->ptr;
}

int rxHashTypeGetValue(void *o, const char *f, unsigned char **vstr, POINTER *vlen, long long *vll)
{
    rxString field = rxStringNew(f);
    int retval = hashTypeGetValue(o, field, vstr, vlen, vll);
    rxStringFree(field);
    return retval;
}

struct rxHashTypeIterator *rxHashTypeInitIterator(void *subject)
{
    return (struct rxHashTypeIterator *)hashTypeInitIterator((robj *)subject);
}

void rxHashTypeReleaseIterator(struct rxHashTypeIterator *hi)
{
    hashTypeReleaseIterator((hashTypeIterator *)hi);
}

int rxHashTypeNext(struct rxHashTypeIterator *hi)
{
    return hashTypeNext((hashTypeIterator *)hi);
}

rxString rxHashTypeCurrentObjectNewSds(struct rxHashTypeIterator *hi, int what)
{
    return hashTypeCurrentObjectNewSds((hashTypeIterator *)hi, what);
}

long long rxCreateTimeEvent(long long milliseconds,
                            aeTimeProc *proc, void *clientData,
                            aeEventFinalizerProc *finalizerProc)
{
    return aeCreateTimeEvent(server.el, milliseconds, proc, clientData, finalizerProc);
}

void rxDeleteTimeEvent(long long id)
{
    aeDeleteTimeEvent(server.el, id);
}

void rxModulePanic(char *msg)
{
    serverPanic("%s", msg);
}

static void HarvestMember(rax *bucket, unsigned char *key, int k_len, double score)
{
    double *member = (double *)raxFind(bucket, key, k_len);
    void *old;
    if (member == raxNotFound)
    {
        member = zmalloc(sizeof(double));
        *member = score;
    }
    else
    {
        *member += score;
    }
    raxInsert(bucket, key, k_len, member, &old);
}

#if REDIS_VERSION_NUM < 0x00070200
extern int dbGenericDelete(redisDb *db, robj *key, int async);
#else
extern int dbGenericDelete(redisDb *db, robj *key, int async, int flags);
#endif
extern int dbSyncDelete(redisDb *db, robj *key);
int rxDbDelete(int dbNo, const char *key)
{
    robj *k = createStringObject(key, strlen(key));
    int rc = dbSyncDelete(&server.db[dbNo], k);
    freeStringObject(k);
    return rc;
}

extern int setTypeAdd(robj *subject, sds value);
int rxAddSetMember(const char *key, int dbNo, rxString member)
{
    robj *sobj = (robj *)rxFindSetKey(dbNo, key);
    if (sobj == NULL)
    {
        sobj = createSetObject();
        robj *k = createStringObject(key, strlen(key));
        dbAdd((&server.db[dbNo]), k, sobj);
        freeStringObject(k);
    }
    return setTypeAdd(sobj, (sds)member);
}

int rxDeleteSetMember(const char *key, int dbNo, rxString member)
{
    robj *sobj = (robj *)rxFindSetKey(dbNo, key);
    if (sobj != NULL)
    {
        // if (setTypeIsMember(sobj, (sds)member))
        return setTypeRemove(sobj, (sds)member);
    }
    return 0;
}

int rxDeleteSetMember2(void *sobj, int dbNo, rxString member)
{
    rxUNUSED(dbNo);
    // if (setTypeIsMember(sobj, (sds)member))
    return setTypeRemove((robj *)sobj, (sds)member);
    return 0;
}

#if REDIS_VERSION_NUM < 0x00070000
extern int zsetAdd(robj *zobj, double score, sds ele, int *flags, double *newscore);
#else
extern int zsetAdd(robj *zobj, double score, sds ele, int in_flags, int *out_flags, double *newscore);
#endif

double rxAddSortedSetMember(const char *key, int dbNo, double score, rxString member)
{
    double newScore;
    robj *zobj = (robj *)rxFindSortedSetKey(dbNo, key);
    if (zobj == NULL)
    {
        zobj = createZsetObject();
        robj *k = createRawStringObject(key, strlen(key));
        dbAdd((&server.db[dbNo]), k, zobj);
        freeStringObject(k);
    }
#if REDIS_VERSION_NUM < 0x00070000
    int score_flag = ZADD_INCR;
    zsetAdd(zobj, score, (sds)member, &score_flag, &newScore);
#else
    int score_flag = ZADD_IN_GT;
    zsetAdd(zobj, score, (sds)member, score_flag, &score_flag, &newScore);
#endif
    return newScore;
}

double rxDeleteSortedSetMember(const char *key, int dbNo, rxString member)
{
    robj *zobj = (robj *)rxFindSortedSetKey(dbNo, key);
    if (zobj != NULL)
    {
        zsetDel(zobj, (sds)member);
    }
    return 0.0;
}

extern int dbDelete(redisDb *db, robj *key);
void *rxRemoveKeyRetainValue(int dbNo, const char *key)
{
    void *obj = rxFindKey(dbNo, key);
    if (obj == NULL)
        return obj;
    incrRefCount(obj);
    robj *k = createStringObject(key, strlen(key));
    dbDelete((&server.db[dbNo]), k);
    freeStringObject(k);
    return obj;
}

void *rxRestoreKeyRetainValue(int dbNo, const char *key, void *obj)
{
    robj *k = createStringObject(key, strlen(key));
    dbDelete((&server.db[dbNo]), k);
    if (obj != NULL)
        dbAdd((&server.db[dbNo]), k, obj);
    freeStringObject(k);
    return obj;
}

extern int zsetDel(robj *zobj, sds ele);
#if REDIS_VERSION_NUM >= 0x00050000
unsigned long zsetLength(const robj *zobj);
#else
unsigned int zsetLength(const robj *zobj);
#endif

void *rxCommitKeyRetainValue(int dbNo, const char *key, void *old_state)
{
    rxString objectIndexkey = rxStringFormat("_ox_:%s", key);
    void *new_state = rxFindKey(dbNo, objectIndexkey);
    rxStringFree(objectIndexkey);

    if (old_state == NULL)
        return new_state;

    rax *new_members = (new_state != NULL)
                           ? rxSetToRax(new_state)
                           : NULL;

    rxSetMembers *mob = rxHarvestSetmembers(old_state);
    char **p = (char **)&mob->members;
    for (size_t n = 0; n < mob->member_count; ++n)
    {
        char *old_member = *p;
        // Any Old state member not in new state is removed from index entry!
        if (new_members == NULL || raxFind(new_members, (unsigned char *)old_member, strlen(old_member)) == raxNotFound)
        {
            robj *index_entry = rxFindSortedSetKey(dbNo, old_member);
            if (index_entry != NULL)
            {
                rxString hkey = rxStringFormat("%s\tH", key);
                if (zsetDel(index_entry, (sds)hkey) == 0)
                {
                    rxString skey = rxStringFormat("%s\ts", key);
                    if (zsetDel(index_entry, (sds)skey) == 0)
                    {
                    };
                    rxStringFree(skey);
                };
                rxStringFree(hkey);
                if (zsetLength(index_entry) == 0)
                {
                    robj *k = createStringObject(old_member, strlen(old_member));
                    dbDelete((&server.db[dbNo]), k);
                    freeStringObject(k);
                }
            }
        }
        ++p;
    }
    rxFreeSetmembers(mob);

    if (new_members != NULL)
        raxFree(new_members);
    freeSetObject(old_state);
    return new_state;
}

void rxHarvestSortedSetMembers(void *obj, rax *bucket)
{
    robj *zobj = (robj *)obj;
    long llen;
    long rangelen;

    if (rxGetObjectType(zobj) != OBJ_ZSET)
        return;

    /* Sanitize indexes. */
    llen = zsetLength(zobj);

    rangelen = llen;

    if (zobj->encoding == HASHTYPE_PACKED)
    {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        eptr = ziplistIndex(zl, 0);

        // serverAssertWithInfo(c,zobj,eptr != NULL);
        sptr = ziplistNext(zl, eptr);

        while (rangelen--)
        {
            // serverAssertWithInfo(c,zobj,eptr != NULL && sptr != NULL);
            ziplistGet(eptr, &vstr, &vlen, &vlong);

            if (vstr == NULL)
            {
                rxServerLog(rxLL_NOTICE, "L:%lld ", vlong);
            }
            else
            {
                HarvestMember(bucket, vstr, vlen, zzlGetScore(sptr));
            }
            zzlNext(zl, &eptr, &sptr);
        }
    }
    else if (zobj->encoding == OBJ_ENCODING_SKIPLIST)
    {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;
        rxString ele;

        /* Check if starting point is trivial, before doing log(N) lookup. */
        ln = zsl->header->level[0].forward;

        while (rangelen--)
        {
            // serverAssertWithInfo(c,zobj,ln != NULL);
            ele = ln->ele;
            HarvestMember(bucket, (unsigned char *)ele, strlen(ele), ln->score);
            ln = ln->level[0].forward;
        }
    }
    else
    {
        serverPanic("Unknown sorted set encoding");
    }
}

// END OF Wrappers

void rxAllocateClientArgs(void *cO, void **argV, int argC)
{
    client *c = (client *)cO;
    c->argc = argC;
    c->argv = (robj **)argV;
}

void rxClientExecute(void *cO, void *pO)
{
    struct redisCommand *p = (struct redisCommand *)pO;
    client *c = (client *)cO;
    #if REDIS_VERSION_NUM > 0x00060000
        c->user = DefaultUser;
    #endif
    // replicate = flags & REDISMODULE_ARGV_REPLICATE;
    c->flags |= CLIENT_MODULE;
    c->db = &server.db[0];
    c->cmd = pO;
    // int call_flags = CMD_CALL_SLOWLOG | CMD_CALL_STATS | CMD_CALL_NOWRAP;
    // call(c, call_flags);
    p->proc(c);
}

#include <sys/resource.h>
#include <sys/sysinfo.h>

int sysinfo(struct sysinfo *info);

unsigned long long mem_avail()
{
    struct sysinfo info;

    if (sysinfo(&info) < 0)
        return 0;

    size_t zmalloc_used = zmalloc_used_memory();
    size_t total_system_mem = (server.system_memory_size < server.maxmemory) ? server.system_memory_size : server.maxmemory;

    return (total_system_mem - zmalloc_used) / info.mem_unit;
}

struct client *rxCreateAOFClient()
{
    // return createClient(NULL);
    return createAOFClient();
}
struct redisCommand *rxLookupCommand(rxString name)
{
    return lookupCommandByCString(name);
}

int rxIsAddrBound(char *addr, int port)
{
    if (server.port != port)
        return 0;
    for (int n = 0; n < server.bindaddr_count; ++n)
    {
        if (strcmp(addr, server.bindaddr[n]) == 0)
            return 1;
    }
    return 0;
}

int compareEquals(const char *l, int, const char *r)
{
    if (isdigit(*l) || isdigit(*r))
    {
        double v = atof(l);
        double t = atof(r);
        return (v == t) ? 1 : 0;
    }
    else
        return (strcmp(l, r) == 0) ? 1 : 0;
}

int compareGreaterEquals(const char *l, int, const char *r)
{
    if (isdigit(*l) || isdigit(*r))
    {
        double v = atof(l);
        double t = atof(r);
        return (v >= t) ? 1 : 0;
    }
    else
        return (strcmp(l, r) >= 0) ? 1 : 0;
}

int compareGreater(const char *l, int, const char *r)
{
    if (isdigit(*l) || isdigit(*r))
    {
        double v = atof(l);
        double t = atof(r);
        return (v > t) ? 1 : 0;
    }
    else
        return (strcmp(l, r) > 0) ? 1 : 0;
}

int compareLessEquals(const char *l, int, const char *r)
{
    if (isdigit(*l) || isdigit(*r))
    {
        double v = atof(l);
        double t = atof(r);
        return (v <= t) ? 1 : 0;
    }
    else
        return (strcmp(l, r) <= 0) ? 1 : 0;
}

int compareLess(const char *l, int, const char *r)
{
    if (isdigit(*l) || isdigit(*r))
    {
        double v = atof(l);
        double t = atof(r);
        int rc = (v < t) ? 1 : 0;
        return rc;
    }
    else
        return (strcmp(l, r) < 0) ? 1 : 0;
}

int compareNotEquals(const char *l, int, const char *r)
{
    if (isdigit(*l) || isdigit(*r))
    {
        double v = atof(l);
        double t = atof(r);
        return (v != t) ? 1 : 0;
    }
    else
        return (strcmp(l, r) != 0) ? 1 : 0;
}

// TODO: Float arithmetic

double computeAdd(const char *l, int, const char *r)
{
    if (isdigit(*l) || isdigit(*r))
    {
        double v = atof(l);
        double t = atof(r);
        return v + t;
    }
    else
        return 0;
}

double computeSubtract(const char *l, int, const char *r)
{
    if (isdigit(*l) || isdigit(*r))
    {
        double v = atof(l);
        double t = atof(r);
        return v - t;
    }
    else
        return 0;
}

double computeMultiply(const char *l, int, const char *r)
{
    if (isdigit(*l) || isdigit(*r))
    {
        double v = atof(l);
        double t = atof(r);
        return v * t;
    }
    else
        return 0;
}

double computeDivide(const char *l, int, const char *r)
{
    if (isdigit(*l) || isdigit(*r))
    {
        double v = atof(l);
        double t = atof(r);
        return v / t;
    }
    else
        return 0;
}

double computeModulus(const char *, int, const char *)
{
    // if (isdigit(*l) || isdigit(*r))
    // {
    //     double v = atof(l);
    //     double t = atof(r);
    //     return modulus(v, t);
    // }
    // else
    return 0;
}

double computePower(const char *l, int, const char *r)
{
    if (isdigit(*l) || isdigit(*r))
    {
        double v = atof(l);
        double t = atof(r);
        return pow(v, t);
    }
    else
        return 0;
}

int calcSqrtInRange(const char *l, int)
{
    double v = atof(l);
    return sqrt(v);
}

int compareContains(const char *l, int ll, const char *r)
{
    rxUNUSED(ll);
    return strstr(l, r) != NULL;
}

int compareInRange(const char *value, int ll, const char *low, const char *high)
{
    rxUNUSED(ll);
    double v = atof(value);
    double t = atof(low);
    double m = atof(high);
    return v >= t && v <= m;
}

rax *rxComparisonsMap = NULL;

void rxInitComparisonsProcs()
{
    if (rxComparisonsMap != NULL)
        return;

    rxComparisonsMap = raxNew();

    void *old;

    raxTryInsert(rxComparisonsMap, (unsigned char *)"+", 1, (void *)computeAdd, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"-", 1, (void *)computeSubtract, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"*", 1, (void *)computeMultiply, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"/", 1, (void *)computeDivide, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"%", 1, (void *)computeModulus, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"=", 1, (void *)compareEquals, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"pow", 3, (void *)computePower, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"POW", 3, (void *)computePower, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"==", 2, (void *)compareEquals, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)">=", 2, (void *)compareGreaterEquals, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"<=", 2, (void *)compareLessEquals, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)">", 1, (void *)compareGreater, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"<", 1, (void *)compareLess, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"!=", 2, (void *)compareNotEquals, &old);

    raxTryInsert(rxComparisonsMap, (unsigned char *)"EQ", 2, (void *)compareEquals, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"NE", 2, (void *)compareEquals, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"GE", 2, (void *)compareGreaterEquals, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"LE", 2, (void *)compareLessEquals, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"GT", 2, (void *)compareGreater, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"LT", 2, (void *)compareLess, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"CONTAINS", 8, (void *)compareContains, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"BETWEEN", 7, (void *)compareInRange, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"SQRT", 4, (void *)calcSqrtInRange, &old);

    raxTryInsert(rxComparisonsMap, (unsigned char *)"eq", 2, (void *)compareEquals, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"ne", 2, (void *)compareEquals, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"ge", 2, (void *)compareGreaterEquals, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"le", 2, (void *)compareLessEquals, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"gt", 2, (void *)compareGreater, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"lt", 2, (void *)compareLess, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"contains", 8, (void *)compareContains, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"between", 7, (void *)compareInRange, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"sqrt", 4, (void *)calcSqrtInRange, &old);
}

rxComputeProc rxFindComputationProc(const char *op)
{
    if (rxComparisonsMap == NULL)
        rxInitComparisonsProcs();
    rxComputeProc computation = (rxComputeProc)raxFind(rxComparisonsMap, (unsigned char *)op, strlen(op));
    if (computation == raxNotFound)
        return NULL;
    return computation;
}

rxComparisonProc rxFindComparisonProc(const char *op)
{
    if (rxComparisonsMap == NULL)
        rxInitComparisonsProcs();
    rxComparisonProc compare = (rxComparisonProc)raxFind(rxComparisonsMap, (unsigned char *)op, strlen(op));
    if (compare == raxNotFound)
        return NULL;
    return compare;
}

// rxMercator overrides for embededded rax object
/* Free a whole radix tree, calling the specified callback in order to
 * free the auxiliary data. */
void rxRaxFreeWithCallback(rax *rax, void (*free_callback)(void *))
{
    raxRecursiveFree(rax, rax->head, free_callback);
}

/* Free a whole radix tree. */
void rxRaxFree(rax *rax)
{
    if (raxSize(rax) != 0)
        rxRaxFreeWithCallback(rax, NULL);
}

double rxGetMemoryUsedPercentage()
{
    if (server.maxmemory == 0)
        return 0.0;
    return zmalloc_used_memory() * 100.0 / server.maxmemory;
}

rxClientInfo rxGetClientInfoForHealthCheck()
{
    struct redisMemOverhead *mh = getMemoryOverheadData();

    rxClientInfo info;
    info.used_memory = zmalloc_used_memory();
    info.used_memory_peak = server.stat_peak_memory;
    info.maxmemory = server.maxmemory;
    info.server_memory_available = zmalloc_used_memory();

    info.connected_clients = listLength(server.clients) - listLength(server.slaves);
    info.cluster_connections = getClusterConnectionsCount();
    #if REDIS_VERSION_NUM >= 0x00050000
        info.blocked_clients = server.blocked_clients;
    #else
        info.blocked_clients = 0;    
    #endif
    info.total_keys = mh->total_keys;
    info.bytes_per_key = mh->bytes_per_key;
    #if REDIS_VERSION_NUM > 0x00060000
        info.tracking_clients = server.tracking_clients;
        info.clients_in_timeout_table = (unsigned long long)raxSize(server.clients_timeout_table);
    #else
        info.tracking_clients = 0;
        info.clients_in_timeout_table = 0;
    #endif
    return info;
}

rxClientInfo rxGetClientInfo()
{
    return rxGetClientInfoForHealthCheck();
}

int rxGetServerPort()
{
    #if REDIS_VERSION_NUM > 0x00060000
        return server.port ? server.port : server.tls_port;
    #else
        return server.port;
    #endif
}

void *rxGetDatabase(int )
{

    return NULL;
}

extern client *moduleGetReplyClient(struct RedisModuleCtx *ctx);
extern int RM_GetSelectedDb(struct RedisModuleCtx *ctx);
void rxSetDatabase(void *c, void *orgC)
{
    if((long long int)orgC < 7){
        serverLog(LL_NOTICE, "rxSetDatabase invalid RedisModuleCtx *ctx: %p", orgC);
        return;
    }

    int dbNo = 0;
    // TODO: Find better way to get the correct database
    //RM_GetSelectedDb((struct RedisModuleCtx *)orgC);
    //
    redisDb *db = (&server.db[dbNo]);
    ((client *)c)->db = db;
}

rxSetMembers *rxHarvestSetmembers(void *obj)
{
    size_t member_count = 0;
    size_t total_member_size = 0;

    robj *r = (robj *)obj;
    switch (r->encoding)
    {
    case OBJ_ENCODING_HT:
    case OBJ_ENCODING_INTSET:
#if REDIS_VERSION_NUM >= 0x00070200
    case OBJ_ENCODING_LISTPACK:
#endif
        break;
    default:
        rxServerLog(rxLL_NOTICE, "Invalid set encoding on %p %d", r, r->encoding);
        return NULL;
    }

    setTypeIterator *si = setTypeInitIterator(r);

    char *member;
    int64_t member_len;

#if REDIS_VERSION_NUM < 0x00070200
    while (setTypeNext(si, &member, &member_len) != -1)
    {
#else
    int64_t llele;
    while (setTypeNext(si, &member, &member_len, &llele) != -1)
    {
#endif
        ++member_count;
        total_member_size = total_member_size + strlen(member) + 1;
    }
    setTypeReleaseIterator(si);
    total_member_size = total_member_size + member_count;

    rxSetMembers *mob = (rxSetMembers *)rxMemAlloc(sizeof(rxSetMembers) + member_count * sizeof(void *) + total_member_size);
    mob->member_count = member_count;
    mob->member_index = 0;
    si = setTypeInitIterator(r);

    char **p = (char **)&mob->members;
    char *m = (char *)((void *)p) + member_count * sizeof(void *);
#if REDIS_VERSION_NUM < 0x00070200
    while (setTypeNext(si, &member, &member_len) != -1)
    {
#else
    while (setTypeNext(si, &member, &member_len, &llele) != -1)
    {
#endif

        strcpy(m, member);
        *p = m;
        m = m + strlen(member) + 1;
        ++p;
    }

    setTypeReleaseIterator(si);
    return mob;
}

    const char *rxGetSetMember(rxSetMembers *mob){
        if(!mob)
            return NULL;
        const char *member = NULL;
        if(mob->member_index < mob->member_count){
            member = mob->members[mob->member_index];
            mob->member_index++;
        }
        return member;
    }

rxSetMembers *rxFreeSetmembers(rxSetMembers *mob)
{
    if (mob)
        rxMemFree(mob);
    return NULL;
}

#if REDIS_VERSION_NUM < 0x00050000
int raxTryInsert(rax *rax, unsigned char *s, size_t len, void *data, void **old){

    void *e = raxFind(rax, s, len);
    if(e != raxNotFound)
        return raxInsert(rax, s, len, data, old);
    return 0;
}
#endif

void *rxFused(const char *key, const char *fuse_key, const char *barKey, void *fuse_subject, void *fuse_object, void *fuse_predicate)
{
    robj *fused = createHashObject(); // hashTypeDup(fuse_subject);
    sds cSubject = sdsnew("subject");
    sds cObject = sdsnew("object");
    sds cPredicate = sdsnew("predicate");
    hashTypeSet(fused, cSubject, key, HASH_SET_COPY);
    hashTypeSet(fused, cObject, barKey, HASH_SET_COPY);
    hashTypeSet(fused, cPredicate, fuse_key + 1, HASH_SET_COPY);
    sdsfree(cSubject);
    sdsfree(cObject);
    sdsfree(cPredicate);

    void **inputs[] = {fuse_subject, fuse_object, fuse_predicate};
    char *prefixes[] = {"S_", "O_", "P_"};
    char prefixed_field[1024];

    for (int n = 0; n < 3; ++n)
    {
        char *pfx = prefixes[n];
        hashTypeIterator *hi = hashTypeInitIterator(inputs[n]);
        while (hashTypeNext(hi) != C_ERR)
        {
            sds f = hashTypeCurrentObjectNewSds(hi, rxOBJ_HASH_KEY);
            strcpy(prefixed_field, pfx);
            strcat(prefixed_field, f);
            sds pfx_fld = sdsnew(&prefixed_field);
            sds v = hashTypeCurrentObjectNewSds(hi, rxOBJ_HASH_VALUE);
            hashTypeSet(fused, pfx_fld, v, HASH_SET_COPY);
            sdsfree(f);
            sdsfree(v);
        }
        hashTypeReleaseIterator(hi);
    }

    return fused;
}

unsigned long rxHashTypeLength(void *o){
    return hashTypeLength(o);
}

unsigned long rxHashTraverse(void *hash, rxTraversedHashField handler, void *prm)
{
    unsigned long tally = 0;
    hashTypeIterator *hi = hashTypeInitIterator(hash);
    while (hashTypeNext(hi) != C_ERR)
    {
        rxString f = hashTypeCurrentObjectNewSds(hi, rxOBJ_HASH_KEY);
        rxString v = hashTypeCurrentObjectNewSds(hi, rxOBJ_HASH_VALUE);
        handler(f, v, prm);
        ++tally;
    }
    hashTypeReleaseIterator(hi);
    return tally;
}

rxSetMembers *rxHarvestSetmembersForKey(int , const char *key)
{
    void *o = rxFindKey(0, key);
    if (o)
        return rxHarvestSetmembers(o);
    return NULL;
}

void rxAlsoPropagate(int dbid, void **argv, int argc, int target)
{
    alsoPropagate(dbid, (robj **)argv, argc, target);
}