
#ifdef __cplusplus
extern "C"
{
#endif
#include "version.h"

#include "/usr/include/arm-linux-gnueabihf/bits/types/siginfo_t.h"
#include <sched.h>
#include <signal.h>
#include <ctype.h>

#include "../../src/sds.h"
#include "../../src/server.h"
#include "rax.h"
    extern struct client *createAOFClient(void);

#include <stdlib.h>
#include <string.h>

#include "rxSuiteHelpers.h"
#ifdef __cplusplus
}
#endif

#define rxUNUSED(x) (void)(x)

long long rust_helper_cron = -1;

extern rxString hashTypeGetFromHashTable(robj *o, rxString field);
extern int hashTypeGetValue(robj *o, rxString field, unsigned char **vstr, POINTER *vlen, long long *vll);
extern int hashTypeGetFromZiplist(robj *o, rxString field,
                                  unsigned char **vstr,
                                  unsigned int *vlen,
                                  long long *vll);

zskiplistNode *zslGetElementByRank(zskiplist *zsl, unsigned long rank);

void installInterceptors(interceptRule *commandTable, int no_of_commands, timeProc *cron_proc)
{
    for (int j = 0; j < no_of_commands; ++j)
    {
        struct redisCommand *cmd = lookupCommandByCString(commandTable[j].name);
        if (cmd)
        {
            commandTable[j].id = cmd->id;
            commandTable[j].redis_proc = (interceptorProc *)cmd->proc;
            cmd->proc = (redisCommandProc *)commandTable[j].proc;
            commandTable[j].no_of_intercepts = 0;
            commandTable[j].no_of_success_intercepts = 0;
            commandTable[j].no_of_failure_intercepts = 0;
            commandTable[j].no_of_fallback_intercepts = 0;
        }
    }

    if (rust_helper_cron == -1)
    {
        rust_helper_cron = aeCreateTimeEvent(server.el, 1, (aeTimeProc *)cron_proc, NULL, NULL);
        int i = cron_proc(NULL, 0, NULL);
        rxServerLog(LL_NOTICE, "Rust cron started: %lld   0x%lx %d", rust_helper_cron, (long)cron_proc, i);
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

void *rxFindKey(int dbNo, const char *key)
{
    if (dbNo < 0 || dbNo >= server.dbnum)
        serverPanic("findKey: Invalid database");
    redisDb *db = (&server.db[dbNo]);
    if (!db)
        serverPanic("findKey: No REDIS DB!");
    if (!key)
        serverPanic("findKey: No key to search!");
    dictEntry *de = dictFind(db->dict, key);
    if (de)
    {
        return dictGetVal(de);
    }
    else
    {
        return NULL;
    }
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
    if (*si == NULL)
    {
        *si = setTypeInitIterator(obj);
    }
    if (setTypeNext(*si, member, member_len) == -1)
    {
        setTypeReleaseIterator(*si);
        *si = NULL;
        return NULL;
    }
    return *member;
}

rxString rxRandomSetMember(void *set)
{
    rxString ele;
    int64_t llele;
    int encoding;

    encoding = setTypeRandomElement(set, (sds *)&ele, &llele);
    if (encoding == OBJ_ENCODING_INTSET)
    {
        ele = rxStringFormat("%lld", llele);
    }
    return ele;
}

rax *rxSetToRax(void *obj)
{
    rax *r = raxNew();
    rxString member;
    int64_t member_len;
    setTypeIterator *si = setTypeInitIterator(obj);

    while (setTypeNext(si, (sds *)&member, &member_len) != -1)
    {
        raxInsert(r, (unsigned char *)member, strlen(member), NULL, NULL);
    }
    setTypeReleaseIterator(si);
    return r;
}

int rxMatchHasValue(void *oO, const char *f, rxString pattern, int plen)
{
    robj *o = (robj *)oO;
    if (!o || o->type != OBJ_HASH)
        return MATCH_IS_FALSE;

    rxString field = rxStringNew(f);

    if (o->encoding == OBJ_ENCODING_ZIPLIST)
    {
        unsigned char *vstr = NULL;
        POINTER vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        int ret = hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll);
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

        // printf(" %s ", k);
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
        // printf(" %s ", vstr);
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

    if (o->encoding == OBJ_ENCODING_ZIPLIST)
    {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        ret = hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll);
        if (ret >= 0)
        {
            if (vstr)
            {
                rxStringFree(field);
                return rxStringNewLen((const char *)vstr, vlen);
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
            return rxStringDup(value);
        }
    }
    else
    {
        serverPanic("Unknown hash encoding");
    }
    rxStringFree(field);
    return rxStringEmpty();
}

rxString rxGetHashField2(void *oO, const char *field)
{
    rxString f = rxStringNew(field);
    rxString v = rxGetHashField(oO, f);
    rxStringFree(f);
    return v;
}

rxString rxHashAsJson(const char *key, void *o)
{
    rxString json = rxStringFormat("{\"%s\":{", key);

    hashTypeIterator *hi = hashTypeInitIterator((robj *)o);
    char sep = ' ';
    while (hashTypeNext(hi) != C_ERR)
    {
        rxString f = hashTypeCurrentObjectNewSds(hi, rxOBJ_HASH_KEY);
        rxString v = hashTypeCurrentObjectNewSds(hi, rxOBJ_HASH_VALUE);
        json = rxStringFormat(json, "%s%c\"%s\":\"%s\"", json, sep, f, v);
        sep = ',';
        rxStringFree(f);
        rxStringFree(v);
    }
    hashTypeReleaseIterator(hi);
    json = rxStringFormat(json, "%s}}", json);
    return json;
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
    return ((robj *)o)->type;
}

void *rxCreateObject(int type, void *ptr)
{
    return createObject(type, ptr);
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
    robj *ro = (robj *)o;
    return ro->ptr;
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
    serverPanic(msg);
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
        if (setTypeIsMember(sobj, (sds)member))
            return setTypeRemove(sobj, (sds)member);
    }
    return 0;
}

#if REDIS_VERSION_NUM < 0x00060200
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
        robj *k = createStringObject(key, strlen(key));
        dbAdd((&server.db[dbNo]), k, zobj);
        freeStringObject(k);
    }
#if REDIS_VERSION_NUM < 0x00060200
    int score_flag = ZADD_INCR;
    zsetAdd(zobj, score, (sds)member, &score_flag, &newScore);
#else
    int score_flag = ZADD_IN_INCR;
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
unsigned long zsetLength(const robj *zobj);

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

    void *si = NULL;
    rxString old_member;
    int64_t l;
    while (rxScanSetMembers(old_state, &si, (char **)&old_member, &l) != NULL)
    {
        // Any Old state member not in new state is removed from index entry!
        if (new_members == NULL || raxFind(new_members, (unsigned char *)old_member, strlen(old_member)) == raxNotFound)
        {
            robj *index_entry = rxFindSortedSetKey(dbNo, old_member);
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

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST)
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
                printf("L:%lld ", vlong);
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
    c->user = DefaultUser;
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

int compareEquals(const char *l, int ll, const char *r)
{
    if (isdigit(*l) || isdigit(*r))
    {
        double v = atof(l);
        double t = atof(r);
        return v == t;
    }
    else
        return strncmp(l, r, ll) == 0;
}

int compareGreaterEquals(const char *l, int ll, const char *r)
{
    if (isdigit(*l) || isdigit(*r))
    {
        double v = atof(l);
        double t = atof(r);
        return v >= t;
    }
    else
        return strncmp(l, r, ll) >= 0;
}

int compareGreater(const char *l, int ll, const char *r)
{
    if (isdigit(*l) || isdigit(*r))
    {
        double v = atof(l);
        double t = atof(r);
        return v > t;
    }
    else
        return strncmp(l, r, ll) > 0;
}

int compareLessEquals(const char *l, int ll, const char *r)
{
    if (isdigit(*l) || isdigit(*r))
    {
        double v = atof(l);
        double t = atof(r);
        return v <= t;
    }
    else
        return strncmp(l, r, ll) <= 0;
}

int compareLess(const char *l, int ll, const char *r)
{
    if (isdigit(*l) || isdigit(*r))
    {
        double v = atof(l);
        double t = atof(r);
        return v < t;
    }
    else
        return strncmp(l, r, ll) < 0;
}

int compareNotEquals(const char *l, int ll, const char *r)
{
    if (isdigit(*l) || isdigit(*r))
    {
        double v = atof(l);
        double t = atof(r);
        return v != t;
    }
    else
        return strncmp(l, r, ll) != 0;
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

    raxTryInsert(rxComparisonsMap, (unsigned char *)"=", 1, (void *)compareEquals, &old);
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

    raxTryInsert(rxComparisonsMap, (unsigned char *)"eq", 2, (void *)compareEquals, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"ne", 2, (void *)compareEquals, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"ge", 2, (void *)compareGreaterEquals, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"le", 2, (void *)compareLessEquals, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"gt", 2, (void *)compareGreater, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"lt", 2, (void *)compareLess, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"contains", 8, (void *)compareContains, &old);
    raxTryInsert(rxComparisonsMap, (unsigned char *)"between", 7, (void *)compareInRange, &old);
}

rxComparisonProc *rxFindComparisonProc(const char *op)
{
    if (rxComparisonsMap == NULL)
        rxInitComparisonsProcs();
    rxComparisonProc *compare = (rxComparisonProc *)raxFind(rxComparisonsMap, (unsigned char *)op, strlen(op));
    if (compare == raxNotFound)
        return NULL;
    return compare;
}
