
#ifdef __cplusplus
extern "C"
{
#endif

#include "../../src/server.h"
#include "rax.h"

#include "rxSuiteHelpers.h"
#ifdef __cplusplus
}
#endif

long long rust_helper_cron = -1;


extern sds hashTypeGetFromHashTable(robj *o, sds field);
extern int hashTypeGetValue(robj *o, sds field, unsigned char **vstr, POINTER *vlen, long long *vll);
extern int hashTypeGetFromZiplist(robj *o, sds field,
                                  unsigned char **vstr,
                                  unsigned int *vlen,
                                  long long *vll);

zskiplistNode *zslGetElementByRank(zskiplist *zsl, unsigned long rank);

void installInterceptors(interceptRule *commandTable, int no_of_commands, timeProc *cron_proc)
{
    for (int j = 0; j < no_of_commands; ++j) {
        struct redisCommand *cmd = lookupCommandByCString(commandTable[j].name);
        if (cmd)
        {
            commandTable[j].id = cmd->id;
            commandTable[j].redis_proc = (interceptorProc*)cmd->proc;
            cmd->proc = (redisCommandProc*)commandTable[j].proc;
            commandTable[j].no_of_intercepts = 0;
            commandTable[j].no_of_success_intercepts = 0;
            commandTable[j].no_of_failure_intercepts = 0;
            commandTable[j].no_of_fallback_intercepts = 0;
        }
    }

        if (rust_helper_cron == -1) {
            rust_helper_cron = aeCreateTimeEvent(server.el, 1, (aeTimeProc*) cron_proc, NULL, NULL);
            int i = cron_proc(NULL, 0, NULL);
            serverLog(LL_NOTICE, "Rust cron started: %lld   0x%lx %d", rust_helper_cron, (long)cron_proc, i);
        }
}

void uninstallInterceptors(interceptRule *commandTable, int no_of_commands) {
    // Restore original command processors
    for (int j = 0; j < no_of_commands; ++j) {
        struct redisCommand *cmd = lookupCommandByCString(commandTable[j].name);
        if (cmd)
        {
            cmd->proc = (redisCommandProc*)commandTable[j].redis_proc;
            commandTable[j].redis_proc = NULL;
        }
    }
} 

void *rxFindKey(int dbNo, sds key){
    if (dbNo < 0 || dbNo >= server.dbnum)
        serverPanic("findKey: Invalid database");
    redisDb *db = (&server.db[dbNo]);
    if (!db)
        serverPanic("findKey: No REDIS DB!");
    if(!key)
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

void *rxFindSetKey(int dbNo, sds key){
    robj *o = rxFindKey(dbNo, key);
    if(o == NULL || o->type != OBJ_SET)
        return NULL;
    return o;
}

void *rxFindSortedSetKey(int dbNo, sds key){
    robj *o = rxFindKey(dbNo, key);
    if(o == NULL || o->type != OBJ_ZSET)
        return NULL;
    return o;
}

void *rxFindHashKey(int dbNo, sds key){
    robj *o = rxFindKey(dbNo, key);
    if(!o)
        return NULL;
    if(o->type == OBJ_HASH)
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


dictIterator *rxGetDatabaseIterator(int dbNo){
    redisDb *db = (&server.db[dbNo]);
    if (!db)
        serverPanic("findKey: No REDIS DB!");
    return dictGetSafeIterator(db->dict);    
}

long long rxGetDatabaseSize(int dbNo){
    redisDb *db = (&server.db[dbNo]);
    if (!db)
        serverPanic("findKey: No REDIS DB!");
    return dictSize(db->dict);    
}

void *rxScanSetMembers(void *obj, void **siO, char **member, int64_t *member_len)
{
    setTypeIterator **si = (setTypeIterator **)siO;
    if(*si == NULL){
        *si = setTypeInitIterator(obj);
    }
    if(setTypeNext(*si, member, member_len) == -1)
    {
        setTypeReleaseIterator(*si);
        *si = NULL;
        return NULL;
    }
    return *member;
}

rax *rxSetToRax(void *obj)
{
    rax *r = raxNew();
    sds member;
    int64_t member_len;
    setTypeIterator *si = setTypeInitIterator(obj);

    while(setTypeNext(si, &member, &member_len) != -1)
    {
        raxInsert(r, (unsigned char *)member, sdslen(member), NULL, NULL);
    }
    setTypeReleaseIterator(si);
    return r;
}

int rxMatchHasValue(void *oO, sds field, sds pattern, int plen)
{
    robj *o = (robj *)oO;
    if (!o || o->type != OBJ_HASH)
        return MATCH_IS_FALSE;

    if (o->encoding == OBJ_ENCODING_ZIPLIST)
    {
        unsigned char *vstr = NULL;
        POINTER vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        int ret = hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll);
        if (ret < 0)
            return MATCH_IS_FALSE;
        if (pattern == NULL)
            return MATCH_IS_TRUE;
        sds k = sdsnewlen(vstr, vlen);
        sdstolower(k);
        // printf(" %s ", k);
        if (ret == 0 && stringmatchlen(pattern, plen, (const char *)k, vlen, 1))
        {
            sdsfree(k);
            return MATCH_IS_TRUE;
        }
        sdsfree(k);
    }
    else if (o->encoding == OBJ_ENCODING_HT)
    {
        unsigned char *vstr;
        POINTER vlen;
        long long vll;
        hashTypeGetValue(o, field, &vstr, &vlen, &vll);
        // printf(" %s ", vstr);
        if (vstr == NULL)
            return MATCH_IS_FALSE;
        if (stringmatchlen(pattern, plen, (const char *)vstr, vlen, 1))
            return MATCH_IS_TRUE;
    }
    return MATCH_IS_FALSE;
}

int rxHashTypeSet(void *o, sds field, sds value, int flags){
    if( ((robj *)o)->type != rxOBJ_HASH)
        return -1;
    return hashTypeSet((robj *)o, field, value, flags);
}

sds rxGetHashField(void *oO, sds field)
{
    robj *o = (robj *)oO;
    int ret;

    if (o == NULL)
    {
        return sdsempty();
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
                return sdsnewlen(vstr, vlen);
            }
            else
            {
                return sdscatfmt(sdsempty(), "%i", vll);
            }
        }
    }
    else if (o->encoding == OBJ_ENCODING_HT)
    {
        sds value = hashTypeGetFromHashTable(o, field);
        if (value != NULL)
            return sdsnewlen(value, sdslen(value));
    }
    else
    {
        serverPanic("Unknown hash encoding");
    }
    return sdsempty();
}

// Wrappers
short rxGetObjectType(void *o){
    return ((robj *)o)->type;
}

void *rxCreateObject(int type, void *ptr){
    return createObject(type, ptr);
}
void *rxCreateStringObject(const char *ptr, size_t len){
    return createStringObject(ptr, len);
}
void *rxCreateHashObject(){
    return createHashObject();
}
void rxFreeStringObject(void *o){
    freeStringObject(o);
}
void rxFreeHashObject(void *o){
    freeHashObject(o);
}

void *rxGetContainedObject(void *o){
    robj *ro = (robj *)o;
    return ro->ptr;
}

int rxHashTypeGetValue(void *o, sds field, unsigned char **vstr, POINTER *vlen, long long *vll){
    return hashTypeGetValue(o, field, vstr, vlen, vll);    
}

long long rxCreateTimeEvent(long long milliseconds,
                            aeTimeProc *proc, void *clientData,
                            aeEventFinalizerProc *finalizerProc)
{
    return aeCreateTimeEvent(server.el, milliseconds, proc, clientData, finalizerProc);
}

void rxModulePanic(char *msg){
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
int rxAddSetMember(sds key, int dbNo, sds member)
{
    robj *sobj = (robj *)rxFindSetKey(dbNo, key);
    if(sobj == NULL){
        sobj = createSetObject();
        robj *k = createStringObject(key, sdslen(key));
        dbAdd((&server.db[dbNo]), k, sobj);
        freeStringObject(k);
    }
    return setTypeAdd(sobj, member);
}

extern int zsetAdd(robj *zobj, double score, sds ele, int *flags, double *newscore);

double rxAddSortedSetMember(sds key, int dbNo, double score, sds member)
{
    double newScore;
    robj *zobj = (robj *)rxFindSortedSetKey(dbNo, key);
    if(zobj == NULL){
        zobj = createZsetObject();
        robj *k = createStringObject(key, sdslen(key));
        dbAdd((&server.db[dbNo]), k, zobj);
        freeStringObject(k);
    }
    int score_flag = ZADD_INCR;
    zsetAdd(zobj, score, member, &score_flag, &newScore);
    return newScore;
}

extern int dbDelete(redisDb *db, robj *key);
void *rxRemoveKeyRetainValue(int dbNo, sds key)
{
    void *obj = rxFindKey(dbNo, key);
    if(obj == NULL)
        return obj;
    incrRefCount(obj);
    robj *k = createStringObject(key, sdslen(key));
    dbDelete((&server.db[dbNo]), k);
    freeStringObject(k);
    return obj;
}

void *rxRestoreKeyRetainValue(int dbNo, sds key, void *obj)
{
    robj *k = createStringObject(key, sdslen(key));
    dbDelete((&server.db[dbNo]), k);
    if(obj != NULL)
        dbAdd((&server.db[dbNo]), k, obj);
    freeStringObject(k);
    return obj;
}

extern int zsetDel(robj *zobj, sds ele);
unsigned long zsetLength(const robj *zobj);

void *rxCommitKeyRetainValue(int dbNo, sds key, void *old_state)
{
    sds objectIndexkey = sdscatfmt(sdsempty(), "_ox_:%s", key);
    void *new_state = rxFindKey(dbNo, objectIndexkey);
    sdsfree(objectIndexkey);

    if(old_state == NULL)
        return new_state;
        
    rax *new_members = (new_state != NULL)
                           ? rxSetToRax(new_state)
                           : NULL;

    void *si = NULL;
    sds old_member;
    int64_t l;
    while (rxScanSetMembers(old_state, &si, &old_member, &l) != NULL)
    {
        // Any Old state member not in new state is removed from index entry!        
        if(new_members == NULL || raxFind(new_members, (unsigned char *)old_member, sdslen(old_member)) == raxNotFound){
            robj *index_entry = rxFindSortedSetKey(dbNo, old_member);
            sds hkey = sdscatfmt(sdsempty(), "%s\tH", key);
            if (zsetDel(index_entry, hkey) == 0)
            {
                sds skey = sdscatfmt(sdsempty(), "%s\ts", key);
                if (zsetDel(index_entry, skey) == 0)
                {
                };
                sdsfree(skey);
            };
            sdsfree(hkey);
            if (zsetLength(index_entry) == 0)
            {
                robj *k = createStringObject(old_member, sdslen(old_member));
                dbDelete((&server.db[dbNo]), k);
                freeStringObject(k);
            }
        }
    }

    if(new_members != NULL)
        raxFree(new_members);
    freeSetObject(old_state);
    return new_state;
}
void rxHarvestSortedSetMembers(void *obj, rax *bucket)
{
    robj *zobj = (robj*)obj;
    long llen;
    long rangelen;

    if (rxGetObjectType(zobj) != OBJ_ZSET) return;

    /* Sanitize indexes. */
    llen = zsetLength(zobj);

    rangelen = llen;

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        eptr = ziplistIndex(zl,0);

        // serverAssertWithInfo(c,zobj,eptr != NULL);
        sptr = ziplistNext(zl,eptr);

        while (rangelen--) {
            // serverAssertWithInfo(c,zobj,eptr != NULL && sptr != NULL);
            ziplistGet(eptr,&vstr,&vlen,&vlong);

            if (vstr == NULL){
                printf( "L:%lld ", vlong);
            }else{
                HarvestMember(bucket, vstr, vlen, zzlGetScore(sptr));
            }
            zzlNext(zl,&eptr,&sptr);
        }

    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;
        sds ele;

        /* Check if starting point is trivial, before doing log(N) lookup. */
            ln = zsl->header->level[0].forward;

        while(rangelen--) {
            // serverAssertWithInfo(c,zobj,ln != NULL);
            ele = ln->ele;
            HarvestMember(bucket, (unsigned char *)ele, sdslen(ele), ln->score);
            ln = ln->level[0].forward;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
}

// END OF Wrappers
