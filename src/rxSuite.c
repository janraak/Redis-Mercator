#include "rxSuite.h"
#include "server.h"

uint64_t tokenSdsHash(const void *key)
{
    return dictGenHashFunction((unsigned char *)key, sdslen((char *)key));
}

int tokenSdsKeyCompare(void *privdata, const void *key1,
                      const void *key2)
{
    int l1, l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2)
        return 0;
    return memcmp(key1, key2, l1) == 0;
}

void tokenSdsDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    sdsfree(val);
}

void *tokenSdsDup(void *privdata, const void *key)
{
    DICT_NOTUSED(privdata);
    return sdsdup((char *const)key);
}

/* Db->dict, keys are sds strings, vals are Redis objects. */
dictType tokenDictTypeDefinition = {
    tokenSdsHash,       /* hash function */
    tokenSdsDup,        /* key dup */
    NULL,              /* val dup */
    tokenSdsKeyCompare, /* key compare */
    tokenSdsDestructor, /* key destructor */
    NULL               /* val destructor */
};

void initRxSuite()
{
    redisDb *db = &server.db[0];
    dict *d = db->dict;
    if (!d->privdata)
    {
        rxSuiteShared *shared = zmalloc(sizeof(rxSuiteShared));
        shared->OperationMap = NULL;
        shared->KeysetCache = NULL;
        shared->parserClaimCount = 0;
        shared->tokenDictType = &tokenDictTypeDefinition;
        d->privdata = shared;
    }
}

rxSuiteShared * getRxSuite(){
    redisDb *db = &server.db[0];
    dict *d = db->dict;
    if (d->privdata == NULL)
        initRxSuite();
    return d->privdata;
}

void finalizeRxSuite(){
    redisDb *db = &server.db[0];
    dict *d = db->dict;
    if (!d->privdata)
    {
        zfree(d->privdata);
    }
   
}


int sdscharcount(char *s, char c)
{
    int tally = 0;
    char *t = s;
    while (*t)
    {
        if (*t == toupper(c))
            ++tally;
        ++t;
    }
    return tally;
}
