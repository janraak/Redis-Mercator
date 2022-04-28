#ifndef __RXFETCH_DUPLEXER_H__
#define __RXFETCH_DUPLEXER_H__

#define VALUE_ONLY 2
#define FIELD_AND_VALUE_ONLY 3
#define FIELD_OP_VALUE 4

#include "command-duplexer.hpp"

typedef int comparisonProc(char *l, int ll, char *r);

#ifdef __cplusplus
extern "C"
{
#endif

#include "util.h"
#include "zmalloc.h"

#ifdef __cplusplus
}
#endif

static void FreeResultDoubleObject(void *o)
{
    zfree(o);
}

class RxFetchDuplexer : public Duplexer
{
public:
    dictIterator *di;
    sds attribute_value;
    int attribute_value_len;
    sds attribute;
    int attribute_len;
    comparisonProc *matchOperation;
    long long hits;
    long long misses;
    long long ignored;
    rax *bucket;
    bool complex_mode;

    RxFetchDuplexer(int argc, int dbNo, sds attribute_value, sds attribute)
        : Duplexer()
    {
        this->argc = argc;
        this->di = rxGetDatabaseIterator(dbNo);
        this->attribute_value = sdsdup(attribute_value);
        this->attribute_value_len = sdslen(attribute_value);
        this->attribute = sdsdup(attribute);
        this->attribute_len = sdslen(attribute);
        this->matchOperation = NULL;
        this->bucket = raxNew();
    }

    RxFetchDuplexer(int argc, int dbNo, sds attribute_value, sds attribute, comparisonProc *matchOperation)
        : RxFetchDuplexer(argc, dbNo, attribute_value, attribute)
    {
        this->matchOperation = matchOperation;
    }

    ~RxFetchDuplexer()
    {
        dictReleaseIterator(this->di);
        raxFreeWithCallback(this->bucket, FreeResultDoubleObject);
        sdsfree(this->attribute);
        sdsfree(this->attribute_value);
    }

    int Execute()
    {
        dictEntry *de;
        if ((de = dictNext(this->di)) == NULL)
            return -1;

        sds key = (char *)dictGetKey(de);
        // if (stringmatchlen(this->attribute_value, this->attribute_value_len, key, sdslen(key), 0))
        // {
        void *value = dictGetVal(de);
        if (rxGetObjectType(value) == rxOBJ_ZSET)
        {
            int segments = 0;
            sds *parts = sdssplitlen(key, sdslen(key), ":", 1, &segments);
            switch (this->argc)
            {
            case VALUE_ONLY:
                // if (stringmatchlen(this->attribute_value, this->attribute_value_len, parts[1], sdslen(parts[1]), 1))
                // {
                //     this->hits++;
                //     rxHarvestSortedSetMembers(value, this->bucket);
                // }
                // else
                //     this->misses++;
                // break;
            case FIELD_AND_VALUE_ONLY:
                if (stringmatchlen(this->attribute_value, this->attribute_value_len, parts[1], sdslen(parts[1]), 1) && stringmatchlen(this->attribute, this->attribute_len, parts[2], sdslen(parts[2]), 1))
                {
                    this->hits++;
                    rxHarvestSortedSetMembers(value, this->bucket);
                }
                else
                    this->misses++;
                break;
            case FIELD_OP_VALUE:
                if (this->matchOperation != NULL)
                {
                    if (this->matchOperation(parts[1], sdslen(parts[1]), this->attribute_value) != 0)
                    {
                        this->hits++;
                        rxHarvestSortedSetMembers(value, this->bucket);
                    }
                    else
                        this->misses++;
                }
                else
                    rxModulePanic((char *)"f op v requires a valid operator");
                break;
            }
            sdsfreesplitres(parts, segments);
        }
        else
            this->ignored++;
        return 1;
    }

    int Write(RedisModuleCtx *ctx)
    {
        RedisModule_ReplyWithArray(ctx, raxSize(this->bucket));

        raxIterator resultsIterator;
        raxStart(&resultsIterator, this->bucket);
        raxSeek(&resultsIterator, "^", NULL, 0);
        while (raxNext(&resultsIterator))
        {
            RedisModule_ReplyWithArray(ctx, 2);
            RedisModule_ReplyWithStringBuffer(ctx, (char *)resultsIterator.key, resultsIterator.key_len);
            RedisModule_ReplyWithDouble(ctx, *(double *)resultsIterator.data);
        }
        raxStop(&resultsIterator);
        return 1;
    }

    int Done()
    {
        return 1;
    }
};
#endif