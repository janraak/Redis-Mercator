#ifndef __RXFETCH_multiplexer_H__
#define __RXFETCH_multiplexer_H__

#define VALUE_ONLY 2
#define FIELD_AND_VALUE_ONLY 3
#define FIELD_OP_VALUE 4

#include "command-multiplexer.hpp"


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

class RxFetchMultiplexer : public Multiplexer
{
public:
    dictIterator *di;
    sds attribute_value;
    int attribute_value_len;
    sds attribute;
    int attribute_len;
    rxComparisonProc *matchOperation;
    long long touched;
    long long strings;
    long long hashes;
    long long hits;
    long long misses;
    long long ignored;
    rax *bucket;
    bool complex_mode;
    long long dbsize;
    int dbNo;

    dictIterator *dup_di;
    rxComparisonProc *dup_matchOperation;
    rax *dup_bucket;

    RxFetchMultiplexer(int argc, int dbNo, sds attribute_value)
        : Multiplexer()
    {
        this->touched = 0;
        this->hits = 0;
        this->misses = 0;
        this->ignored = 0;
        this->strings = 0;
        this->hashes = 0;
        this->dbNo = dbNo;
        this->argc = argc;
        this->di = rxGetDatabaseIterator(dbNo);
        this->attribute_value = sdsdup(attribute_value);
        this->attribute_value_len = sdslen(attribute_value);
        this->attribute = sdsempty();
        this->attribute_len = 0;
        this->matchOperation = NULL;
        this->bucket = raxNew();

        this->dup_di = this->di;
        this->dup_matchOperation = this->matchOperation;
        this->dup_bucket = this->bucket;
    }

    RxFetchMultiplexer(int argc, int dbNo, sds attribute_value, sds attribute)
        : RxFetchMultiplexer(argc, dbNo, attribute_value)
    {
        this->attribute = sdsdup(attribute);
        this->attribute_len = sdslen(attribute);
    }

    RxFetchMultiplexer(int argc, int dbNo, sds attribute_value, sds attribute, rxComparisonProc *matchOperation)
        : RxFetchMultiplexer(argc, dbNo, attribute_value, attribute)
    {
        this->matchOperation = matchOperation;
    }

    void checkIntegrity()
    {
        if (this->dup_di != this->di)
        {
            printf("corrupted di\n");
        }
        if (this->dup_bucket != this->bucket)
        {
            printf("corrupted bucket\n");
        }
        if (this->dup_matchOperation != this->matchOperation)
        {
            printf("corrupted matchOperation\n");
        }
    }

    ~RxFetchMultiplexer()
    {
        this->checkIntegrity();
        dictReleaseIterator(this->di);
        raxFreeWithCallback(this->bucket, FreeResultDoubleObject);
        sdsfree(this->attribute);
        sdsfree(this->attribute_value);
    }

    int Execute()
    {
        dictEntry *de;
        this->checkIntegrity();
        if ((de = dictNext(this->di)) == NULL)
            return -1;
        this->checkIntegrity();

        sds key = (char *)dictGetKey(de);
        // if (stringmatchlen(this->attribute_value, this->attribute_value_len, key, sdslen(key), 0))
        // {
        void *value = dictGetVal(de);
        this->touched++;
        if (rxGetObjectType(value) == rxOBJ_ZSET)
        {
            int segments = 0;
            sds *parts = sdssplitlen(key, sdslen(key), ":", 1, &segments);
            switch (this->argc)
            {
            case VALUE_ONLY:
                this->strings++;
                if (stringmatchlen(this->attribute_value, this->attribute_value_len, parts[1], sdslen(parts[1]), 1))
                {
                    this->hits++;
                    rxHarvestSortedSetMembers(value, this->bucket);
                }
                else
                    this->misses++;
                break;
            case FIELD_AND_VALUE_ONLY:
                this->hashes++;
                if (this->attribute_len == 0)
                {
                    if (stringmatchlen(this->attribute_value, this->attribute_value_len, parts[1], sdslen(parts[1]), 1))
                    {
                        this->hits++;
                        rxHarvestSortedSetMembers(value, this->bucket);
                    }
                    else
                        this->misses++;
                }
                else
                {
                    if (stringmatchlen(this->attribute_value, this->attribute_value_len, parts[1], sdslen(parts[1]), 1) && stringmatchlen(this->attribute, this->attribute_len, parts[2], sdslen(parts[2]), 1))
                    {
                        this->hits++;
                        rxHarvestSortedSetMembers(value, this->bucket);
                    }
                    else
                        this->misses++;
                }
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
        this->checkIntegrity();
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