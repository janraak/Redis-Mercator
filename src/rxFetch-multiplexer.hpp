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

#include "sdsWrapper.h"

#ifdef __cplusplus
}
#endif

static void FreeResultDoubleObject(void *o)
{
    rxMemFree(o);
}

class RxFetchMultiplexer : public Multiplexer
{
public:
    dictIterator *di;
    rxString attribute_value;
    int attribute_value_len;
    rxString attribute;
    int attribute_len;
    rxComparisonProc matchOperation;
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
    rxComparisonProc dup_matchOperation;
    rax *dup_bucket;

    RxFetchMultiplexer(int argc, int dbNo, rxString attribute_value)
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
        this->attribute_value = rxStringDup(attribute_value);
        this->attribute_value_len = strlen(attribute_value);
        this->attribute = rxStringEmpty();
        this->attribute_len = 0;
        this->matchOperation = NULL;
        this->bucket = raxNew();

        this->dup_di = this->di;
        this->dup_matchOperation = this->matchOperation;
        this->dup_bucket = this->bucket;
    }

    RxFetchMultiplexer(int argc, int dbNo, rxString attribute_value, rxString attribute)
        : RxFetchMultiplexer(argc, dbNo, attribute_value)
    {
        this->attribute = rxStringDup(attribute);
        this->attribute_len = strlen(attribute);
    }

    RxFetchMultiplexer(int argc, int dbNo, rxString attribute_value, rxString attribute, rxComparisonProc matchOperation)
        : RxFetchMultiplexer(argc, dbNo, attribute_value, attribute)
    {
        this->matchOperation = matchOperation;
    }

    void checkIntegrity()
    {
        if (this->dup_di != this->di)
        {
            rxServerLog(rxLL_NOTICE, "corrupted di\n");
        }
        if (this->dup_bucket != this->bucket)
        {
            rxServerLog(rxLL_NOTICE, "corrupted bucket\n");
        }
        // if (this->dup_matchOperation != this->matchOperation)
        // {
        //     rxServerLog(rxLL_NOTICE, "corrupted matchOperation\n");
        // }
    }

    ~RxFetchMultiplexer()
    {
        this->checkIntegrity();
        dictReleaseIterator(this->di);
        raxFreeWithCallback(this->bucket, FreeResultDoubleObject);
        rxStringFree(this->attribute);
        rxStringFree(this->attribute_value);
    }

    int Execute()
    {
        dictEntry *de;
        this->checkIntegrity();
        if ((de = dictNext(this->di)) == NULL)
            return -1;
        this->checkIntegrity();

        rxString key = (char *)dictGetKey(de);
        // if (rxStringMatchLen(this->attribute_value, this->attribute_value_len, key, strlen(key), 0))
        // {
        void *value = dictGetVal(de);
        this->touched++;
        if (rxGetObjectType(value) == rxOBJ_ZSET)
        {
            int segments = 0;
            rxString *parts = rxStringSplitLen(key, strlen(key), ":", 1, &segments);
            switch (this->argc)
            {
            case VALUE_ONLY:
                this->strings++;
                if (rxStringMatchLen(this->attribute_value, this->attribute_value_len, parts[1], strlen(parts[1]), 1))
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
                    if (rxStringMatchLen(this->attribute_value, this->attribute_value_len, parts[1], strlen(parts[1]), 1))
                    {
                        this->hits++;
                        rxHarvestSortedSetMembers(value, this->bucket);
                    }
                    else
                        this->misses++;
                }
                else
                {
                    if (rxStringMatchLen(this->attribute_value, this->attribute_value_len, parts[1], strlen(parts[1]), 1) && rxStringMatchLen(this->attribute, this->attribute_len, parts[2], strlen(parts[2]), 1))
                    {
                        this->hits++;
                        rxHarvestSortedSetMembers(value, this->bucket);
                    }
                    else
                        this->misses++;
                }
                break;
            case FIELD_OP_VALUE:
                if (rxStringMatchLen(this->attribute, this->attribute_len, parts[2], strlen(parts[2]), 1))
                {
                    if (this->matchOperation != NULL)
                    {
                        if (this->matchOperation(parts[1], strlen(parts[1]), this->attribute_value) != 0)
                        {
                            this->hits++;
                            rxHarvestSortedSetMembers(value, this->bucket);
                        }
                        else
                            this->misses++;
                    }
                    else
                        rxModulePanic((char *)"f op v requires a valid operator");
                }
                break;
            }
            rxStringFreeSplitRes(parts, segments);
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