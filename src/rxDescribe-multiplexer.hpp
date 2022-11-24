#ifndef __RXDESCRIBE_multiplexer_H__
#define __RXDESCRIBE_multiplexer_H__

#define VALUE_ONLY 2
#define FIELD_AND_VALUE_ONLY 3
#define FIELD_OP_VALUE 4

#include "command-multiplexer.hpp"

typedef int comparisonProc(char *l, int ll, char *r);

#ifdef __cplusplus
extern "C"
{
#endif

#include "string.h"
#include "zmalloc.h"

#ifdef __cplusplus
}
#endif

class RxDescribeMultiplexer : public Multiplexer
{
public:
    dictIterator *di;
    rxString attribute_value;
    int attribute_value_len;
    rxString attribute;
    int attribute_len;
    rax *bucket;

    RxDescribeMultiplexer(int dbNo, rxString attribute_value, rxString attribute)
        : Multiplexer()
    {
        this->di = rxGetDatabaseIterator(dbNo);
        this->attribute_value = rxStringDup(attribute_value);
        this->attribute_value_len = strlen(attribute_value);
        this->attribute = rxStringDup(attribute);
        this->attribute_len = strlen(attribute);
        this->bucket = raxNew();
    }

    ~RxDescribeMultiplexer()
    {
        dictReleaseIterator(this->di);
        raxFree(this->bucket);
        rxStringFree(this->attribute);
        rxStringFree(this->attribute_value);
    }

    int Execute()
    {
        dictEntry *de;
        if ((de = dictNext(this->di)) == NULL)
            return -1;

        rxString key = (char *)dictGetKey(de);
        void *value = dictGetVal(de);
        if (rxGetObjectType(value) == rxOBJ_ZSET)
        {
            int segments = 0;
            rxString *parts = rxStringSplitLen(key, strlen(key), ":", 1, &segments);
            if (rxStringMatchLen(this->attribute_value, this->attribute_value_len, parts[1], strlen(parts[1]), 1) && rxStringMatchLen(this->attribute, this->attribute_len, parts[2], strlen(parts[2]), 1))
            {
                this->hits++;
                rxString avkey = rxStringFormat("%s:%s", parts[1], parts[2]);
                raxInsert(this->bucket, (UCHAR *)avkey, strlen(avkey), NULL, NULL);
                rxStringFree(avkey);
            }
            else
                this->misses++;
            rxStringFreeSplitRes(parts, segments);
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
            char *colon = strchr((char *)resultsIterator.key, ':');
            RedisModule_ReplyWithArray(ctx, 2);
            RedisModule_ReplyWithStringBuffer(ctx, (char *)resultsIterator.key, colon - (char *)resultsIterator.key);
            RedisModule_ReplyWithStringBuffer(ctx, (char *)colon + 1, strlen(colon + 1) - 1);
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