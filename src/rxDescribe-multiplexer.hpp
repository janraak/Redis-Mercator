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
#include "sdsWrapper.h"

#ifdef __cplusplus
}
#endif


static void FreeRxDescribeObject(void *o)
{
    rxMemFree(o);
}

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
        raxFreeWithCallback(this->bucket, FreeRxDescribeObject);
        rxStringFree(this->attribute);
        rxStringFree(this->attribute_value);
    }

    /*
        Hits are stashed in a single allocated block of memory.

        struct{
            char *value = &value_bucket;
            char *attribute = &attribute_bucket;

            char value_bucket[*];
            char attribute_bucket[*];
        }
    */
    int Execute()
    {
        dictEntry *de;
        if ((de = dictNext(this->di)) == NULL)
            return -1;

        rxString key = (char *)dictGetKey(de);
        void *value = dictGetVal(de);
        if (rxGetObjectType(value) == rxOBJ_ZSET)
        {
            char *colon1 = strstr((char *)key, ":");
            char *colon2 = strstr(colon1 + 1, ":");
            if (   rxStringMatchLen(this->attribute_value, this->attribute_value_len, colon1 + 1, colon2 - colon1 - 1, 1) 
                && rxStringMatchLen(this->attribute, this->attribute_len, colon2 + 1, strlen( colon2 + 1), 1))
            {

            int sz = (2 * sizeof(char *) + strlen(key) + 1);
            void *mem = rxMemAlloc(sz);
            memset(mem, 0x00, sz);
            char *v = (char *)(mem + 2 * sizeof(char *));
            ((char **)mem)[0] = v;
            strcpy(v, colon1 + 1);
            colon2 = strstr(v, ":");
            ((char **)mem)[1] = colon2 + 1;
            *colon2 = 0x0;
            char *end = strchr(colon2 + 1, 0x00);
            // int segments = 0;
            // rxString *parts = rxStringSplitLen(key, strlen(key), ":", 1, &segments);
                this->hits++;
                // rxString avkey = rxStringFormat("%s:%s", parts[1], parts[2]);
                // raxInsert(this->bucket, (UCHAR *)avkey, strlen(avkey), NULL, NULL);
                // rxStringFree(avkey);
                raxInsert(this->bucket, (UCHAR *)v, end - v, mem, NULL);
            }
            else
                this->misses++;
            // rxStringFreeSplitRes(parts, segments);
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
            char *v = ((char **)resultsIterator.data)[0];
            char *a = ((char **)resultsIterator.data)[1];
            RedisModule_ReplyWithStringBuffer(ctx, v, strlen(v));
            RedisModule_ReplyWithStringBuffer(ctx, a, strlen(a));
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