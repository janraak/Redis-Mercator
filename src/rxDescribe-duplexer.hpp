#ifndef __RXDESCRIBE_DUPLEXER_H__
#define __RXDESCRIBE_DUPLEXER_H__

#define VALUE_ONLY 2
#define FIELD_AND_VALUE_ONLY 3
#define FIELD_OP_VALUE 4

#include "command-duplexer.hpp"

typedef int comparisonProc(char *l, int ll, char *r);

#ifdef __cplusplus
extern "C"
{
#endif

#include "string.h"
#include "util.h"
#include "zmalloc.h"

#ifdef __cplusplus
}
#endif

class RxDescribeDuplexer : public Duplexer
{
public:
    dictIterator *di;
    sds attribute_value;
    int attribute_value_len;
    sds attribute;
    int attribute_len;
    rax *bucket;

    RxDescribeDuplexer(int dbNo, sds attribute_value, sds attribute)
        : Duplexer()
    {
        this->di = rxGetDatabaseIterator(dbNo);
        this->attribute_value = sdsdup(attribute_value);
        this->attribute_value_len = sdslen(attribute_value);
        this->attribute = sdsdup(attribute);
        this->attribute_len = sdslen(attribute);
        this->bucket = raxNew();
    }

    ~RxDescribeDuplexer()
    {
        dictReleaseIterator(this->di);
        raxFree(this->bucket);
        sdsfree(this->attribute);
        sdsfree(this->attribute_value);
    }

    int Execute()
    {
        dictEntry *de;
        if ((de = dictNext(this->di)) == NULL)
            return -1;

        sds key = (char *)dictGetKey(de);
        void *value = dictGetVal(de);
        if (rxGetObjectType(value) == rxOBJ_ZSET)
        {
            int segments = 0;
            sds *parts = sdssplitlen(key, sdslen(key), ":", 1, &segments);
            if (stringmatchlen(this->attribute_value, this->attribute_value_len, parts[1], sdslen(parts[1]), 1) && stringmatchlen(this->attribute, this->attribute_len, parts[2], sdslen(parts[2]), 1))
            {
                this->hits++;
                sds avkey = sdscatfmt(sdsempty(), "%s:%s", parts[1], parts[2]);
                raxInsert(this->bucket, (UCHAR *)avkey, sdslen(avkey), NULL, NULL);
                sdsfree(avkey);
            }
            else
                this->misses++;
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