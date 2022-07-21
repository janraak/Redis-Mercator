#ifndef __RXDESCRIBE_DUPLEXER_H__
#define __RXDESCRIBE_DUPLEXER_H__

#define VALUE_ONLY 2
#define FIELD_AND_VALUE_ONLY 3
#define FIELD_OP_VALUE 4

#include "command-duplexer.hpp"
#include "simpleQueue.hpp"

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

class RxWaitIndexingDuplexer : public Duplexer
{
public:
    SimpleQueue *queue;
    dictIterator **reindex_iterator;

    RxWaitIndexingDuplexer(SimpleQueue *queue, dictIterator **reindex_iterator)
        : Duplexer()
    {
        this->queue = queue;
        this->reindex_iterator = reindex_iterator;
    }


    long long Timeout(){
        return 0;
    }

    int Execute()
    {
        if (this->reindex_iterator == NULL)
            return -1;

        if(this->queue->QueueLength() == 0)
            return -1;
        return 1;
    }

    int Write(RedisModuleCtx *ctx)
    {
        RedisModule_ReplyWithSimpleString(ctx, "OK");
        return 1;
    }

    int Done()
    {
        return 1;
    }
};
#endif