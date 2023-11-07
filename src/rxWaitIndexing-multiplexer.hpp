#ifndef __RXDESCRIBE_multiplexer_H__
#define __RXDESCRIBE_multiplexer_H__

#define VALUE_ONLY 2
#define FIELD_AND_VALUE_ONLY 3
#define FIELD_OP_VALUE 4

#include "command-multiplexer.hpp"
#include "simpleQueue.hpp"
#include "rule.hpp"

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

class RxWaitIndexingMultiplexer : public Multiplexer
{
public:
    SimpleQueue *queue;
    dictIterator **reindex_iterator;

    RxWaitIndexingMultiplexer(SimpleQueue *queue, dictIterator **reindex_iterator)
        : Multiplexer()
    {
        this->queue = queue;
        this->reindex_iterator = reindex_iterator;
    }


    long long Timeout(){
        return 0;
    }

    int Execute()
    {
        if(this->queue == NULL)
            return -1;
            
        if (this->reindex_iterator == NULL)
            return -1;

        if(this->queue->QueueLength() == 0 && BusinessRule::QueuedTouchesCount() == 0)
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