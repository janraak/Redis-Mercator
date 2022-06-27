#ifndef __SIMPLEQQUEUE_H__
#define __SIMPLEQQUEUE_H__
#include <pthread.h>

#include "adlist.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef WRAPPER 
// #include "../deps/concurrentqueue/concurrentqueue.h"
   #include "../deps/concurrentqueue/blockingconcurrentqueue.h"

class SimpleQueue
{
public:
    moodycamel::ConcurrentQueue<sds *> fifo;

    std::atomic<int> enqueue_fifo_tally;
    std::atomic<int> dequeue_fifo_tally;
    std::atomic<bool> must_stop;
    std::atomic<int> thread_count;

    SimpleQueue()
    {
        this->dequeue_fifo_tally.store(0);
        this->enqueue_fifo_tally.store(0);
        this->must_stop.store(false);
        this->thread_count.store(0);
    }

    SimpleQueue(void *handler, int nThreads)
        : SimpleQueue()
    {
        pthread_t fifo_thread;
        while (nThreads > 0)
        {
            if (pthread_create(&fifo_thread, NULL, (void* (*)(void*))handler, this))
            {
                fprintf(stderr, "FATAL: Failed to start indexer thread\n");
                exit(1);
            }
            nThreads--;
        }
    }

    void Enqueue(sds *o)
    {
        this->fifo.enqueue((sds *)o);
        this->enqueue_fifo_tally++;
    }

    sds *Dequeue()
    {
        if (this->fifo.size_approx() == 0)
            return NULL;
        sds *item;
        bool found = this->fifo.try_dequeue(item);
        if (found)
        {
            this->dequeue_fifo_tally++;
            return item;
        }
        return NULL;
    }

    int CanDequeueSimpleQueue()
    {
        return this->QueueLength() > 0;
    }

    unsigned long QueueLength()
    {
        return this->enqueue_fifo_tally - this->dequeue_fifo_tally;
    }

    void Started()
    {
        this->thread_count++;
    }

    void Stopped()
    {
        this->thread_count--;
    }

    void Release()
    {
        this->must_stop.store(true);
    }
};

#ifdef __cplusplus
extern "C" {
#endif
void enqueueSimpleQueue(void *qO, void *o)
{
    SimpleQueue *q = (SimpleQueue *) qO;
    q->Enqueue((sds *)o);
}

void *dequeueSimpleQueue(void *qO)
{
    SimpleQueue *q = (SimpleQueue *) qO;
    return q->Dequeue();
}

int canDequeueSimpleQueue(SimpleQueue *qO)
{
    SimpleQueue *q = (SimpleQueue *) qO;
    return q->Dequeue() ? 1 : 0;
}
#ifdef __cplusplus
}
#endif
#else
typedef void SimpleQueue;

#ifdef __cplusplus
extern "C" {
#endif
extern void enqueueSimpleQueue(SimpleQueue *q, void *o);
extern void *dequeueSimpleQueue(SimpleQueue *q);
    int canDequeueSimpleQueue(SimpleQueue *qO);
#ifdef __cplusplus
}
#endif
#endif

#endif