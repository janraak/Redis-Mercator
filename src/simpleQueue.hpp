#ifndef __SIMPLEQQUEUE_HPP__
#define __SIMPLEQQUEUE_HPP__
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
    const char *name;
    moodycamel::BlockingConcurrentQueue<void *> fifo;

    std::atomic<int> enqueue_fifo_tally;
    std::atomic<int> dequeue_fifo_tally;
    std::atomic<bool> must_stop;
    std::atomic<int> thread_count;
    SimpleQueue *response_queue;
    SimpleQueue(const char *name)
    {
        this->dequeue_fifo_tally.store(0);
        this->enqueue_fifo_tally.store(0);
        this->must_stop.store(false);
        this->thread_count.store(0);
        this->response_queue = NULL;
        this->name = name;
    }
    SimpleQueue(const char *name, SimpleQueue *response_queue)
    :SimpleQueue(name)
    {
        this->response_queue = response_queue;
    }

    SimpleQueue(const char *name, void *handler, int nThreads)
        : SimpleQueue(name, handler, nThreads, NULL)
    {
    }

    SimpleQueue(const char *name, void *handler, int nThreads, SimpleQueue *response_queue)
        : SimpleQueue(name, response_queue)
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

    void Enqueue(void *o)
    {
        this->fifo.enqueue((void *)o);
        this->enqueue_fifo_tally++;
    }

    void *Dequeue()
    {
        // if (this->fifo.size_approx() == 0)
        //     return NULL;
        void *item;
        bool found = this->fifo.try_dequeue(item);
        if (found)
        {
            this->dequeue_fifo_tally++;
            return item;
        }
        return NULL;
    }

    int CanDequeue()
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
const char *getQueueName(void *qO)
{
    SimpleQueue *q = (SimpleQueue *) qO;
    return q->name;
}

void enqueueSimpleQueue(void *qO, void *o)
{
    SimpleQueue *q = (SimpleQueue *) qO;
    q->Enqueue((void *)o);
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
extern const char *getQueueName(void *qO);
extern void enqueueSimpleQueue(SimpleQueue *q, void *o);
extern void *dequeueSimpleQueue(SimpleQueue *q);
    int canDequeueSimpleQueue(SimpleQueue *qO);
#ifdef __cplusplus
}
#endif
#endif

#endif