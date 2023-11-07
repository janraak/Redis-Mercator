
#ifndef __SIMPLEQQUEUE_HPP__
#define __SIMPLEQQUEUE_HPP__
#include <pthread.h>

#include "adlist.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
extern "C"
{
#endif
extern int pthread_kill (pthread_t __threadid, int __signo) __THROW;

#include "rxSuiteHelpers.h"
#ifdef __cplusplus
}
#endif

#ifndef WRAPPER
// #include "../deps/concurrentqueue/concurrentqueue.h"
#include "../deps/concurrentqueue/blockingconcurrentqueue.h"

class SimpleQueue
{
public:
    const char *name;
    moodycamel::BlockingConcurrentQueue<void *> fifo;
    pthread_t *fifo_thread;

    std::atomic<int> enqueue_fifo_tally;
    std::atomic<int> dequeue_fifo_tally;
    std::atomic<bool> must_stop;
    int thread_count_requested;
    std::atomic<int> thread_count;
    SimpleQueue *response_queue;
    SimpleQueue(const char *name)
    {
        this->dequeue_fifo_tally.store(0);
        this->enqueue_fifo_tally.store(0);
        this->must_stop.store(false);
        this->thread_count_requested = 0;
        this->thread_count.store(0);
        this->response_queue = NULL;
        this->name = name;
        this->fifo_thread = NULL;
    }
    SimpleQueue(const char *name, SimpleQueue *response_queue)
        : SimpleQueue(name)
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
        this->thread_count_requested = nThreads;
        if (nThreads > 0)
        {
            this->fifo_thread = (pthread_t*)rxMemAlloc(this->thread_count_requested * sizeof(pthread_t));
            while (nThreads > 0)
            {
                nThreads--;
                if (pthread_create(&this->fifo_thread[nThreads], NULL, (void *(*)(void *))handler, this))
                {
                    fprintf(stderr, "FATAL: Failed to start indexer thread\n");
                    exit(1);
                }
                pthread_setname_np(this->fifo_thread[nThreads], name);
                rxServerLog(rxLL_NOTICE, "Read thread started %p for %s", this->fifo_thread[nThreads], name);
            }
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
        this->thread_count.store(this->thread_count.load() + 1);
    }

    void Stopped()
    {
        this->thread_count.store(thread_count.load() - 1);
    }

    bool HasFinished()
    {
        return this->thread_count.load() <= 0;
    }

    void Release()
    {
        void *status = NULL;
        this->must_stop.store(true);
        if (this->thread_count_requested > 0)
        {
            for (; this->thread_count_requested > 0; this->thread_count_requested--)
            {
                pthread_join(this->fifo_thread[this->thread_count_requested - 1], &status);
                pthread_kill(this->fifo_thread[this->thread_count_requested - 1], 1);
            }
            rxMemFree(this->fifo_thread);
        }
    }

    static SimpleQueue *Release(SimpleQueue *q)
    {
        if(q == NULL)
            return NULL;
        q->Release();
        delete q;
        return NULL;
    }
};

#ifdef __cplusplus
extern "C"
{
#endif
    const char *getQueueName(void *qO)
    {
        SimpleQueue *q = (SimpleQueue *)qO;
        return q->name;
    }

    void enqueueSimpleQueue(void *qO, void *o)
    {
        SimpleQueue *q = (SimpleQueue *)qO;
        q->Enqueue((void *)o);
    }

    void *dequeueSimpleQueue(void *qO)
    {
        SimpleQueue *q = (SimpleQueue *)qO;
        return q->Dequeue();
    }

    int canDequeueSimpleQueue(SimpleQueue *qO)
    {
        SimpleQueue *q = (SimpleQueue *)qO;
        return q->Dequeue() ? 1 : 0; 
    }

    int lengthSimpleQueue(SimpleQueue *qO)
    {
        SimpleQueue *q = (SimpleQueue *)qO;
        return q->QueueLength(); 
    }
#ifdef __cplusplus
}
#endif
#else
typedef void SimpleQueue;

#ifdef __cplusplus
extern "C"
{
#endif
    extern const char *getQueueName(void *qO);
    extern void enqueueSimpleQueue(SimpleQueue *q, void *o);
    extern void *dequeueSimpleQueue(SimpleQueue *q);
    int canDequeueSimpleQueue(SimpleQueue *qO);
    int lengthSimpleQueue(SimpleQueue *qO);
#ifdef __cplusplus
}
#endif
#endif

#endif