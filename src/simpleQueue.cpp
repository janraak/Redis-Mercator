#include "simpleQueue.hpp"
#include "zmalloc.h"
#include <string.h>
#define UNUSED(x) (void)(x)

void enqueueSimpleQueue(SimpleQueue *q, void *o)
{
  q->fifo.enqueue((sds *)o);
  q->enqueue_fifo_tally++;
}

void *dequeueSimpleQueue(SimpleQueue *q)
{
    sds * item;
    bool found = q->fifo.try_dequeue(item);
    if (found)
    {
      q->dequeue_fifo_tally++;
      return item;
    }
    return NULL;
}

int canDequeueSimpleQueue(SimpleQueue *q)
{
  return 1;
}

unsigned long dequeueSimpleQueueLength(SimpleQueue *q)
{
  return q->enqueue_fifo_tally - q->dequeue_fifo_tally;
}

SimpleQueue *newSimpleQueue()
{
  SimpleQueue *q = new SimpleQueue();
  return q;
}

SimpleQueue *newSimpleQueueAsync(void *handler, int nThreads)
{
  UNUSED(nThreads);
  
  SimpleQueue *q = newSimpleQueue();
  // if (nThreads <= 0)
    // nThreads = 1;
  // for (; nThreads > 0; nThreads--)
  {
    if (pthread_create(&q->fifo_thread, NULL, handler, q))
    {
      fprintf(stderr, "FATAL: Failed to start indexer thread\n");
      exit(1);
    }
    q->thread_count++;
  }
  return q;
}

void releaseSimpleQueue(SimpleQueue *q)
{
  q->must_stop = true;
  // while (q->thread_count > 0)
  // {
  //   pthread_cancel(q->fifo_thread);
  //   pthread_join(q->fifo_thread, NULL);
  // }
  delete q;
}
