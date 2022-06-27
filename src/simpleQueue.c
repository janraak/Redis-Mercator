#include "simpleQueue.h"
#include "zmalloc.h"
#include <string.h>
#define UNUSED(x) (void)(x)

void enqueueSimpleQueue(SimpleQueue *q, void *o)
{
  pthread_mutex_lock(&q->fifo_mutex);
  q->enqueue_fifo_tally++;
  listAddNodeTail(q->fifo, o);
  pthread_mutex_unlock(&q->fifo_mutex);
}

void *dequeueSimpleQueue(SimpleQueue *q)
{
  pthread_mutex_lock(&q->fifo_mutex);
  if(q->fifo->len == 0){
    pthread_mutex_unlock(&q->fifo_mutex);
    return NULL;
  }
  q->dequeue_fifo_tally++;
  listNode *head = listIndex(q->fifo, 0);
  if (head == NULL)
  {
    pthread_mutex_unlock(&q->fifo_mutex);
    return NULL;
  }
  void *o = head->value;
  listDelNode(q->fifo, head);
  pthread_mutex_unlock(&q->fifo_mutex);
  return o;
}

int canDequeueSimpleQueue(SimpleQueue *q)
{
  return q && q->fifo->len > 0;
}

unsigned long dequeueSimpleQueueLength(SimpleQueue *q)
{
  return q->fifo->len;
}

SimpleQueue *newSimpleQueue()
{
  SimpleQueue *q = zmalloc(sizeof(SimpleQueue));
  memset(q, 0x00, sizeof(SimpleQueue));
  q->fifo = listCreate();
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
  }
  return q;
}

void releaseSimpleQueue(SimpleQueue *q)
{
  if (q->fifo_thread)
  {
    pthread_cancel(q->fifo_thread);
    pthread_join(q->fifo_thread, NULL);
  }
  listRelease(q->fifo);
  zfree(q);
}
