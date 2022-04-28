#ifndef __SIMPLEQQUEUE_H__
#define __SIMPLEQQUEUE_H__
#include <pthread.h>

#include "adlist.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    list *fifo;

    pthread_t fifo_thread;
    pthread_mutex_t fifo_mutex;

    int enqueue_fifo_tally;
    int dequeue_fifo_tally;
} SimpleQueue;

SimpleQueue *newSimpleQueue();
SimpleQueue *newSimpleQueueAsync(void *handler, int nThreads);
void releaseSimpleQueue(SimpleQueue *q);

void enqueueSimpleQueue(SimpleQueue *q, void *o);

int canDequeueSimpleQueue(SimpleQueue *q);
void *dequeueSimpleQueue(SimpleQueue *q);
unsigned long dequeueSimpleQueueLength(SimpleQueue *q);
#endif