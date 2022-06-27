#ifndef __GRAPHSTACK_H__
#define __GRAPHSTACK_H__

#ifdef __cplusplus
extern "C" {
#endif
#include <stddef.h>
#include "adlist.h"
#ifdef __cplusplus
}
#endif

template <typename T>
class GraphStack
{
public:
    GraphStack()
    {
        this->sequencer = listCreate();
    };

    GraphStack(list *base)
    {
        this->sequencer = listDup(base);
    };


    void Use(list *base)
    {
        if(this->sequencer != NULL)
            listRelease(this->sequencer);
        this->sequencer = base;
    };

    ~GraphStack()
    {
        listRelease(this->sequencer);
    };

    GraphStack<T> *Copy()
    {
        return new GraphStack<T>(this->sequencer);
    };

    list *sequencer = NULL;

    bool HasEntries()
    {
        return listLength(this->sequencer) > 0;
    }

    int Size()
    {
        return listLength(this->sequencer);
    }

    void Push(T *t)
    {
        listAddNodeHead(this->sequencer, t);
    }

    void Add(T *t)
    {
        listAddNodeTail(this->sequencer, t);
    }

    void Remove(T *t)
    {
        listNode *ln = listSearchKey(this->sequencer, t);
        if (ln)
            listDelNode(this->sequencer, ln);
    }

    T *Pop()
    {
        listNode *node = listIndex(this->sequencer, 0);
        if (node)
        {
            T *t = (T *)node->value;
            listDelNode(this->sequencer, node);
            return t;
        }
        return NULL;
    }

    void Enqueue(T *t)
    {
        listAddNodeTail(this->sequencer, t);
    }

    T *Dequeue()
    {
        listNode *node = listIndex(this->sequencer, 0);
        if (node)
        {
            T *t = (T *)node->value;
            listDelNode(this->sequencer, node);
            return t;
        }
        return NULL;
    }

    T *Pop_Last()
    {
        listNode *node = listIndex(this->sequencer, this->sequencer->len - 1);
        if (node)
        {
            T *t = (T *)node->value;
            listDelNode(this->sequencer, node);
            return t;
        }
        return NULL;
    }
    
    T *Peek()
    {
        listNode *node = listIndex(this->sequencer, 0);
        if (node)
        {
            T *t = (T *)node->value;
            return t;
        }
        return NULL;
    }

    T *Last()
    {
        listNode *node = listIndex(this->sequencer, this->sequencer->len - 1);
        if (node)
        {
            T *t = (T *)node->value;
            return t;
        }
        return NULL;
    }
    listIter *sequencer_iter = NULL;

    void StartHead()
    {
        sequencer_iter = listGetIterator(this->sequencer, AL_START_HEAD);
    }

    void StartTail()
    {
        sequencer_iter = listGetIterator(this->sequencer, AL_START_TAIL);
    }

    T *Next()
    {
        auto *node = listNext(this->sequencer_iter);
        if (node){
            return (T *)node->value;
        }
        return NULL;
    }

    void Stop()
    {
        listReleaseIterator(sequencer_iter);
        sequencer_iter = NULL;
    }

    void Join(GraphStack<T> *adjacent){
        if(adjacent && adjacent->sequencer)
            listJoin(this->sequencer, listDup(adjacent->sequencer));
    }
};

#endif