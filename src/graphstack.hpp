#ifndef __GRAPHSTACK_H__
#define __GRAPHSTACK_H__

#ifdef __cplusplus
extern "C"
{
#endif
#include "../../src/adlist.h"
#include <stddef.h>
#ifdef __cplusplus
}
#endif
typedef int EnqueueInOrder(void *left, void *right, void *parm); // Comparator prototype for priority enqueue

template <typename T>
class GraphStack
{
public:
    GraphStack()
    {
        this->Init();
    };

    GraphStack(list *base)
    {
        this->sequencer = base;
    };

    void Init()
    {
        this->sequencer = listCreate();
    };

    void Use(list *base)
    {
        if(this->sequencer != NULL)
            listRelease(this->sequencer);
        this->sequencer = base;
    };

    virtual void PopAndDeleteValue();

    ~GraphStack()
    {
        // listRelease(this->sequencer);// TODO FIX!
        while (this->HasEntries())
        {
            this->PopAndDeleteValue();
        }
        this->sequencer = NULL;
    };

    GraphStack<T> *Copy()
    {
        return new GraphStack<T>(this->sequencer);
    };

    list *sequencer = NULL;

    bool HasEntries()
    {
        if(this->sequencer == NULL)
            return false;
        return listLength(this->sequencer) > 0;
    }

    int Size()
    {
        return listLength(this->sequencer);
    }

    void Push(T *t)
    {
        listAddNodeHead(this->sequencer, (void *)t);
    }

    void Add(T *t)
    {
        listAddNodeTail(this->sequencer, (void *)t);
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
        listAddNodeTail(this->sequencer, (void *)t);
    }

    void Enqueue(T *t, EnqueueInOrder *sequencer, void* parm)
    {
        auto *li = listGetIterator(this->sequencer, AL_START_HEAD);
        listNode *node;
        while ((node = listNext(li)))
        {
            if ((*sequencer)(t, node->value, parm) >= 0)
            {
                listInsertNode(this->sequencer, node, (void *)t, 0);
                listReleaseIterator(li);
                return;
            }
        }
        listAddNodeTail(this->sequencer, (void *)t);
        listReleaseIterator(li);
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


    void RemoveFirst()
    {
        if (this->sequencer)
        {
            listDelNode(this->sequencer, listFirst(this->sequencer));
        }
    }

    void RemoveLast()
    {
        if (this->sequencer)
        {
            listDelNode(this->sequencer, listLast(this->sequencer));
        }
    }

    void RemoveAllBut(int pivot_pos)
    {
        if (this->sequencer)
        {
            int from_head = pivot_pos;
            int from_tail = this->sequencer->len - pivot_pos - 1;
            while (from_tail--)
                listDelNode(this->sequencer, listLast(this->sequencer));
            if (pivot_pos > 0)
            {
                while (from_head--)
                    listDelNode(this->sequencer, listFirst(this->sequencer));
            }
        }
    }

    void Purge()
    {
        do
        {
            listNode *node = listIndex(this->sequencer, 0);
            if (node == NULL)
                return;
            T *t = (T *)node->value;
            listDelNode(this->sequencer, node);
            delete t;
        } while (1 == 1);
    }

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

    void Join(GraphStack<T> *adjacent)
    {
        if (adjacent && adjacent->sequencer)
            listJoin(this->sequencer, listDup(adjacent->sequencer));
    }

    void CopyTo(GraphStack<T> *receiver)
    {
        if (receiver && receiver->sequencer)
            listJoin(receiver->sequencer, listDup(this->sequencer));
    }
};

#endif