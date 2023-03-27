#ifndef __SJIBOLETH_GRAPH_HPP__
#define __SJIBOLETH_GRAPH_HPP__

#ifdef __cplusplus
extern "C"
{
#endif

#include "rax.h"
#include "rxSuiteHelpers.h"
#include "sdsWrapper.h"
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define REDISMODULE_EXPERIMENTAL_API
#include "../../src/redismodule.h"

#ifdef __cplusplus
}
#endif

#ifndef UCHAR
#define UCHAR unsigned char
#endif

#include "graphstack.hpp"

#include "sjiboleth-fablok.hpp"

class Graph_Leg
{
public:
    rxString key;
    float length;
    Graph_Leg *start;
    Graph_Leg *origin;
    void *obj;

public:
    Graph_Leg(rxString key, float weight)
    {
        this->key = rxStringDup(key);
        this->length = weight;
        this->start = NULL;
        this->origin = NULL;
        this->obj = NULL;
    }

    Graph_Leg(rxString key, float weight, Graph_Leg *origin)
        : Graph_Leg(key, origin == NULL ? weight : origin->length + weight)
    {
        this->origin = origin;
    }

    ~Graph_Leg()
    {
        rxStringFree(this->key);
    }

    static Graph_Leg *Add(rxString key, double const weight, Graph_Leg *origin, GraphStack<Graph_Leg> *bsf_q, rax *touches)
    {
        if (touches)
        {
            void *data = raxFind(touches, (UCHAR *)key, strlen(key));
            if (data != raxNotFound)
                return NULL;
        }
        auto *leg = new Graph_Leg(key, weight, origin);
        bsf_q->Enqueue(leg);
        if (touches)
            raxInsert(touches, (UCHAR *)key, strlen(key), leg, NULL);
        return leg;
    }

    static Graph_Leg *Add(rxString key, double weight, GraphStack<Graph_Leg> *bsf_q)
    {
        auto *leg = new Graph_Leg(key, weight);
        bsf_q->Enqueue(leg);
        return leg;
    }
};

typedef void CGraph_Triplet;      // Opaque Redis Object
typedef void CGraph_Triplet_Edge; // Opaque Redis Object

class Graph_Triplet_Edge
{
public:
    rxString object_key;
    void *object;
    list *path;
    int refCnt;
    void *legit;

public:
    Graph_Triplet_Edge()
    {
        this->object = NULL;
        this->object_key = rxStringEmpty();
        this->path = NULL;
        this->refCnt = 1;
    }

    // Graph_Triplet_Edge(rxString object_key, void *object, list *path)
    //     : Graph_Triplet_Edge()
    // {
    //     this->object = object;
    //     this->object_key = rxStringDup(object_key);
    //     this->path = path;
    // }

    // Graph_Triplet_Edge(rxString object_key, void *object, rxString step)
    //     : Graph_Triplet_Edge()
    // {
    //     this->object = object;
    //     this->object_key = rxStringDup(object_key);
    //     this->path = listCreate();
    //     listAddNodeTail(this->path, (void *)step);
    // }

    static Graph_Triplet_Edge *New(rxString object_key, void *object, list *path){
        if(strlen(object_key) == 0)
            rxServerLog(rxLL_NOTICE, "Suspicious call");
        auto *edge = (Graph_Triplet_Edge *)rxMemAlloc(sizeof(Graph_Triplet_Edge) + strlen(object_key) + 1);
        auto *ok = (char*)((void *)edge) + sizeof(Graph_Triplet_Edge);
        strcpy(ok, object_key);
        edge->object = object;
        edge->object_key = ok;
        edge->path = path;
        edge->refCnt = 1;
        return edge;
    }

    static Graph_Triplet_Edge *New(rxString object_key, void *object, rxString step){
        if(strlen(object_key) == 0)
            rxServerLog(rxLL_NOTICE, "Suspicious call");
        auto *edge = (Graph_Triplet_Edge *)rxMemAlloc(sizeof(Graph_Triplet_Edge) + strlen(object_key) + 1 + strlen(step) + 1);
        auto *ok = (char*)((void *)edge) + sizeof(Graph_Triplet_Edge);
        auto *section = ok + 1 +  strlen(object_key);
        strcpy(ok, object_key);
        strcpy(section, step);
        edge->object = object;
        edge->object_key = ok;
        edge->path = listCreate();
        edge->refCnt = 1;
        listAddNodeTail(edge->path, (void *)section);
        return edge;
    }

    int IncrRefCnt()
    {
        int c = this->refCnt;
        this->refCnt++;
        return c;
    }

    int DecrRefCnt()
    {
        this->refCnt--;
        return this->refCnt;
    }

    ~Graph_Triplet_Edge()
    {
        this->object = NULL;
        rxStringFree(this->object_key);
        this->object_key = rxStringEmpty();
        while (listLength(this->path))
        {
            listNode *node = listIndex(this->path, 0);
            if (node)
            {
                rxStringFree((rxString)node->value);
                listDelNode(this->path, node);
            }
        }
        listRelease(this->path);
        this->path = NULL;
    }

    void Show()
    {
        return;
        if (this->object == NULL)
        {
            return;
        }
        listIter *li = listGetIterator(this->path, 0);
        listNode *ln;
        while ((ln = listNext(li)) != NULL)
        {
            rxServerLog(rxLL_NOTICE, " => %s", (char *)ln->value);
        }
        rxServerLog(rxLL_NOTICE, "\n");
        listReleaseIterator(li);
    }

    rxString Json(rxString json)
    {
        json = rxStringFormat("%s{\"object\":\"%s\", ",
                              json, this->object_key);

        if (listLength(this->path) > 0)
        {
            char sep = ' ';
            listIter *li = listGetIterator(this->path, 0);
            listNode *ln;
            while ((ln = listNext(li)) != NULL)
            {
                json = rxStringFormat("%s%c\"%s\"", json, sep, (char *)ln->value);
                sep = ',';
            }
            json = rxStringFormat("%s%s", json, "]");
            listReleaseIterator(li);
        }
        json = rxStringFormat("%s%s", json, "}");
        return json;
    }

    void Write(RedisModuleCtx *ctx, bool nested)
    {
        if (nested)
            RedisModule_ReplyWithArray(ctx, 4);

        RedisModule_ReplyWithStringBuffer(ctx, "object", 6);
        RedisModule_ReplyWithStringBuffer(ctx, (char *)this->object_key, strlen((char *)this->object_key));
        RedisModule_ReplyWithStringBuffer(ctx, "path", 4);
        if (this->path && listLength(this->path))
        {
            RedisModule_ReplyWithArray(ctx, listLength(this->path));
            listIter *li = listGetIterator(this->path, 0);
            listNode *ln;
            while ((ln = listNext(li)))
            {
                rxString k = (rxString)ln->value;
                RedisModule_ReplyWithStringBuffer(ctx, (char *)k, strlen(k));
            }
            listReleaseIterator(li);
        }
    }

    static rxString ObjectKey(CGraph_Triplet_Edge *t)
    {
        return ((Graph_Triplet_Edge *)t)->object_key;
    }
};

/*
 * Describes a path from a subject to the object (endpoint)
 *
 * The path is describes by a series of edges.
 *
 *                Fred -------- Wilma
 *                     \      /
 *                      \    /
 *                     pebbles
 *                        |
 *                      bambam
 *                      /    \
 *                     /      \
 *              Barney -------- Betty
 *
 *   Triplet:
 *       subject: Fred
 *       edges:   fred -> pebbles
 *                pebbles -> bambam
 *                bambam -> betty (terminal)
 *
 *   Triplet:
 *       subject: pebbles
 *       edges:   pebbles -> bambam (terminal)
 */
class Graph_Triplet
{
public:
    rxString subject_key;
    void *subject;
    double length;
    GraphStack<Graph_Triplet_Edge> edges;
    GraphStack<FaBlok> containers;
    int refCnt;
    void *legit;

public:
    // Graph_Triplet()
    // {
    //     this->subject_key = rxStringEmpty();
    //     this->subject = NULL;
    //     this->refCnt = 0;
    // }

    // Graph_Triplet(rxString subject_key, void *subject)
    // {
    //     this->subject_key = rxStringDup(subject_key);
    //     this->subject = subject;
    //     this->length = 0.0;
    //     this->refCnt = 0;
    // }

    // Graph_Triplet(rxString subject_key, void *subject, double length)
    //     : Graph_Triplet(subject_key, subject)
    // {
    //     this->length = length;
    // }


    static Graph_Triplet *New(rxString subject_key, void *subject, double length)
    {
        auto *t = (Graph_Triplet *)rxMemAlloc(sizeof(Graph_Triplet) + strlen(subject_key) + 1);
        auto * sk = (char *)((void *) t ) + sizeof(Graph_Triplet);
        strcpy(sk, subject_key);

        t->subject_key = sk;
        t->subject = subject;
        t->refCnt = 0;
        t->length = length;
        t->edges.Init();
        t->containers.Init();
        return t;
    }

    static Graph_Triplet *New(rxString subject_key, void *subject)
    {
        return New(subject_key, subject, 0);
    }

    static CGraph_Triplet *InitializeResult(int db, Graph_Leg *terminal, rxString subject_key, rxString member, rxString edge)
    {
        void *subject = rxFindHashKey(db, subject_key);
        if (subject == NULL)
            subject = rxCreateHashObject();

        list *path = listCreate();

        Graph_Leg *p = terminal;
        void *object = NULL;
        rxString object_key = member;
        if (member == NULL || strlen(member) == 0)
        {
            while (p)
            {
                if (!p->origin)
                {
                    object = rxFindHashKey(db, p->key);
                    if (object == NULL)
                        return NULL;
                    object_key = rxStringDup(p->key);
                }
                listAddNodeTail(path, (void *)rxStringDup(p->key));
                p = p->origin;
            }
        }
        else
        {
            member = rxStringTrim(member, "^");
            object = rxFindKey(db, member);
            if (edge != NULL)
                listAddNodeTail(path, (void *)rxStringDup(edge));
        }
        if (!subject /*|| !object*/)
            return NULL;
        Graph_Triplet *triplet = Graph_Triplet::New(subject_key, subject);
        triplet->length = terminal ? terminal->length : 0;
        if (object)
        {
            Graph_Triplet_Edge *e = Graph_Triplet_Edge::New(object_key, object, path);
            triplet->edges.Enqueue(e);
        }
        // triplet->Show();
        void *t = rxCreateObject(rxOBJ_TRIPLET, triplet);
        return t;
    }

    static CGraph_Triplet *InitializeResultForMatch(int db, Graph_Leg *terminal, rxString subject_key, rxString member, rxString edge)
    {
        void *subject = rxFindHashKey(db, subject_key);
        if (subject == NULL)
            subject = rxCreateHashObject();

        list *path = listCreate();

        Graph_Leg *p = terminal;
        void *object = NULL;
        rxString object_key = (member) ? member : terminal->key;
        double path_length = (terminal != NULL) ? terminal->length : 0.0;
        while (p)
        {
            listAddNodeTail(path, (void *)rxStringDup(p->key));
            p = p->origin;
        }
        object_key = rxStringTrim(object_key, "^");
        object = rxFindKey(db, object_key);
        if (edge)
            listAddNodeTail(path, (void *)rxStringDup(edge));

        if (!subject /*|| !object*/)
            return NULL;
        Graph_Triplet *triplet = Graph_Triplet::New(subject_key, subject, path_length);
        if (object)
        {
            Graph_Triplet_Edge *e = Graph_Triplet_Edge::New(object_key, object, path);
            triplet->edges.Add(e);
        }
        void *t = rxCreateObject(rxOBJ_TRIPLET, triplet);
        return t;
    }

    static CGraph_Triplet *AddObjectToTripletResult(void *ge, rxString object_key, rxString edge)
    {
        Graph_Triplet *t = (Graph_Triplet *)rxGetContainedObject(ge);
        void *object = rxFindKey(0, object_key);
        Graph_Triplet_Edge *e = Graph_Triplet_Edge::New(object_key, object, edge);
        t->edges.Enqueue(e);
        return ge;
    }

    static CGraph_Triplet *New(int db, Graph_Leg *terminal, rxString member, rxString edge)
    {
        if ((!terminal || (terminal == terminal->start)) && !member)
            return NULL;
        return Graph_Triplet::InitializeResult(db, terminal, terminal->key, member, edge);
    }

    static CGraph_Triplet *NewForMatch(int db, Graph_Leg *terminal, rxString member, rxString edge)
    {
        if ((!terminal || (terminal == terminal->start)) && !member)
            return NULL;
        return Graph_Triplet::InitializeResultForMatch(db, terminal, terminal->key, member, edge);
    }

    static rxString SubjectKey(CGraph_Triplet *t)
    {
        Graph_Triplet *source = (Graph_Triplet *)rxGetContainedObject(t);
        return source->subject_key;
    }

    void JoinEdges(Graph_Triplet *source)
    {
        source->edges.StartHead();
        Graph_Triplet_Edge *e;
        while ((e = source->edges.Next()) != NULL)
        {
            e->IncrRefCnt();
            this->edges.Push(e);
        }
        source->edges.Stop();
    }

    static void Join(CGraph_Triplet *ge, CGraph_Triplet *t)
    {
        Graph_Triplet *target = (Graph_Triplet *)rxGetContainedObject(ge);
        Graph_Triplet *source = (Graph_Triplet *)rxGetContainedObject(t);
        target->JoinEdges(source);
    }

    static void Join(CGraph_Triplet *ge, Graph_Triplet *source)
    {
        Graph_Triplet *target = (Graph_Triplet *)rxGetContainedObject(ge);
        target->JoinEdges(source);
    }

    static void StartObjectBrowse(CGraph_Triplet *t)
    {
        Graph_Triplet *source = (Graph_Triplet *)rxGetContainedObject(t);
        source->edges.StartHead();
    }

    static Graph_Triplet_Edge *NextObject(CGraph_Triplet *t)
    {
        Graph_Triplet *source = (Graph_Triplet *)rxGetContainedObject(t);
        return source->edges.Next();
    }

    static void StopObjectBrowse(CGraph_Triplet *t)
    {
        Graph_Triplet *source = (Graph_Triplet *)rxGetContainedObject(t);
        source->edges.Stop();
    }

    void StartObjectBrowse()
    {
        this->edges.StartHead();
    }

    Graph_Triplet_Edge *NextObject()
    {
        return this->edges.Next();
    }

    void StopObjectBrowse()
    {
        this->edges.Stop();
    }

    void Link(FaBlok *c)
    {
        this->containers.Add(c);
    }

    static void Link(CGraph_Triplet *t, FaBlok *c)
    {
        Graph_Triplet *source = (Graph_Triplet *)rxGetContainedObject(t);
        source->Link(c);
    }

    void Unlink(FaBlok *c)
    {
        this->containers.Remove(c);
    }

    static void Unlink(CGraph_Triplet *t, FaBlok *c)
    {
        Graph_Triplet *source = (Graph_Triplet *)rxGetContainedObject(t);
        source->Unlink(c);
    }

    ~Graph_Triplet()
    {
        if (this->subject != NULL)
        {
            // this->Show();
            this->subject = NULL;
            rxStringFree(this->subject_key);
            this->subject_key = rxStringEmpty();
        }
        while (this->edges.HasEntries())
        {
            Graph_Triplet_Edge *e = (Graph_Triplet_Edge *)this->edges.Pop();
            if(e->object_key != (char *)((void*)e + sizeof(Graph_Triplet_Edge))){
                rxServerLog(rxLL_NOTICE, "Suspicious Graph_Triplet_Edge e:%p ok:%p not %p", e, e->object_key, (void*)e + sizeof(Graph_Triplet_Edge));
                continue;
            }
            if (e)
            {
                // e->Show();
                if (e->DecrRefCnt() <= 0)
                {
                    // delete e;

                    rxMemFree(e);
                }
            }
        }
    }

    void Show()
    {
        // return;
        rxServerLog(rxLL_NOTICE, "== Triplet ==\n");
        rxServerLog(rxLL_NOTICE, " %s length=%f\n", this->subject_key, this->length);

        if (this->containers.HasEntries())
        {
            this->containers.StartHead();
            FaBlok *e;
            while ((e = this->containers.Next()) != NULL)
            {
                rxServerLog(rxLL_NOTICE, "--> %s\n", e->AsSds());
            }
            this->containers.Stop();
        }
        else
            rxServerLog(rxLL_NOTICE, "roving\n");

        if (this->edges.HasEntries())
        {
            this->edges.StartHead();
            Graph_Triplet_Edge *e;
            while ((e = this->edges.Next()) != NULL)
            {
                e->Show();
            }
            this->edges.Stop();
        }
        rxServerLog(rxLL_NOTICE, "== End of Triplet ==\n");
    }

    rxString Json(rxString key)
    {
        rxString json = rxStringFormat("{\"key\":\"%s\", \"value\":\"{\"", key);
        json = rxStringFormat("%s\"subject\":\"%s\", \"length\":\"%f\" ",
                              json, subject_key, length);

        // if (this->containers.HasEntries())
        // {
        //     this->containers.StartHead();
        //     FaBlok *e;
        //     while ((e = this->containers.Next()) != NULL)
        //     {
        //         json = rxStringFormat( "%s --> %s\n", json, e->AsSds());
        //     }
        //     this->containers.Stop();
        // }else
        //         json = rxStringFormat( "%s%s", json, "roving\n");

        if (this->edges.HasEntries())
        {
            json = rxStringFormat("%s%s", json, ", \"edges\": [");
            this->edges.StartHead();
            Graph_Triplet_Edge *e;
            char sep[2] = {' ', 0x00};
            while ((e = this->edges.Next()) != NULL)
            {
                json = rxStringFormat("%s%s", json, sep);
                json = e->Json(json);
                sep[0] = ',';
            }
            this->edges.Stop();
            json = rxStringFormat("%s%s", json, "]");
        }
        json = rxStringFormat("%s%s", json, "}");
        return json;
    }

    void Write(RedisModuleCtx *ctx)
    {
        bool nested = (this->edges.Size() > 1);
        bool has_edges = (this->edges.HasEntries());
        if(this->length < 0.0)
            this->length = 0.0;
        bool has_length = (this->length >= 0.0);
        int no_of_elements = 2 + (has_length ? 2 : 0) + (nested ? 2 : 0) + (has_edges ? (nested ? 0 : 4) : 0);
        RedisModule_ReplyWithArray(ctx, no_of_elements);
        RedisModule_ReplyWithStringBuffer(ctx, "subject", 7);
        RedisModule_ReplyWithStringBuffer(ctx, (char *)this->subject_key, strlen((char *)this->subject_key));
        if (has_length)
        {
            RedisModule_ReplyWithStringBuffer(ctx, "length", 6);
            RedisModule_ReplyWithDouble(ctx, this->length);
        }
        if (nested)
        {
            RedisModule_ReplyWithStringBuffer(ctx, "objects", 7);
            RedisModule_ReplyWithArray(ctx, this->edges.Size());
        }
        if (has_edges)
        {
            this->edges.StartHead();
            Graph_Triplet_Edge *e;
            while ((e = this->edges.Next()) != NULL)
            {
                e->Write(ctx, nested);
            }
            this->edges.Stop();
        }
    }

    int IncrRefCnt()
    {
        int c = this->refCnt;
        this->refCnt++;
        return c;
    }

    int DecrRefCnt()
    {
        this->refCnt--;
        return this->refCnt;
    }
};

#endif