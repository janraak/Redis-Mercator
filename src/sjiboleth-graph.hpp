#ifndef __SJIBOLETH_GRAPH_HPP__
#define __SJIBOLETH_GRAPH_HPP__

#ifdef __cplusplus
extern "C"
{
#endif

#include "rax.h"
#include "sdsWrapper.h"
#include <stddef.h>
#include <stdint.h>

#define REDISMODULE_EXPERIMENTAL_API
#include "../../src/redismodule.h"

#ifdef __cplusplus
}
#endif

#ifndef UCHAR
#define UCHAR unsigned char
#endif

#include "graphstack.hpp"

#define OBJ_TRIPLET 15 /* rxSuite graph triplet Predicate(Subject,Object) object. */

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
        /////**/ rxServerLog(LL_NOTICE, "0x%x %s #add_graph_leg# ", (POINTER)this, key);
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

public:
    Graph_Triplet_Edge()
    {
        this->object = NULL;
        this->object_key = rxStringEmpty();
        this->path =  NULL;
        this->refCnt = 1;
    }

    Graph_Triplet_Edge(rxString object_key, void *object, list *path)
    :Graph_Triplet_Edge()
    {
        this->object = object;
        this->object_key = rxStringDup(object_key);
        this->path = path;
    }

    Graph_Triplet_Edge(rxString object_key, void *object, rxString step)
    :Graph_Triplet_Edge()
    {
        this->object = object;
        this->object_key = rxStringDup(object_key);
        this->path = listCreate();
        listAddNodeTail(this->path, (void*)step);
    }

    int IncrRefCnt(){
        int c = this->refCnt;
        this->refCnt++;
        return c;
    }

    int DecrRefCnt(){
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


    void Show(){
        return;
        printf("tally:%d object: 0x%x %s\n   path:", this->refCnt, (POINTER)this->object, this->object_key);
        if(this->object == NULL){
            printf(" ODD object = NULL: 0x%x %s\n   path:", (POINTER)this->object, this->object_key);
            return;
        }
        listIter *li = listGetIterator(this->path, 0);
        listNode *ln;
        while ((ln = listNext(li)) != NULL)
        {
            printf(" => %s", (char *)ln->value);
        }
        printf("\n");
        listReleaseIterator(li);
    }

    rxString Json(rxString json)
    {
        json = rxStringFormat("%s{\"object\":\"%s\", \"data\":\"%x\"", 
            json, this->object_key, (POINTER)this->object);

        if(listLength(this->path) > 0){
            char sep = ' ';
            listIter *li = listGetIterator(this->path, 0);
            listNode *ln;
            while ((ln = listNext(li)) != NULL)
            {
                json = rxStringFormat("%s%c\"%s\"", json, sep, (char *)ln->value);
                sep = ',';
            }
            json = rxStringFormat("%s%s" ,json, "]");
            listReleaseIterator(li);
        }
        json = rxStringFormat("%s%s" ,json, "}");
        return json;
    }

    void Write(RedisModuleCtx * ctx, bool nested){
        if(nested)
            RedisModule_ReplyWithArray(ctx, 4);

        RedisModule_ReplyWithStringBuffer(ctx, "object", 6);
        RedisModule_ReplyWithStringBuffer(ctx, (char *)this->object_key, strlen((char *)this->object_key));
        RedisModule_ReplyWithStringBuffer(ctx, "path", 4);
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

    static rxString ObjectKey(CGraph_Triplet_Edge *t){
        return ((Graph_Triplet_Edge *)t)->object_key;
    }
};

class Graph_Triplet
{
public:
    rxString subject_key;
    void *subject;
    double length;
    GraphStack<Graph_Triplet_Edge> edges;
    GraphStack<FaBlok> containers;
    int refCnt;

public:
    Graph_Triplet()
    {
        this->subject_key = rxStringEmpty();
        this->subject = NULL;
        this->refCnt = 0;
    }

    Graph_Triplet(rxString subject_key, void *subject)
    {
        this->subject_key = rxStringDup(subject_key);
        this->subject = subject;
        this->length = 0.0;
        this->refCnt = 0;
    }

    Graph_Triplet(rxString subject_key, void *subject, double length)
    :Graph_Triplet(subject_key, subject)
    {
        this->length = length;
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
        if (member == NULL)
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
                listAddNodeTail(path, (void*)rxStringDup(p->key));
                p = p->origin;
            }
        }
        else
        {
            member = rxStringTrim(member, "^");
            object = rxFindKey(db, member);
            if (edge != NULL)
                listAddNodeTail(path, (void*)rxStringDup(edge));
        }
        if (!subject /*|| !object*/)
            return NULL;
        Graph_Triplet *triplet = new Graph_Triplet(subject_key, subject);
        if (object)
        {
            Graph_Triplet_Edge *e = new Graph_Triplet_Edge(object_key, object, path);
            triplet->edges.Enqueue(e);
        }
        // triplet->Show();
        void *t = rxCreateObject(OBJ_TRIPLET, triplet);
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
            listAddNodeTail(path, (void*)rxStringDup(p->key));
            p = p->origin;
        }
        object_key = rxStringTrim(object_key, "^");
        object = rxFindKey(db, object_key);
        if (edge)
            listAddNodeTail(path, (void*)rxStringDup(edge));

        if (!subject /*|| !object*/)
            return NULL;
        Graph_Triplet *triplet = new Graph_Triplet(subject_key, subject, path_length);
        if (object)
        {
            Graph_Triplet_Edge *e = new Graph_Triplet_Edge(object_key, object, path);
            triplet->edges.Add(e);
        }
        void *t = rxCreateObject(OBJ_TRIPLET, triplet);
        // printf("triplet  0x%x in  0x%x\n", (POINTER) triplet, (POINTER) t);
        // triplet->Show();
        return t;
    }

    static CGraph_Triplet *AddObjectToTripletResult(void *ge, rxString object_key, rxString edge)
    {
        Graph_Triplet *t = (Graph_Triplet *)rxGetContainedObject(ge);
        void *object = rxFindKey(0, object_key);
        Graph_Triplet_Edge *e = new Graph_Triplet_Edge(object_key, object, edge);
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

    static rxString SubjectKey(CGraph_Triplet *t){
        Graph_Triplet *source = (Graph_Triplet *)rxGetContainedObject(t);
        return source->subject_key;
    }

    void JoinEdges(Graph_Triplet *source){
        source->edges.StartHead();
        Graph_Triplet_Edge *e;
        while((e = source->edges.Next()) != NULL){
            e->IncrRefCnt();
            this->edges.Push(e);
        }
        source->edges.Stop();
    }

    static void Join(CGraph_Triplet *ge, CGraph_Triplet* t){
        Graph_Triplet *target = (Graph_Triplet *)rxGetContainedObject(ge);
        Graph_Triplet *source = (Graph_Triplet *)rxGetContainedObject(t);        
        target->JoinEdges(source);
    }

    static void Join(CGraph_Triplet *ge, Graph_Triplet *source){
        Graph_Triplet *target = (Graph_Triplet *)rxGetContainedObject(ge);
        target->JoinEdges(source);
    }

    static void StartObjectBrowse(CGraph_Triplet *t){
        Graph_Triplet *source = (Graph_Triplet *)rxGetContainedObject(t);
        source->edges.StartHead();
    }

    static Graph_Triplet_Edge *NextObject(CGraph_Triplet *t){
        Graph_Triplet *source = (Graph_Triplet *)rxGetContainedObject(t);
        return source->edges.Next();
    }

    static void StopObjectBrowse(CGraph_Triplet *t){
        Graph_Triplet *source = (Graph_Triplet *)rxGetContainedObject(t);
        source->edges.Stop();
    }


    void StartObjectBrowse(){
        this->edges.StartHead();
    }

    Graph_Triplet_Edge *NextObject(){
        return this->edges.Next();
    }

    void StopObjectBrowse(){
        this->edges.Stop();
    }


    void Link(FaBlok *c){
        this->containers.Add(c);
    }


    static void Link(CGraph_Triplet *t, FaBlok *c){
        Graph_Triplet *source = (Graph_Triplet *)rxGetContainedObject(t);
        source->Link(c);
    }

    void Unlink(FaBlok *c){
        this->containers.Remove(c);
    }

    static void Unlink(CGraph_Triplet *t, FaBlok *c){
        Graph_Triplet *source = (Graph_Triplet *)rxGetContainedObject(t);
        source->Unlink(c);
    }

    ~Graph_Triplet()
    {
        if(this->subject != NULL){
        this->Show();
        this->subject = NULL;
        rxStringFree(this->subject_key);
        this->subject_key = rxStringEmpty();
        }
        while (this->edges.HasEntries())
        {
            Graph_Triplet_Edge *e = (Graph_Triplet_Edge *)this->edges.Pop();
            e->Show();
            if(e->DecrRefCnt() <= 0){
                delete e;
            }
        }
    }

    void Show()
    {
        return;
        printf("== Triplet ==\n");
        printf(" 0x%x subject: 0x%x %s length=%f\n", (POINTER)this, (POINTER)this->subject, this->subject_key, this->length);

        
        if (this->containers.HasEntries())
        {
            this->containers.StartHead();
            FaBlok *e;
            while ((e = this->containers.Next()) != NULL)
            {
                printf("--> %s\n", e->AsSds());
            }
            this->containers.Stop();
        }else
                printf("roving\n");
        
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
        printf("== End of Triplet ==\n");
    }

    rxString Json(rxString key)
    {
        rxString json = rxStringFormat( "{\"key\":\"%s\", \"value\":\"{\"", key);
        json = rxStringFormat("%s\"subject\":\"%s\", \"data\":\"%x\", \"length\":\"%f\" ", 
        json, subject_key, (POINTER)subject, length);

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
            json = rxStringFormat("%s%s" ,json, ", \"edges\": [");
            this->edges.StartHead();
            Graph_Triplet_Edge *e;
            char sep[2] = {' ', 0x00};
            while ((e = this->edges.Next()) != NULL)
            {
                json = rxStringFormat("%s%s" ,json, sep);
                json = e->Json(json);
                sep[0] = ',';
            }
            this->edges.Stop();
            json = rxStringFormat("%s%s", json, "]");
        }
        json = rxStringFormat("%s%s", json, "}");
        return json;
    }

    void Write(RedisModuleCtx * ctx){
        bool nested = (this->edges.Size() > 1);
        bool has_edges = (this->edges.HasEntries());
        bool has_length = (this->length != 0.0);
        int no_of_elements = 2 + (has_length ? 2 : 0 )  + (nested ? 2 : 0 ) + (has_edges ? (nested ? 0 : 4 ) : 0);
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
    
    int IncrRefCnt(){
        int c = this->refCnt;
        this->refCnt++;
        return c;
    }

    int DecrRefCnt(){
        this->refCnt--;
        return this->refCnt;
    }

};

#endif