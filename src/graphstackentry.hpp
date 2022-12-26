#ifndef __GRAPHSTACKENTRY_HPP__
#define __GRAPHSTACKENTRY_HPP__


#include <cstdarg>
#include <typeinfo>

#include "client-pool.hpp"
#include "simpleQueue.hpp"





template class RedisClientPool<struct client>;

#include "graphstackentry.h"


const char *CRLF = "\r\n";
const char *COLON = ":";
const char *COMMA = ",";
const char *EMPTY_STRING = "";
const char *BEGIN_ARRAY = "[";
const char *END_ARRAY = "]";
const char *BEGIN_OBJECT = "{";
const char *END_OBJECT = "}";
const char *TANDEM_PREFIX = "^";
const char *TANDEM_LINK_SEP = "|";
const char *ESCAPED_APOSTROPH = "\"";

const char *EDGE = "edge";
const char *VERTICE = "vertice";
const char *SUBJECT = "subject";
const char *OBJECT = "object";
const char *PREDICATE = "predicate";
const char *TERTIARY = "tertiary";
const char *IRI = "iri";
const char *INVERSE_IRI = "inverse_iri";
const char *ENTITY_TYPE = "type";
const char *ENTITY_INVERSE_TYPE = "inverse";
const char *WEIGHT = "weight";

const char *EDGE_TYPE_EDGE_TO_SUBJECT = "ES";
const char *EDGE_TYPE_SUBJECT_TO_EDGE = "SE";
const char *EDGE_TYPE_EDGE_TO_OBJECT = "EO";
const char *EDGE_TYPE_OBJECT_TO_EDGE = "OE";

const char *WREDIS_CMD_SADD = "sadd";
const char *WREDIS_CMD_SMEMBERS = "SMEMBERS";

const char *WREDIS_CMD_SET = "set";
const char *WREDIS_CMD_GET = "get";
const char *WREDIS_CMD_HSET = "hset";
const char *WREDIS_CMD_HGETALL = "HGETALL";
const char *WOK = "WOK";

struct client *graphStackEntry_fakeClient = NULL;
#include "graphstackentry_abbreviated.hpp"

void ExecuteRedisCommand(SimpleQueue *ctx, void *stash, const char *host_reference)
{
    int argc = *((int *)stash);
    void **argv = (void **)(stash + sizeof(void *));
    auto *c = (struct client *)RedisClientPool<struct client>::Acquire(host_reference);
    rxAllocateClientArgs(c, argv, argc);
    rxString commandName = (rxString)rxGetContainedObject(argv[0]);
    void *command_definition = rxLookupCommand((rxString)commandName);
    if(command_definition)
        rxClientExecute(c, command_definition);
    else
        rxServerLog(rxLL_WARNING, "Unknown command %s,  arg count: %d", commandName, argc);
    RedisClientPool<struct client>::Release(c);
    if(ctx)
        enqueueSimpleQueue(ctx, stash);
}

redisContext *saved_redisClient = NULL;
void ExecuteRedisCommandRemote(SimpleQueue *ctx, void *stash, const char *host_reference)
{
    int argc = *((int *)stash);
    // use the sds pointers as arguments!
    // auto *argv = (const char **)(stash + sizeof(void *));
    auto *sdsv = (void **)(stash + (argc + 3) * sizeof(void *));
    void *sp = (void *)sdsv; 
    // char *cp = NULL;
    rxString cmd = rxStringEmpty();
    for(int n = 0 ; n < argc; ++n ){
        // cp = *((char **)sp);        
        cmd = rxStringFormat((n < 1000000 ?  "%s%s " : "%s\"%s\" "), cmd, *((char **)sp));
        // cp += sizeof(char *);
        // TODO: refactor
        sp = sp + 16;
    }
    size_t argv_len = 0;
    if(saved_redisClient == NULL)
        saved_redisClient = RedisClientPool<redisContext>::Acquire(host_reference);

    // auto *rcc = (redisReply *)redisCommandArgv(saved_redisClient, argc, argv, &argv_len);
    auto *rcc = (redisReply *)redisCommand(saved_redisClient, cmd, "");
    if(rcc != NULL){
        freeReplyObject(rcc);
    }
    if(saved_redisClient->err > 0){
        rxServerLog(rxLL_WARNING, "Execution error %s for %s", saved_redisClient->errstr, cmd);
        // RedisClientPool<redisContext>::Release(saved_redisClient);
    }
    rxStringFree(cmd);
    if(ctx)
        enqueueSimpleQueue(ctx, stash);
}

void FreeStash(void *stash)
{
    rxMemFree(stash);
}

#ifndef STASHERS_ONLY
template <typename T>
class GraphIdentity
{
public:
    rax *r;
    raxIterator ri;

    GraphIdentity()
    {
        this->r = raxNew();
    }

    ~GraphIdentity()
    {
        // string ti = typeid(this).name();
        // if(ti.compare("c") == 0)
        // {
        //     raxFreeWithCallback(this->r, [](T *o)
        //                         { delete o; });
        // }
        // else
        // {
        raxFree(this->r);
        // }
    }

    int StartIterator()
    {
        raxStart(&this->ri, this->r);
        return raxSeek(&this->ri, "^", NULL, 0);
    }

    int SeekIterator(const char *op, unsigned char *k, size_t kl)
    {
        return raxSeek(&this->ri, op, k, kl);
    }

    int SeekTop()
    {
        return raxSeek(&this->ri, "^", NULL, 0);
    }

    char *Next(size_t *kl, T **o)
    {
        if (raxNext(&this->ri))
        {
            *kl = this->ri.key_len;
            *o = (T *)this->ri.data;
            return (char *)this->ri.key;
        }
        else
            return NULL;
    }

    void StopIterator()
    {
        raxStop(&this->ri);
    }

    bool Contains(char const *key)
    {
        void *m = raxFind(this->r, (UCHAR *)key, strlen(key));
        return (m != raxNotFound);
    }

    T *Find(char const *key)
    {
        return this->Find(key, strlen(key));
    }

    T *Find(char const *key, size_t keyl)
    {
        T *m = (T *)raxFind(this->r, (UCHAR *)key, keyl);
        if (m != raxNotFound)
            return m;
        return NULL;
    }

    int Insert(char const *key, size_t keyl, T *o)
    {
        return raxInsert(this->r, (UCHAR *)key, keyl, o, NULL);
    }

    int Insert(char const *key, T *o)
    {
        return this->Insert(key, strlen(key), o);
    }

    int Remove(char const *key, size_t keyl)
    {
        void *old;
        return raxRemove(this->r, (UCHAR *)key, keyl, &old);
    }

    int Remove(char const *key)
    {
        return this->Remove(key, strlen(key));
    }
};

class GraphStackEntry
{
public:
    /* const char * */ rxString token_key;
    /* const char * */ rxString inverse_token_key;
    /* const char * */ rxString token_value;
    /* const char * */ rxString token_type;
    // predicate weight
    double weight;
    GraphIdentity<char> *entity;
    SimpleQueue *ctx;
    GraphStackEntry *parent;

    GraphStackEntry(const char *token_value, GraphStackEntry *parent)
    {
        this->token_key = NULL;
        this->inverse_token_key = NULL;
        this->token_value = rxStringDup(rxStringNew(token_value));
        this->token_type = NULL;
        this->ctx = NULL;
        this->entity = NULL;
        this->parent = parent;
        this->weight = 0.0;
    }

    GraphStackEntry(SimpleQueue *ctx, GraphStackEntry *parent)
    {
        this->token_key = NULL;
        this->inverse_token_key = NULL;
        this->token_value = NULL;
        this->token_type = NULL;
        this->parent = parent;
        this->ctx = ctx;
        this->entity = new GraphIdentity<char>();
        this->weight = 0.0;
    }

    ~GraphStackEntry()
    {
        if (this->token_key)
            free((void *)this->token_key);
        token_key = NULL;
        if (this->token_value)
            free((void *)this->token_value);
        token_value = NULL;
        if (this->entity)
            delete this->entity;
        this->ctx = NULL;
        this->entity = NULL;
    }

    void ComposeEdgeIRI(GraphStackEntry *target)
    {
        // Pre-Resolve predicate IRI
        /* const char * */ rxString verb = target->token_type;
        if (!verb)
            verb = this->GetFromParent(ENTITY_TYPE);
        /* const char * */ rxString inverse_verb = target->inverse_token_key;
        if (!inverse_verb)
            inverse_verb = this->GetFromParent(ENTITY_INVERSE_TYPE);
        /* const char * */ rxString object_value = this->GetFromParent(OBJECT);
        /* const char * */ rxString subject_value = this->GetFromParent(SUBJECT);
        rxString composite_key = rxStringEmpty();
        if (verb && object_value && subject_value)
        {
            composite_key = rxStringFormat("%s%s:%s:%s", composite_key, verb, subject_value, object_value);
            target->entity->Insert(IRI, 3, (char *)rxStringDup(composite_key));
            target->token_key = rxStringDup(composite_key);
            // target->Dump("TARGET AFTER ComposeEdgeIRI");
        }
        if (inverse_verb && object_value && subject_value)
        {
            composite_key = rxStringFormat("%s%s:%s:%s", composite_key, inverse_verb, object_value, subject_value);
            target->entity->Insert(INVERSE_IRI, 3, strdup(composite_key));
            target->inverse_token_key = rxStringDup(composite_key);
            // target->Dump("TARGET AFTER ComposeEdgeIRI");
        }
    }

    void Set(GraphStackEntry *k, GraphStackEntry *v)
    {
        this->entity->Insert(k->token_value, (char *)v->token_value);
        if (k->HasParent(TERTIARY))
        {
            // this->Dump("THIS FOR TERTIARY");
            this->ComposeEdgeIRI(this);
        }
        if (strcmp(k->token_value, IRI) == 0)
        {
            this->token_key = strdup(v->token_value);
        }
        else if (strcmp(k->token_value, ENTITY_TYPE) == 0)
        {
            this->parent->token_type = strdup(v->token_value);
            if (k->HasParent(PREDICATE))
            {
                k->ComposeEdgeIRI(this);
            }
        }
        else if (strcmp(k->token_value, ENTITY_INVERSE_TYPE) == 0)
        {
            this->parent->inverse_token_key = strdup(v->token_value);
            if (k->HasParent(PREDICATE))
            {
                k->ComposeEdgeIRI(this);
            }
        }
        else if (strcmp(k->token_value, WEIGHT) == 0)
        {

            k->parent->weight = atof(v->token_value);
        }
    }

    void Set(GraphStackEntry *k)
    {
        // Set entity type as key
        this->token_key = strdup(k->token_value);
    }

    bool Contains(const char *key)
    {
        if (!this->entity)
            return false;
        return this->entity->Contains(key);
    }

    bool isEdge()
    {
        return strcmp(this->token_key, PREDICATE) == 0;
    }

    void Link(rxString edge_key, rxString vertice, const char *direction, double weight)
    {
        rxString type;
        char *colon = strstr((char *)edge_key, COLON);
        if (colon)
        {
            type = rxStringNewLen(edge_key, colon - edge_key);
        }
        else
        {
            colon = strstr((char *)vertice, COLON);
            type = rxStringNewLen(vertice, colon - vertice);
        }
        if (type[0] == TANDEM_PREFIX[0])
            type = rxStringNewLen(type + 1, strlen(type) - 1);
        rxString link = rxStringEmpty();
        link = rxStringFormat("%s%s|%s|%s\%0.0f",
                            link, type,
                            vertice,
                            direction,
                            weight);
        rxStashCommand(this->ctx, WREDIS_CMD_SADD, 2, edge_key, link);
        rxStringFree(link);
    }

    /* const char * */ rxString Get(const char *key)
    {
        return this->entity->Find(key);
    }

    void Persist(SimpleQueue *ctx)
    {
        if (ctx == NULL)
            ctx = this->ctx;
        if (this->token_key == NULL)
        {
            printf("Missing key\n");
            return;
        }
        if (strcmp(this->token_key, TERTIARY) == 0)
        {
            return;
        }
        if (this->parent)
        {
            this->parent->token_key = strdup(this->token_key);
            if (this->inverse_token_key)
                this->parent->inverse_token_key = strdup(this->inverse_token_key);
            if (this->parent->parent)
            {
                this->parent->parent->entity->Insert(
                    this->parent->token_value,
                    strdup(this->token_key));
                if (this->inverse_token_key)
                    this->parent->parent->entity->Insert(
                        "inverse",
                        7,
                        strdup(this->inverse_token_key));
            }
        }
        if (this->isEdge())
        {
            this->token_key = GetFromParent(PREDICATE);
            if (!this->token_key)
            {
                // printf("#110# Persist # %s\n", this->token_key);
                this->token_key = GetFromParent(IRI);
                // printf("#115# Persist # %s\n", this->token_key);
            }
            this->inverse_token_key = GetFromParent("inverse");
            /* const char * */ rxString weight = GetFromParent(WEIGHT);
            /* const char * */ rxString subject = GetFromParent(SUBJECT);
            /* const char * */ rxString object = GetFromParent(OBJECT);
            if (!this->token_key || !subject || !object)
            {
                this->Dump("Missing entities on PERSIST");
                return;
            }
            else
            {
                double w = weight ? atof(weight) : 1.0;
                rxString subject_key = rxStringNew(TANDEM_PREFIX);
                subject_key = rxStringFormat("%s%s", subject_key, subject);
                rxString object_key = rxStringNew(TANDEM_PREFIX);
                object_key = rxStringFormat("%s%s", object_key, object);
                rxString predicate_key = rxStringNew(TANDEM_PREFIX);
                predicate_key = rxStringFormat("%s%s", predicate_key, this->token_key);
                rxString inverse_predicate_key = rxStringNew(TANDEM_PREFIX);
                if (this->inverse_token_key)
                    inverse_predicate_key = rxStringFormat("%s%s", inverse_predicate_key, this->inverse_token_key);
                else
                    inverse_predicate_key = rxStringFormat("%s%s", inverse_predicate_key, this->token_key);
                this->Link(inverse_predicate_key, subject_key, EDGE_TYPE_EDGE_TO_SUBJECT, w);
                this->Link(subject_key, predicate_key, EDGE_TYPE_SUBJECT_TO_EDGE, w);
                this->Link(predicate_key, object_key, EDGE_TYPE_EDGE_TO_OBJECT, w);
                this->Link(object_key, inverse_predicate_key, EDGE_TYPE_OBJECT_TO_EDGE, w);
                this->entity->Remove(PREDICATE, 9);
            }
        }

        // printf("#200# Persist # %s\n", this->token_key);
        this->entity->StartIterator();
        char *key;
        size_t keylen;
        char *value;
        rxString iri = rxStringNew(this->token_key);
        while ((key = this->entity->Next(&keylen, &value)) != NULL)
        {
            // if (strncmp(WEIGHT, (/* const char * */ rxString )key, keylen) == 0)
            //     continue;
            if (strncmp(IRI, (/* const char * */ rxString)key, keylen) == 0)
                continue;
            if (strncmp("inverse", (/* const char * */ rxString)key, keylen) == 0)
            {
                int segments = 0;
                rxString v = rxStringNew((char *)value);
                rxString *parts = rxStringSplitLen(v, strlen(v), ":", 1, &segments);
                rxString e = rxStringNew("edge");
                rxString t = rxStringNew("type");
                rxStashCommand(this->ctx, WREDIS_CMD_HSET, 3, inverse_token_key, e, token_key);
                rxStashCommand(this->ctx, WREDIS_CMD_HSET, 3, inverse_token_key, t, parts[0]);
                rxStashCommand(this->ctx, WREDIS_CMD_HSET, 3, inverse_token_key, e, inverse_token_key);
                rxStringFreeSplitRes(parts, segments);
                rxStringFree(e);
                rxStringFree(t);
                continue;
            }
            if (strncmp("inv", (/* const char * */ rxString)key, keylen) == 0)
                continue;
            if (strncmp(SUBJECT, (/* const char * */ rxString)key, keylen) == 0)
                continue;
            if (strncmp(OBJECT, (/* const char * */ rxString)key, keylen) == 0)
                continue;
            rxString k = rxStringNewLen(key, keylen);
            // printf("#500# Persist # %p %s\n", iri, iri);
            rxStashCommand(this->ctx, WREDIS_CMD_HSET, 3, iri, k, value);
            rxStringFree(k);
        }
        rxStringFree(iri);
        this->entity->StopIterator();
        ;
        // The hashtable is no longer needed
        // delete this->entity;
        this->entity = NULL;
        this->ctx = NULL;
    }

    void Dump(const char *label)
    {
        return;
        GraphStackEntry *e = this;
        int depth = 0;
        while (e)
        {
            printf("++++++++++++++++++ DUMP ++++ %d ++ %s ++++++++++++++++\n", depth, label);
            printf("token_key:    %s\n", e->token_key);
            printf("inv_token_key:%s\n", e->inverse_token_key);
            printf("token_value:  %s\n", e->token_key);
            printf("token_type:   %s\n", e->token_type);
            printf("weight:       %0.0f\n", e->weight);
            if (e->entity)
            {
                e->entity->StartIterator();
                char *key;
                size_t keylen;
                char *value;
                while ((key = e->entity->Next(&keylen, &value)) != NULL)
                {
                    rxString k = rxStringNewLen(key, keylen);
                    printf("...%s=         %s\n", k, (char *)value);
                }
                e->entity->StopIterator();
                printf("++++++++++++++++++ DUMP END+ %d ++ %s ++++++++++++++++\n", depth, label);
            }
            --depth;
            e = e->parent;
        }
    }

    bool HasParent(const char *key)
    {
        GraphStackEntry *e = this;
        while (e)
        {
            // if(e->token_key != NULL && strcmp(e->token_key, key) == 0 )
            //     return true;
            if (e->token_value != NULL && strcmp(e->token_value, key) == 0)
                return true;
            e = e->parent;
        }
        return false;
    }

    /* const char * */ rxString GetFromParent(const char *key)
    {
        GraphStackEntry *e = this;
        while (e)
        {
            if (e->entity != NULL)
            {
                /* const char * */ rxString value = e->Get(key);
                if (value != NULL)
                    return value;
            }
            e = e->parent;
        }
        return NULL;
    }
};

#endif
#endif