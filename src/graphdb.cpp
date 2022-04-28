
#define REDISMODULE_EXPERIMENTAL_API
#include "graphdb.h"
#include <string>

#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>

using std::string;
#include "rxSuite.h"
#include "graphstack.hpp"

extern "C"
{
#include "parser.h"
#include "queryEngine.h"

    int g_set(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
    int g_get(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

    int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

    int RedisModule_OnUnload(RedisModuleCtx *ctx);
}

static Parser *graph_parser = NULL;
static Parser *sentence_parser = NULL;

extern dictType tokenDictType;

string readFileIntoString3(const string &path)
{
    struct stat sb
    {
    };
    string res;

    FILE *input_file = fopen(path.c_str(), "r");
    if (input_file == nullptr)
    {
        perror("fopen");
        return res;
    }

    stat(path.c_str(), &sb);
    res.resize(sb.st_size);
    fread(const_cast<char *>(res.data()), sb.st_size, 1, input_file);
    fclose(input_file);

    return res;
}
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

const char *REDIS_CMD_SADD = "SADD";
const char *REDIS_CMD_SMEMBERS = "SMEMBERS";

const char *REDIS_CMD_HSET = "HSET";
const char *REDIS_CMD_HGETALL = "HGETALL";
const char *OK = "OK";

class GraphStackEntry
{
public:
    const char *token_key;
    const char *inverse_token_key;
    const char *token_value;
    const char *token_type;
    // predicate weight
    double weight;
    RedisModuleDict *entity;
    RedisModuleCtx *ctx;
    GraphStackEntry *parent;

    GraphStackEntry(const char *token_value, GraphStackEntry *parent)
    {
        this->token_key = NULL;
        this->inverse_token_key = NULL;
        this->token_value = strdup(token_value);
        this->token_type = NULL;
        this->ctx = NULL;
        this->entity = NULL;
        this->parent = parent;
        this->weight = 0.0;
    }

    GraphStackEntry(RedisModuleCtx *ctx, GraphStackEntry *parent)
    {
        this->token_key = NULL;
        this->inverse_token_key = NULL;
        this->token_value = NULL;
        this->token_type = NULL;
        this->parent = parent;
        this->ctx = ctx;
        this->entity = RedisModule_CreateDict(ctx);
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
            RedisModule_FreeDict(this->ctx, this->entity);
        // dictRelease(this->entity);
        this->ctx = NULL;
        this->entity = NULL;
    }

    void ComposeEdgeIRI(GraphStackEntry *target)
    {
        // Pre-Resolve predicate IRI
        const char *verb = target->token_type;
        if (!verb)
            verb = this->GetFromParent(ENTITY_TYPE);
        const char *inverse_verb = target->inverse_token_key;
        if (!inverse_verb)
            inverse_verb = this->GetFromParent(ENTITY_INVERSE_TYPE);
        const char *object_value = this->GetFromParent(OBJECT);
        const char *subject_value = this->GetFromParent(SUBJECT);
        if (verb && object_value && subject_value)
        {
            string composite_key;
            composite_key.append((char *)verb);
            composite_key.append(COLON);
            composite_key.append((char *)subject_value);
            composite_key.append(COLON);
            composite_key.append((char *)object_value);
            RedisModule_DictSetC(target->entity, (void *)strdup(IRI), 3, (void *)strdup(composite_key.c_str()));
            target->token_key = strdup(composite_key.c_str());
            // target->Dump("TARGET AFTER ComposeEdgeIRI");
        }
        if (inverse_verb && object_value && subject_value)
        {
            string composite_key;
            composite_key.append((char *)inverse_verb);
            composite_key.append(COLON);
            composite_key.append((char *)object_value);
            composite_key.append(COLON);
            composite_key.append((char *)subject_value);
            RedisModule_DictSetC(target->entity, (void *)strdup(INVERSE_IRI), 3, (void *)strdup(composite_key.c_str()));
            target->inverse_token_key = strdup(composite_key.c_str());
            // target->Dump("TARGET AFTER ComposeEdgeIRI");
        }
    }

    void Set(GraphStackEntry *k, GraphStackEntry *v)
    {
        RedisModule_DictSetC(this->entity, (void *)k->token_value, strlen(k->token_value), (void *)v->token_value);
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
        int nokey;
        RedisModule_DictGetC(this->entity,
                             (void *)key,
                             strlen(key),
                             &nokey);
        return nokey != 1;
    }

    bool isEdge()
    {
        return strcmp(this->token_key, PREDICATE) == 0;
    }

    void Link(RedisModuleCtx *ctx, string &edge_key, string &vertice, const char *direction, double weight)
    {
        string type;
        char *colon = strstr((char *)edge_key.c_str(), COLON);
        if (colon)
        {
            type.assign(edge_key, 0, colon - edge_key.c_str());
        }
        else
        {
            colon = strstr((char *)vertice.c_str(), COLON);
            type.assign(vertice, 0, colon - vertice.c_str());
        }
        if (type.at(0) == TANDEM_PREFIX[0])
            type = type.substr(1, type.length() - 1);
        string link(type);
        link.append(TANDEM_LINK_SEP);
        link.append(vertice);
        link.append(TANDEM_LINK_SEP);
        link.append(direction);
        link.append(TANDEM_LINK_SEP);
        char w[32];
        snprintf(w, sizeof(w), "%0.0f", weight);
        link.append(w);

        // printf("LINK %s\nTO%s \n\n", link.c_str(), edge_key.c_str());
        RedisModuleCallReply *r = RedisModule_Call(ctx,
                                                   REDIS_CMD_SADD, "cc",
                                                   edge_key.c_str(),
                                                   link.c_str());
        if (r)
            RedisModule_FreeCallReply(r);
    }

    const char *Get(const char *key)
    {
        int nokey;
        const char *value = (const char *)RedisModule_DictGetC(this->entity,
                                                               (void *)key,
                                                               strlen(key),
                                                               &nokey);
        if (nokey == 1)
            return NULL;
        return value;
    }

    void Persist(RedisModuleCtx *ctx)
    {
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
                RedisModule_DictSetC(this->parent->parent->entity,
                                     (void *)this->parent->token_value,
                                     strlen(this->parent->token_value),
                                     (void *)strdup(this->token_key));
                if (this->inverse_token_key)
                    RedisModule_DictSetC(this->parent->parent->entity,
                                         (void *)"inverse",
                                         7,
                                         (void *)strdup(this->inverse_token_key));
            }
        }
        if (this->isEdge())
        {
            this->token_key = GetFromParent(PREDICATE);
            if (!this->token_key)
            {
                this->token_key = GetFromParent(IRI);
            }
            this->inverse_token_key = GetFromParent("inverse");
            const char *weight = GetFromParent(WEIGHT);
            const char *subject = GetFromParent(SUBJECT);
            const char *object = GetFromParent(OBJECT);
            if (!this->token_key || !subject || !object)
            {
                this->Dump("Missing entities on PERSIST");
                return;
            }
            else
            {
                double w = weight ? atof(weight) : 1.0;
                string subject_key(TANDEM_PREFIX);
                subject_key.append(subject);
                string object_key(TANDEM_PREFIX);
                object_key.append(object);
                string predicate_key(TANDEM_PREFIX);
                predicate_key.append(this->token_key);
                string inverse_predicate_key(TANDEM_PREFIX);
                if (this->inverse_token_key)
                    inverse_predicate_key.append(this->inverse_token_key);
                else
                    inverse_predicate_key.append(this->token_key);
                this->Link(ctx, inverse_predicate_key, subject_key, EDGE_TYPE_EDGE_TO_SUBJECT, w);
                this->Link(ctx, subject_key, predicate_key, EDGE_TYPE_SUBJECT_TO_EDGE, w);
                this->Link(ctx, predicate_key, object_key, EDGE_TYPE_EDGE_TO_OBJECT, w);
                this->Link(ctx, object_key, inverse_predicate_key, EDGE_TYPE_OBJECT_TO_EDGE, w);
                RedisModule_DictDelC(this->entity,
                                     (void *)PREDICATE,
                                     9, NULL);
            }
        }

        RedisModuleDictIter *di = RedisModule_DictIteratorStartC(this->entity, TANDEM_PREFIX, NULL, 0);
        void *key;
        size_t keylen;
        void *value;
        RedisModuleCallReply *r;
        while ((key = RedisModule_DictNextC(di, &keylen, &value)) != NULL)
        {
            // if (strncmp(WEIGHT, (const char *)key, keylen) == 0)
            //     continue;
            if (strncmp(IRI, (const char *)key, keylen) == 0)
                continue;
            if (strncmp("inverse", (const char *)key, keylen) == 0){
                int segments = 0;
                sds v = sdsnew((char *)value);
                sds *parts = sdssplitlen(v, sdslen(v), ":", 1, &segments);
                r = RedisModule_Call(ctx, REDIS_CMD_HSET, "ccc", inverse_token_key, "edge", token_key);
                if (r)
                    RedisModule_FreeCallReply(r);
                r = RedisModule_Call(ctx, REDIS_CMD_HSET, "ccc", inverse_token_key, "type", parts[0]);
                if (r)
                    RedisModule_FreeCallReply(r);
                r = RedisModule_Call(ctx, REDIS_CMD_HSET, "ccc", token_key, "edge", inverse_token_key);
                if (r)
                    RedisModule_FreeCallReply(r);
                sdsfreesplitres(parts, segments);
                continue;
            }
            if (strncmp("inv", (const char *)key, keylen) == 0)
                continue;
            if (strncmp(SUBJECT, (const char *)key, keylen) == 0)
                continue;
            if (strncmp(OBJECT, (const char *)key, keylen) == 0)
                continue;
            r = RedisModule_Call(ctx, REDIS_CMD_HSET, "cbc", token_key, key, keylen, value);
            if (r)
                RedisModule_FreeCallReply(r);
        }
        RedisModule_DictIteratorStop(di);
        // The hashtable is no longer needed
        RedisModule_FreeDict(this->ctx, this->entity);
        this->entity = NULL;
        this->ctx = NULL;
    }

    void Dump(const char *label)
    {
        // return;
        GraphStackEntry *e = this;
        int depth = 0;
        while (e)
        {
            printf("++++++++++++++++++ DUMP ++++ %d ++ %s ++++++++++++++++\n", depth, label);
            printf("this:         0x%x\n", (unsigned int)e);
            printf("token_key:    %s\n", e->token_key);
            printf("inv_token_key:%s\n", e->inverse_token_key);
            printf("token_value:  %s\n", e->token_key);
            printf("token_type:   %s\n", e->token_type);
            printf("weight:       %0.0f\n", e->weight);
            printf("parent:       0x%x\n", (unsigned int)e->parent);
            printf("hash:         0x%x\n", (unsigned int)e->entity);
            if (e->entity)
            {
                RedisModuleDictIter *di = RedisModule_DictIteratorStartC(e->entity, TANDEM_PREFIX, NULL, 0);
                void *key;
                size_t keylen;
                void *value;
                while ((key = RedisModule_DictNextC(di, &keylen, &value)) != NULL)
                {
                    sds k = sdsnewlen(key, keylen);
                    printf("...%s=         %s\n", k, (char *)value);
                }
                RedisModule_DictIteratorStop(di);
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

    const char *GetFromParent(const char *key)
    {
        GraphStackEntry *e = this;
        while (e)
        {
            if (e->entity != NULL)
            {
                const char *value = e->Get(key);
                if (value != NULL)
                    return value;
            }
            e = e->parent;
        }
        return NULL;
    }
};

int g_set(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    size_t len;
    const char *argS = RedisModule_StringPtrLen(argv[1], &len);
    sds arg = sdsnew(argS);
    sdstoupper(arg);
    sds keyword = sdsnew("FILE");
    if (argc == 3 && sdscmp(keyword, arg) == 0)
    {
        const char *pathS = RedisModule_StringPtrLen(argv[2], &len);
        sds path = sdsnew(pathS);

        string graph = readFileIntoString3(path);

        parseGraph(graph_parser, graph.c_str());
        Token *t3 = NULL;
        listIter *li = listGetIterator(graph_parser->rpn, AL_START_HEAD);
        listNode *ln;
        GraphStackEntry *se;
        auto *graph_stack = new GraphStack<GraphStackEntry>();
        // int item = 0;
        while ((ln = listNext(li)))
        {
            t3 = (Token *)ln->value;
            // printf("token: %d %d %s\n", item++, t3->token_type, t3->token);
            switch (t3->token_type)
            {
            case 2: // A string token
            {
                // printf("-->ITEM %d --> ", item);
                se = new GraphStackEntry(t3->token, graph_stack->Peek());
                graph_stack->Push(se);
                break;
            }
            case 3: // An operator
            {
                if (strcmp(t3->token, COMMA) == 0)
                    break;
                if (strcmp(t3->token, COLON) == 0)
                {
                    // printf("-->ITEM %d --> ", item);
                    GraphStackEntry *v = graph_stack->Pop();
                    GraphStackEntry *k = graph_stack->Pop();
                    if (k->token_value)
                    {
                        GraphStackEntry *d = graph_stack->Pop();
                        d->Set(k, v);
                        free(v);
                        free(k);
                        graph_stack->Push(d);
                    }
                    else
                    {
                        k->Set(v);
                        graph_stack->Push(k);
                    }
                }
                break;
            }
            case 4: // Open Bracket
            {
                if (strcmp(t3->token, BEGIN_ARRAY) == 0)
                    continue;
                se = new GraphStackEntry(ctx, graph_stack->Peek());
                graph_stack->Push(se);
                break;
            }
            case 5: // Close Bracket
            {
                if (strcmp(t3->token, END_ARRAY) == 0)
                    continue;
                se = graph_stack->Pop();
                // check edge/vertex vs graph!!!
                if (se->Contains(SUBJECT) && se->Contains(OBJECT) && se->Contains(PREDICATE))
                {
                    se->Dump("Edge");
                    se->Persist(ctx);
                }
                else
                {
                    se->Dump("Vertice");
                    se->Persist(ctx);
                }
                break;
            }
            }
        }
        listReleaseIterator(li);
        resetParser(graph_parser);
        free(graph_stack);

        return RedisModule_ReplyWithSimpleString(ctx, OK);
    }
    if (argc == -10)
        return RedisModule_WrongArity(ctx);
    const char *response = "Not yet omplemented";
    return RedisModule_ReplyWithSimpleString(ctx, response);
}

/*
  Return the graph for the given keys.

  The graph is a JSON with the following structure


  {
      entities: [
        "%iri%": {
                %hash%
        }, ...
      ],
      triplets:[
          {
              "object": "%iri%",
              "subject": "%iri%",
              "predicate": "%iri%"
          }, ...
      ]
  }

*4\r\n
$5\r\n
verse\r\n
$155\r\n
7 When Hiram heard Solomon's words, he rejoiced greatly and said: `May Jehovah be praised today, for he has given David a wise son over this great people!`\r\n
$4\r\n
type\r\n
$5\r\n
verse\r\n"
*/

char *consume_element(char *start, char *boundary, char **end, size_t *len, char **next)
{
    *end = strstr(start, CRLF);
    if (!end)
        printf("odd!\n");
    *len = *end - start;
    *next = *end + strlen(CRLF);
    if (*next >= boundary)
        *next = NULL;
    return start;
}

void emit_json(string &json, char *f, size_t fl, char *v, size_t vl, string &sep)
{
    json.append(sep);
    json.append(ESCAPED_APOSTROPH);
    json.append(f, fl);
    json.append("\": ");
    json.append(ESCAPED_APOSTROPH);
    json.append(v, vl);
    json.append(ESCAPED_APOSTROPH);
    sep.assign(",");
}

void emit_entity(RedisModuleCtx *ctx, const char *key, size_t keylen, string &json, string &json_sep, RedisModuleDict *touches);

class Triplet;

void emit_triplet(Triplet *triplet, string &json, string &json_sep, RedisModuleDict *touches);

class Triplet
{
private:
    void init(char *s, size_t sl, Triplet *origin, double weight)
    {
        this->subject = strdup(s);
        this->subject_len = sl;
        this->object = NULL;
        this->object_len = 0;
        this->predicate = NULL;
        this->predicate_len = 0;
        if (origin)
        {
            this->graph_length = origin->graph_length + weight;
        }
        else
        {
            this->graph_length = 0.0;
        }
    }

public:
    char *subject;
    size_t subject_len;
    char *object;
    size_t object_len;
    char *predicate;
    size_t predicate_len;
    double graph_length;

    Triplet(char *s, size_t sl)
    {
        init(s, sl, NULL, 0.0);
    }

    Triplet(char *s, size_t sl, char *o, size_t ol, char *p, size_t pl, Triplet *origin, double weight)
    {
        init(s, sl, origin, weight);
        if (o)
        {
            this->object = strdup(o);
            this->object_len = ol;
        }
        if (p)
        {
            this->predicate = strdup(p);
            this->predicate_len = pl;
        }
    }

    bool visited(char *key, size_t keylen, RedisModuleDict *entities_visited)
    {
        if (!key || keylen == 0)
            return false;
        int nokey;
        RedisModule_DictGetC(entities_visited, (void *)key, keylen, &nokey);
        if (nokey == 0)
            return true;
        RedisModule_DictSetC(entities_visited, (void *)key, keylen, (void *)NULL);
        return false;
    }

    void enqueue(char *key, size_t keylen, GraphStack<Triplet> &key_queue, RedisModuleCtx *ctx, RedisModuleDict *entities_visited, Triplet *origin)
    {
        if (key && !this->visited(key, keylen, entities_visited))
        {
            string edge_key;
            if (key[0] != TANDEM_PREFIX[0])
                edge_key.assign(TANDEM_PREFIX);
            edge_key.append(key, keylen);
            auto *r = RedisModule_Call(ctx, REDIS_CMD_SMEMBERS, "c",
                                       edge_key.c_str());
            if (r)
            {
                size_t proto_len;
                char *head = (char *)RedisModule_CallReplyProto(r, &proto_len);
                char *tail = head + proto_len;
                char *elem_start = head;
                char *elem_end;
                char *elem_next;
                size_t elem_len;
                elem_start = consume_element(elem_start, tail, &elem_end, &elem_len, &elem_next);
                elem_start = elem_next;

                GraphStack<Triplet> subjects;
                GraphStack<Triplet> objects;

                while (elem_next != NULL && *elem_next != 0)
                {
                    size_t skip_len;
                    size_t row_len;
                    // skip size
                    elem_start = consume_element(elem_start, tail, &elem_end, &skip_len, &elem_next);
                    elem_start = elem_next;
                    // get row name
                    char *row = elem_start = consume_element(elem_start, tail, &elem_end, &row_len, &elem_next);
                    char *bar_left = strchr(row, TANDEM_LINK_SEP[0]);
                    char *bar_right = strchr(bar_left + 1, TANDEM_LINK_SEP[0]);
                    char *weight_left = strchr(bar_right + 1, TANDEM_LINK_SEP[0]);
                    char *link = bar_right + 1;
                    double d = weight_left ? atof(weight_left + 1) : 1.0;
                    // printf("link from %s to %s flow %s weight: %0.0f\n",
                    //        sdsnewlen(key, keylen),
                    //        sdsnewlen(bar_left + 1, bar_right - bar_left - 1),
                    //        sdsnewlen(link, 2), d);
                    if (link[0] == toupper(SUBJECT[0]) || link[1] == toupper(SUBJECT[0]))
                    {
                        subjects.Add(new Triplet(bar_left + 1, bar_right - bar_left - 1, NULL, 0, key, keylen, origin, d));
                    }
                    else
                    {
                        objects.Add(new Triplet(bar_left + 1, bar_right - bar_left - 1, NULL, 0, key, keylen, origin, d));
                    }
                    elem_start = elem_next;
                }
                Triplet *s = NULL;
                Triplet *o = NULL;
                if (subjects.HasEntries() && objects.HasEntries())
                {
                    subjects.StartHead();
                    while ((s = subjects.Next()) != NULL)
                    {
                        objects.StartHead();
                        while ((o = objects.Next()) != NULL)
                        {
                            key_queue.Add(new Triplet(s->object ? s->object : s->subject, s->object ? s->object_len : s->subject_len,
                                                      o->object ? o->object : o->subject, o->object ? o->object_len : o->subject_len,
                                                      key, keylen,
                                                      origin,
                                                      0.0));
                        }
                        objects.Stop();
                    }
                    subjects.Stop();
                }
                else
                {
                    subjects.Join(&objects);
                    subjects.StartHead();
                    while ((s = subjects.Next()) != NULL)
                    {
                        key_queue.Add(new Triplet(s->object ? s->object : s->subject, s->object ? s->object_len : s->subject_len));
                    }
                    subjects.Stop();
                }
                RedisModule_FreeCallReply(r);
            }
        }
    }

    void enqueue(GraphStack<Triplet> &key_queue, RedisModuleCtx *ctx, RedisModuleDict *entities_visited, Triplet *origin)
    {
        this->enqueue(this->subject, this->subject_len, key_queue, ctx, entities_visited, origin);
        this->enqueue(this->object, this->object_len, key_queue, ctx, entities_visited, origin);
        this->enqueue(this->predicate, this->predicate_len, key_queue, ctx, entities_visited, origin);
    }

    void Expand(RedisModuleCtx *ctx, string &entities, string &entities_sep, string &triplets, string &triplets_sep, size_t max_graph_length, RedisModuleDict *entities_emitted, RedisModuleDict *triplets_emitted, RedisModuleDict *entities_visited)
    {

        string edge_key;
        // if(this->predicate){
        //     edge_key.assign(this->predicate, this->predicate_len);
        // }
        // else
        // {
        edge_key.assign(this->subject, this->subject_len);
        // }
        if (edge_key[0] != TANDEM_PREFIX[0])
            edge_key.insert(0, 1, TANDEM_PREFIX[0]);

        if (!this->visited((char *)edge_key.c_str(), edge_key.length(), entities_visited))
        {
            if (this->subject)
                emit_entity(ctx, this->subject, this->subject_len, entities, entities_sep, entities_emitted);
            if (this->object)
                emit_entity(ctx, this->object, this->object_len, entities, entities_sep, entities_emitted);
            if (this->predicate)
                emit_entity(ctx, this->predicate, this->predicate_len, entities, entities_sep, entities_emitted);

            auto *r = RedisModule_Call(ctx, REDIS_CMD_SMEMBERS, "c",
                                       edge_key.c_str());
            if (r)
            {
                size_t proto_len;
                char *head = (char *)RedisModule_CallReplyProto(r, &proto_len);
                char *tail = head + proto_len;
                char *elem_start = head;
                char *elem_end;
                char *elem_next;
                size_t elem_len;
                elem_start = consume_element(elem_start, tail, &elem_end, &elem_len, &elem_next);
                elem_start = elem_next;

                GraphStack<Triplet> subjects;
                GraphStack<Triplet> objects;

                while (elem_next != NULL && *elem_next != 0)
                {
                    size_t skip_len;
                    size_t row_len;
                    // skip size
                    elem_start = consume_element(elem_start, tail, &elem_end, &skip_len, &elem_next);
                    elem_start = elem_next;
                    // get row name
                    char *row = elem_start = consume_element(elem_start, tail, &elem_end, &row_len, &elem_next);
                    char *bar_left = strchr(row, TANDEM_LINK_SEP[0]);
                    char *bar_right = strchr(bar_left + 1, TANDEM_LINK_SEP[0]);
                    char *weight_left = strchr(bar_right + 1, TANDEM_LINK_SEP[0]);
                    char *link = bar_right + 1;
                    double d = weight_left ? atof(weight_left + 1) : 1.0;
                    // printf("link from %s to %s flow %s weight: %0.0f\n",
                    //        edge_key.c_str(),
                    //        sdsnewlen(bar_left + 1, bar_right - bar_left - 1),
                    //        sdsnewlen(link, 2), d);
                    if (link[0] == toupper(SUBJECT[0]) || link[1] == toupper(SUBJECT[0]))
                    {
                        subjects.Add(new Triplet(bar_left + 1, bar_right - bar_left - 1, NULL, 0, (char *)edge_key.c_str(), edge_key.length(), this, d));
                    }
                    else
                    {
                        objects.Add(new Triplet(bar_left + 1, bar_right - bar_left - 1, NULL, 0, (char *)edge_key.c_str(), edge_key.length(), this, d));
                    }
                    elem_start = elem_next;
                }
                Triplet *s = NULL;
                Triplet *o = NULL;
                if (subjects.HasEntries() && objects.HasEntries())
                {
                    subjects.StartHead();
                    while ((s = subjects.Next()) != NULL)
                    {
                        objects.StartHead();
                        while ((o = objects.Next()) != NULL)
                        {
                            if (this->graph_length < max_graph_length)
                            {
                                Triplet sub(s->object ? s->object : s->subject, s->object ? s->object_len : s->subject_len,
                                            o->object ? o->object : o->subject, o->object ? o->object_len : o->subject_len,
                                            (char *)edge_key.c_str(), edge_key.length(),
                                            this,
                                            0.0);
                                emit_triplet(&sub, triplets, triplets_sep, triplets_emitted);
                                s->Expand(ctx, entities, entities_sep, triplets, triplets_sep, max_graph_length, entities_emitted, triplets_emitted, entities_visited);
                                o->Expand(ctx, entities, entities_sep, triplets, triplets_sep, max_graph_length, entities_emitted, triplets_emitted, entities_visited);
                            }
                        }
                        objects.Stop();
                    }
                    subjects.Stop();
                }
                else
                {
                    subjects.Join(&objects);
                    subjects.StartHead();
                    while ((s = subjects.Next()) != NULL)
                    {
                        if (this->graph_length < max_graph_length)
                        {
                            s->Expand(ctx, entities, entities_sep, triplets, triplets_sep, max_graph_length, entities_emitted, triplets_emitted, entities_visited);
                        }
                    }
                    subjects.Stop();
                }
                RedisModule_FreeCallReply(r);
            }
        }
    }
};

size_t tally(const char *key, size_t keylen, const char token)
{
    char *p = (char *)strchr(key, token);
    char *pe = (char *)key + keylen;
    size_t count = 0;
    while (p && p < pe)
    {
        ++count;
        ++p;
        p = (char *)strchr(p, token);
    }
    return count;
}
void emit_entity(RedisModuleCtx *ctx, const char *key, size_t keylen, string &json, string &json_sep, RedisModuleDict *touches)
{
    if (!key || keylen == 0 || tally(key, keylen, COLON[0]) >= 2)
        return;
    if (key[0] == TANDEM_PREFIX[0])
    {
        ++key;
        --keylen;
    }
    int nokey;
    RedisModule_DictGetC(touches, (void *)key, keylen, &nokey);
    if (nokey == 0)
        return;
    RedisModule_DictSetC(touches, (void *)key, keylen, (void *)key);

    RedisModuleCallReply *r = RedisModule_Call(ctx, REDIS_CMD_HGETALL, "b", key, keylen);
    switch (RedisModule_CallReplyType(r))
    {
    case REDISMODULE_REPLY_ARRAY:
    {
        json.append(json_sep);
        json.append("{\"iri\":\"");
        json.append(key, keylen);
        json.append("\", \"entity\":\"");
        json.append(tally(key, keylen, COLON[0]) >= 2 ? EDGE : VERTICE);
        json.append(ESCAPED_APOSTROPH);
        string hash_sep(",");
        size_t proto_len;
        char *head = (char *)RedisModule_CallReplyProto(r, &proto_len);
        char *tail = head + proto_len;
        char *elem_start = head;
        char *elem_end;
        char *elem_next;
        size_t elem_len;
        elem_start = consume_element(elem_start, tail, &elem_end, &elem_len, &elem_next);
        elem_start = elem_next;
        while (elem_next != NULL && *elem_next != 0)
        {
            size_t skip_len;
            size_t field_len;
            // skip size
            elem_start = consume_element(elem_start, tail, &elem_end, &skip_len, &elem_next);
            elem_start = elem_next;
            // get field name
            char *field = elem_start = consume_element(elem_start, tail, &elem_end, &field_len, &elem_next);
            elem_start = elem_next;
            size_t value_len;
            // skip size
            elem_start = consume_element(elem_start, tail, &elem_end, &skip_len, &elem_next);
            elem_start = elem_next;
            // get field value
            char *value = elem_start = consume_element(elem_start, tail, &elem_end, &value_len, &elem_next);
            emit_json(json, field, field_len, value, value_len, hash_sep);
            elem_start = elem_next;
        }
        json.append("}");
        json_sep.assign((","));
    }
    break;
    default:
        break;
    }
    if (r)
        RedisModule_FreeCallReply(r);
}

void emit_triplet(Triplet *triplet, string &json, string &json_sep, RedisModuleDict *touches)
{
    int nokey;
    RedisModule_DictGetC(touches, (void *)triplet->predicate, triplet->predicate_len, &nokey);
    if (nokey == 0)
        return;
    RedisModule_DictSetC(touches, (void *)triplet->predicate, triplet->predicate_len, (void *)NULL);

    json.append(json_sep);
    json.append("{\"predicate\":\"");
    json.append(triplet->predicate + 1, triplet->predicate_len - 1);
    json.append("\", \"subject\":\"");
    json.append(triplet->subject + 1, triplet->subject_len - 1);
    json.append("\", \"object\":\"");
    json.append(triplet->object + 1, triplet->object_len - 1);
    json.append("\", \"depth\":");
    char buf[32];
    snprintf(buf, sizeof(buf), "%0.0f", triplet->graph_length);
    json.append(buf);
    json.append("}");
    json_sep.assign((","));
}

int g_get(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    size_t rootKey_len;
    size_t max_graph_length = 8;
    string entities(BEGIN_ARRAY);
    string entities_sep(EMPTY_STRING);
    string triplets(BEGIN_ARRAY);
    string triplets_sep(EMPTY_STRING);
    if (argc > 2)
    {
        const char *number = RedisModule_StringPtrLen(argv[2], &rootKey_len);
        max_graph_length = atol(number);
    }

    const char *rootKey = RedisModule_StringPtrLen(argv[1], &rootKey_len);
    RedisModuleDict *entities_emitted = RedisModule_CreateDict(ctx);
    RedisModuleDict *triplets_emitted = RedisModule_CreateDict(ctx);
    RedisModuleDict *entities_visited = RedisModule_CreateDict(ctx);
    GraphStack<Triplet> key_queue;
    key_queue.Add(new Triplet((char *)rootKey, rootKey_len));

    while (key_queue.HasEntries())
    {
        if (RedisModule_DictSize(entities_emitted) > 32)
            break;
        auto *triplet = key_queue.Pop();
        emit_entity(ctx, triplet->subject, triplet->subject_len, entities, entities_sep, entities_emitted);
        emit_entity(ctx, triplet->object, triplet->object_len, entities, entities_sep, entities_emitted);
        emit_entity(ctx, triplet->predicate, triplet->predicate_len, entities, entities_sep, entities_emitted);
        if (triplet->subject && triplet->object)
            emit_triplet(triplet, triplets, triplets_sep, triplets_emitted);
        if (triplet->graph_length < max_graph_length)
            triplet->enqueue(key_queue, ctx, entities_visited, triplet);
    }
    triplets.append(END_ARRAY);
    entities.append(END_ARRAY);

    RedisModule_FreeDict(ctx, entities_emitted);
    RedisModule_FreeDict(ctx, entities_visited);
    RedisModule_FreeDict(ctx, triplets_emitted);
    string result("{\"entities\":");
    result.append(entities);
    result.append(", \"triplets\":");
    result.append(triplets);
    result.append("}");

    return RedisModule_ReplyWithSimpleString(ctx, strdup(result.c_str()));
}

int get_deep(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    size_t rootKey_len;
    size_t max_graph_length = 8;
    string entities(BEGIN_ARRAY);
    string entities_sep(EMPTY_STRING);
    string triplets(BEGIN_ARRAY);
    string triplets_sep(EMPTY_STRING);
    if (argc > 2)
    {
        const char *number = RedisModule_StringPtrLen(argv[2], &rootKey_len);
        max_graph_length = atol(number);
    }

    const char *rootKey = RedisModule_StringPtrLen(argv[1], &rootKey_len);
    RedisModuleDict *entities_emitted = RedisModule_CreateDict(ctx);
    RedisModuleDict *triplets_emitted = RedisModule_CreateDict(ctx);
    RedisModuleDict *entities_visited = RedisModule_CreateDict(ctx);
    Triplet root((char *)rootKey, rootKey_len);
    root.Expand(ctx, entities, entities_sep, triplets, triplets_sep, max_graph_length, entities_emitted, triplets_emitted, entities_visited);

    triplets.append(END_ARRAY);
    entities.append(END_ARRAY);

    RedisModule_FreeDict(ctx, entities_emitted);
    RedisModule_FreeDict(ctx, entities_visited);
    RedisModule_FreeDict(ctx, triplets_emitted);
    string result("{\"entities\":");
    result.append(entities);
    result.append(", \"triplets\":");
    result.append(triplets);
    result.append("}");

    return RedisModule_ReplyWithSimpleString(ctx, strdup(result.c_str()));
}

int get_uml(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    UNUSED(ctx);
    UNUSED(argv);
    UNUSED(argc);
    return RedisModule_ReplyWithSimpleString(ctx, get_uml());
}

/* This function must be present on each R
edis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    UNUSED(argv);
    UNUSED(argc);
    initRxSuite();
    if (!graph_parser)
        graph_parser = newParser("graph");
    if (!sentence_parser)
        sentence_parser = newParser("text");

    if (RedisModule_Init(ctx, "graphdb", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    claimParsers();
    if (RedisModule_CreateCommand(ctx, "g.set",
                                  g_set, EMPTY_STRING, 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "g.get",
                                  g_get, EMPTY_STRING, 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "g.get_deep",
                                  get_deep, EMPTY_STRING, 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "g.uml",
                                  get_uml, EMPTY_STRING, 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx)
{
    UNUSED(ctx);
    if (!graph_parser)
        freeParser(graph_parser);
    if (!sentence_parser)
        freeParser(sentence_parser);
    releaseParsers();
    finalizeRxSuite();
    return REDISMODULE_OK;
}
