
#define REDISMODULE_EXPERIMENTAL_API
#include "rxGraphdb.h"
#include <string>

#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>

#include "client-pool.hpp"

using std::string;
#include "graphstack.hpp"
#include "rxGraphLoad-multiplexer.hpp"
#include "rxTextLoad-multiplexer.hpp"
#include "rxSuite.h"
#include "sjiboleth.hpp"

extern "C"
{
    int g_set(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
    int g_get(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

    int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

    int RedisModule_OnUnload(RedisModuleCtx *ctx);
}

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

#include "graphstackentry.hpp"

int text_load_async(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    auto *multiplexer = new RxTextLoadMultiplexer(argv, argc);
    multiplexer->Start(ctx);
    return REDISMODULE_OK;
};

int g_set_async(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    auto *multiplexer = new RxGraphLoadMultiplexer(argv, argc);
    multiplexer->Start(ctx);
    return REDISMODULE_OK;
};

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
        rxServerLog(rxLL_NOTICE, "odd!\n");
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
            auto *r = RedisModule_Call(ctx, WREDIS_CMD_SMEMBERS, "c",
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
                    // rxServerLog(rxLL_NOTICE, "link from %s to %s flow %s weight: %0.0f\n",
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

            auto *r = RedisModule_Call(ctx, WREDIS_CMD_SMEMBERS, "c",
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
                    // rxServerLog(rxLL_NOTICE, "link from %s to %s flow %s weight: %0.0f\n",
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

    RedisModuleCallReply *r = RedisModule_Call(ctx, WREDIS_CMD_HGETALL, "b", key, keylen);
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

/* This function must be present on each R
edis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    initRxSuite();

    if (RedisModule_Init(ctx, "rxGraphdb", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
    {
        rxServerLog(rxLL_NOTICE, "OnLoad rxGraphdb. Init error!");
        return REDISMODULE_ERR;
    }

    rxRegisterConfig((void **)argv, argc);

    if (RedisModule_CreateCommand(ctx, "g.set",
                                  g_set_async, "write", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "g.get",
                                  g_get, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "g.get_deep",
                                  get_deep, "readonly", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "load.text",
                                  text_load_async, "write", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    rxServerLog(rxLL_NOTICE, "OnLoad rxGraphdb. Done!");
    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx)
{
    UNUSED(ctx);
    finalizeRxSuite();
    return REDISMODULE_OK;
}
