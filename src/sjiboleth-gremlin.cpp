#include <cstring>
#include <cassert>

#include "sjiboleth-fablok.hpp"
#include "sjiboleth-graph.hpp"
#include "sjiboleth.h"
#include "sjiboleth.hpp"
#define STASHERS_ONLY
#include "graphstackentry.hpp"
#include "rxGraphdb.h"
#include "sdsWrapper.h"
#include "rule.hpp"

#include <math.h>

extern void *rxStashCommand(SimpleQueue *ctx, const char *command, int argc, ...);
extern void ExecuteRedisCommand(SimpleQueue *ctx, void *stash, const char *address, int port);
extern void FreeStash(void *stash);

#define MATCH_PATTERN 1
#define NO_MATCH_PATTERN 0
#define MATCH_REGEX 2

#define TRAVERSE_GETDATA 0
#define TRAVERSE_FILTERONLY 1

#define TRAVERSE_FINAL_VERTEX 0
#define TRAVERSE_FINAL_EDGE 1
#define TRAVERSE_FINAL_TRIPLET 2

static FaBlok *getAllKeysByRegex(int dbNo, const char *regex, int on_matched, const char *set_name);
static FaBlok *getAllKeysByType(int dbNo, const char *regex, int on_matched, rxString type_name);
static FaBlok *getAllKeysByField(int dbNo, const char *regex, int on_matched, rxString field, list *value, rxString keyset_name);

#define Graph_vertex_or_edge_type rxStringNew("type")

#define Graph_Subject_to_Edge rxStringNew("SE")
#define Graph_Object_to_Edge rxStringNew("OE")
#define Graph_Edge_to_Subject rxStringNew("ES")
#define Graph_Edge_to_Object rxStringNew("EO")

#define GRAPH_TRAVERSE_OUT 1
#define GRAPH_TRAVERSE_IN 2
#define GRAPH_TRAVERSE_INOUT 3

#define MATCH_MINIMIZE_PATH_LENGTH 1
#define MATCH_MAXIMIZE_PATH_LENGTH -1

#define MATCH_OUTPUT_DEFAULT 0
#define MATCH_OUTPUT_VERTEX 1
#define MATCH_OUTPUT_EDGE 2
#define MATCH_OUTPUT_TRIPLET 4

#define OBJECT_EXPRESSION_IS_CALCULATION 0
#define OBJECT_EXPRESSION_IS_PREDICATE 1023

#define TREND_OUTPUT_BASE 1
#define TREND_OUTPUT_SLOPE 2
#define TREND_OUTPUT_BASE_AND_SLOPE 3

typedef int (*tCalculateFunction)(const char *, int, const char *);
typedef double (*tComputeFunction)(const char *, int, const char *);

struct for_object_expressions_parameters
{
    // Object Expression Parameters
    ParserToken *t;
    SilNikParowy_Kontekst *stack;
    FaBlok *attr;
    FaBlok *val;
    short calc_or_test;
    short output;
    GraphStack<FaBlok> *fields;
};

struct for_filters_parameters
{
    // Filter parameters
    short must_match;
    rxString field;
    int flen;
    rxString pattern;
    int plen;
    rxComputeProc operatorFn;
    rxComparisonProc comparisonFn;
    rxString max_pattern;
};
struct for_calculations_parameters
{
    // Filter parameters
};

typedef union
{
    for_object_expressions_parameters for_object_expressions;
    for_filters_parameters for_filters;
    for_calculations_parameters for_calculations;
} operation_parameters;

// Saved context for asyn functions
RedisModuleCtx *loadCtx = NULL;

void *get_For_calculations(operation_parameters *p, ParserToken *t, SilNikParowy_Kontekst *stack, FaBlok *attr, GraphStack<FaBlok> *list, short output)
{
    p->for_object_expressions.t = t;
    p->for_object_expressions.stack = stack;
    p->for_object_expressions.attr = attr;
    p->for_object_expressions.calc_or_test = 0;
    p->for_object_expressions.fields = list;
    p->for_object_expressions.output = output;
    p->for_object_expressions.attr = attr;
    return (void *)p;
}

void *get_For_object_expressions(operation_parameters *p, ParserToken *t, SilNikParowy_Kontekst *stack, FaBlok *attr, FaBlok *val, short calc_or_test)
{
    p->for_object_expressions.t = t;
    p->for_object_expressions.stack = stack;
    p->for_object_expressions.attr = attr;
    p->for_object_expressions.val = val;
    p->for_object_expressions.calc_or_test = calc_or_test;
    return (void *)p;
}

void *get_For_filters(operation_parameters *p, short must_match, rxString field, rxString pattern, rxComputeProc operatorFn, rxString max_pattern)
{
    p->for_filters.must_match = must_match;
    p->for_filters.field = field;
    p->for_filters.flen = strlen(field);
    p->for_filters.pattern = pattern;
    p->for_filters.plen = strlen(pattern);
    p->for_filters.operatorFn = operatorFn;
    p->for_filters.max_pattern = max_pattern;
    return (void *)p;
}

static bool ExecuteObjectExpression(unsigned char *, size_t, void *data, void *privData)
{
    for_object_expressions_parameters params = ((operation_parameters *)privData)->for_object_expressions;

    auto inlen = params.stack->Size();
    params.stack->Execute(params.t->ObjectExpression(), data);
    auto outlen = params.stack->Size();
    if ((outlen - inlen) == 1)
    {

        FaBlok *r = params.stack->Pop();
        if (params.calc_or_test == OBJECT_EXPRESSION_IS_PREDICATE)
        {
            double v = atof(r->AsSds());
            return v != 0;
        }
        auto *receptor = params.t->BracketResult();
        if (r->value_type != KeysetDescriptor_TYPE_UNKNOWN)
        {
            rxHashTypeSet(data, (params.attr ? params.attr->AsSds() : (const char *)receptor->Operation()), r->AsSds(), 0);
        }
        else
        {
            rxHashTypeDelete(data, (params.attr ? params.attr->AsSds() : (const char *)receptor->Operation()));
        }
    }
    return true;
}

static int FilterTypes(unsigned char *, size_t, void *data, void *privData)
{
    for_filters_parameters params = ((operation_parameters *)privData)->for_filters;

    if (params.operatorFn == NULL)
        return rxMatchHasValue(data, params.field, params.pattern, params.plen) == params.must_match;
    else
    {
        params.comparisonFn = (rxComparisonProc)params.operatorFn;
        rxString value = rxStringEmpty();
        bool must_free = false;
        switch (rxGetObjectType(data))
        {
        case rxOBJ_STRING:
            value = (rxString)rxGetContainedObject(data);
            break;
        case rxOBJ_HASH:
            value = (rxString)rxGetHashField(data, params.field);
            must_free = true;
            break;
        }
        if (params.max_pattern != NULL)
            return ((rxComparisonProc2)params.operatorFn)(value ? value : "0", params.flen, params.pattern, params.max_pattern);
        int rc = params.comparisonFn(value ? value : "0", params.flen, params.pattern);
        if (must_free && value)
            rxStringFree(value);
        return rc;
    }
}
class MatchParameters
{
    /*
        The match routine(s) have many parameters.

        The match parameters are now collected in a single structure. This structure:
        1. Make it easier to pass the parameters thru many layers.
        2. Extend the possible parameters.
    */
public:
    SilNikParowy_Kontekst *stack;
    list *patterns;
    FaBlok *kd;
    int traverse_direction;
    short filter_only;
    rax *includes;
    rax *excludes;
    short final_vertex_or_edge;
    int optimization;
    const char *length_field;
    ParserToken *object_expression;

    MatchParameters(SilNikParowy_Kontekst *stack)
    {
        this->stack = stack;
        this->kd = NULL;
        this->traverse_direction = GRAPH_TRAVERSE_INOUT;
        this->filter_only = TRAVERSE_GETDATA;
        // Traversal edge type filters
        this->patterns = NULL;
        this->includes = NULL;
        this->excludes = NULL;
        // Result selection
        this->final_vertex_or_edge = MATCH_OUTPUT_DEFAULT;
        // Match Optimization
        this->optimization = MATCH_MINIMIZE_PATH_LENGTH;
        this->length_field = NULL;
        this->object_expression = NULL;
    }

    static MatchParameters *Get(SilNikParowy_Kontekst *stack)
    {
        auto *parameters = (MatchParameters *)stack->Recall("MatchParameters");
        if (parameters == NULL)
        {
            parameters = new MatchParameters(stack);
        }
        stack->Memoize("MatchParameters", parameters);
        return parameters;
    }

    static void Delete(SilNikParowy_Kontekst *stack)
    {
        auto *parameters = (MatchParameters *)stack->Recall("MatchParameters");
        if (parameters != NULL)
        {
            delete parameters;
        }
        stack->Forget("MatchParameters");
    }

    static MatchParameters *AddOptimization(SilNikParowy_Kontekst *stack, int optimization, FaBlok *p)
    {
        auto *parameters = MatchParameters::Get(stack);
        parameters->optimization = optimization;
        parameters->length_field = p->AsSds();
        if (p->ObjectExpression())
        {
            parameters->object_expression = p->ObjectExpression();
        }
        stack->Memoize("MatchParameters", parameters);
        return parameters;
    }

    static MatchParameters *AddIncludeEdgeFilter(SilNikParowy_Kontekst *stack, char inEx, rax *filter)
    {
        auto *parameters = MatchParameters::Get(stack);
        ((inEx == 'I') ? parameters->includes : parameters->excludes) = filter;
        return parameters;
    }

    static MatchParameters *AddPatterns(SilNikParowy_Kontekst *stack, list *patterns)
    {
        auto *parameters = MatchParameters::Get(stack);
        parameters->patterns = patterns;
        return parameters;
    }

    static MatchParameters *SetOutputType(SilNikParowy_Kontekst *stack, short traverse_direction, short filter_only, short final_vertex_or_edge)
    {
        auto *parameters = MatchParameters::Get(stack);
        parameters->traverse_direction = traverse_direction;
        parameters->filter_only = filter_only;
        parameters->final_vertex_or_edge = final_vertex_or_edge;
        return parameters;
    }
};

FaBlok *persistTriplet(SilNikParowy_Kontekst *stack, FaBlok *edge_set, const char *subject, const char *fwd_predicate, const char *bwd_predicate, const char *object, double weight, redisNodeInfo *data_config);
int breadthFirstSearch(FaBlok *leaders, FaBlok *kd, rax *terminators, MatchParameters *parms);

static int redis_HSET(SilNikParowy_Kontekst *, const char *key, const char *field, const char *value, const char *host_reference)
{
    auto *stash = rxStashCommand(NULL, "HSET", 3, key, field, value);
    ExecuteRedisCommand(NULL, stash, host_reference);
    FreeStash(stash);
    return C_OK;
}

static int redis_SADD(SilNikParowy_Kontekst *, const char *key, const char *member, const char *host_reference)
{
    auto *stash = rxStashCommand(NULL, "SADD", 2, key, member);
    ExecuteRedisCommand(NULL, stash, host_reference);
    FreeStash(stash);
    return C_OK;
}
static int redis_HSET_EDGE(SilNikParowy_Kontekst *stack, const char *key, const char *field, const char *value, const char *inverse_key, const char *host_reference)
{
    auto *stash = rxStashCommand(NULL, "HMSET", 5, key, field, value, "half", inverse_key);
    ExecuteRedisCommand(NULL, stash, host_reference);
    FreeStash(stash);
    rxString metatype = rxStringFormat("~%s", value);
    redis_SADD(stack, metatype, key, host_reference);
    rxStringFree(metatype);

    return C_OK;
}

FaBlok *AddKeyForEmptyBlok(FaBlok *e, SilNikParowy_Kontekst *stack)
{
    // TODO: Load index key set! lastly!
    if (e->size == 0)
    {
        auto *reuse = (FaBlok *)stack->Recall(e->setname);
        if (reuse)
        {
            // Reuse set
            FaBlok::Delete(e);
            return reuse;
        }

        void *o = rxFindKey(0, e->setname);
        if (o)
        {
            e->InsertKey(e->setname, o);
            e->size++;
        }
    }
    return e;
}

void AddKeyForEmptyBlok(FaBlok *e)
{
    // TODO: Load index key set! lastly!
    if (e->size == 0)
    {
        void *o = rxFindKey(0, e->setname);
        if (o)
        {
            e->InsertKey(e->setname, o);
            e->size++;
        }
    }
}
/*
127.0.0.1:6401> rxquery g:match(pebbles,bammbamm)
1) 1) "subject"
   2) "bammbamm"
   3) "length"
   4) "2"
   5) "object"
   6) "pebbles"
   7) "path"
   8) 1) "bammbamm"
      2) "wife_of:pebbles:bammbamm"
      3) "pebbles"
127.0.0.1:6401> rxquery g:match(bammbamm,roxy)
1) 1) "subject"
   2) "roxy"
   3) "length"
   4) "2"
   5) "object"
   6) "bammbamm"
   7) "path"
   8) 1) "roxy"
      2) "daugther_of:bammbamm:roxy"
      3) "bammbamm"
127.0.0.1:6401>

127.0.0.1:6401> rxquery g:match(barney,betty)
1) 1) "subject"
   2) "betty"                                   != (1)
   3) "length"
   4) "2"
   5) "object"
   6) "barney"
   7) "path"
   8) 1) "betty"                                              == (2a)
      2) "husband_of:barney:betty"
      3) "barney"
127.0.0.1:6401> rxquery g:match(betty, fred)
1) 1) "subject"
   2) "fred"                                    != (1)                        == (2b)
   3) "length"
   4) "6"
   5) "object"
   6) "betty"
   7) "path"
   8) 1) "fred"                                                               == (2b)
      2) "daugther_of:pebbles:fred"
      3) "pebbles"
      4) "wife_of:bammbamm:pebbles"
      5) "bammbamm"
      6) "mother_of:betty:bammbamm"
      7) "betty"                                              == (2a)

1) 1) "subject"
   2) "fred"
   3) "length"
   4) "6"
   5) "object"
   6) "betty" -----> barney
   7) "path"
   8) 1) "fred"
      2) "daugther_of:pebbles:fred"
      3) "pebbles"
      4) "wife_of:bammbamm:pebbles"
      5) "bammbamm"
      6) "mother_of:betty:bammbamm"
      7) "betty"
   8) 1) "betty"              -----> append
      2) "husband_of:barney:betty"
      3) "barney"



127.0.0.1:6401> rxquery g:match(fred, wilma)
1) 1) "subject"
   2) "wilma"
   3) "length"
   4) "2"
   5) "object"
   6) "fred"
   7) "path"
   8) 1) "wilma"
      2) "husband_of:fred:wilma"
      3) "fred"



1) 1) "subject"
   2) "fred" ----> wilma
   3) "length"
   4) "6"
   5) "object"
   6) barney
   7) "path"
   8) 1) "wilma"                      ----->prepend
      2) "husband_of:fred:wilma"
      3) "fred"
   8) 1) "fred"
      2) "daugther_of:pebbles:fred"
      3) "pebbles"
      4) "wife_of:bammbamm:pebbles"
      5) "bammbamm"
      6) "mother_of:betty:bammbamm"
      7) "betty"
   8) 1) "betty"
      2) "husband_of:barney:betty"
      3) "barney"

*/
void mergeMatches(FaBlok *result)
{
    if (raxSize(result->keyset) <= 1)
        return;
    raxIterator setIterator;
    raxStart(&setIterator, result->keyset);
    raxSeek(&setIterator, "^", NULL, 0);

    rxString first_key = rxStringNewLen((const char *)setIterator.key, setIterator.key_len);
    auto *first_data = setIterator.data;
    auto *first_triplet = (Graph_Triplet *)rxGetContainedObject(first_data);
    raxRemove(result->keyset, setIterator.key, setIterator.key_len, NULL);

    raxSeek(&setIterator, "^", NULL, 0);
    while (raxSize(result->keyset) > 0)
    {
        raxRemove(result->keyset, setIterator.key, setIterator.key_len, NULL);
        auto *second_data = setIterator.data;
        rxString second_key = rxStringNewLen((const char *)setIterator.key, setIterator.key_len);
        auto *second_triplet = (Graph_Triplet *)rxGetContainedObject(second_data);

        first_triplet->JoinEdges(second_triplet);

        first_key = rxStringFormat("%s->%s", first_key, second_key);
        rxStringFree(second_key);

        first_triplet = Graph_Triplet::Rename(first_triplet, first_key);
        // rxFreeObject(first_data);
        first_data = rxCreateObject(rxOBJ_TRIPLET, first_triplet);

        rxFreeObject(second_data);

        raxSeek(&setIterator, "^", NULL, 0);
    }
    result->InsertKey(first_key, first_data);
    rxStringFree(first_key);
}

int executeSingleMatch(const char *first_key, const char *second_key, SilNikParowy_Kontekst *, FaBlok *from, FaBlok *to, FaBlok *result, MatchParameters *parms)
{
    if (strcmp(first_key, second_key) == 0)
    {
        // No self matches!
        return 0;
    }
    from->InsertKey(first_key, rxFindKey(0, first_key));
    to->InsertKey(second_key, rxFindKey(0, second_key));

    int this_matches = breadthFirstSearch(from, result, to->keyset, parms);

    if (this_matches == 0)
    {
        // No match for section == no match for path!
        return -1;
    }
    mergeMatches(result);
    from->ClearKeys();
    to->ClearKeys();
    first_key = second_key;
    return 1;
}

void OptimizeMatch(FaBlok *result, int64_t row_size, rax *steps, const char *joined_keys)
{
    /*
        a) find the short edge aggregation=> 0...n - 1 vs 1 ... n
        b) any edge that touches all steps
    */
    raxIterator setIterator;
    raxStart(&setIterator, result->keyset);
    raxSeek(&setIterator, "^", NULL, 0);
    if (raxNext(&setIterator))
    {
        // rename to unified reference
        raxRemove(result->keyset, setIterator.key, setIterator.key_len, NULL);
        raxInsert(result->keyset, (UCHAR *)joined_keys, strlen(joined_keys), setIterator.data, NULL);
        auto *rim = (Graph_Triplet *)rxGetContainedObject(setIterator.data);
        double head_tally = 0;
        double tail_tally = 0;
        auto last_edge = rim->edges.Size() - 1;
        rim->edges.StartHead();
        Graph_Triplet_Edge *e = NULL;
        int pos = 0;
        int pivot_pos = -1;
        while ((e = rim->edges.Next()) != NULL)
        {
            if (pos != last_edge)
                head_tally += e->CalcLength();
            if (pos != 0)
                tail_tally += e->CalcLength();

            int touches = e->Number_of_Touches(steps);
            if (touches == row_size)
            {
                pivot_pos = pos;
                break;
            }
            pos++;
        }
        if (pivot_pos >= 0)
        {
            // delete all but touchdown_pos edge;
            rim->RemoveAllEdgesBut(pivot_pos);
        }
        else if (head_tally <= tail_tally)
        {
            // delete head edge;
            rim->RemoveLastEdge();
        }
        else
        {
            // delete tail edge;
            rim->RemoveFirstEdge();
        }
    }
    raxStop(&setIterator);
}

const char *Join_Keys(rax *steps)
{
    rxString j = rxStringEmpty();
    raxIterator setIterator;
    raxStart(&setIterator, steps);
    raxSeek(&setIterator, "^", NULL, 0);
    while (raxNext(&setIterator))
    {
        rxString k = rxStringNewLen((const char *)setIterator.key, setIterator.key_len);
        j = rxStringFormat("%s%s\t", j, k);
        rxStringFree(k);
    }
    raxStop(&setIterator);
    return j;
}

int executeMultiMatch(ParserToken *, SilNikParowy_Kontekst *stack, FaBlok *pl, MatchParameters *parms)
{
    GraphStack<GraphStack<const char>> permutes;
    FaBlok *set = AddKeyForEmptyBlok(pl->parameter_list->Dequeue(), stack);
    // 1. Baseline first set
    raxIterator setIterator;
    raxStart(&setIterator, set->keyset);
    raxSeek(&setIterator, "^", NULL, 0);
    while (raxNext(&setIterator))
    {
        rxString inner_key = rxStringNewLen((const char *)setIterator.key, setIterator.key_len);
        auto *inner = new GraphStack<const char>();
        inner->Enqueue(inner_key);
        permutes.Enqueue(inner);

        // rxStringFree(inner_key);
    }
    raxStop(&setIterator);
    // 2. Expand with next sets
    while (pl->parameter_list->HasEntries())
    {
        set = AddKeyForEmptyBlok(pl->parameter_list->Dequeue(), stack);
        AddKeyForEmptyBlok(set);
        // Permutes grows by each extra set
        int outer_size = permutes.Size();
        while (outer_size > 0)
        {
            outer_size--;
            auto *outer = permutes.Dequeue();
            raxStart(&setIterator, set->keyset);
            raxSeek(&setIterator, "^", NULL, 0);
            while (raxNext(&setIterator))
            {
                rxString inner_key = rxStringNewLen((const char *)setIterator.key, setIterator.key_len);
                auto *inner = new GraphStack<const char>();
                outer->CopyTo(inner);
                inner->Enqueue(inner_key);
                permutes.Enqueue(inner);
            }
            raxStop(&setIterator);
            delete outer;
        }
        FaBlok::Delete(set);
    }
    // 3. Find path for each adjacent pair
    // E.g. match(A, B, C) = match(A, B) + match(B, C)
    // Matching stops when no more pairs or when no match is found
    rxString leader_temp_setname = rxStringFormat("MULTIMATCH:::PATH:::%lld::FROM", ustime());
    auto *from = FaBlok::New(leader_temp_setname, KeysetDescriptor_TYPE_GREMLINSET);
    rxString terminator_temp_setname = rxStringFormat("MULTIMATCH:::PATH:::%lld::TO", ustime());
    auto *to = FaBlok::New(terminator_temp_setname, KeysetDescriptor_TYPE_GREMLINSET);
    rxString setname = rxStringFormat("MULTIMATCH:::PATH:::%lld::OUT", ustime());
    auto *final_result = FaBlok::New(setname, KeysetDescriptor_TYPE_GREMLINSET);
    setname = rxStringFormat("MULTIMATCH:::PATH:::%lld::OUT", ustime());
    auto *result = FaBlok::New(setname, KeysetDescriptor_TYPE_GREMLINSET);

    while (permutes.HasEntries())
    {
        auto *row = permutes.Dequeue();
        rax *steps = raxNew();
        row->StartHead();
        // rxString trace = rxStringNew("row: ");
        const char *k;
        while ((k = row->Next()) != NULL)
        {
            raxInsert(steps, (UCHAR *)k, strlen(k), NULL, NULL);
        }
        const char *joined_keys = Join_Keys(steps);
        if (!raxContains(final_result->keyset, joined_keys))
        {
            int64_t row_size = row->Size();

            const char *first_key = row->Dequeue();
            const char *start_key = first_key;
            const char *end_key = NULL;
            int nof_matches = 0;
            while (row->HasEntries())
            {
                const char *second_key = row->Dequeue();
                end_key = second_key;
                nof_matches += executeSingleMatch(first_key, second_key, stack, from, to, result, parms);
                first_key = second_key;
            }
            nof_matches += executeSingleMatch(start_key, end_key, stack, from, to, result, parms);

            // rxServerLog(rxLL_NOTICE, "%s => %d", trace, nof_matches);
            if (nof_matches == row_size)
            {
                OptimizeMatch(result, row_size, steps, joined_keys);
                result->CopyTo(final_result);
            }
        }
        result->ClearKeys();
        delete row;
        raxFree(steps);
        rxStringFree(joined_keys);
    }
    FaBlok::Delete(to);
    FaBlok::Delete(from);
    FaBlok::Delete(pl);
    PushResult(final_result, stack);
    return C_OK;
}

SJIBOLETH_HANDLER(GremlinDialect::executeMatch)
{
    STACK_CHECK(1);
    auto *parms = MatchParameters::SetOutputType(stack, GRAPH_TRAVERSE_INOUT, TRAVERSE_GETDATA, MATCH_OUTPUT_VERTEX);
    if (parms->includes && parms->excludes)
    {
        ERROR("Cannot use an exclude and an include filter together!");
    }

    FaBlok *pl = stack->Pop();
    FaBlok *result = NULL;
    raxIterator leadersIterator;
    if (pl->IsParameterList() && pl->parameter_list->Size() > 2)
    {
        executeMultiMatch(t, stack, pl, parms);
    }
    else if (pl->IsParameterList())
    {
        GraphStack<FaBlok> *cpy = pl->parameter_list->Copy();
        FaBlok *leaders = cpy->Dequeue();
        AddKeyForEmptyBlok(leaders);
        FaBlok *terminators = cpy->Dequeue();
        AddKeyForEmptyBlok(terminators);
        rxString setname = rxStringFormat("%s %s %s", leaders->setname, t->Token(), terminators->setname);
        result = FaBlok::Get(setname, KeysetDescriptor_TYPE_GREMLINSET);
        result->AsTemp();
        result->Open();

        raxStart(&leadersIterator, leaders->keyset);
        raxSeek(&leadersIterator, "^", NULL, 0);

        // Temporary stores for pairs<leader, terminator> to match!
        rxString leader_temp_setname = rxStringFormat("%s:::PATH:::%lld::FROM", t->Token(), ustime());
        FaBlok *from = FaBlok::New(leader_temp_setname, KeysetDescriptor_TYPE_GREMLINSET);
        rxStringFree(leader_temp_setname);
        rxString terminator_temp_setname = rxStringFormat("%s:::PATH:::%lld::TO", t->Token(), ustime());
        FaBlok *to = FaBlok::New(terminator_temp_setname, KeysetDescriptor_TYPE_GREMLINSET);
        rxStringFree(terminator_temp_setname);

        raxIterator terminatorsIterator;
        raxStart(&terminatorsIterator, terminators->keyset);

        while (raxNext(&leadersIterator))
        {
            rxString leader = rxStringNewLen((const char *)leadersIterator.key, leadersIterator.key_len);
            if (rxStringCharCount(leader, ':') < 2)
            {
                from->InsertKey(leadersIterator.key, leadersIterator.key_len, leadersIterator.data);

                raxSeek(&terminatorsIterator, "^", NULL, 0);
                while (raxNext(&terminatorsIterator))
                {
                    rxString terminator = rxStringNewLen((const char *)terminatorsIterator.key, terminatorsIterator.key_len);
                    if (rxStringMatch(leader, terminator, MATCH_IGNORE_CASE))
                        continue;
                    if (rxStringCharCount(terminator, ':') < 2)
                    {
                        to->InsertKey(terminatorsIterator.key, terminatorsIterator.key_len, terminatorsIterator.data);
                        breadthFirstSearch(from, result, to->keyset, parms);
                    }
                    to->RemoveKey(terminatorsIterator.key, terminatorsIterator.key_len);
                    rxStringFree(terminator);
                }
            }
            from->RemoveKey(leadersIterator.key, leadersIterator.key_len);
            raxStop(&terminatorsIterator);
            rxStringFree(leader);
        }
        raxStop(&leadersIterator);
        FaBlok::Delete(to);
        FaBlok::Delete(from);
        // result = FaBlok::Get(setname, KeysetDescriptor_TYPE_GREMLINSET);
        PushResult(result, stack);
        rxStringFree(setname);
        delete cpy;
    }
    else
        PushResult(pl, stack);

    FaBlok::Delete(pl);
    MatchParameters::Delete(stack);
}
END_SJIBOLETH_HANDLER(GremlinDialect::executeMatch)

SJIBOLETH_HANDLER(GremlinDialect::executeNomatch)
{
    STACK_CHECK(1);
    auto *parms = MatchParameters::SetOutputType(stack, GRAPH_TRAVERSE_INOUT, TRAVERSE_GETDATA, MATCH_OUTPUT_VERTEX);
    if (parms->includes && parms->excludes)
    {
        ERROR("Cannot use an exclude and an include filter together!");
    }

    FaBlok *pl = stack->Pop();
    raxIterator leadersIterator;
    raxIterator terminatorsIterator;
    if (pl->IsParameterList())
    {
        GraphStack<FaBlok> *cpy = pl->parameter_list->Copy();
        FaBlok *leaders = cpy->Dequeue();
        AddKeyForEmptyBlok(leaders);
        FaBlok *terminators = cpy->Dequeue();
        AddKeyForEmptyBlok(terminators);
        rxString setname = rxStringFormat("%s %s %s", leaders->setname, t->Token(), terminators->setname);
        FaBlok *nomatches = FaBlok::Get(setname, KeysetDescriptor_TYPE_GREMLINSET);
        FaBlok *matches_for_leader = FaBlok::Get(setname, KeysetDescriptor_TYPE_GREMLINSET);
        rxStringFree(setname);
        matches_for_leader->Open();
        raxStart(&leadersIterator, leaders->keyset);
        raxStart(&terminatorsIterator, terminators->keyset);

        raxSeek(&leadersIterator, "^", NULL, 0);

        // Temporary stores for pairs<leader, terminator> to match!
        rxString leader_temp_setname = rxStringFormat("%s:::PATH:::%lld::FROM", t->Token(), ustime());
        FaBlok *from = FaBlok::New(leader_temp_setname, KeysetDescriptor_TYPE_GREMLINSET);
        rxString terminator_temp_setname = rxStringFormat("%s:::PATH:::%lld::TO", t->Token(), ustime());
        FaBlok *to = FaBlok::New(terminator_temp_setname, KeysetDescriptor_TYPE_GREMLINSET);
        rxStringFree(leader_temp_setname);
        rxStringFree(terminator_temp_setname);

        raxIterator terminatorsIterator;
        raxStart(&terminatorsIterator, terminators->keyset);

        auto *parms = MatchParameters::Get(stack);
        while (raxNext(&leadersIterator))
        {
            rxString leader = rxStringNewLen((const char *)leadersIterator.key, leadersIterator.key_len);
            if (rxStringCharCount(leader, ':') < 2)
            {
                from->InsertKey(leadersIterator.key, leadersIterator.key_len, leadersIterator.data);

                raxSeek(&terminatorsIterator, "^", NULL, 0);
                while (raxNext(&terminatorsIterator))
                {
                    rxString terminator = rxStringNewLen((const char *)terminatorsIterator.key, terminatorsIterator.key_len);
                    if (rxStringMatch(leader, terminator, MATCH_IGNORE_CASE))
                        continue;
                    if (rxStringCharCount(terminator, ':') < 2)
                    {
                        rxString pair = rxStringFormat("%s->%s", leader, terminator);
                        FaBlok *result = FaBlok::New(terminator_temp_setname, KeysetDescriptor_TYPE_GREMLINSET);
                        to->InsertKey(terminatorsIterator.key, terminatorsIterator.key_len, terminatorsIterator.data);
                        breadthFirstSearch(from, result, to->keyset, parms);
                        if (raxSize(result->keyset) == 0)
                        {
                            auto *origin = Graph_Leg::New(leader, 0.0, NULL);
                            auto *terminal = Graph_Leg::New(terminator, -1.0, NULL);
                            terminal->origin = origin;
                            auto *tobj = (Graph_Triplet *)Graph_Triplet::NewForMatch(0, terminal, leader, NULL, parms->optimization);
                            Graph_Triplet::Link(tobj, nomatches);
                            tobj->CalcLength();
                            nomatches->AddKey(pair, tobj);
                        }
                        FaBlok::Delete(result);
                        rxStringFree(pair);
                    }
                    to->RemoveKey(terminatorsIterator.key, terminatorsIterator.key_len);
                    rxStringFree(terminator);
                }
            }
            from->RemoveKey(leadersIterator.key, leadersIterator.key_len);
            raxStop(&terminatorsIterator);
            rxStringFree(leader);
        }
        raxStop(&leadersIterator);
        FaBlok::Delete(to);
        FaBlok::Delete(from);
        // result = FaBlok::Get(setname, KeysetDescriptor_TYPE_GREMLINSET);
        PushResult(nomatches, stack);
        rxStringFree(setname);
        delete cpy;
    }
    else
        PushResult(pl, stack);

    FaBlok::Delete(pl);
    MatchParameters::Delete(stack);
}
END_SJIBOLETH_HANDLER(GremlinDialect::executeNomatch)

// SJIBOLETH_HANDLER(executeNomatch_ORG)
// {
//     rxUNUSED(t);
//     STACK_CHECK(1);

//     FaBlok *pl = stack->Pop();
//     if (pl->IsParameterList())
//     {
//         while (pl->parameter_list->Size() >= 2)
//         {
//             Sjiboleth::executeNotIn(pO, tO, pl->parameter_list);
//         }

//         PushResult(pl->parameter_list->Pop(), stack);
//         FaBlok::Delete(pl);
//     }
//     else
//         PushResult(pl, stack);
// }
// END_SJIBOLETH_HANDLER(GremlinDialect::executeNomatch)

SJIBOLETH_HANDLER(GremlinDialect::executeGremlinMatchInExclude)
{

    STACK_CHECK(1);
    rax *filter = raxNew();
    FaBlok *pl = stack->Pop();
    GraphStack<const char> q;

    if (pl->IsParameterList())
    {
        while (pl->parameter_list->HasEntries())
        {
            FaBlok *f = pl->parameter_list->Dequeue();
            q.Push(f->setname);
            FaBlok::Delete(f);
        }
    }
    else
    {
        q.Push(pl->setname);
    }
    FaBlok::Delete(pl);

    while (q.HasEntries())
    {
        const char *predicate = q.Pop();
        rxString metatype = rxStringFormat("~%s", predicate);
        auto *members = rxFindSetKey(0, metatype);
        if (members != NULL)
        {
            void *si = NULL;
            rxString member;
            int64_t l;
            while (rxScanSetMembers(members, &si, (char **)&member, &l) != NULL)
            {
                if (member[0] == '`' || member[0] == '~')
                {
                    q.Push(member);
                    rxStringFree(member);
                    continue;
                }
                rxStringFree(member);
                // Skip nodes!
            }
        }
        else
        {
            int offset = (predicate[0] == '`' || predicate[0] == '~') ? 1 : 0;
            raxInsert(filter, (UCHAR *)predicate + offset, strlen(predicate) - offset, stack, NULL);
        }
        rxStringFree(metatype);
    }

    stack->Memoize((toupper(t->Token()[0]) == 'I') ? "matchIncludes" : "matchExcludes", filter);
    MatchParameters::AddIncludeEdgeFilter(stack, toupper(t->Token()[0]), filter);
}
END_SJIBOLETH_HANDLER(GremlinDialect::executeGremlinMatchInExclude)

/*
    [ minimise | maximize ](field)
 */
SJIBOLETH_HANDLER(GremlinDialect::executeGremlinMatchMinimize)
{
    rxUNUSED(t);
    STACK_CHECK(1);
    FaBlok *p = stack->Pop();
    MatchParameters::AddOptimization(stack, MATCH_MINIMIZE_PATH_LENGTH, p);
    FaBlok::Delete(p);
}
END_SJIBOLETH_HANDLER(GremlinDialect::executeGremlinMatchMinimize)
SJIBOLETH_HANDLER(GremlinDialect::executeGremlinMatchMaximize)
{
    rxUNUSED(t);
    STACK_CHECK(1);
    FaBlok *p = stack->Pop();
    MatchParameters::AddOptimization(stack, MATCH_MAXIMIZE_PATH_LENGTH, p);
    FaBlok::Delete(p);
}
END_SJIBOLETH_HANDLER(GremlinDialect::executeGremlinMatchMaximize)

FaBlok *LoadKeySetFromMetaType(FaBlok *kd, rxString metaType)
{
    GraphStack<const char> q;

    q.Push(metaType);

    while (q.HasEntries())
    {
        const char *a_type = q.Pop();
        auto *members = rxFindSetKey(0, a_type);
        if (members == NULL)
            continue;
        auto mob = rxHarvestSetmembers(members);
        if (mob)
        {
            char **p = (char **)&mob->members;
            for (size_t n = 0; n < mob->member_count; ++n)
            {
                char *member = *p;
                if (member[0] == '`' || member[0] == '~')
                {
                    q.Push(member);
                    continue;
                }
                kd->InsertKey(member, rxFindKey(0, member));
                ++p;
            }
            rxFreeSetmembers(mob);
        }
    }
    return kd;
}
/*
    GremlinDialect::executeAllVertices

    Create a stack entry with a set of all or selected vertices
    and push it onto the stack.

    1) When the stack has entries:
        v(%setname%) -> Not needed just (%setname%)
        v(%iri%)
        v(%vertice type%)
        v(%label%, %value%)
        v(%label%, (%low%,%high%)
        v(%label%, %value% %op% %value)
        a) When the stack entry is a parameter list:
        b) When the stack entry is NOT a parameter list:

    2) When the stack is empty:


*/

SJIBOLETH_HANDLER(GremlinDialect::executeAllVertices)
{
    rxUNUSED(t);

    FaBlok *kd = NULL;
    if (stack->HasEntries())
    {
        kd = stack->Pop();
        if (kd->IsValueType(KeysetDescriptor_TYPE_GREMLIN_EDGE_SET) || kd->IsValueType(KeysetDescriptor_TYPE_GREMLIN_VERTEX_SET) || kd->IsValueType(KeysetDescriptor_TYPE_GREMLINSET))
        {
            PushResult(kd, stack);
            // No tokens on Stack
            kd = getAllKeysByRegex(0, "[^~`][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*", NO_MATCH_PATTERN, "vertices");
        }
        else
        {
            kd->Open();
            if (kd->IsParameterList())
            {
                FaBlok *ad = kd->parameter_list->Dequeue();
                rxString setname = rxStringDup(ad->AsSds());
                list *patterns = listCreate();
                int key_or_pattern_mode = 2;
                auto *key_entry = rxFindKey(0, ad->setname);
                if (key_entry != NULL)
                {
                    key_or_pattern_mode = 4;
                    kd->AddKey(ad->AsSds(), key_entry);
                    setname = rxStringNew("Keys: ");
                    setname = rxStringFormat("%s%s", setname, ad->AsSds());
                }
                else
                {
                    setname = rxStringDup(ad->AsSds());
                    setname = rxStringFormat("%s%s", setname, " in");
                }
                while (kd->parameter_list->HasEntries())
                {
                    FaBlok *vd = kd->parameter_list->Dequeue();
                    setname = rxStringFormat("%s%s", setname, " ");
                    setname = rxStringFormat("%s%s", setname, vd->AsSds());
                    if (key_or_pattern_mode == 2)
                        patterns = listAddNodeTail(patterns, strdup(vd->AsSds()));
                    else
                    {
                        key_entry = rxFindKey(0, vd->setname);
                        kd->AddKey(vd->AsSds(), key_entry);
                    }
                }
                if (key_or_pattern_mode == 2)
                {
                    FaBlok::Delete(kd);
                    kd = getAllKeysByField(0, "[^~`][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*", NO_MATCH_PATTERN, ad->AsSds(), patterns, setname);
                }
                else
                    kd->Rename(setname);
                listRelease(patterns);
            }
            else
            {
                // One token on Stack!
                redisNodeInfo *index_config = rxIndexNode();

                rxString metatype = rxStringFormat("`%s", kd->AsSds());
                auto *key_entry = rxFindKey(0, metatype);
                if (key_entry != NULL && rxGetObjectType(key_entry) == rxOBJ_SET)
                    LoadKeySetFromMetaType(kd, metatype);
                else
                {
                    key_entry = rxFindKey(0, kd->AsSds());
                    if (key_entry != NULL)
                        kd->LoadKey(0, kd->setname);
                    else if (kd->FetchKeySet(index_config, kd->setname) == C_ERR)
                        kd = getAllKeysByType(0, "[^~`][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*", NO_MATCH_PATTERN, kd->AsSds());
                }
                rxStringFree(metatype);
            }
        }
    }
    else
    {
        // No tokens on Stack
        kd = getAllKeysByRegex(0, "[A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*", NO_MATCH_PATTERN, "vertices");
    }
    kd->AsTemp();
    kd->vertices_or_edges = VERTEX_SET;
    kd->ValueType(KeysetDescriptor_TYPE_GREMLIN_VERTEX_SET);
    kd->Close();
    PushResult(kd, stack);
    // stack->Memoize("V", kd);
}
END_SJIBOLETH_HANDLER(GremlinDialect::executeAllVertices)

SJIBOLETH_HANDLER(executeAllEdges)
{
    rxUNUSED(t);

    FaBlok *kd = NULL;
    if (stack->HasEntries())
    {
        kd = stack->Pop();
        if (kd->IsValueType(KeysetDescriptor_TYPE_GREMLIN_EDGE_SET) || kd->IsValueType(KeysetDescriptor_TYPE_GREMLIN_VERTEX_SET) || kd->IsValueType(KeysetDescriptor_TYPE_GREMLINSET))
        {
            // Ignore stack entry for sets!
            PushResult(kd, stack);
            return C_OK;
            // No tokens on Stack
        }
        else
        {
            if (kd->IsParameterList())
            {
                // 1: e(pred0, pred1, ...)
                // 2: e(pred0, subject)
                // 3: e(pred0, subject, object)
                FaBlok *predicate0 = kd->parameter_list->Dequeue();
                FaBlok *predicate1 = kd->parameter_list->Dequeue();
                rxString metatype = rxStringFormat("~%s", predicate1->AsSds());
                auto *key_entry = rxFindKey(0, metatype);
                rxStringFree(metatype);
                kd->parameter_list->Push(predicate1);
                kd->parameter_list->Push(predicate0);
                if (key_entry != NULL)
                {
                    // Assume all predicates
                    while (kd->parameter_list->HasEntries())
                    {
                        FaBlok *vd = kd->parameter_list->Dequeue();
                        metatype = rxStringFormat("~%s", vd->AsSds());
                        auto *key_entry = rxFindKey(0, metatype);
                        if (key_entry != NULL)
                            LoadKeySetFromMetaType(kd, metatype);
                        rxStringFree(metatype);
                        FaBlok::Delete(vd);
                    }
                }
                else
                {
                    // Assume, partial, triplet
                    FaBlok *predicate = kd->parameter_list->Dequeue();
                    FaBlok *subject = kd->parameter_list->Dequeue();
                    rxString pattern = rxStringFormat("%s:%s", predicate->AsSds(), subject->AsSds());
                    if (kd->parameter_list->HasEntries())
                    {
                        FaBlok *object = kd->parameter_list->Dequeue();
                        pattern = rxStringFormat("%s:%s", pattern, object->AsSds());
                        FaBlok::Delete(object);
                    }
                    else
                    {
                        pattern = rxStringFormat("%s*", pattern);
                    }
                    FaBlok::Delete(subject);
                    FaBlok::Delete(predicate);
                    kd = getAllKeysByRegex(0, pattern, MATCH_REGEX, pattern);
                    rxStringFree(pattern);
                }
            }
            else
            {
                redisNodeInfo *index_config = rxIndexNode();
                rxString metatype = rxStringFormat("~%s", kd->AsSds());
                auto *key_entry = rxFindKey(0, metatype);
                if (key_entry != NULL)
                    LoadKeySetFromMetaType(kd, metatype);
                else
                {
                    if (kd->FetchKeySet(index_config, kd->setname) == C_ERR)
                        kd = getAllKeysByType(0, "[A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*", MATCH_PATTERN, kd->AsSds());
                }
                rxStringFree(metatype);
            }
        }
    }
    else
    {
        // Filter vertices from top stack entry
        kd = getAllKeysByRegex(0, "[A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*", MATCH_PATTERN, "edges");
    }
    kd->AsTemp();
    kd->vertices_or_edges = EDGE_SET;
    kd->ValueType(KeysetDescriptor_TYPE_GREMLIN_EDGE_SET);
    PushResult(kd, stack);
    // stack->Memoize("E", kd);
}
END_SJIBOLETH_HANDLER(executeAllEdges)

/*
    executeGremlinParameters

    Collects 2 or more parameters.

    a: (%p0% , %p1%)
                v(familiy, raak)
    b: (%pl% , %pn%)
                v(birthyear, 1960, 1970)
*/
SJIBOLETH_HANDLER(GremlinDialect::executeGremlinParameters)
{
    rxUNUSED(t);

    STACK_CHECK(2);

    FaBlok *second = stack->Pop();
    FaBlok *first = stack->Pop();
    if (first->IsParameterList())
    {
        first->parameter_list->Enqueue(second);
        PushResult(first, stack);
        return C_OK;
    }
    rxString setname = rxStringFormat("P_%lld", ustime());
    FaBlok *pl = FaBlok::Get(setname, KeysetDescriptor_TYPE_PARAMETER_LIST);
    pl->AsTemp();
    pl->parameter_list = new GraphStack<FaBlok>();
    pl->parameter_list->Enqueue(first);
    pl->parameter_list->Enqueue(second);
    rxStringFree(setname);
    PushResult(pl, stack);
}
END_SJIBOLETH_HANDLER(GremlinDialect::executeGremlinParameters)

// Pop the 'as' set
// When the top stack entry has a dictionary
// Then
//     a) push the that set
// Else
//     a) pop the
//     b) copy set from the stack
//     c) push the copied set
SJIBOLETH_HANDLER(executeGremlinAs)
{

    rxUNUSED(t);
    STACK_CHECK(2);

    FaBlok *sd = stack->Pop();
    sd->Open();
    FaBlok *base = stack->Pop();
    base->ValueType(KeysetDescriptor_TYPE_GREMLIN_VERTEX_SET + KeysetDescriptor_TYPE_GREMLIN_AS_SET);
    stack->Memoize(sd->setname, base->Copy(NULL, KeysetDescriptor_TYPE_GREMLIN_VERTEX_SET + KeysetDescriptor_TYPE_GREMLIN_AS_SET, NULL, NULL));
    FaBlok::Delete(sd);    
    PushResult(base, stack);
}
END_SJIBOLETH_HANDLER(executeGremlinAs)

SJIBOLETH_HANDLER(executeGremlinUse)
{

    rxUNUSED(t);
    STACK_CHECK(1);

    FaBlok *sd = stack->Pop();
    sd->Open();
    FaBlok *reuse = (FaBlok *)stack->Recall(sd->setname);
    if (reuse)
    {
        // Reuse set
        FaBlok::Delete(sd);
        sd = reuse;
        sd->reuse_count++;
    }
    PushResult(sd, stack);
}
END_SJIBOLETH_HANDLER(executeGremlinUse)

#define MATCH_IS_FALSE 0
#define MATCH_IS_TRUE 1
static SJIBOLETH_HANDLER(executeGremlinHas)
{
    rxUNUSED(t);
    rxUNUSED(stack);

    short must_match = t->Is("HAS") || t->Is("EQ") ? MATCH_IS_TRUE : MATCH_IS_FALSE;

    /* 1) Use top of stack = dict of keys/values.
       2) Filter all keys matching kvp
       3) Push filtered dict on the stack
    */

    FaBlok *vd = stack->Pop();
    FaBlok *ad;
    if (vd->IsParameterList())
    {
        FaBlok *p = vd;
        ad = p->parameter_list->Dequeue();
        vd = p->parameter_list->Dequeue();
        FaBlok::Delete(p);
    }
    else
        ad = stack->Pop();
    if (ad == NULL)
    {
        rxString msg = rxStringFormat("%s requires an attribute value pair to test", t->Token());
        ERROR(msg);
        rxServerLogRaw(rxLL_WARNING, msg);
        // rxStringFree(msg);
        return C_ERR;
    }
    else if (!ad->IsValueType(KeysetDescriptor_TYPE_SINGULAR))
    {
        rxString msg = rxStringFormat("%s requires an attribute value pair to test", t->Token());
        ERROR(msg);
        rxServerLogRaw(rxLL_WARNING, msg);
        // rxStringFree(msg);
        return C_ERR;
    }
    if (!stack->HasEntries())
        GremlinDialect::executeAllVertices(tO, stackO);
    FaBlok *od = stack->Pop();
    if (od == NULL)
        od = (FaBlok *)stack->Recall("V");
    if (od == NULL)
    {
        od = getAllKeysByRegex(0, "[A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*", NO_MATCH_PATTERN, "vertices");
    }
    rxString set_name;
    if (od)
    {
        set_name = rxStringFormat("%s:%s==%s", od->AsSds(), ad->AsSds(), vd->AsSds());
    }
    else
    {
        set_name = rxStringFormat("%s==%s", ad->AsSds(), vd->AsSds());
    }
    // defaultKeysetDescriptorAsTemp(kd);
    od->AsTemp();
    // rxServerLog(rxLL_DEBUG, "About to execute executeGremlinHas on %s", set_name);
    // rxStringFree(msg);
    operation_parameters p;
    auto *params = get_For_filters(&p, must_match, ad->AsSds(), vd->AsSds(), NULL, NULL);
    FaBlok *kd = od->Copy(set_name, KeysetDescriptor_TYPE_GREMLINSET, FilterTypes, params);
    kd->AsTemp();
    PushResult(kd, stack);
}
END_SJIBOLETH_HANDLER(executeGremlinHas)

SJIBOLETH_HANDLER(executeGremlinComparePropertyToValue)
{
    STACK_CHECK(2);
    /* 1) Use top of stack = dict of keys/values.
       2) Filter all keys matching kvp
       3) Push filtered dict on the stack
    */
    stack->DumpStack();
    FaBlok *condition = stack->Pop();
    if (condition->IsParameterList())
    {
        FaBlok *keyset = stack->Pop();
        // if (!(condition->IsParameterList() && condition->parameter_list->Size() == 2)){
        //     ERROR("property and number required")
        // }

        FaBlok *property = condition->parameter_list->Dequeue();
        FaBlok *value = condition->parameter_list->Dequeue();
        FaBlok::Delete(condition);

        rxString set_name = rxStringFormat("%s:%s%s%s", keyset->AsSds(), property->AsSds(), t->Operation(), value->AsSds());

        keyset->AsTemp();
        operation_parameters p;
        auto *params = get_For_filters(&p, 1, property->AsSds(), value->AsSds(), (rxComputeProc)rxFindComputationProc((char *)t->Operation()), NULL);
        FaBlok *kd = keyset->Copy(set_name, KeysetDescriptor_TYPE_GREMLINSET, FilterTypes, params);
        kd->AsTemp();
        rxStringFree(set_name);
        PushResult(kd, stack);
    }
    else
    {
        FaBlok *property = stack->Pop();
        FaBlok *keyset = stack->Pop();
        if (keyset)
        {
            rxString set_name = rxStringFormat("%s:%s%s%s", keyset->AsSds(), property->AsSds(), t->Operation(), condition->AsSds());

            keyset->AsTemp();
            operation_parameters p;
            auto *params = get_For_filters(&p, 1, property->AsSds(), condition->AsSds(), rxFindComputationProc((char *)t->Operation()), NULL);
            FaBlok *kd = keyset->Copy(set_name, KeysetDescriptor_TYPE_GREMLINSET, FilterTypes, params);
            kd->AsTemp();
            rxStringFree(set_name);
            PushResult(kd, stack);
            FaBlok::Delete(property);
            FaBlok::Delete(condition);
            FaBlok::Delete(keyset);
        }
        else
        {
            auto fnCallback = (rxComparisonProc)rxFindComparisonProc((char *)t->Operation());
            FaBlok *kd;
            if (fnCallback != NULL)
            {
                rxString l = property->AsSds();
                size_t ll = strlen(l);
                rxString r = condition->AsSds();
                kd = FaBlok::New((fnCallback(l, ll, r) == 0 ? "1" : "0"), KeysetDescriptor_TYPE_SINGULAR);
            }
            else
                kd = FaBlok::New("0", KeysetDescriptor_TYPE_SINGULAR);
            kd->AsTemp();
            FaBlok::Delete(condition);
            FaBlok::Delete(property);
            PushResult(kd, stack);
        }
    }
    rxServerLog(rxLL_NOTICE, "executeGremlinComparePropertyToValue %s 0000", t->TokenAsSds());
    stack->DumpStack();
    rxServerLog(rxLL_NOTICE, "executeGremlinComparePropertyToValue %s 9999", t->TokenAsSds());
}
END_SJIBOLETH_HANDLER(executeGremlinComparePropertyToValue)

SJIBOLETH_HANDLER(executeGremlinCompareExpression)
{
    STACK_CHECK(2);
    /* 1) Use top of stack = dict of keys/values.
       2) Filter all keys matching kvp
       3) Push filtered dict on the stack
    */
    stack->DumpStack();
    FaBlok *condition = stack->Pop();
    if (!condition->ObjectExpression())
    {
        stack->AddError("where requires an object expression");
        return C_ERR;
    }
    FaBlok *keyset = stack->Pop();
    // void *params[] = {condition->ObjectExpression(), stack, NULL, NULL, OBJECT_EXPRESSION_IS_PREDICATE};
    operation_parameters p;
    auto *params = get_For_object_expressions(&p, condition->ObjectExpression(), stack, NULL, NULL, OBJECT_EXPRESSION_IS_PREDICATE);
    FaBlok *output_set = keyset->Copy(keyset->AsSds(), KeysetDescriptor_TYPE_GREMLINSET, ExecuteObjectExpression, params);
    PushResult(output_set, stack);
}
END_SJIBOLETH_HANDLER(executeGremlinCompareExpression)

SJIBOLETH_HANDLER(executeGremlinComputeOnPropertyValue)
{
    STACK_CHECK(1);
    /* 1) Use top of stack = dict of keys/values.
       2) Filter all keys matching kvp
       3) Push filtered dict on the stack
    */
    stack->DumpStack();
    // if (condition->IsParameterList())
    // {
    // }
    // else
    {
        FaBlok *checkFirst = stack->Pop();
        FaBlok *right;
        FaBlok *left;
        if (checkFirst->IsParameterList())
        {
            right = checkFirst->parameter_list->Pop();
            left = checkFirst->parameter_list->Pop();
        }
        else
        {
            right = checkFirst;
            left = stack->Pop();
        }

        if (
            (right->ValueType() == KeysetDescriptor_TYPE_SINGULAR || right->ValueType() == KeysetDescriptor_TYPE_UNKNOWN) &&
            (left->ValueType() == KeysetDescriptor_TYPE_SINGULAR || left->ValueType() == KeysetDescriptor_TYPE_UNKNOWN))
        {
            int right_len = strlen(right->AsSds());
            auto fnCallback = rxFindComputationProc((char *)t->Operation());
            double outcome = fnCallback(right->AsSds(), right_len, left->AsSds());
            char propertyValue[64];
            snprintf(propertyValue, sizeof(propertyValue), "%f", outcome);
            FaBlok *kd = FaBlok::Get(propertyValue, KeysetDescriptor_TYPE_SINGULAR);
            PushResult(kd, stack);
        }
        else
        {

            rxString set_name = rxStringFormat("%s%s%s", left->AsSds(), t->Operation(), right->AsSds());
            operation_parameters p;
            auto *params = get_For_filters(&p, 1, right->AsSds(), left->AsSds(), rxFindComputationProc((char *)t->Operation()), NULL);
            FaBlok *kd = left->Copy(set_name, KeysetDescriptor_TYPE_GREMLINSET, FilterTypes, params);
            kd->AsTemp();
            rxStringFree(set_name);
            PushResult(kd, stack);
        }
    }
}
END_SJIBOLETH_HANDLER(executeGremlinComputeOnPropertyValue)

SJIBOLETH_HANDLER(executeGremlinComputeOnSinglePropertyValue)
{
    STACK_CHECK(1);
    /* 1) Use top of stack = dict of keys/values.
       2) Filter all keys matching kvp
       3) Push filtered dict on the stack
    */
    // if (condition->IsParameterList())
    // {
    // }
    // else
    {
        FaBlok *left = stack->Pop();

        if (left->ValueType() == KeysetDescriptor_TYPE_SINGULAR)
        {
            int left_len = strlen(left->AsSds());
            auto fnCallback = rxFindComputationProc((char *)t->Operation());
            if (fnCallback)
            {
                double outcome = fnCallback(left->AsSds(), left_len, "0.5");
                char propertyValue[64];
                snprintf(propertyValue, sizeof(propertyValue), "%f", outcome);
                FaBlok *kd = FaBlok::Get(propertyValue, KeysetDescriptor_TYPE_SINGULAR);
                PushResult(kd, stack);
            }
            else
                PushResult(FaBlok::Get("0", KeysetDescriptor_TYPE_SINGULAR), stack);
        }
        else
        {

            // rxString set_name = rxStringFormat("%s%s%s", left->AsSds(), t->Operation(), right->AsSds());
            // operation_parameters p;
            // auto *params = get_For_filters(&p, 1, right->AsSds(), (void *)left->AsSds(), (tCalculateFunction)rxFindComparisonProc((char *)t->Operation()), NULL);
            // FaBlok *kd = left->Copy(set_name, KeysetDescriptor_TYPE_GREMLINSET, FilterTypes, params);
            // kd->AsTemp();
            // rxStringFree(set_name);
            // PushResult(kd, stack);
        }
    }
}
END_SJIBOLETH_HANDLER(executeGremlinComputeOnSinglePropertyValue)

// this->RegisterSyntax("trend_slope", 30, 2, 1, &calculateTrendSlope);
// this->RegisterSyntax("trend_base", 30, 2, 1, &calculateTrendBase);
// this->RegisterSyntax("trend", 30, 2, 1, &calculateTrend);

typedef struct
{
    double boundary;
    char *label;
} slope_range;

slope_range ranges[] = {
    /*slope_range*/ {-1.0, "Agressive Shrinking"}, // [indef .. -1]
    /*slope_range*/ {-0.5, "Shrinking"},           // [-1 .. -0.5]
    /*slope_range*/ {-0.25, "Steady Shrinking"},   // [-0.5 .. -0.25]
    /*slope_range*/ {-0.1, "Agressive Shrink"},    // [-0.25 .. -0.1]
    /*slope_range*/ {-0.1, "Stable"},              // [-0.1 .. 0.1]
    /*slope_range*/ {-0.25, "Steady Growth"},
    /*slope_range*/ {-0.5, "Growth"},
    /*slope_range*/ {-1.0, "Agressive Growth"}};

static bool
calculateTrend(unsigned char *, size_t, void *data, void *privData)
{
    for_object_expressions_parameters params = ((operation_parameters *)privData)->for_object_expressions;

    auto inlen = params.stack->Size();
    params.stack->Execute(params.t->ObjectExpression(), data);
    auto outlen = params.stack->Size();
    if ((outlen - inlen) == 1)
    {

        FaBlok *r = params.stack->Pop();
        auto fields = r->parameter_list;
        FaBlok *field;
        double n = 0.0;
        double sum_var_Y_X = 0.0;
        double sum_var_Y_Y = 0.0;
        double sum_Y = 0.0;
        double sum_Y_Y = 0.0;
        double sum_X = 0.0;
        double sum_X_X = 0.0;
        double sum_Y_X = 0.0;
        double avg_Y = 0.0;
        double avg_X = 0.0;
        double B1 = 0.0;
        double B0 = 0.0;
        // 1) Calc average
        fields->StartHead();
        while ((field = fields->Next()) != NULL)
        {
            double v = atof(field->AsSds());
            n += 1.0;
            sum_Y += v;
            sum_Y_Y += pow(v, 2);
            sum_X += n;
            sum_X_X += pow(n, 2);
            sum_Y_X += v * n;
        }
        avg_Y = sum_Y / n;
        avg_X = sum_X / n;
        fields->Stop();
        // 2) Calc squates
        fields->StartHead();
        n = 0.0;
        while ((field = fields->Next()) != NULL)
        {
            double v = atof(field->AsSds());
            n += 1.0;
            sum_var_Y_X += ((v - avg_Y) * (n - avg_X));
            sum_var_Y_Y += pow(v - avg_Y, 2);
        }
        // https://math.stackexchange.com/questions/204020/what-is-the-equation-used-to-calculate-a-linear-trendline
        //
        // https://www.graphpad.com/quickcalcs/linear1/

        B0 = (n * sum_Y_X - sum_X * sum_Y) / (n * sum_X_X - pow(sum_X, 2));
        B1 = (sum_Y - B0 * sum_X) / n;
        fields->Stop();

        slope_range *range = (slope_range *)&ranges;
        int n_ranges = sizeof(ranges) / sizeof(slope_range);
        while (n_ranges)
        {
            if (B0 <= range->boundary)
            {
                break;
            }
            ++range;
            --n_ranges;
        }

        auto *receptor = params.t->BracketResult();
        char buf[64];
        snprintf(buf, sizeof(buf), "a=%0.3f b=%0.3f t=%0.3f t2=%0.3f 1/slope=%0.3f trend:%s", B0, B1, atan(B0), atan2(B0, 1), 1 / B0, range->label);
        if (r->value_type != KeysetDescriptor_TYPE_UNKNOWN)
        {
            rxHashTypeSet(data, (params.attr ? params.attr->AsSds() : (const char *)receptor->Operation()), buf, 0);
        }
        else
        {
            rxHashTypeDelete(data, (params.attr ? params.attr->AsSds() : (const char *)receptor->Operation()));
        }
    }
    return true;
}

SJIBOLETH_HANDLER(calculateTrendSlope)
{
    STACK_CHECK(1);
    FaBlok *parm_list = stack->Pop();
    FaBlok *input_set = stack->Pop();
    if (parm_list->IsParameterList() || parm_list->ObjectExpression())
    {

        operation_parameters p;
        auto *params = get_For_calculations(&p, parm_list->ObjectExpression(), stack, NULL, parm_list->parameter_list, TREND_OUTPUT_SLOPE);
        FaBlok *output_set = input_set->Copy("trend", KeysetDescriptor_TYPE_GREMLINSET, calculateTrend, params);
        output_set->AsTemp();
        PushResult(output_set, stack);
    }
}
END_SJIBOLETH_HANDLER(calculateTrendSlope)

// TODO: Implement functions like sqrt, will add field to edge or vertex (temporary)
// TODO: this is true for any + - * / operator
// TODO: What will be the property?
SJIBOLETH_HANDLER(executeGremlinCalcSingleProperty)
{
    STACK_CHECK(1);
    /* 1) Use top of stack = dict of keys/values.
       2) Filter all keys matching kvp
       3) Push filtered dict on the stack
    */
    stack->DumpStack();
    // FaBlok *property = stack->Pop();
    // FaBlok *keyset = stack->Pop();

    // rxString set_name = rxStringFormat("%s:%s:%s", keyset->AsSds(), t->Operation(), property->AsSds());

    // operation_parameters p;
    // auto *params = get_For_filters(&p, 1, property->AsSds(), value->AsSds(), (tCalculateFunction)rxFindComparisonProc((char *)t->Operation()), NULL);
    // FaBlok *kd = keyset->Copy(set_name, KeysetDescriptor_TYPE_GREMLINSET, FilterTypes, params);
    // kd->AsTemp();
    // rxStringFree(set_name);
    // PushResult(kd, stack);
}
END_SJIBOLETH_HANDLER(executeGremlinCalcSingleProperty)

SJIBOLETH_HANDLER(executeGremlinComparePropertyToRangeValue)
{
    STACK_CHECK(2);
    /* 1) Use top of stack = dict of keys/values.
       2) Filter all keys matching kvp
       3) Push filtered dict on the stack
    */

    FaBlok *condition = stack->Pop();
    FaBlok *keyset = stack->Pop();
    // if (!(condition->IsParameterList() && condition->parameter_list->Size() == 2)){
    //     ERROR("property and number required")
    // }

    FaBlok *property = condition->parameter_list->Dequeue();
    FaBlok *minValue = condition->parameter_list->Dequeue();
    FaBlok *maxValue = condition->parameter_list->Dequeue();
    FaBlok::Delete(condition);

    rxString set_name = rxStringFormat("%s:%s %s %s %s", keyset->AsSds(), property->AsSds(), t->Operation(), minValue->AsSds(), maxValue->AsSds());

    keyset->AsTemp();

    operation_parameters p;
    auto *params = get_For_filters(&p, 1, property->AsSds(), minValue->AsSds(), rxFindComputationProc((char *)t->Operation()), maxValue->AsSds());
    FaBlok *kd = keyset->Copy(set_name, KeysetDescriptor_TYPE_GREMLINSET, FilterTypes, params);
    kd->AsTemp();
    PushResult(kd, stack);
}
END_SJIBOLETH_HANDLER(executeGremlinComparePropertyToRangeValue)

// TODO: implement pow
SJIBOLETH_HANDLER(executeGremlinExecutePropertyWithParm)
{
    STACK_CHECK(2);
    /* 1) Use top of stack = dict of keys/values.
       2) Filter all keys matching kvp
       3) Push filtered dict on the stack
    */

    FaBlok *condition = stack->Pop();
    FaBlok *keyset = stack->Pop();
    // if (!(condition->IsParameterList() && condition->parameter_list->Size() == 2)){
    //     ERROR("property and number required")
    // }

    FaBlok *property = condition->parameter_list->Dequeue();
    FaBlok *minValue = condition->parameter_list->Dequeue();
    FaBlok *maxValue = condition->parameter_list->Dequeue();
    FaBlok::Delete(condition);

    rxString set_name = rxStringFormat("%s:%s %s %s %s", keyset->AsSds(), property->AsSds(), t->Operation(), minValue->AsSds(), maxValue->AsSds());

    keyset->AsTemp();

    operation_parameters p;
    auto computation = rxFindComputationProc((char *)t->Operation());
    auto *params = get_For_filters(&p, 1, property->AsSds(), minValue->AsSds(), computation, maxValue->AsSds());
    FaBlok *kd = keyset->Copy(set_name, KeysetDescriptor_TYPE_GREMLINSET, FilterTypes, params);
    kd->AsTemp();
    PushResult(kd, stack);
}
END_SJIBOLETH_HANDLER(executeGremlinExecutePropertyWithParm)

static SJIBOLETH_HANDLER(executeGremlinHasLabel)
{
    rxUNUSED(t);
    rxUNUSED(stack);

    short must_match = t->Is("HASLABEL") ? MATCH_IS_TRUE : MATCH_IS_FALSE;

    /* 1) Use top of stack = dict of keys/values.
       2) Filter all keys matching kvp
       3) Push filtered dict on the stack
    */

    FaBlok *ad = stack->Pop();

    FaBlok *od = stack->Pop();
    if (od == NULL)
        od = (FaBlok *)stack->Recall("V");
    if (od == NULL)
        od = getAllKeysByRegex(0, "[A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*", NO_MATCH_PATTERN, "vertices");
    rxString set_name;
    if (od)
    {
        set_name = rxStringFormat("%s:%s %s", od->AsSds(), t->Token(), ad->AsSds());
    }
    else
    {
        set_name = rxStringFormat("%s %s", t->Token(), ad->AsSds());
    }
    operation_parameters p;
    auto *params = get_For_filters(&p, must_match, ad->AsSds(), rxStringNew("*"), 0, NULL);
    FaBlok *kd = od->Copy(set_name, KeysetDescriptor_TYPE_GREMLINSET, FilterTypes, params);
    od->AsTemp();
    kd->AsTemp();
    PushResult(kd, stack);
}
END_SJIBOLETH_HANDLER(executeGremlinHasLabel)

/*
 * Aggregate a set of triplets, grouped by subject or object.
 * The subject/object are swapped on the result.
 */
SJIBOLETH_HANDLER(executeGremlinGroupby)
{

    rxUNUSED(t);
    STACK_CHECK(2);
    FaBlok *gbd = stack->Pop();
    FaBlok *kd = stack->Pop();
    gbd->IsValid();
    kd->IsValid();
    rxString setname = rxStringFormat("%s %s %s", kd->setname, t->Token(), gbd->setname);
    FaBlok *output_set = FaBlok::Get(setname, KeysetDescriptor_TYPE_GREMLINSET);
    output_set->Open();
    {
        raxIterator ungroupedIterator;
        raxStart(&ungroupedIterator, kd->keyset);
        raxSeek(&ungroupedIterator, "^", NULL, 0);
        while (raxNext(&ungroupedIterator))
        {
            if (ungroupedIterator.data != NULL)
            {
                rxString key_to_match = rxStringNewLen((const char *)ungroupedIterator.key, ungroupedIterator.key_len);
                rxUNUSED(key_to_match); // TODO
                rxString groupKey;
                switch (rxGetObjectType(ungroupedIterator.data))
                {
                case rxOBJ_HASH:
                {
                    groupKey = rxGetHashField(ungroupedIterator.data, gbd->setname);
                    CGraph_Triplet *ge = groupKey ? (Graph_Triplet *)output_set->LookupKey(groupKey) : NULL;
                    if (ge != NULL)
                    {
                        Graph_Triplet::AddObjectToTripletResult(ge, key_to_match, rxStringEmpty());
                    }
                    else
                    {
                        ge = Graph_Triplet::InitializeResult(0, NULL, groupKey, key_to_match, rxStringEmpty(), 1);
                        output_set->AddKey(groupKey, ge);
                    }
                }
                break;
                case rxOBJ_TRIPLET:
                {
                    Graph_Triplet *t = (Graph_Triplet *)rxGetContainedObject(ungroupedIterator.data);
                    if (toupper(gbd->setname[0]) == 'S')
                    {
                        groupKey = t->subject_key;
                        CGraph_Triplet *ge = (CGraph_Triplet *)output_set->LookupKey(groupKey);
                        if (ge == NULL)
                        {
                            ge = Graph_Triplet::InitializeResult(0, NULL, groupKey, NULL, rxStringEmpty(), 1);
                            output_set->AddKey(groupKey, ge);
                        }
                        Graph_Triplet::Join(ge, t);
                    }
                    else
                    {
                        t->StartObjectBrowse();
                        Graph_Triplet_Edge *e;
                        while ((e = t->NextObject()) != NULL)
                        {
                            groupKey = e->object_key;
                            CGraph_Triplet *ge = (CGraph_Triplet *)output_set->LookupKey(groupKey);
                            if (ge == NULL)
                            {
                                ge = Graph_Triplet::InitializeResult(0, NULL, groupKey, NULL, rxStringEmpty(), 1);
                                output_set->AddKey(groupKey, ge);
                            }
                            Graph_Triplet::AddObjectToTripletResult(ge, t->subject_key, rxStringEmpty());
                        }
                        t->StopObjectBrowse();
                    }
                }
                break;
                default:
                    ERROR("Unsupported object type, only hash keys or rx triplets are supported.");
                    // return C_ERR;
                    break;
                }
            }
        }
        raxStop(&ungroupedIterator);
    }
    rxStringFree(setname);
    PushResult(output_set, stack);
}
END_SJIBOLETH_HANDLER(executeGremlinGroupby)

// Do a breadth first for any vertex or edge of type <out>
int executeGremlinTraverse(SilNikParowy_Kontekst *stack, MatchParameters *parms)
{
    /* 1) Use top of stack = dict of keys/values.
       2) Filter all outgoing edges having the designated attribute value
       3) Push filtered dict on the stack
    */

    FaBlok *vd = stack->Pop();
    FaBlok *od = stack->Pop();
    list *patterns = listCreate();
    if (vd->IsParameterList())
    {
        while (vd->parameter_list->HasEntries())
        {
            FaBlok *p = vd->parameter_list->Dequeue();
            listAddNodeTail(patterns, (void *)strdup(p->setname));
            if (p->is_temp == KeysetDescriptor_TYPE_MONITORED_SET || p->IsValueType(KeysetDescriptor_TYPE_MONITORED_SET))
            {
                continue;
            }
            // deleteKeysetDescriptor(p);
        }
        vd->parameter_list = NULL;
        FaBlok::Delete(vd);
    }
    else
    {
        listAddNodeHead(patterns, (void *)strdup(vd->setname));
    }
    if (!od)
    {
        od = getAllKeysByRegex(0, "[A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*", NO_MATCH_PATTERN, "vertices");
    }

    rxString set_name = rxStringFormat("%s->%s", od->setname, vd->setname);
    FaBlok *kd = FaBlok::Get(set_name, KeysetDescriptor_TYPE_GREMLINSET);
    parms = MatchParameters::AddPatterns(stack, patterns);
    breadthFirstSearch(od, kd, NULL, parms);
    PushResult(kd, stack);
    listRelease(patterns);
    rxStringFree(set_name);
    MatchParameters::Delete(stack);
    return C_OK;
}

// Do a breadth first for any vertex or edge of type <out>
SJIBOLETH_HANDLER(executeGremlinOut)
{

    rxUNUSED(t);
    STACK_CHECK(1);
    auto *parms = MatchParameters::SetOutputType(stack, GRAPH_TRAVERSE_OUT, TRAVERSE_GETDATA, TRAVERSE_FINAL_VERTEX);
    return executeGremlinTraverse(stack, parms);
}
END_SJIBOLETH_HANDLER_X(executeGremlinOut)

// Do a breadth first for any vertex or edge of type <out>
SJIBOLETH_HANDLER(executeGremlinIn)
{

    rxUNUSED(t);
    STACK_CHECK(1);
    auto *parms = MatchParameters::SetOutputType(stack, GRAPH_TRAVERSE_IN, TRAVERSE_GETDATA, TRAVERSE_FINAL_VERTEX);
    return executeGremlinTraverse(stack, parms);
}
END_SJIBOLETH_HANDLER_X(executeGremlinIn)

// Do a breadth first for any vertex or edge of type <out>
SJIBOLETH_HANDLER(executeGremlinInOut)
{

    rxUNUSED(t);
    STACK_CHECK(1);
    auto *parms = MatchParameters::SetOutputType(stack, GRAPH_TRAVERSE_INOUT, TRAVERSE_GETDATA, TRAVERSE_FINAL_VERTEX);
    return executeGremlinTraverse(stack, parms);
}
END_SJIBOLETH_HANDLER_X(executeGremlinInOut)

// Do a breadth first for any vertex or edge of type <out>
SJIBOLETH_HANDLER(executeGremlinOutTriplet)
{

    rxUNUSED(t);
    STACK_CHECK(1);
    auto *parms = MatchParameters::SetOutputType(stack, GRAPH_TRAVERSE_OUT, TRAVERSE_GETDATA, TRAVERSE_FINAL_EDGE);
    return executeGremlinTraverse(stack, parms);
}
END_SJIBOLETH_HANDLER_X(executeGremlinOutTriplet)

// Do a breadth first for any vertex or edge of type <out>
SJIBOLETH_HANDLER(executeGremlinInTriplet)
{

    rxUNUSED(t);
    STACK_CHECK(1);
    auto *parms = MatchParameters::SetOutputType(stack, GRAPH_TRAVERSE_IN, TRAVERSE_GETDATA, TRAVERSE_FINAL_EDGE);
    return executeGremlinTraverse(stack, parms);
}
END_SJIBOLETH_HANDLER_X(executeGremlinInTriplet)

// Do a breadth first for any vertex or edge of type <out>
SJIBOLETH_HANDLER(executeGremlinInOutTriplet)
{

    rxUNUSED(t);
    STACK_CHECK(1);
    auto *parms = MatchParameters::SetOutputType(stack, GRAPH_TRAVERSE_IN, TRAVERSE_GETDATA, TRAVERSE_FINAL_EDGE);
    return executeGremlinTraverse(stack, parms);
}
END_SJIBOLETH_HANDLER_X(executeGremlinInOutTriplet)

// Do a breadth first for any edge of type <out>
SJIBOLETH_HANDLER(executeGremlinOutEdge)
{

    rxUNUSED(t);
    STACK_CHECK(1);
    auto *parms = MatchParameters::SetOutputType(stack, GRAPH_TRAVERSE_OUT, TRAVERSE_GETDATA, TRAVERSE_FINAL_EDGE);
    return executeGremlinTraverse(stack, parms);
}
END_SJIBOLETH_HANDLER_X(executeGremlinOutEdge)

// Do a breadth first for any vertex or edge of type <out>
SJIBOLETH_HANDLER(executeGremlinInEdge)
{

    rxUNUSED(t);
    STACK_CHECK(1);
    auto *parms = MatchParameters::SetOutputType(stack, GRAPH_TRAVERSE_IN, TRAVERSE_GETDATA, TRAVERSE_FINAL_EDGE);
    return executeGremlinTraverse(stack, parms);
}
END_SJIBOLETH_HANDLER_X(executeGremlinInEdge)

// Do a breadth first for any vertex or edge of type <out>
SJIBOLETH_HANDLER(executeGremlinInOutEdge)
{

    rxUNUSED(t);
    STACK_CHECK(1);
    auto *parms = MatchParameters::SetOutputType(stack, GRAPH_TRAVERSE_INOUT, TRAVERSE_GETDATA, TRAVERSE_FINAL_EDGE);
    return executeGremlinTraverse(stack, parms);
}
END_SJIBOLETH_HANDLER_X(executeGremlinInOutEdge)

// Do a breadth first to filter any vertex or edge with an <out> type
SJIBOLETH_HANDLER(executeGremlinHasOut)
{

    rxUNUSED(t);
    STACK_CHECK(1);
    auto *parms = MatchParameters::SetOutputType(stack, GRAPH_TRAVERSE_OUT, TRAVERSE_FILTERONLY, TRAVERSE_FINAL_VERTEX);
    return executeGremlinTraverse(stack, parms);
}
END_SJIBOLETH_HANDLER_X(executeGremlinHasOut)

// Do a breadth first to filter any vertex or edge with an <in> type
SJIBOLETH_HANDLER(executeGremlinHasIn)
{

    rxUNUSED(t);
    STACK_CHECK(1);
    auto *parms = MatchParameters::SetOutputType(stack, GRAPH_TRAVERSE_IN, TRAVERSE_FILTERONLY, TRAVERSE_FINAL_VERTEX);
    return executeGremlinTraverse(stack, parms);
}
END_SJIBOLETH_HANDLER_X(executeGremlinHasIn)

// Do a breadth first to filter any vertex or edge with an <in/out> type
SJIBOLETH_HANDLER(executeGremlinHasInOut)
{

    rxUNUSED(t);
    STACK_CHECK(1);
    auto *parms = MatchParameters::SetOutputType(stack, GRAPH_TRAVERSE_INOUT, TRAVERSE_FILTERONLY, TRAVERSE_FINAL_VERTEX);
    return executeGremlinTraverse(stack, parms);
}
END_SJIBOLETH_HANDLER_X(executeGremlinHasInOut)

FaBlok *createVertex(SilNikParowy_Kontekst *stack, FaBlok *pl)
{
    if (!pl->IsParameterList() || pl->parameter_list->Size() < 2)
    {
        ERROR_RETURN_NULL("Incorrect parameters for AddV(ertex), must be <iri>, <type>");
    }
    redisNodeInfo *data_config = rxDataNode();

    FaBlok *i = pl->parameter_list->Dequeue();
    void *o = rxFindKey(0, i->setname);
    if (o)
    {
        i->LoadKey(0, i->setname);
        FaBlok::Delete(pl);
        return i;
    }
    FaBlok *t = pl->parameter_list->Dequeue();
    redis_HSET(stack, i->setname, Graph_vertex_or_edge_type, t->setname, data_config->host_reference);
    o = rxFindKey(0, i->setname);
    FaBlok *v = FaBlok::Get(i->setname, KeysetDescriptor_TYPE_GREMLINSET);
    v->AddKey(i->setname, o);
    rxString metatype = rxStringFormat("`%s", t->setname);
    redis_SADD(stack, metatype, i->setname, data_config->host_reference);
    rxStringFree(metatype);
    FaBlok::Delete(pl);
    return v;
}

SJIBOLETH_HANDLER(executeGremlinAddVertex)
{
    rxUNUSED(t);
    // Get parameters g:addV(<iri>, <type>)
    // Get parameters g:e().addV(<iri>, <type>)
    FaBlok *pl = stack->Pop();
    FaBlok *v = createVertex(stack, pl);
    if (v)
    {
        v->vertices_or_edges = VERTEX_SET;
        PushResult(v, stack);
    }
}
END_SJIBOLETH_HANDLER(executeGremlinAddVertex)

class droppert
{
public:
    const char *to_drop_key;
    const char *to_drop_members_key;
    void *to_drop_hash;
    const char *one_side;
    const char *other_side;
    void *to_drop_set;
    rxSetMembers *members;
};

SJIBOLETH_HANDLER(executeGremlinDropVertexOrEdge)
{
    rxUNUSED(t);
    // Get parameters g:V(...).drop()
    FaBlok *v = stack->Pop();

    GraphStack<const char> queue{};
    droppert dropper{};

    raxIterator drop_iterator;
    raxStart(&drop_iterator, v->keyset);
    raxSeek(&drop_iterator, "^", NULL, 0);
    while ((raxNext(&drop_iterator)))
    {
        dropper.to_drop_key = rxStringNewLen((const char *)drop_iterator.key, drop_iterator.key_len);
        dropper.to_drop_members_key = rxStringFormat("^%s", dropper.to_drop_key);
        rxServerLog(rxLL_NOTICE, "--A--> From key set to queue: %s", dropper.to_drop_key);

        if (rxStringMatch("*[:]*[:]*", dropper.to_drop_key, 1))
        { // Is Edge
            rxServerLog(rxLL_NOTICE, "--AA--> drop edge: %s", dropper.to_drop_key);
            dropper.to_drop_hash = rxFindHashKey(0, dropper.to_drop_key);
            dropper.one_side = rxGetHashField(dropper.to_drop_hash, "edge");
            if (dropper.one_side && strlen(dropper.one_side))
            {
                // queue.Push(dropper.one_side);
                rxServerLog(rxLL_NOTICE, "--AA--> Purge (edge): %s", dropper.one_side);
                rxString contra = rxStringFormat("^%s", dropper.one_side);
                rxServerLog(rxLL_NOTICE, "--AB--> DEL %s, %d", dropper.one_side, rxDbDelete(0, dropper.one_side));
                rxServerLog(rxLL_NOTICE, "--AC--> DEL %s, %d", contra, rxDbDelete(0, contra));
                rxStringFree(contra);
            }
            dropper.other_side = rxGetHashField(dropper.to_drop_hash, "half");
            if (dropper.other_side && strlen(dropper.other_side))
            {
                // queue.Push(dropper.other_side);
                rxServerLog(rxLL_NOTICE, "To queue (half): %s", dropper.other_side);
                rxString contra = rxStringFormat("^%s", dropper.other_side);
                rxServerLog(rxLL_NOTICE, "--AB--> DEL %s, %d", dropper.one_side, rxDbDelete(0, dropper.other_side));
                rxServerLog(rxLL_NOTICE, "--AC--> DEL %s, %d", contra, rxDbDelete(0, contra));
                rxStringFree(contra);
            }
        }
        else
        {
            // Is Vertex
            void *set_drop_set = rxFindSetKey(0, dropper.to_drop_members_key);

            if (set_drop_set)
            {
                auto mob = rxHarvestSetmembers(set_drop_set);
                char **p = (char **)&mob->members;
                for (size_t n = 0; n < mob->member_count; ++n)
                {
                    char *member = *p;
                    rxServerLog(rxLL_NOTICE, "--AD--> member to be purged: %s", member);

                    auto caret = strstr(member, "|^");
                    auto mk = caret + 2;
                    auto link = strstr(caret + 2, "|");
                    int mkl = 0;
                    if (link)
                    {
                        *link = 0x00;
                        mkl = link - mk;
                    }
                    else 
                        mkl = strlen(mk);
                    auto hk = rxStringNewLen((const char *)mk, mkl);
                    if (rxStringMatch("*[:]*[:]*", hk, 1))
                    {
                        void *edge_hash = rxFindHashKey(0, hk);
                        const char *one_side = rxGetHashField(edge_hash, "edge");
                        const char *other_side = rxGetHashField(edge_hash, "half");
                        if (other_side && strlen(other_side))
                        {
                            rxServerLog(rxLL_NOTICE, "--H--> To queue (half): %s", other_side);
                            rxString contra = rxStringFormat("^%s", other_side);
                            rxServerLog(rxLL_NOTICE, "--AE--> DEL %s, %d", other_side, rxDbDelete(0, other_side));
                            rxServerLog(rxLL_NOTICE, "--AF--> DEL %s, %d", contra, rxDbDelete(0, contra));
                            rxStringFree(contra);
                        }

                        if (other_side && strlen(other_side))
                        {
                            rxString contra = rxStringFormat("^%s", one_side);
                            rxServerLog(rxLL_NOTICE, "--J--> To queue (edge): %s", one_side);
                            rxServerLog(rxLL_NOTICE, "--AJ--> DEL %s, %d", one_side, rxDbDelete(0, one_side));
                            rxServerLog(rxLL_NOTICE, "--AK--> DEL %s, %d", contra, rxDbDelete(0, contra));
                            rxStringFree(contra);
                        }
                        rxServerLog(rxLL_NOTICE, "--K--> To queue (edge): %s", dropper.to_drop_key);
                        rxServerLog(rxLL_NOTICE, "--AJ--> DEL %s, %d", dropper.to_drop_key, rxDbDelete(0, dropper.to_drop_key));
                        rxServerLog(rxLL_NOTICE, "--AK--> DEL %s, %d", dropper.to_drop_members_key, rxDbDelete(0, dropper.to_drop_members_key));
                    }
                }
            }
        }
        raxStop(&drop_iterator);
    }
}
END_SJIBOLETH_HANDLER(executeGremlinDropVertexOrEdge)

FaBlok *persistTriplet(SilNikParowy_Kontekst *stack, FaBlok *edge_set, const char *subject, const char *fwd_predicate, const char *bwd_predicate, const char *object, double weight, redisNodeInfo *data_config)
{
    // addE(dge)(<subject>, <type>, <object>)
    rxString backLink = rxStringEmpty();

    rxString edge_name = rxStringFormat("%s:%s:%s", fwd_predicate, subject, object);
    rxString edge_link = rxStringFormat("^%s", edge_name);
    const char *inv_edge_name;
    const char *inv_edge_link;
    // int single_predicate = rxStringMatch(fwd_predicate, bwd_predicate, MATCH_IGNORE_CASE);
    inv_edge_name = rxStringFormat("%s:%s:%s", bwd_predicate, object, subject);
    inv_edge_link = rxStringFormat("^%s", inv_edge_name);
    backLink = rxStringFormat("%s:%s:%s", bwd_predicate, object, subject);
    rxString subject_link = rxStringFormat("^%s", subject);
    rxString object_link = rxStringFormat("^%s", object);

    const char *bridge_keys[2] = {edge_name, inv_edge_name};
    const char *bridges[2] = {bwd_predicate, bwd_predicate};
    for (int j = 0; j < 2; ++j)
    {
        redis_HSET_EDGE(stack, bridge_keys[j], Graph_vertex_or_edge_type, bridges[j], backLink, data_config->host_reference);
        redis_HSET_EDGE(stack, bridge_keys[j], "edge", backLink, edge_name, data_config->host_reference);
    }

    rxString SE = rxStringFormat("%s|%s|SE|%f|+", fwd_predicate, edge_link, weight);
    rxString ES = rxStringFormat("%s|^%s|ES|%f", bwd_predicate, subject, weight);
    rxString OE = rxStringFormat("%s|%s|OE|%f", bwd_predicate, inv_edge_link, weight);
    rxString EO = rxStringFormat("%s|^%s|EO|%f", fwd_predicate, object, weight);

    rxString keys[4] = {subject_link, edge_link, object_link, inv_edge_link};
    rxString member[4] = {SE, EO, OE, ES};
    for (int j = 0; j < 4; ++j)
    {
        redis_SADD(stack, keys[j], member[j], data_config->host_reference);
    }
    if (!edge_set)
    {
        edge_set = FaBlok::Get(edge_name, KeysetDescriptor_TYPE_GREMLINSET);
    }
    edge_set->InsertKey(edge_name, rxFindKey(0, edge_name));
    edge_set->InsertKey(backLink, rxFindKey(0, backLink));
    edge_set->vertices_or_edges = EDGE_SET;
    rxStringFree(edge_name);
    rxStringFree(edge_link);
    rxStringFree(backLink);
    rxStringFree(inv_edge_name);
    rxStringFree(SE);
    rxStringFree(ES);
    rxStringFree(OE);
    rxStringFree(EO);
    rxStringFree(subject_link);
    rxStringFree(object_link);
    return edge_set;
}

/*
Commit edge will create  edge for all subject X object vertice combinations
*/
void CommitEdge(SilNikParowy_Kontekst *stack, FaBlok *edge_set)
{
    redisNodeInfo *data_config = rxDataNode();
    int predicates_count;
    rxString *predicates = rxStringSplitLen(edge_set->setname, strlen(edge_set->setname), ":", 1, &predicates_count);
    raxIterator subject_iterator;
    raxStart(&subject_iterator, edge_set->left->keyset);
    raxSeek(&subject_iterator, "^", NULL, 0);
    while ((raxNext(&subject_iterator)))
    {
        rxString subject_key = rxStringNewLen((const char *)subject_iterator.key, subject_iterator.key_len);

        raxIterator object_iterator;
        raxStart(&object_iterator, edge_set->right->keyset);
        raxSeek(&object_iterator, "^", NULL, 0);
        while ((raxNext(&object_iterator)))
        {
            rxString object_key = rxStringNewLen((const char *)object_iterator.key, object_iterator.key_len);
            if (strcmp(subject_key, object_key) == 0)
                continue;
            if (strlen(predicates[1]) == 0 || strcmp(predicates[1], predicates[0]) == 0)
                edge_set = persistTriplet(stack, edge_set, subject_key, predicates[0], strlen(predicates[1]) ? predicates[1] : predicates[0], object_key, 1.0, data_config);
            else
            {
                // two predicates are considered indepents facts
                edge_set = persistTriplet(stack, edge_set, subject_key, predicates[0], predicates[1], object_key, 1.0, data_config);
                edge_set = persistTriplet(stack, edge_set, object_key, predicates[1], predicates[0], subject_key, 1.0, data_config);
            }
            BusinessRule::Touched(object_key);
        }
        raxStop(&object_iterator);
        BusinessRule::Touched(subject_key);
    }
    raxStop(&subject_iterator);
    rxStringFreeSplitRes(predicates, predicates_count);

    edge_set->vertices_or_edges = VERTEX_SET;
    PushResult(edge_set, stack);
}

/*
    IN:  (0) <vertex> := <iri>[, <type>]
        (-1) <edge set>
    OUT: (0) <edge set>

    {from|subject}({ <iri>[,<type>] | <set> })
    {to|object}({ <iri>[,<type>] | <set> })
*/
SJIBOLETH_HANDLER(executeGremlinLinkVertex)
{
    rxUNUSED(t);

    bool is_to = (t->Is("to") || t->Is("object"));
    FaBlok *v = stack->Pop();
    FaBlok *e = stack->Pop();

    if (v->IsParameterList() && v->parameter_list->Size() == 2)
    {
        FaBlok *nv = createVertex(stack, v); // Use new vertex!
        if (is_to)
            e->right = nv;
        else
        {
            if (!e->right)
                e->right = e->left;
            e->left = nv;
        }
        FaBlok::Delete(v);
    }
    else
    {
        if (v->size == 0)
        {
            v->InsertKey(v->setname, rxFindKey(0, v->setname));
            v->size++;
        }
        if (is_to)
            e->right = v;
        else
        {
            if (e == NULL)
            {
                stack->AddError("%s in wrong position. Proper sequence: addE(...).from(...).to(...)");
                return C_ERR;
            }
            if (!e->right)
                e->right = e->left;
            e->left = v;
        }
    }
    if (e->left != NULL && e->right != NULL)
        CommitEdge(stack, e);
    else
        PushResult(e, stack);
}
END_SJIBOLETH_HANDLER(executeGremlinLinkVertex)

/*
g.V(1).as('a').out('created').in('created').where(neq('a')).
  addE('co-developer').from('a').property('year',2009) // (1)
g.V(3,4,5).aggregate('x').has('name','josh').as('a').
  select('x').unfold().hasLabel('software').addE('createdBy').to('a') // (2)
g.V().as('a').out('created').addE('createdBy').to('a').property('acl','public') // (3)
g.V(1).as('a').out('knows').
  addE('livesNear').from('a').property('year',2009).
  inV().inE('livesNear').values('year') // (4)
g.V().match(
        __.as('a').out('knows').as('b'),
        __.as('a').out('created').as('c'),
        __.as('b').out('created').as('c')).
      addE('friendlyCollaborator').from('a').to('b').
        property(id,23).property('project',select('c').values('name')) // (5)
g.E(23).valueMap()
vMarko = g.V().has('name','marko').next()
vPeter = g.V().has('name','peter').next()
g.V(vMarko).addE('knows').to(vPeter) // (6)
g.addE('knows').from(vMarko).to(vPeter) //7
*/
void executeGremlinAddEdgeUsingProperty(SilNikParowy_Kontekst *stack, FaBlok *et, size_t no_vertex_parms, redisNodeInfo *data_config)
{
    FaBlok *w = et->parameter_list->Last(); // Peek if last parameter is a float
    double weight = atof(w->setname);
    if (weight == 0.0)
    {
        weight = 1.0;
    }
    else
    {
        et->parameter_list->Pop_Last();
        no_vertex_parms--;
    }

    FaBlok *e = FaBlok::Get(et->setname, KeysetDescriptor_TYPE_GREMLINSET);
    // FaBlok *pk = stack->Peek();
    // rxServerLog(rxLL_DEBUG, "executeGremlinAddEdge stack peek %s", pk->setname);
    FaBlok *input_set = et->parameter_list->Dequeue(); // Ignore @ marker
    input_set = stack->Pop();                          // left side subject set
    // rxServerLog(rxLL_DEBUG, "executeGremlinAddEdge subject set %s %d entries ", input_set->setname, input_set->size);
    FaBlok *pred = et->parameter_list->Dequeue(); // predicate(type)
    // rxServerLog(rxLL_DEBUG, "executeGremlinAddEdge predicate %s", pred->setname);
    FaBlok *inv_pred;
    rxString backLink = rxStringEmpty();
    if (no_vertex_parms >= 3)
        inv_pred = et->parameter_list->Dequeue();
    else
        inv_pred = pred;
    const char *fwd_predicate = pred->setname;
    const char *bwd_predicate = inv_pred->setname;
    // rxServerLog(rxLL_DEBUG, "executeGremlinAddEdge inverse predicate %s", bwd_predicate);
    //  FaBlok *o = NULL; // iri of object by materializing iri from pred property on subject!

    raxIterator SubjectIterator;
    raxStart(&SubjectIterator, input_set->keyset);
    raxSeek(&SubjectIterator, "^", NULL, 0);
    while (raxNext(&SubjectIterator))
    {
        rxString key = rxStringNewLen((const char *)SubjectIterator.key, SubjectIterator.key_len);
        void *subject = rxFindHashKey(0, key);
        if (subject == NULL)
            continue;
        rxString objectKey = rxGetHashField(subject, fwd_predicate);
        if (objectKey && strlen(objectKey) == 0)
        {
            rxStringFree(objectKey);
            continue;
        }
        // void *object = rxFindHashKey(0, objectKey);
        // if(subject == NULL){
        //     object = rxCreateHashObject();
        // }

        rxString edge_name = rxStringFormat("%s:%s:%s", fwd_predicate, key, objectKey);
        rxString edge_link = rxStringFormat("^%s", edge_name);
        rxString inv_edge_name;
        rxString inv_edge_link;
        if (strcmp(fwd_predicate, bwd_predicate) == 0)
        {
            inv_edge_name = edge_name;
            inv_edge_link = edge_link;
        }
        else
        {
            inv_edge_name = rxStringFormat("%s:%s:%s", bwd_predicate, objectKey, key);
            inv_edge_link = rxStringFormat("^%s", inv_edge_name);
        }
        rxString subject_link = rxStringFormat("^%s", key);
        rxString object_link = rxStringFormat("^%s", objectKey);

        // rxServerLog(rxLL_DEBUG, "edge_name: %s\nedge_link: %s\ninv_edge_name: %s\ninv_edge_link: %s\nsubjectKey: %s\nobjectKey: %s\n",
        // edge_name, edge_link, inv_edge_name, inv_edge_link, key, objectKey);
        if (pred != inv_pred)
        {
            backLink = rxStringFormat("%s%s:%s:%s:%s", key, backLink, fwd_predicate, fwd_predicate, objectKey);
        }

        rxString bridge_keys[2] = {edge_name, inv_edge_name};
        FaBlok *bridges[2] = {pred, inv_pred};
        for (int j = 0; j < 2; ++j)
        {
            redis_HSET(stack, bridge_keys[j], Graph_vertex_or_edge_type, bridges[j]->setname, data_config->host_reference);
            if (strlen(backLink))
            {
                redis_HSET(stack, bridge_keys[j], "edge", backLink, data_config->host_reference);
            }
        }

        rxString SE = rxStringFormat("%s|%s|SE|%f|+", fwd_predicate, edge_link, weight);
        rxString ES = rxStringFormat("%s|^%s|EO|%f", bwd_predicate, key, weight);
        rxString EO = rxStringFormat("%s|%s|SE|%f", bwd_predicate, inv_edge_link, weight);
        rxString OE = rxStringFormat("%s|^%s|EO|%f", fwd_predicate, objectKey, weight);

        rxString keys[4] = {subject_link, edge_link, inv_edge_link, object_link};
        rxString member[4] = {SE, OE, ES, EO};
        for (int j = 0; j < 4; ++j)
        {
            redis_SADD(stack, keys[j], member[j], data_config->host_reference);
        }
        void *oo = rxFindKey(0, edge_name);
        e->InsertKey(edge_name, oo);
        e->vertices_or_edges = EDGE_SET;
        rxStringFree(objectKey);
        rxStringFree(SE);
        rxStringFree(ES);
        rxStringFree(EO);
        rxStringFree(OE);
        rxStringFree(edge_name);
        rxStringFree(edge_link);
        rxStringFree(inv_edge_name);
        rxStringFree(inv_edge_link);
        rxStringFree(subject_link);
        rxStringFree(object_link);
        rxStringFree(backLink);
    }
    raxStop(&SubjectIterator);
    PushResult(e, stack);
    FaBlok::Delete(et);
}

void executeGremlinAddEdgeUsingSubjectEdgeNamesObject(SilNikParowy_Kontekst *stack, FaBlok *et, size_t no_vertex_parms, redisNodeInfo *data_config)
{
    // addE(dge)(<subject>, <type>, <object>)
    FaBlok *w = et->parameter_list->Last(); // Peek if last parameter is a float
    double weight = atof(w->setname);
    if (weight == 0.0)
    {
        weight = 1.0;
    }
    else
    {
        et->parameter_list->Pop_Last();
        no_vertex_parms--;
    }

    FaBlok *input_set = et->parameter_list->Dequeue(); // iri of subject
    FaBlok *pred = et->parameter_list->Dequeue();      // predicate(type)
    FaBlok *inv_pred;
    if (no_vertex_parms >= 4)
        inv_pred = et->parameter_list->Dequeue();
    else
        inv_pred = pred;
    FaBlok *o = /*stack->Pop(); //*/ et->parameter_list->Dequeue(); // iri of object

    FaBlok *e = persistTriplet(stack, NULL, input_set->setname, pred->setname, pred->setname, o->setname, weight, data_config);
    if (strcmp(pred->setname, inv_pred->setname) != 0)
        e = persistTriplet(stack, e, o->setname, inv_pred->setname, inv_pred->setname, input_set->setname, weight, data_config);
    PushResult(e, stack);
    FaBlok::Delete(et);
}

void executeGremlinAddEdgePostponed(
    SilNikParowy_Kontekst *stack,
    FaBlok *fwd_predicate,
    FaBlok *bwd_predicate)
{
    FaBlok *subject = stack->Pop(); // predicate(type)

    // addE(dge)(<type>)
    // Create temp key
    // the temp key will be renamed from the to(subject)/from(object) vertices
    // A new edge is pushed to the stack
    rxString temp_key = rxStringFormat("%s:%s::%lld", fwd_predicate->setname, bwd_predicate ? bwd_predicate->setname : "", ustime());
    // redis_HSET(stack, temp_key, Graph_vertex_or_edge_type, et->setname, data_config->host_reference);
    FaBlok *e = FaBlok::Get(temp_key, KeysetDescriptor_TYPE_GREMLINSET);
    e->left = subject;
    e->right = NULL;

    // void *oo = rxFindKey(0, temp_key);
    // e->InsertKey(temp_key, oo);

    e->vertices_or_edges = EDGE_TEMPLATE;
    PushResult(e, stack);
}

SJIBOLETH_HANDLER(executeGremlinAddEdge)
{
    rxUNUSED(t);

    // Get parameters g:addE(<iri>, <type>)
    // Get parameters g:v().addV(<iri>, <type>)
    redisNodeInfo *data_config = rxDataNode();

    FaBlok *et = stack->Pop();
    size_t no_vertex_parms = et->IsParameterList() ? et->parameter_list->Size() : 0;
    int is_link_from_property = 0;
    if (et->IsParameterList())
    {
        is_link_from_property = rxStringMatch(et->parameter_list->Peek()->setname, "@", 1);
    }
    // rxServerLog(rxLL_DEBUG, "executeGremlinAddEdge =%d %d parameters %d parameters", is_link_from_property, et->parameter_list ? et->parameter_list->Size() : 0, stack->Size());
    if (et->IsParameterList() && no_vertex_parms >= 3 && is_link_from_property == 1)
    {
        executeGremlinAddEdgeUsingProperty(stack, et, no_vertex_parms, data_config);
    }
    else if (et->IsParameterList() && no_vertex_parms >= 3)
    {
        executeGremlinAddEdgeUsingSubjectEdgeNamesObject(stack, et, no_vertex_parms, data_config);
    }
    else if (et->IsParameterList() && no_vertex_parms == 2)
    {
        FaBlok *fwd_predicate = et->parameter_list->Dequeue(); // predicate(type)
        FaBlok *bwd_predicate = et->parameter_list->Dequeue();
        executeGremlinAddEdgePostponed(stack, fwd_predicate, bwd_predicate);
        FaBlok::Delete(et);
    }
    else
    {
        executeGremlinAddEdgePostponed(stack, et, NULL);
    }
}
END_SJIBOLETH_HANDLER(executeGremlinAddEdge)

SJIBOLETH_HANDLER(executeGremlinAddTriplet)
{
    rxUNUSED(t);

    // Get parameters g:addTriplet(<subject>, <predicate>, <object>)
    // Get parameters g:addTriplet(<subject>, <predicate>, <object>, <weight>)
    // Get parameters g:addTriplet(<subject>, <predicate>, <inverse predicate>, <object>)
    // Get parameters g:addTriplet(<subject>, <predicate>, <inverse predicate>, <object>, <weight>)
    redisNodeInfo *data_config = rxDataNode();

    FaBlok *et = stack->Pop();
    size_t no_vertex_parms = et->IsParameterList() ? et->parameter_list->Size() : 0;
    if (et->IsParameterList() && no_vertex_parms >= 3)
    {
        executeGremlinAddEdgeUsingSubjectEdgeNamesObject(stack, et, no_vertex_parms, data_config);
    }
    else
        stack->AddError("Invalid function call. addTriplet(<subject>, [<predicate>,] <object>)");
}
END_SJIBOLETH_HANDLER(executeGremlinAddTriplet)

SJIBOLETH_HANDLER(executeGremlinAddProperty)
{
    rxUNUSED(t);

    FaBlok *pl = stack->Pop();
    FaBlok *input_set = stack->Pop();
    if (input_set == 0)
    {
        stack->AddError("No input set");
        return C_ERR;
    }
    redisNodeInfo *data_config = rxDataNode();

    if (pl->IsParameterList())
    {
        if ((pl->parameter_list->Size() & 1) == 1)
        {
            ERROR("Incorrect parameter list for AddProperty, must be [<a>, <v>]...");
        }

        FaBlok *a = pl->parameter_list->Dequeue();
        FaBlok *v = pl->parameter_list->Dequeue();

        if (v->ObjectExpression())
        {
            operation_parameters p;
            auto *params = get_For_object_expressions(&p, v->ObjectExpression(), stack, NULL, NULL, OBJECT_EXPRESSION_IS_CALCULATION);
            FaBlok *output_set = input_set->Copy(input_set->AsSds(), KeysetDescriptor_TYPE_GREMLINSET, ExecuteObjectExpression, params);
            PushResult(output_set, stack);
            return C_OK;
        }
        raxIterator targetObjectIterator;
        raxStart(&targetObjectIterator, input_set->keyset);
        raxSeek(&targetObjectIterator, "^", NULL, 0);
        while (raxNext(&targetObjectIterator))
        {
            rxString key = rxStringNewLen((const char *)targetObjectIterator.key, targetObjectIterator.key_len);
            // TODO: Allow multiple properties to be set!
            // while (pl->parameter_list->Size() >= 2)
            // {
            if (stack->module_contex || targetObjectIterator.data == NULL)
                redis_HSET(stack, key, a->setname, v->setname, data_config->host_reference);
            else
            {
                rxHashTypeSet(targetObjectIterator.data, a->setname, v->setname, 0);
            }
            // }
        }
        raxStop(&targetObjectIterator);
        FaBlok::Delete(pl);
        PushResult(input_set, stack);
    }
    else if (pl->ObjectExpression())
    {
        operation_parameters p;
        auto *params = get_For_object_expressions(&p, pl->ObjectExpression(), stack, NULL, NULL, OBJECT_EXPRESSION_IS_CALCULATION);
        FaBlok *output_set = input_set->Copy(input_set->AsSds(), KeysetDescriptor_TYPE_GREMLINSET, ExecuteObjectExpression, params);
        PushResult(output_set, stack);
        FaBlok::Delete(pl);
        return C_OK;
    }
    else
    {
        rxServerLog(rxLL_NOTICE, "parameter list: %s %p\ninput_set: %s %ld entries", pl->setname, pl->parameter_list, input_set->setname, raxSize(input_set->keyset));
        raxShow(input_set->keyset);
        ERROR("Incorrect parameters for AddProperty, must be [<a>, <v>]...");
    }
}
END_SJIBOLETH_HANDLER(executeGremlinAddProperty)

/*
    Execute a Redis command

    The redis command is constructed by concatenating the parameters.

    When a token matches an object field then the field value is used.

    Special tokens:
        @key
        @graph
        @tuple


*/
SJIBOLETH_HANDLER(executeGremlinRedisCommand)
{
    rxUNUSED(t);

    STACK_CHECK(2);
    stack->DumpStack();
    FaBlok *pl = stack->Pop();
    FaBlok *input_set = stack->Pop();
    redisNodeInfo *dataConfig = rxDataNode();

    if (pl->IsParameterList())
    {
        int no_of_parts = pl->parameter_list->Size();

        raxIterator keysetIterator;
        raxStart(&keysetIterator, input_set->keyset);
        raxSeek(&keysetIterator, "^", NULL, 0);
        while (raxNext(&keysetIterator))
        {
            rxString key = rxStringNewLen((const char *)keysetIterator.key, keysetIterator.key_len);

            void *obj = rxFindKey(0, key);

            rxString *parts = new rxString[no_of_parts];
            int n = 0;
            pl->parameter_list->StartHead();
            FaBlok *p = NULL;
            while ((p = pl->parameter_list->Next()) != NULL)
            {
                if (strcmp(p->setname, "*") == 0)
                    parts[n] = rxStringNew(p->setname);
                else if (rxStringMatch(p->setname, "@key", 1))
                    parts[n] = key;
                else if (rxStringMatch(p->setname, "@tuple", 1))
                {
                    switch (rxGetObjectType(obj))
                    {
                    case rxOBJ_HASH:
                        parts[n] = rxHashAsJson(key, obj);
                        break;
                    case rxOBJ_TRIPLET:
                    {
                        Graph_Triplet *t = (Graph_Triplet *)rxGetContainedObject(obj);
                        parts[n] = t->Json(key);
                    }
                    break;
                    default:
                        parts[n] = rxStringNew("{}");
                        break;
                    }
                }
                else if (rxStringMatch(p->setname, "@graph", 1))
                {
                    parts[n] = get_deeper(key, strlen(key), 3);
                }
                else
                    parts[n] = rxStringNew(p->setname);
                ++n;
            }

            void *stashed_cmd = rxStashCommand2(NULL, parts[0], /*rxStringTrim*/ 1, no_of_parts - 1, (void **)&parts[1]);
            ExecuteRedisCommand(NULL, stashed_cmd, dataConfig->host_reference);
            FreeStash(stashed_cmd);
            rxStringFree(key);
        }
        raxStop(&keysetIterator);
        FaBlok::Delete(pl);
    }
    else
    {
        ERROR("Incorrect parameters for executeGremlinRedisCommand, must be [<token> | <literal>]...");
    }
    PushResult(input_set, stack);
    rxServerLog(rxLL_NOTICE, "REDIS 0000");
    stack->DumpStack();
    rxServerLog(rxLL_NOTICE, "REDIS 9999");
}
END_SJIBOLETH_HANDLER(executeGremlinRedisCommand)

/*
 Copy the input vertex or edge set and apply the object expression to each item.

 The output is the copied key set.

 When then a dynamic field name is in use, then the field name is pushed to
 the stack
 */
SJIBOLETH_HANDLER(executeObjectExpression)
{
    STACK_CHECK(1); // The vertex/edge set to work!
    stack->DumpStack();
    FaBlok *input_set = stack->Pop();
    // redisNodeInfo *dataConfig = rxDataNode();
    operation_parameters p;
    auto *params = get_For_object_expressions(&p, t, stack, NULL, NULL, OBJECT_EXPRESSION_IS_CALCULATION);
    FaBlok *output_set = input_set->Copy(input_set->AsSds(), KeysetDescriptor_TYPE_GREMLINSET, ExecuteObjectExpression, params);
    FaBlok::Delete(input_set);
    PushResult(output_set, stack);
    auto *bracket = t->BracketResult();
    if (bracket)
    {
        auto *temp_field = (const char *)bracket->Operation();
        if (strlen(temp_field) == 36 && temp_field[8] == '-' && temp_field[13] == '-' && temp_field[18] == '-' && temp_field[23] == '-')
        {
            FaBlok *ref = FaBlok::New(temp_field, KeysetDescriptor_TYPE_SINGULAR);
            PushResult(ref, stack);
        }
    }
}
END_SJIBOLETH_HANDLER(executeObjectExpression)
SJIBOLETH_HANDLER(executeWhereExpression)
{
    STACK_CHECK(1); // The vertex/edge set to work!
    stack->DumpStack();
    FaBlok *expr = stack->Pop();
    FaBlok *input_set = stack->Pop();
    // redisNodeInfo *dataConfig = rxDataNode();
    operation_parameters p;
    auto *params = get_For_object_expressions(&p, expr->ObjectExpression(), stack, NULL, NULL, OBJECT_EXPRESSION_IS_PREDICATE);
    FaBlok *output_set = input_set->Copy(input_set->AsSds(), KeysetDescriptor_TYPE_GREMLINSET, ExecuteObjectExpression, params);
    FaBlok::Delete(input_set);
    PushResult(output_set, stack);
}
END_SJIBOLETH_HANDLER(executeWhereExpression)

/*
 Copy the input vertex or edge set and apply the object expression to each item.

 The output is the copied key set.

 When then a dynamic field name is in use, then the field name is pushed to
 the stack
 */
SJIBOLETH_HANDLER(executeResetStack)
{
    while (stack->HasEntries())
    {
        FaBlok *input_set = stack->Pop();
        FaBlok::Delete(input_set);
    }
}
END_SJIBOLETH_HANDLER(executeResetStack)

/*
 Push a copy of the top stack onto the stack

 Allows a subtraversal and continuation
 */
SJIBOLETH_HANDLER(executePushDupStack)
{
    rxServerLog(rxLL_NOTICE, "executePushDupStack 0000");
    if (stack->HasEntries())
    {
            FaBlok *input_set = stack->Pop();
        PushResult(input_set->Copy(NULL, input_set->value_type, NULL, NULL), stack);
        PushResult(input_set, stack);
        stack->DumpStack();
    }
    rxServerLog(rxLL_NOTICE, "executePushDupStack 9999");
}
END_SJIBOLETH_HANDLER(executePushDupStack)

/*
 Pops the the top stack from the stack

 To continue after a subtraversal
 */
SJIBOLETH_HANDLER(executePopStack)
{
    rxServerLog(rxLL_NOTICE, "executePopStack 0000");
    if (stack->HasEntries())
    {
        FaBlok *input_set = stack->Pop();
        FaBlok::Delete(input_set);
    }
    stack->DumpStack();
    rxServerLog(rxLL_NOTICE, "executePopStack 9999");
}
END_SJIBOLETH_HANDLER(executePopStack)

SJIBOLETH_HANDLER(debugBreak)
{
    rxServerLog(rxLL_NOTICE, "----- BREAKPOINT ---- %d stackentries", stack->Size());
    rxServerLog(rxLL_NOTICE, "debugBreak 0000");
    stack->DumpStack();
    rxServerLog(rxLL_NOTICE, "debugBreak 9999");
}
END_SJIBOLETH_HANDLER(debugBreak)

SJIBOLETH_HANDLER(executeComment)
{
    // Do nothing note is just source documentation
}
END_SJIBOLETH_HANDLER(executeComment)

GremlinDialect::GremlinDialect()
    : Sjiboleth("GremlinDialect")
{
    this->default_operator = rxStringEmpty();
    this->RegisterDefaultSyntax();
}

/*
 * GremlinScopeCheck
 *
 * Change token type and priority for 'subject' and 'object'
 * When in the context of a 'by' operation.
 */
SJIBOLETH_PARSER_CONTEXT_CHECKER(GremlinScopeCheck)
{
    rxUNUSED(head);
    rxUNUSED(expression);
    rxUNUSED(pO);
    if (HasParkedToken(expression, "by"))
    {
        ParserToken *to_cpy = (ParserToken *)t;
        ParserToken *new_cpy = to_cpy->Copy(722764040);
        new_cpy->TokenType(_literal);
        new_cpy->Priority(100);
        t = (CParserToken *)new_cpy;
    }
}
END_SJIBOLETH_PARSER_CONTEXT_CHECKER(GremlinScopeCheck)

bool GremlinDialect::RegisterDefaultSyntax()
{
    Sjiboleth::RegisterDefaultSyntax();
    // this->RegisterSyntax("g", 500, 2, 1, &executeGremlin);
    this->RegisterSyntax("v", pri500, 1, 1, &executeAllVertices);
    this->RegisterSyntax("V", pri500, 1, 1, &executeAllVertices);
    this->RegisterSyntax("e", 500, 2, 1, &executeAllEdges);
    this->RegisterSyntax("E", 500, 2, 1, &executeAllEdges);
    this->RegisterSyntax(".", 0, 0, priIgnore, NULL);
    this->RegisterSyntax(",", pri200, 2, 1, &executeGremlinParameters);
    // Pattern matching
    this->RegisterSyntax("match", pri500, 1, 1, &executeMatch);
    this->RegisterSyntax("nomatch", 500, 2, 1, &executeNomatch);
    this->RegisterSyntax("incl", 500, 2, 1, &executeGremlinMatchInExclude);
    this->RegisterSyntax("include", 500, 2, 1, &executeGremlinMatchInExclude);
    this->RegisterSyntax("excl", 500, 2, 1, &executeGremlinMatchInExclude);
    this->RegisterSyntax("exclude", 500, 2, 1, &executeGremlinMatchInExclude);
    this->RegisterSyntax("minimize", 500, 2, 1, &executeGremlinMatchMinimize, PARSER_OPTION_DELAY_OBJECT_EXPRESSION);
    this->RegisterSyntax("maximize", 500, 2, 1, &executeGremlinMatchMaximize, PARSER_OPTION_DELAY_OBJECT_EXPRESSION);
    //
    this->RegisterSyntax("as", 500, 2, 1, &executeGremlinAs);
    this->RegisterSyntax("use", 500, 2, 1, &executeGremlinUse);
    //
    this->RegisterSyntax("has", 500, 2, 1, &executeGremlinHas);
    this->RegisterSyntax("hasNot", 500, 2, 1, &executeGremlinHas);
    this->RegisterSyntax("hasToken", 500, 2, 1, &executeGremlinHas);
    this->RegisterSyntax("missingToken", 500, 2, 1, &executeGremlinHas);
    this->RegisterSyntax("missing", 500, 2, 1, &executeGremlinHas);
    this->RegisterSyntax("hasLabel", 500, 2, 1, &executeGremlinHasLabel);
    this->RegisterSyntax("missingLabel", 500, 2, 1, &executeGremlinHasLabel);
    //
    this->RegisterSyntax("by", 500, 2, 1, &executeGremlinGroupby);
    //
    this->RegisterSyntax("subjects", 500, 2, 1, &executeGremlinIn);
    this->RegisterSyntax("subjectTriplets", 500, 2, 1, &executeGremlinInTriplet);
    this->RegisterSyntax("out", 500, 2, 1, &executeGremlinOut);

    this->RegisterSyntax("objects", 500, 2, 1, &executeGremlinOut);
    this->RegisterSyntax("objectTriplets", 500, 2, 1, &executeGremlinOutTriplet);
    this->RegisterSyntax("in", 500, 2, 1, &executeGremlinIn);
    this->RegisterSyntax("inout", 500, 2, 1, &executeGremlinInOut);
    this->RegisterSyntax("outT", 500, 2, 1, &executeGremlinInOutTriplet);

    this->RegisterSyntax("inT", 500, 2, 1, &executeGremlinInOutTriplet);
    this->RegisterSyntax("inoutT", 500, 2, 1, &executeGremlinInOutTriplet);
    this->RegisterSyntax("outTriplet", 500, 2, 1, &executeGremlinInOutTriplet);
    this->RegisterSyntax("inTriplet", 500, 2, 1, &executeGremlinInOutTriplet);
    this->RegisterSyntax("inoutTriplet", 500, 2, 1, &executeGremlinInOutTriplet);
    this->RegisterSyntax("outEdge", 500, 2, 1, &executeGremlinOutEdge);
    this->RegisterSyntax("inEdge", 500, 2, 1, &executeGremlinInEdge);
    this->RegisterSyntax("inoutEdge", 500, 2, 1, &executeGremlinInOutEdge);
    this->RegisterSyntax("outE", 500, 2, 1, &executeGremlinOutEdge);
    this->RegisterSyntax("inE", 500, 2, 1, &executeGremlinInEdge);
    this->RegisterSyntax("inoutE", 500, 2, 1, &executeGremlinInOutEdge);
    this->RegisterSyntax("isSubject", 500, 2, 1, &executeGremlinHasOut);
    this->RegisterSyntax("hasOut", 500, 2, 1, &executeGremlinHasOut);
    this->RegisterSyntax("isObject", 500, 2, 1, &executeGremlinHasIn);
    this->RegisterSyntax("hasIn", 500, 2, 1, &executeGremlinHasIn);
    this->RegisterSyntax("hasInout", 500, 2, 1, &executeGremlinHasInOut);
    this->RegisterSyntax("addEdge", 500, 2, 1, &executeGremlinAddEdge);
    this->RegisterSyntax("addE", 500, 2, 1, &executeGremlinAddEdge);
    this->RegisterSyntax("predicate", 500, 2, 1, &executeGremlinAddEdge);
    this->RegisterSyntax("triplet", 500, 2, 1, &executeGremlinAddTriplet);
    this->RegisterSyntax("property", 500, 2, 1, &executeGremlinAddProperty, PARSER_OPTION_DELAY_OBJECT_EXPRESSION);
    this->RegisterSyntax("addVertex", 500, 2, 1, &executeGremlinAddVertex);
    this->RegisterSyntax("addV", 500, 2, 1, &executeGremlinAddVertex);
    this->RegisterSyntax("to", 500, 2, 1, &executeGremlinLinkVertex);
    this->RegisterSyntax("object", 500, 2, 1, &executeGremlinLinkVertex, &GremlinScopeCheck);
    this->RegisterSyntax("from", 500, 2, 1, &executeGremlinLinkVertex);
    this->RegisterSyntax("subject", 500, 2, 1, &executeGremlinLinkVertex, &GremlinScopeCheck);
    this->RegisterSyntax("drop", 500, 2, 1, &executeGremlinDropVertexOrEdge);
    this->RegisterSyntax("join", 500, 2, 1, &Sjiboleth::executeOr);
    this->RegisterSyntax("merge", 500, 2, 1, &Sjiboleth::executeAnd);
    this->RegisterSyntax("where", 500, 2, 1, &executeWhereExpression, PARSER_OPTION_DELAY_OBJECT_EXPRESSION);
    this->RegisterSyntax("eq", 500, 2, 1, &executeGremlinComparePropertyToValue, PARSER_OPTION_DELAY_OBJECT_EXPRESSION);
    this->RegisterSyntax("ne", 500, 2, 1, &executeGremlinComparePropertyToValue, PARSER_OPTION_DELAY_OBJECT_EXPRESSION);
    this->RegisterSyntax("gt", 500, 2, 1, &executeGremlinComparePropertyToValue, PARSER_OPTION_DELAY_OBJECT_EXPRESSION);
    this->RegisterSyntax("ge", 500, 2, 1, &executeGremlinComparePropertyToValue, PARSER_OPTION_DELAY_OBJECT_EXPRESSION);
    this->RegisterSyntax("lt", 500, 2, 1, &executeGremlinComparePropertyToValue, PARSER_OPTION_DELAY_OBJECT_EXPRESSION);
    this->RegisterSyntax("LT", 500, 2, 1, &executeGremlinComparePropertyToValue, PARSER_OPTION_DELAY_OBJECT_EXPRESSION);
    this->RegisterSyntax("le", 500, 2, 1, &executeGremlinComparePropertyToValue, PARSER_OPTION_DELAY_OBJECT_EXPRESSION);

    this->RegisterSyntax("-", 29, 2, 1, &executeGremlinComputeOnPropertyValue);
    this->RegisterSyntax("+", 29, 2, 1, &executeGremlinComputeOnPropertyValue);
    this->RegisterSyntax("/", 30, 2, 1, &executeGremlinComputeOnPropertyValue);
    this->RegisterSyntax("*", 30, 2, 1, &executeGremlinComputeOnPropertyValue);
    this->RegisterSyntax("==", 20, 2, 1, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax(">=", 20, 2, 1, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax("<=", 20, 2, 1, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax("<", 20, 2, 1, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax(">", 20, 2, 1, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax("!=", 20, 2, 1, &executeGremlinComparePropertyToValue);

    this->RegisterSyntax("sqrt", 30, 2, 1, &executeGremlinComputeOnSinglePropertyValue);
    this->RegisterSyntax("pow", 30, 2, 1, &executeGremlinComputeOnPropertyValue);
    this->RegisterSyntax("trend_slope", 30, 2, 1, &calculateTrendSlope, PARSER_OPTION_DELAY_OBJECT_EXPRESSION);
    // this->RegisterSyntax("trend_base", 30, 2, 1, &calculateTrendBase, PARSER_OPTION_DELAY_OBJECT_EXPRESSION);
    // this->RegisterSyntax("trend", 30, 2, 1, &calculateTrend, PARSER_OPTION_DELAY_OBJECT_EXPRESSION);

    this->RegisterSyntax("between", 500, 2, 1, &executeGremlinComparePropertyToRangeValue);
    this->RegisterSyntax("contains", 500, 2, 1, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax("redis", 500, 2, 1, &executeGremlinRedisCommand);
    this->RegisterSyntax("{", 50, -1, -1, NULL);
    this->RegisterSyntax("}", 50, -1, -1, &executeObjectExpression);
    this->RegisterSyntax("reset", 50, -1, -1, &executeResetStack);
    this->RegisterSyntax("pushdup", 50, -1, -1, &executePushDupStack);
    this->RegisterSyntax("pop", 50, -1, -1, &executePopStack);
    this->RegisterSyntax("break", 50, -1, -1, &debugBreak);
    this->RegisterSyntax("note", 50, -1, -1, &executeComment);
    return true;
}

FaBlok *getAllKeysByRegex(int dbNo, const char *regex, int on_matched, const char *set_name)
{
    rxString keyset_name = rxStringFormat(set_name);
    long long start = ustime();
    FaBlok *kd = FaBlok::Get(keyset_name, KeysetDescriptor_TYPE_GREMLINSET);
    kd->Open();
    rxString pattern = rxStringNew(regex);
    int allkeys;
    unsigned long numkeys = 0;

    void *iter = NULL;
    rxString key;
    void *obj;

    allkeys = strcmp(pattern, "*") == 0;
    while ((obj = rxScanKeys(dbNo, &iter, (char **)&key)) != NULL)
    {

        if (*key == '^' || *key == '~' || *key == '`')
            continue;
        int stringmatch = rxStringCharCount(key, ':') >= 2;
        /*
         * Match:
         *       All keys
         *       edge of vertex
         *       regex
         */
        if (allkeys || on_matched == stringmatch || ((on_matched == MATCH_REGEX) && rxStringMatch(regex, key, MATCH_IGNORE_CASE)))
        {
            numkeys++;
            kd->InsertKey(key, obj);
        }
    }

    // TODO
    kd->latency = ustime() - start;
    kd->size = numkeys;
    return kd;
}

FaBlok *getAllKeysByField(int dbNo, const char *regex, int on_matched, rxString field, list *values, rxString keyset_name)
{
    long long start = ustime();
    FaBlok *kd = FaBlok::Get(keyset_name, KeysetDescriptor_TYPE_GREMLINSET);
    rxString pattern = rxStringNew(regex);
    int allkeys;
    unsigned long numkeys = 0;

    void *iter = NULL;
    rxString key;
    void *obj;

    listIter *li = listGetIterator(values, 0);
    listNode *ln;

    allkeys = strcmp(pattern, "*") == 0;
    while ((obj = rxScanKeys(dbNo, &iter, (char **)&key)) != NULL)
    {
        if (rxGetObjectType(obj) != rxOBJ_HASH)
            continue;
        if (*key == '^')
            continue;

        int stringmatch = rxStringCharCount(key, ':') >= 2;
        listRewind(values, li);
        while ((ln = listNext(li)) != NULL)
        {
            rxString value = (rxString)ln->value;
            int value_len = strlen((const char *)ln->value);
            if ((allkeys || on_matched == stringmatch) && rxMatchHasValue(obj, field, value, value_len) == MATCH_IS_TRUE)
            {
                numkeys++;
                kd->InsertKey(key, obj);
                break;
            }
        }
    }
    listReleaseIterator(li);
    // TODO
    kd->latency = ustime() - start;
    kd->size = numkeys;
    return kd;
}

FaBlok *getAllKeysByType(int dbNo, const char *regex, int on_matched, rxString type_name)
{
    list *patterns = listCreate();
    listAddNodeTail(patterns, (void *)type_name);
    rxString keyset_name = rxStringFormat("type == %s", type_name);
    rxString field = rxStringNew("type");
    FaBlok *b = getAllKeysByField(dbNo, regex, on_matched, field, patterns, keyset_name);
    rxStringFree(field);
    rxStringFree(keyset_name);
    listRelease(patterns);
    return b;
}

bool raxContains(rax *tree, char const *key)
{
    void *m = raxFind(tree, (UCHAR *)key, strlen(key));
    return (m != raxNotFound);
}

int AddMemberToKeysetForMatch(int db, unsigned char *vstr, size_t vlen, FaBlok *kd, Graph_Leg *terminal, unsigned char * /*key*/, MatchParameters *parms)
{
    // if (!filter_only && terminal && terminal == terminal->start)
    //     return 0;
    // if (!filter_only && terminal && terminal == terminal->start)
    //     return 0;
    // look up target key
    int segments = 0;
    int link_segments = 0;
    rxString some_str = rxStringNewLen((char *)vstr, vlen);
    rxString *parts = rxStringSplitLen(some_str, vlen, "|", 1, &segments);
    rxStringFree(some_str);

    if (parms->filter_only /*&& terminal->de*/)
    {
        Graph_Leg *leader = terminal;
        while (leader->origin)
        {
            leader = leader->origin;
        }
        if (leader != NULL && leader != terminal)
        {
            auto *tobj = (Graph_Triplet *)Graph_Triplet::NewForMatch(db, terminal, leader->key, NULL, parms->optimization);
            Graph_Triplet::Link(tobj, kd);
            rxString tk = rxStringFormat(":%s:%s:", leader->key, terminal->key);
            kd->AddKey(tk, tobj);
            rxStringFree(tk);
            tobj->CalcLength();
            tobj->CalcLength();
            return 1;
        }

        if (terminal->obj == NULL)
            return 0;
        rxString k = terminal->key;
        void *v = terminal->obj;
        void *tobj = rxFindKey(db, k);
        rxString tk = (strcmp(terminal->key, leader->key) == 0)
                          ? rxStringDup(leader->key)
                          : rxStringFormat(":%s:%s:", terminal->key, leader->key);
        kd->AddKey(tk, tobj ? tobj : v);
        rxStringFree(tk);
        return 1;
    }
    Graph_Triplet *tobj = (Graph_Triplet *)Graph_Triplet::New(db, terminal, rxStringEmpty(), rxStringEmpty(), parms->optimization);
    if (segments == 1)
    {
        rxString member = rxStringNewLen((const char *)vstr, vlen);
        if (!tobj)
            if ((tobj = (Graph_Triplet *)rxFindHashKey(db, member)) == NULL)
                return 0;
        Graph_Triplet::Link(tobj, kd);
        // Find start key
        // TODO: may be needed on more places!!
        rxString origin_key = NULL;
        Graph_Leg *path = terminal->origin;
        terminal->Claim(); // Add claim to prevent early deletion!
        while (path)
        {
            path->Claim(); // Add claim to prevent early deletion!
            origin_key = path->key;
            path = path->origin;
        }
        rxString match_key = rxStringFormat("%s->%s", origin_key, vstr);
        // tobj->CalcLength();

        if (origin_key != NULL)
            kd->AddKey(match_key, tobj);
        else
            rxMemFree(tobj);
        return 1;
    }

    rxString link = rxStringNew(parts[1] + 1);
    rxString *link_parts = rxStringSplitLen(link, strlen(link), ":", 1, &link_segments);
    if (link_segments == 1)
    {
        if (!tobj)
            if ((tobj = (Graph_Triplet *)rxFindHashKey(db, link)) == NULL)
                return 0;
        assert(rxGetObjectType(tobj) == rxOBJ_TRIPLET);
        kd->AddKey(link, tobj);
        return 1;
    }
    rxString edge_key = rxStringFormat("^%s", link);
    void *members = rxFindSetKey(db, edge_key);
    if (members != NULL)
    {
        auto mob = rxHarvestSetmembers(members);
        char **p = (char **)&mob->members;
        for (size_t n = 0; n < mob->member_count; ++n)
        {
            char *member = *p;
            rxServerLog(rxLL_NOTICE, "AddMemberToKeysetForMatch for %s : %ld: %s", edge_key, n, p);
            ++p;
            int msegments = 0;
            rxString *mparts = rxStringSplitLen(member, strlen(member), "|", 1, &msegments);
            if (!rxStringMatch(parts[0], mparts[0], MATCH_IGNORE_CASE))
            {
                rxStringFreeSplitRes(mparts, msegments);
                continue;
            }
            void *mobj = kd->LookupKey(link /*terminal->key*/);
            if (mobj == NULL)
            {
                mobj = Graph_Triplet::New(db, terminal, mparts[1], rxStringTrim(parts[1], "^"), parms->optimization);
            }
            Graph_Triplet::Link(mobj, kd);
            if (parms->final_vertex_or_edge == TRAVERSE_FINAL_VERTEX)
            {
                rxString mkey = rxStringTrim(mparts[1], "^");
                // assert(rxGetObjectType(rxFindKey(0, mkey)) == rxOBJ_TRIPLET);
                kd->AddKey(mkey /*terminal->key*/, rxFindKey(0, mkey));
                rxStringFree(mkey);
                // ((Graph_Triplet *)mobj)->Show();
            }
            else
            {
                kd->AddKey(link /*terminal->key*/, mobj);
            }
        }
        rxFreeSetmembers(mob);
    }
    else
    {

        if (*parts[2] == 'S' || *(parts[2] + 1) == 'S')
            link = rxStringNew(link_parts[2]);
        if (*parts[2] == 'O' || *(parts[2] + 1) == 'O')
            link = rxStringNew(link_parts[1]);

        if (!tobj)
            if ((tobj = (Graph_Triplet *)rxFindHashKey(db, link)) == NULL)
                return 0;
        assert(rxGetObjectType(tobj) == rxOBJ_TRIPLET);
        kd->AddKey(link, tobj);
    }
    rxStringFreeSplitRes(parts, segments);
    rxStringFreeSplitRes(link_parts, link_segments);
    return 1;
}

int getEdgeDirection(rxString flowy)
{
    // if(flowy[0] == 'S')
    //     return GRAPH_TRAVERSE_OUT;
    // if(flowy[1] == 'O')
    //     return GRAPH_TRAVERSE_IN;
    if (strcmp(flowy, Graph_Subject_to_Edge) == 0)
        return GRAPH_TRAVERSE_OUT;
    if (strcmp(flowy, Graph_Edge_to_Object) == 0)
        return GRAPH_TRAVERSE_IN;

    if (strcmp(flowy, Graph_Object_to_Edge) == 0)
        return GRAPH_TRAVERSE_IN;
    if (strcmp(flowy, Graph_Edge_to_Subject) == 0)
        return GRAPH_TRAVERSE_OUT;
    return GRAPH_TRAVERSE_INOUT;
}

static double ExecuteExpressionParameter(MatchParameters *parms, void *data)
{

    auto inlen = parms->stack->Size();
    parms->stack->Execute(parms->object_expression->ObjectExpression(), data);
    auto outlen = parms->stack->Size();
    if ((outlen - inlen) == 1)
    {
        FaBlok *r = parms->stack->Pop();
        double f = atof(r->AsSds());
        return f;
    }
    return 0.0;
}

int matchEdges(int db, Graph_Leg *leg, FaBlok *kd, GraphStack<Graph_Leg> *bsf_q, rax *touches, rax *terminators, MatchParameters *parms)
{
    if (terminators)
    {
        if (raxContains(terminators, leg->key))
        {
            AddMemberToKeysetForMatch(db, (unsigned char *)leg->key, strlen(leg->key), kd, leg, NULL, parms);
            return 1;
        }
    }
    rxString key = rxStringFormat("^%s", leg->key);
    void *zobj = rxFindSetKey(db, key);
    if (zobj == NULL)
    {
        rxStringFree(key);
        return 0;
    }
    int numkeys = 0;

    auto mob = rxHarvestSetmembers(zobj);
    char **p = (char **)&mob->members;
    for (size_t n = 0; n < mob->member_count; ++n)
    {
        char *elesds = *p;
        rxServerLog(rxLL_NOTICE, "matchEdges for %s : %d: %s", key, n, elesds);
        int segments = 0;
        rxString *parts = rxStringSplitLen(elesds, strlen(elesds), "|", 1, &segments);
        double weight = segments > 3 ? atof(parts[3]) : 1.0;
        rxString link = rxStringNew(parts[1] + 1);
        int link_segments = 0;
        rxString *link_parts = rxStringSplitLen(link, strlen(link), ":", 1, &link_segments);
        // Does the link match the  pattern?
        if (segments >= 3)
        {
            if (parms->excludes != NULL)
            {
                void *excluded = raxFind(parms->excludes, (UCHAR *)parts[0], strlen(parts[0]));
                if (excluded != raxNotFound)
                {
                    parts = rxStringFreeSplitRes(parts, segments);
                    continue;
                }
            }
            if (parms->includes != NULL)
            {
                void *included = raxFind(parms->includes, (UCHAR *)parts[0], strlen(parts[0]));
                if (included == raxNotFound)
                {
                    parts = rxStringFreeSplitRes(parts, segments);
                    continue;
                }
            }
            // Only traverse edges in the requested direction
            // Only traverse edges in the requested direction
            if (parms->traverse_direction != GRAPH_TRAVERSE_INOUT && getEdgeDirection(parts[2]) != parms->traverse_direction)
            {
                parts = rxStringFreeSplitRes(parts, segments);
                link_parts = rxStringFreeSplitRes(link_parts, link_segments);
                rxStringFree(link);
                continue;
            }
        }
        int doesMatchOneOfThePatterns = 0;
        // void *included = (parms->includes) ? raxFind(parms->includes, (UCHAR *)parts[0], strlen(parts[0])) : raxNotFound;
        // if (included != raxNotFound)
        //     doesMatchOneOfThePatterns = 1;
        if (doesMatchOneOfThePatterns == 0 && parms->patterns != NULL)
        {
            listIter *li = listGetIterator(parms->patterns, 0);
            listNode *ln;
            while ((ln = listNext(li)) && doesMatchOneOfThePatterns == 0)
            {
                rxString pattern = (rxString)ln->value;

                doesMatchOneOfThePatterns += (rxStringMatch(pattern, (const char *)parts[0], 1));
            }
            listReleaseIterator(li);
        }
        if (doesMatchOneOfThePatterns != 0)
        {
            Graph_Leg *path = leg;
            while (path)
            {
                path = path->origin;
            }
            numkeys += AddMemberToKeysetForMatch(db, (unsigned char *)elesds, strlen(elesds), kd, leg, (UCHAR *)key, parms);
        }
        else if (parms->patterns == NULL /*&& parms->includes == NULL*/)
        {
            // Is the subject or object of the requested type?
            void *next_obj = rxFindKey(db, link);
            if (rxGetObjectType(next_obj) == rxOBJ_HASH)
            {
                // TODO: Use ParserToken *
                if (parms->object_expression)
                {
                    weight = ExecuteExpressionParameter(parms, next_obj);
                }
                else
                {
                    rxString optimiseFieldValue = rxGetHashField(next_obj, parms->length_field);
                    if (optimiseFieldValue && strlen(optimiseFieldValue) > 0)
                    {
                        double value = atof(optimiseFieldValue);
                        if (value != 0)
                            weight = value;
                    }
                }
            }
            rxString half = rxGetHashField(next_obj, "half");
            rxString edge = rxGetHashField(next_obj, "edge");

            Graph_Leg *next = Graph_Leg::Add(link, weight * parms->optimization, leg, bsf_q, touches, 1, (half && edge) ? ((strcmp(half, link) == 0) ? edge : half) : NULL);

            if (next)
            {
                next->obj = next_obj;
            }
        }
        rxStringFreeSplitRes(parts, segments);
        rxStringFreeSplitRes(link_parts, link_segments);
        rxStringFree(link);
        ++p;
    }
    rxFreeSetmembers(mob);
    rxStringFree(key);
    return numkeys;
}

int breadthFirstSearch(FaBlok *leaders, FaBlok *kd, rax *terminators, MatchParameters *parms)
{
    int numkeys = 0;

    if (raxSize(leaders->keyset) <= 0)
        leaders->LoadKey(0, leaders->setname);

    if (leaders && raxSize(leaders->keyset) > 0)
    {
        GraphStack<Graph_Leg> bsf_q;
        GraphStack<Graph_Leg> bsf_c;
        rax *touches = raxNew();
        // Seed the search
        raxIterator leadersIterator;
        raxStart(&leadersIterator, leaders->keyset);
        raxSeek(&leadersIterator, "^", NULL, 0);
        while (raxNext(&leadersIterator))
        {
            rxString key = rxStringNewLen((const char *)leadersIterator.key, leadersIterator.key_len);
            Graph_Leg *leg = Graph_Leg::Add(key, 0.0, &bsf_q, parms->optimization);
            leg->start = leg;
            leg->obj = leadersIterator.data;
            raxInsert(touches, (UCHAR *)leg->key, strlen(leg->key), leg, NULL);
        }
        raxStop(&leadersIterator);

        int db = 0;
        while (bsf_q.HasEntries())
        {
            Graph_Leg *leg = bsf_q.Pop();
            // printf("Evaluating %s %f\n", leg->key, leg->length);
            numkeys += matchEdges(db, leg, kd, &bsf_q, touches, terminators, parms);
            bsf_c.Enqueue(leg);
        }

        while (bsf_c.HasEntries())
        {
            bsf_c.Dequeue();
        }

        raxIterator releaseIterator;
        raxStart(&releaseIterator, touches);
        raxSeek(&releaseIterator, "^", NULL, 0);
        while (raxNext(&releaseIterator))
        {
            auto *leg = (Graph_Leg *)releaseIterator.data;
            Graph_Leg::Release(leg);
            raxRemove(touches, releaseIterator.key, releaseIterator.key_len, NULL);
            raxSeek(&releaseIterator, "^", NULL, 0);
        }
        raxStop(&releaseIterator);

        raxFree(touches);
    }
    return numkeys;
}
