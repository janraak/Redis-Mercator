#include <cstring>

#include "sjiboleth-fablok.hpp"
#include "sjiboleth-graph.hpp"
#include "sjiboleth.h"
#include "sjiboleth.hpp"
#define STASHERS_ONLY
#include "graphstackentry.hpp"
// #include "simpleQueue.hpp"

extern void *rxStashCommand(SimpleQueue *ctx, const char *command, int argc, ...);
extern void ExecuteRedisCommand(SimpleQueue *ctx, void *stash, const char *address, int port);
extern void FreeStash(void *stash);

#define MATCH_PATTERN 1
#define NO_MATCH_PATTERN 0

static FaBlok *getAllKeysByRegex(int dbNo, const char *regex, int on_matched, const char *set_name);
static FaBlok *getAllKeysByType(int dbNo, const char *regex, int on_matched, rxString type_name);
static FaBlok *getAllKeysByField(int dbNo, const char *regex, int on_matched, rxString field, list *value, rxString keyset_name);
// static FaBlok *getAllKeysByField(int dbNo, const char *regex, int on_matched, rxString field, rxString value);
// static FaBlok *getAllKeysByType(int dbNo, const char *regex, int on_matched, field, list *patterns, rxString set_name);

#define Graph_vertex_or_edge_type rxStringNew("type")

#define Graph_Subject_to_Edge rxStringNew("SE")
#define Graph_Object_to_Edge rxStringNew("OE")
#define Graph_Edge_to_Subject rxStringNew("ES")
#define Graph_Edge_to_Object rxStringNew("EO")

#define GRAPH_TRAVERSE_OUT 1
#define GRAPH_TRAVERSE_IN 2
#define GRAPH_TRAVERSE_INOUT 3

int breadthFirstSearch(FaBlok *leaders, list *patterns, FaBlok *kd, int traverse_direction, short filter_only, rax *terminators, rax *includes, rax *excludes);

static int redis_HSET(SilNikParowy_Kontekst *, const char *key, const char *field, const char *value, const char *host_reference)
{
    auto *stash = rxStashCommand(NULL, "HSET", 3, key, field, value);
    ExecuteRedisCommand(NULL, stash, host_reference);
    FreeStash(stash);

    // RedisModuleCallReply *reply = RedisModule_Call(
    //     p->module_contex,
    //     "HSET",
    //     "ccc",
    //     key,
    //     field,
    //     value);
    // if (reply)
    //     RedisModule_FreeCallReply(reply);
    return C_OK;
}

static int redis_SADD(SilNikParowy_Kontekst *, const char *key, const char *member, const char *host_reference)
{
    auto *stash = rxStashCommand(NULL, "SADD", 2, key, member);
    ExecuteRedisCommand(NULL, stash, host_reference);
    FreeStash(stash);
    // RedisModuleCallReply *reply = RedisModule_Call(
    //     p->module_contex,
    //     "SADD",
    //     "cc",
    //     key,
    //     member);
    // if (reply)
    //     RedisModule_FreeCallReply(reply);
    return C_OK;
}

SJIBOLETH_HANDLER(GremlinDialect::executeMatch)
{
    STACK_CHECK(1);
    if (stack->IsMemoized("matchIncludes") && stack->IsMemoized("matchExcludes"))
    {
        ERROR("Cannot use an exclude and an include filter together!");
    }

    FaBlok *pl = stack->Pop();
    FaBlok *result = NULL;
    raxIterator leadersIterator;
    if (pl->IsParameterList())
    {
        GraphStack<FaBlok> *cpy = pl->parameter_list->Copy();
        FaBlok *leaders = cpy->Dequeue();
        FaBlok *terminators = cpy->Dequeue();
        rxString setname = rxStringFormat("%s %s %s", leaders->setname, t->Token(), terminators->setname);
        result = FaBlok::Get(setname, KeysetDescriptor_TYPE_GREMLINSET);
        result->AsTemp();
        result->Open();

        raxStart(&leadersIterator, &leaders->keyset);
        raxSeek(&leadersIterator, "^", NULL, 0);
        result = FaBlok::Get(setname, KeysetDescriptor_TYPE_GREMLINSET);

        // Temporary stores for pairs<leader, terminator> to match!
        rxString leader_temp_setname = rxStringFormat("%s:::PATH:::%lld::FROM", t->Token(), ustime());
        FaBlok *from = FaBlok::New(leader_temp_setname, KeysetDescriptor_TYPE_GREMLINSET);
        rxString terminator_temp_setname = rxStringFormat("%s:::PATH:::%lld::TO", t->Token(), ustime());
        FaBlok *to = FaBlok::New(terminator_temp_setname, KeysetDescriptor_TYPE_GREMLINSET);

        raxIterator terminatorsIterator;
        raxStart(&terminatorsIterator, &terminators->keyset);

        while (raxNext(&leadersIterator))
        {
            rxString leader = rxStringNewLen((const char*)leadersIterator.key, leadersIterator.key_len);
            if (rxStringCharCount(leader, ':') < 2)
            {
                from->InsertKey(leadersIterator.key, leadersIterator.key_len, leadersIterator.data);

                raxSeek(&terminatorsIterator, "^", NULL, 0);
                while (raxNext(&terminatorsIterator))
                {
                    rxString terminator = rxStringNewLen((const char*)terminatorsIterator.key, terminatorsIterator.key_len);
                    if (rxStringCharCount(terminator, ':') < 2)
                    {
                        to->InsertKey(terminatorsIterator.key, terminatorsIterator.key_len, terminatorsIterator.data);
                        breadthFirstSearch(from, NULL, result, GRAPH_TRAVERSE_INOUT, 1, &to->keyset, (rax *)stack->Recall("matchIncludes"), (rax *)stack->Recall("matchExcludes"));
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
        delete to;
        delete from;
        result = FaBlok::Get(setname, KeysetDescriptor_TYPE_GREMLINSET);
        PushResult(result, stack);
        rxStringFree(setname);
        delete cpy;
    }
    else
        PushResult(pl, stack);

    FaBlok::Delete(pl);
    if (stack->IsMemoized("matchIncludes"))
    {
        rax *set = (rax *)stack->Forget((char *)"matchIncludes");
        raxFree(set);
    }
    if (stack->IsMemoized("matchExcludes"))
    {
        rax *set = (rax *)stack->Forget((char *)"matchExcludes");
        raxFree(set);
    }
}
END_SJIBOLETH_HANDLER(GremlinDialect::executeMatch)

SJIBOLETH_HANDLER(GremlinDialect::executeNomatch)
{
    STACK_CHECK(1);
    if (stack->IsMemoized("matchIncludes") && stack->IsMemoized("matchExcludes"))
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
        FaBlok *terminators = cpy->Dequeue();
        rxString setname = rxStringFormat("%s %s %s", leaders->setname, t->Token(), terminators->setname);
        FaBlok *nomatches = FaBlok::Get(setname, KeysetDescriptor_TYPE_GREMLINSET);
        FaBlok *matches_for_leader = FaBlok::Get(setname, KeysetDescriptor_TYPE_GREMLINSET);
        matches_for_leader->Open();
        raxStart(&leadersIterator, &leaders->keyset);
        raxStart(&terminatorsIterator, &terminators->keyset);

        raxSeek(&leadersIterator, "^", NULL, 0);

        // Temporary stores for pairs<leader, terminator> to match!
        rxString leader_temp_setname = rxStringFormat("%s:::PATH:::%lld::FROM", t->Token(), ustime());
        FaBlok *from = FaBlok::New(leader_temp_setname, KeysetDescriptor_TYPE_GREMLINSET);
        rxString terminator_temp_setname = rxStringFormat("%s:::PATH:::%lld::TO", t->Token(), ustime());
        FaBlok *to = FaBlok::New(terminator_temp_setname, KeysetDescriptor_TYPE_GREMLINSET);

        while (raxNext(&leadersIterator))
        {
            rxString leader = rxStringNewLen((const char*)leadersIterator.key, leadersIterator.key_len);
            if (rxStringCharCount(leader, ':') < 2)
            {
                from->InsertKey(leadersIterator.key, leadersIterator.key_len, leadersIterator.data);

                raxSeek(&terminatorsIterator, "^", NULL, 0);
                while (raxNext(&terminatorsIterator))
                {
                    rxString terminator = rxStringNewLen((const char*)terminatorsIterator.key, terminatorsIterator.key_len);
                    if (rxStringCharCount(terminator, ':') < 2)
                    {
                        to->InsertKey(terminatorsIterator.key, terminatorsIterator.key_len, terminatorsIterator.data);
                        breadthFirstSearch(from, NULL, matches_for_leader, GRAPH_TRAVERSE_INOUT, 1, &to->keyset, (rax *)stack->Recall("matchIncludes"), (rax *)stack->Recall("matchExcludes"));
                    }
                    to->RemoveKey(terminatorsIterator.key, terminatorsIterator.key_len);
                    rxStringFree(terminator);
                }
            }
            from->RemoveKey(leadersIterator.key, leadersIterator.key_len);
            raxStop(&terminatorsIterator);
            matches_for_leader->Close();
            if (matches_for_leader->size == 0)
                nomatches->AddKey(leader, rxFindKey(0, leader));
            rxStringFree(leader);
        }
        raxStop(&leadersIterator);
        delete to;
        delete from;
        nomatches = FaBlok::Get(setname, KeysetDescriptor_TYPE_GREMLINSET);
        PushResult(nomatches, stack);
        rxStringFree(setname);
        delete cpy;
    }
    else
        PushResult(pl, stack);

    FaBlok::Delete(pl);
    if (stack->IsMemoized("matchIncludes"))
    {
        rax *set = (rax *)stack->Forget((char *)"matchIncludes");
        raxFree(set);
    }
    if (stack->IsMemoized("matchExcludes"))
    {
        rax *set = (rax *)stack->Forget((char *)"matchExcludes");
        raxFree(set);
    }
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
    if (pl->IsParameterList())
    {
        while (pl->parameter_list->HasEntries())
        {
            FaBlok *f = pl->parameter_list->Dequeue();
            raxInsert(filter, (UCHAR *)f->setname, strlen(f->setname), stack, NULL);
            FaBlok::Delete(f);
        }
    }
    else
    {
        raxInsert(filter, (UCHAR *)pl->setname, strlen(pl->setname), stack, NULL);
    }
    FaBlok::Delete(pl);
    if (toupper(t->Token()[0]) == 'I')
    {
        stack->Memoize("matchIncludes", filter);
    }
    else
    {
        stack->Memoize("matchExcludes", filter);
    }
}
END_SJIBOLETH_HANDLER(GremlinDialect::executeGremlinMatchInExclude)

/*
    GremlinDialect::executeAllVertices

    Create a stack entry with a set of all or selected vertices
    and push it onto the stack.

    1) When the stack has entries:
        v(%setname%) -> Not needed just (%setname%)
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
                kd = getAllKeysByField(0, "[A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*", NO_MATCH_PATTERN, ad->AsSds(), patterns, setname);
            }
            else
                kd->Rename(setname);
            listRelease(patterns);
        }
        else
        {
            auto *key_entry = rxFindKey(0, kd->setname);
            if (key_entry != NULL)
                kd->LoadKey(0, kd->setname);
            else
                kd = getAllKeysByType(0, "[A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*", NO_MATCH_PATTERN, kd->AsSds());
        }
    }
    else
    {
        // Filter vertices from top stack entry
        kd = getAllKeysByRegex(0, "[A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*", NO_MATCH_PATTERN, "vertices");
    }
    kd->AsTemp();
    kd->vertices_or_edges = VERTEX_SET;
    kd->ValueType(KeysetDescriptor_TYPE_GREMLIN_VERTEX_SET);
    kd->Close();
    // PushResult(kd, stack);
    stack->Memoize("V", kd);
}
END_SJIBOLETH_HANDLER(GremlinDialect::executeAllVertices)

SJIBOLETH_HANDLER(executeAllEdges)
{
    rxUNUSED(t);

    FaBlok *kd = NULL;
    if (stack->HasEntries())
    {
        kd = stack->Pop();
        if (kd->IsParameterList())
        {
            FaBlok *ad = kd->parameter_list->Dequeue();
            list *patterns = listCreate();
            rxString setname = rxStringDup(ad->AsSds());
            setname = rxStringFormat("%s%s", setname, " in");
            while (kd->parameter_list->HasEntries())
            {
                FaBlok *vd = kd->parameter_list->Dequeue();
                setname = rxStringFormat("%s%s", setname, " ");
                setname = rxStringFormat("%s%s", setname, vd->AsSds());
                patterns = listAddNodeTail(patterns, strdup(vd->AsSds()));
            }
            kd = getAllKeysByField(0, "[A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*", MATCH_PATTERN, ad->AsSds(), patterns, setname);
            listRelease(patterns);
        }
        else
        {
            kd = getAllKeysByType(0, "[A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*", MATCH_PATTERN, kd->AsSds());
        }
    }
    else
    {
        // Filter vertices from top stack entry
        kd = getAllKeysByRegex(0, "[A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*", MATCH_PATTERN, "edges");
    }
    kd->AsTemp();
    kd->vertices_or_edges = VERTEX_SET;
    kd->ValueType(KeysetDescriptor_TYPE_GREMLIN_VERTEX_SET);
    PushResult(kd, stack);
    stack->Memoize("E", kd);
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
    STACK_CHECK(1);

    FaBlok *sd = stack->Pop();
    sd->Open();
    FaBlok *memo = (FaBlok *)stack->Recall("V");
    if (sd->IsValueType(KeysetDescriptor_TYPE_MONITORED_SET) || sd->IsValueType(KeysetDescriptor_TYPE_GREMLIN_AS_SET))
    {
        // Reuse set
        sd->reuse_count++;
    }
    else
    {
        // Save set as
        FaBlok *base = memo ? memo : stack->Pop();
        if (base != NULL)
        {
            base->CopyTo(sd);
        }
        else
        {
            base = getAllKeysByRegex(0, "[A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*", NO_MATCH_PATTERN, "vertices");
            base->CopyTo(sd);
        }
        sd->ValueType(KeysetDescriptor_TYPE_GREMLIN_VERTEX_SET + KeysetDescriptor_TYPE_GREMLIN_AS_SET);
    }
    PushResult(sd, stack);
}
END_SJIBOLETH_HANDLER(executeGremlinAs)

static bool FilterTypes(unsigned char *, size_t, void *data, void **privData)
{
    short *must_match = (short *)privData[0];
    rxString field = (rxString)privData[1];
    int *flen = (int *)privData[2];
    rxString pattern = (rxString)privData[3];
    int *plen = (int *)privData[4];
    rxComparisonProc *operatorFn = (rxComparisonProc *)privData[5];
    rxString max_pattern = (rxString)privData[6];

    if (operatorFn == NULL)
        return rxMatchHasValue(data, field, pattern, *plen) == *must_match;
    else
    {
        rxString value = rxStringEmpty();
        bool must_free = false;
        switch (rxGetObjectType(data))
        {
        case rxOBJ_STRING:
            value = (rxString)rxGetContainedObject(data);
            break;
        case rxOBJ_HASH:
            value = (rxString)rxGetHashField(data, field);
            must_free = true;
            break;
        }
        if (max_pattern != NULL)
            return ((rxComparisonProc2 *)operatorFn)(value, *flen, pattern, max_pattern);
        int rc = operatorFn(value, *flen, pattern);
        if (must_free)
            rxStringFree(value);
        return rc;
    }
}

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
    int field_len = strlen(ad->AsSds());
    int plen = strlen(vd->AsSds());
    od->AsTemp();
    rxServerLogRaw(rxLL_WARNING, rxStringFormat("About to execute executeGremlinHas on %s\n", set_name));
    // rxStringFree(msg);
    void *params[] = {&must_match, (void *)ad->AsSds(), &field_len, (void *)vd->AsSds(), &plen, NULL, NULL};
    FaBlok *kd = od->Copy(set_name, KeysetDescriptor_TYPE_GREMLINSET, FilterTypes, params);
    kd->AsTemp();
    PushResult(kd, stack);
}
END_SJIBOLETH_HANDLER(executeGremlinHas)

static SJIBOLETH_HANDLER(executeGremlinComparePropertyToValue)
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

        int field_len = strlen(property->AsSds());
        int plen = strlen(value->AsSds());
        keyset->AsTemp();
        void *params[] = {&plen, (void *)property->AsSds(), &field_len, (void *)value->AsSds(), &plen, (void *)rxFindComparisonProc((char *)t->Operation()), NULL};
        FaBlok *kd = keyset->Copy(set_name, KeysetDescriptor_TYPE_GREMLINSET, FilterTypes, params);
        kd->AsTemp();
        rxStringFree(set_name);
        PushResult(kd, stack);
    }
    else
    {
        FaBlok *property = stack->Pop();
        FaBlok *keyset = stack->Pop();
        // if (!(condition->IsParameterList() && condition->parameter_list->Size() == 2)){
        //     ERROR("property and number required")
        // }

        FaBlok *value = condition;

        rxString set_name = rxStringFormat("%s:%s%s%s", keyset->AsSds(), property->AsSds(), t->Operation(), value->AsSds());

        int field_len = strlen(property->AsSds());
        int plen = strlen(value->AsSds());
        keyset->AsTemp();
        void *params[] = {&plen, (void *)property->AsSds(), &field_len, (void *)value->AsSds(), &plen, (void *)rxFindComparisonProc((char *)t->Operation()), NULL};
        FaBlok *kd = keyset->Copy(set_name, KeysetDescriptor_TYPE_GREMLINSET, FilterTypes, params);
        kd->AsTemp();
        rxStringFree(set_name);
        PushResult(kd, stack);
    }
}
END_SJIBOLETH_HANDLER(executeGremlinComparePropertyToValue)

static SJIBOLETH_HANDLER(executeGremlinComparePropertyToRangeValue)
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

    int field_len = strlen(property->AsSds());
    int plen = strlen(minValue->AsSds());
    keyset->AsTemp();

    void *params[] = {&plen, (void *)property->AsSds(), &field_len, (void *)minValue->AsSds(), &plen, (void *)rxFindComparisonProc((char *)t->Operation()), (void *)maxValue->AsSds()};
    FaBlok *kd = keyset->Copy(set_name, KeysetDescriptor_TYPE_GREMLINSET, FilterTypes, params);
    kd->AsTemp();
    PushResult(kd, stack);
}
END_SJIBOLETH_HANDLER(executeGremlinComparePropertyToRangeValue)

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
    int field_len = strlen(ad->AsSds());
    int plen = 1;
    void *params[] = {&must_match, (void *)ad->AsSds(), &field_len, (void *)rxStringNew("*"), &plen, NULL, NULL};
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
    FaBlok *out = FaBlok::Get(setname, KeysetDescriptor_TYPE_GREMLINSET);
    out->Open();
    {
        raxIterator ungroupedIterator;
        raxStart(&ungroupedIterator, &kd->keyset);
        raxSeek(&ungroupedIterator, "^", NULL, 0);
        while (raxNext(&ungroupedIterator))
        {
            if (ungroupedIterator.data != NULL)
            {
                rxString key_to_match = rxStringNewLen((const char*)ungroupedIterator.key, ungroupedIterator.key_len);
                rxUNUSED(key_to_match); // TODO
                rxString groupKey;
                switch (rxGetObjectType(ungroupedIterator.data))
                {
                case rxOBJ_HASH:
                {
                    groupKey = rxGetHashField(ungroupedIterator.data, gbd->setname);
                    CGraph_Triplet *ge = (Graph_Triplet *)out->LookupKey(groupKey);
                    if (ge != NULL)
                    {
                        Graph_Triplet::AddObjectToTripletResult(ge, key_to_match, rxStringEmpty());
                    }
                    else
                    {
                        ge = Graph_Triplet::InitializeResult(0, NULL, groupKey, key_to_match, rxStringEmpty());
                        out->AddKey(groupKey, ge);
                    }
                }
                break;
                case OBJ_TRIPLET:
                {
                    Graph_Triplet *t = (Graph_Triplet *)rxGetContainedObject(ungroupedIterator.data);
                    if (toupper(gbd->setname[0]) == 'S')
                    {
                        groupKey = t->subject_key;
                        CGraph_Triplet *ge = (CGraph_Triplet *)out->LookupKey(groupKey);
                        if (ge == NULL)
                        {
                            ge = Graph_Triplet::InitializeResult(0, NULL, groupKey, NULL, rxStringEmpty());
                            out->AddKey(groupKey, ge);
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
                            CGraph_Triplet *ge = (CGraph_Triplet *)out->LookupKey(groupKey);
                            if (ge == NULL)
                            {
                                ge = Graph_Triplet::InitializeResult(0, NULL, groupKey, NULL, rxStringEmpty());
                                out->AddKey(groupKey, ge);
                            }
                            Graph_Triplet::AddObjectToTripletResult(ge, t->subject_key, rxStringEmpty());
                        }
                        t->StopObjectBrowse();
                    }
                }
                break;
                default:
                    ERROR("Unsupported object type, only hash keys or rx triplets are supported.");
                    return C_ERR;
                }
            }
        }
        raxStop(&ungroupedIterator);
    }
    rxStringFree(setname);
    PushResult(out, stack);
}
END_SJIBOLETH_HANDLER(executeGremlinGroupby)

// Do a breadth first for any vertex or edge of type <out>
int executeGremlinTraverse(SilNikParowy_Kontekst *stack, int traverse_direction, short filterOnly)
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
            listAddNodeTail(patterns, (void *)rxStringNew(p->setname));
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
        listAddNodeHead(patterns, (void *)rxStringNew(vd->setname));
    }
    if (!od)
    {
        od = getAllKeysByRegex(0, "[A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*[:][A-Z0-9@#$!%&_-]*", NO_MATCH_PATTERN, "vertices");
    }

    rxString set_name = rxStringFormat("%s->%s", od->setname, vd->setname);
    FaBlok *kd = FaBlok::Get(set_name, KeysetDescriptor_TYPE_GREMLINSET);
    breadthFirstSearch(od, patterns, kd, traverse_direction, filterOnly, NULL, NULL, NULL);
    PushResult(kd, stack);
    listRelease(patterns);
    rxStringFree(set_name);
    return C_OK;
}

// Do a breadth first for any vertex or edge of type <out>
SJIBOLETH_HANDLER(executeGremlinOut)
{

    rxUNUSED(t);
    STACK_CHECK(1);
    return executeGremlinTraverse(stack, GRAPH_TRAVERSE_OUT, 0);
}
END_SJIBOLETH_HANDLER_X(executeGremlinOut)

// Do a breadth first for any vertex or edge of type <out>
SJIBOLETH_HANDLER(executeGremlinIn)
{

    rxUNUSED(t);
    STACK_CHECK(1);
    return executeGremlinTraverse(stack, GRAPH_TRAVERSE_IN, 0);
}
END_SJIBOLETH_HANDLER_X(executeGremlinIn)

// Do a breadth first for any vertex or edge of type <out>
SJIBOLETH_HANDLER(executeGremlinInOut)
{

    rxUNUSED(t);
    STACK_CHECK(1);
    return executeGremlinTraverse(stack, GRAPH_TRAVERSE_INOUT, 0);
}
END_SJIBOLETH_HANDLER_X(executeGremlinInOut)

// Do a breadth first to filter any vertex or edge with an <out> type
SJIBOLETH_HANDLER(executeGremlinHasOut)
{

    rxUNUSED(t);
    STACK_CHECK(1);
    return executeGremlinTraverse(stack, GRAPH_TRAVERSE_OUT, 1);
}
END_SJIBOLETH_HANDLER_X(executeGremlinHasOut)

// Do a breadth first to filter any vertex or edge with an <in> type
SJIBOLETH_HANDLER(executeGremlinHasIn)
{

    rxUNUSED(t);
    STACK_CHECK(1);
    return executeGremlinTraverse(stack, GRAPH_TRAVERSE_IN, 1);
}
END_SJIBOLETH_HANDLER_X(executeGremlinHasIn)

// Do a breadth first to filter any vertex or edge with an <in/out> type
SJIBOLETH_HANDLER(executeGremlinHasInOut)
{

    rxUNUSED(t);
    STACK_CHECK(1);
    return executeGremlinTraverse(stack, GRAPH_TRAVERSE_INOUT, 1);
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
    FaBlok *t = pl->parameter_list->Dequeue();
    redis_HSET(stack, i->setname, Graph_vertex_or_edge_type, t->setname, data_config->host_reference);
    void *o = rxFindKey(0, i->setname);
    FaBlok *v = FaBlok::Get(i->setname, KeysetDescriptor_TYPE_GREMLINSET);
    v->AddKey(i->setname, o);
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
    v->vertices_or_edges = VERTEX_SET;
    PushResult(v, stack);
}
END_SJIBOLETH_HANDLER(executeGremlinAddVertex)

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

    redisNodeInfo *data_config = rxDataNode();

    rxString forward_edge;
    rxString backward_edge;
    if (t->Is("to") || t->Is("object"))
    {
        forward_edge = rxStringNew("SE");
        backward_edge = rxStringNew("ES");
    }
    else
    {
        forward_edge = rxStringNew("OE");
        backward_edge = rxStringNew("EO");
    }
    FaBlok *v = stack->Pop();
    FaBlok *e = stack->Pop();

    rax *vertex_set = NULL;
    if (v->IsParameterList() && v->parameter_list->Size() == 2)
    {
        FaBlok *nv = createVertex(stack, v); // Use new vertex!
        vertex_set = &nv->keyset;
        FaBlok::Delete(v);
    }
    else
    {
        // Use existing vertex!
        vertex_set = raxNew();
        void *o = rxFindKey(0, v->setname);
        if (o == NULL)
        {
            // v->setname is not a local key set!!
            // Assume the setname to be an attribute of the left side of the predicate!
        }
        raxInsert(vertex_set, (UCHAR *)v->setname, strlen(v->setname), o, NULL);
    }
    raxIterator edge_iterator;
    raxStart(&edge_iterator, &e->keyset);
    raxSeek(&edge_iterator, "^", NULL, 0);
    while ((raxNext(&edge_iterator)))
    {
        rxString e_key = rxStringNewLen((const char *)edge_iterator.key, edge_iterator.key_len);
        void *e = rxFindKey(0, e_key);
        unsigned char *vstr;
        POINTER vlen;
        long long vll;
        rxHashTypeGetValue(e, Graph_vertex_or_edge_type, &vstr, &vlen, &vll);
        rxString edge_type = rxStringNewLen((const char*)vstr, vlen);
        raxIterator vertex_iterator;
        raxStart(&vertex_iterator, vertex_set);
        while ((raxNext(&vertex_iterator)))
        {
            rxString v_key = rxStringNewLen((const char *)vertex_iterator.key, vertex_iterator.key_len);
            // rxServerLog(LL_NOTICE, "rename? e:%s p:%s s|o:%s", e_key, tok->token, v_key);
            rxString edge_key = rxStringFormat("^%s", e_key);
            rxString vertex_key = rxStringFormat("^%s", v_key);
            rxString link = rxStringFormat("%s|%s|%s|1.0", edge_type, edge_key, forward_edge);

            // rxServerLog(LL_NOTICE, "%s | %s | %s", e_key, v_key, link);
            redis_SADD(stack, vertex_key, link, data_config->host_reference);
            link = rxStringFormat("%s|%s|%s|1.0", edge_type, vertex_key, backward_edge);

            // rxServerLog(LL_NOTICE, "%s | %s | %s", e_key, v_key, link);
            redis_SADD(stack, edge_key, link, data_config->host_reference);
        }
        raxStop(&vertex_iterator);
    }
    raxStop(&edge_iterator);

    e->vertices_or_edges = VERTEX_SET;
    PushResult(e, stack);
}
END_SJIBOLETH_HANDLER(executeGremlinLinkVertex)

/*
g.V(1).as('a').out('created').in('created').where(neq('a')).
  addE('co-developer').from('a').property('year',2009) //// (1)
g.V(3,4,5).aggregate('x').has('name','josh').as('a').
  select('x').unfold().hasLabel('software').addE('createdBy').to('a') //// (2)
g.V().as('a').out('created').addE('createdBy').to('a').property('acl','public') //// (3)
g.V(1).as('a').out('knows').
  addE('livesNear').from('a').property('year',2009).
  inV().inE('livesNear').values('year') //// (4)
g.V().match(
        __.as('a').out('knows').as('b'),
        __.as('a').out('created').as('c'),
        __.as('b').out('created').as('c')).
      addE('friendlyCollaborator').from('a').to('b').
        property(id,23).property('project',select('c').values('name')) //// (5)
g.E(23).valueMap()
vMarko = g.V().has('name','marko').next()
vPeter = g.V().has('name','peter').next()
g.V(vMarko).addE('knows').to(vPeter) //// (6)
g.addE('knows').from(vMarko).to(vPeter) //7
*/
SJIBOLETH_HANDLER(executeGremlinAddEdge)
{
    rxUNUSED(t);

    // Get parameters g:addE(<iri>, <type>)
    // Get parameters g:v().addV(<iri>, <type>)

    redisNodeInfo *data_config = rxDataNode();

    FaBlok *et = stack->Pop();
    size_t no_vertex_parms = et->IsParameterList() ? et->parameter_list->Size() : 0;
    rxString asterisk = rxStringNew("@");
    int is_link_from_property = 0;
    if (et->IsParameterList())
    {
        is_link_from_property = (strcmp(et->parameter_list->Peek()->setname, asterisk) == 0);
    }
    rxServerLogRaw(rxLL_WARNING, rxStringFormat("executeGremlinAddEdge %s=%d %d parameters %d parameters", asterisk, is_link_from_property, et->parameter_list->Size(), stack->Size()));
    rxStringFree(asterisk);
    if (et->IsParameterList() && no_vertex_parms >= 3 && is_link_from_property == 1)
    {
        FaBlok *e = FaBlok::Get(et->setname, KeysetDescriptor_TYPE_GREMLINSET);
        FaBlok *pk = stack->Peek();
        rxServerLogRaw(rxLL_WARNING, rxStringFormat("executeGremlinAddEdge stack peek %s", pk->setname));
        FaBlok *s = et->parameter_list->Dequeue(); // Ignore @ marker
        s = stack->Pop();                          // left side subject set
        rxServerLogRaw(rxLL_WARNING, rxStringFormat("executeGremlinAddEdge subject set %s %d entries ", s->setname, s->size));
        FaBlok *pred = et->parameter_list->Dequeue(); // predicate(type)
        rxServerLogRaw(rxLL_WARNING, rxStringFormat("executeGremlinAddEdge predicate %s", pred->setname));
        FaBlok *inv_pred;
        rxString backLink = rxStringEmpty();
        if (no_vertex_parms >= 3)
            inv_pred = et->parameter_list->Dequeue();
        else
            inv_pred = pred;
        const char *fwd_predicate = pred->setname;
        const char *bwd_predicate = inv_pred->setname;
        rxServerLogRaw(rxLL_WARNING, rxStringFormat("executeGremlinAddEdge inverse predicate %s", bwd_predicate));
        // FaBlok *o = NULL; // iri of object by materializing iri from pred property on subject!

        raxIterator SubjectIterator;
        raxStart(&SubjectIterator, &s->keyset);
        raxSeek(&SubjectIterator, "^", NULL, 0);
        while (raxNext(&SubjectIterator))
        {
            rxString key = rxStringNewLen((const char*)SubjectIterator.key, SubjectIterator.key_len);
            void *subject = rxFindHashKey(0, key);
            if (subject == NULL)
                continue;
            rxString objectKey = rxGetHashField(subject, fwd_predicate);
            if (strlen(objectKey) == 0)
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

            rxServerLogRaw(rxLL_WARNING, rxStringFormat("edge_name: %s\nedge_link: %s\ninv_edge_name: %s\ninv_edge_link: %s\nsubjectKey: %s\nobjectKey: %s\n",
                                                      edge_name, edge_link, inv_edge_name, inv_edge_link, key, objectKey));
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

            rxString SE = rxStringFormat("%s|%s|SE|1.0|+", fwd_predicate, edge_link);
            rxString ES = rxStringFormat("%s|^%s|ES|1.0", bwd_predicate, key);
            rxString EO = rxStringFormat("%s|%s|OE|1.0", bwd_predicate, inv_edge_link);
            rxString OE = rxStringFormat("%s|^%s|EO|1.0", fwd_predicate, objectKey);

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
    else if (et->IsParameterList() && no_vertex_parms >= 3)
    {
        // addE(dge)(<subject>, <type>, <object>)
        FaBlok *s = et->parameter_list->Dequeue();    // iri of subject
        FaBlok *pred = et->parameter_list->Dequeue(); // predicate(type)
        FaBlok *inv_pred;
        rxString backLink = rxStringEmpty();
        if (no_vertex_parms >= 4)
            inv_pred = et->parameter_list->Dequeue();
        else
            inv_pred = pred;
        FaBlok *o = et->parameter_list->Dequeue(); // iri of object

        rxString edge_name = rxStringFormat("%s:%s:%s", pred->setname, s->setname, o->setname);
        rxString edge_link = rxStringFormat("^%s", edge_name);
        rxString inv_edge_name;
        rxString inv_edge_link;
        if (pred == inv_pred)
        {
            inv_edge_name = edge_name;
            inv_edge_link = edge_link;
        }
        else
        {
            inv_edge_name = rxStringFormat("%s:%s:%s", inv_pred->setname, o->setname, s->setname);
            inv_edge_link = rxStringFormat("^%s", inv_edge_name);
        }
        rxString subject_link = rxStringFormat("^%s", s->setname);
        rxString object_link = rxStringFormat("^%s", o->setname);
        if (pred != inv_pred)
        {
            backLink = rxStringFormat("%s%s:%s:%s:%s", backLink, s->setname, pred->setname, inv_pred->setname, o->setname);
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

        rxString SE = rxStringFormat("%s|%s|SE|1.0|+", pred->setname, edge_link);
        rxString ES = rxStringFormat("%s|^%s|ES|1.0", inv_pred->setname, s->setname);
        rxString EO = rxStringFormat("%s|%s|OE|1.0", inv_pred->setname, inv_edge_link);
        rxString OE = rxStringFormat("%s|^%s|EO|1.0", pred->setname, o->setname);

        rxString keys[4] = {subject_link, edge_link, inv_edge_link, object_link};
        rxString member[4] = {SE, OE, ES, EO};
        for (int j = 0; j < 4; ++j)
        {
            redis_SADD(stack, keys[j], member[j], data_config->host_reference);
        }
        FaBlok *e = FaBlok::Get(edge_name, KeysetDescriptor_TYPE_GREMLINSET);
        void *oo = rxFindKey(0, edge_name);
        e->InsertKey(edge_name, oo);
        e->vertices_or_edges = EDGE_SET;
        PushResult(e, stack);
        FaBlok::Delete(et);
    }
    else
    {
        // addE(dge)(<type>)
        // Create temp key
        // the temp key will be renamed from the to(subject)/from(object) vertices
        rxString temp_key = rxStringFormat("%s:::%lld", et->setname, ustime());
        redis_HSET(stack, temp_key, Graph_vertex_or_edge_type, et->setname, data_config->host_reference);
        FaBlok *v = FaBlok::Get(rxStringNew(rxStringDup(temp_key)), KeysetDescriptor_TYPE_GREMLINSET);
        void *oo = rxFindKey(0, temp_key);
        v->InsertKey(temp_key, oo);

        v->vertices_or_edges = EDGE_SET;
        PushResult(v, stack);
    }
}
END_SJIBOLETH_HANDLER(executeGremlinAddEdge)

SJIBOLETH_HANDLER(executeGremlinAddProperty)
{
    rxUNUSED(t);

    FaBlok *pl = stack->Pop();
    FaBlok *s = stack->Pop();
    redisNodeInfo *data_config = rxDataNode();

    if (pl->IsParameterList())
    {
        if ((pl->parameter_list->Size() & 1) == 1)
        {
            ERROR("Incorrect parameters for AddProperty, must be [<a>, <v>]...");
        }

        FaBlok *a = pl->parameter_list->Dequeue();
        FaBlok *v = pl->parameter_list->Dequeue();

        raxIterator targetObjectIterator;
        raxStart(&targetObjectIterator, &s->keyset);
        raxSeek(&targetObjectIterator, "^", NULL, 0);
        while (raxNext(&targetObjectIterator))
        {
            rxString key = rxStringNewLen((const char*)targetObjectIterator.key, targetObjectIterator.key_len);
            // TODO: Allow multiple properties to be set!
            // while (pl->parameter_list->Size() >= 2)
            // {
            if (stack->module_contex)
                redis_HSET(stack, key, a->setname, v->setname, data_config->host_reference);
            else
            {
                rxHashTypeSet(targetObjectIterator.data, a->setname, v->setname, 0);
            }
            // }
        }
        raxStop(&targetObjectIterator);
        FaBlok::Delete(pl);
    }
    else
    {
        ERROR("Incorrect parameters for AddProperty, must be [<a>, <v>]...");
    }
    PushResult(s, stack);
}
END_SJIBOLETH_HANDLER(executeGremlinAddProperty)

/*
    Execute a Redis command

    The redis command is constructed by concatenating the parameters.

    When a token matches an object field then the field value is used.

    Special tokens:
        @key
        @tuple


*/
SJIBOLETH_HANDLER(executeGremlinRedisCommand)
{
    rxUNUSED(t);

    STACK_CHECK(2);
    stack->DumpStack();
    FaBlok *pl = stack->Pop();
    FaBlok *s = stack->Pop();
    redisNodeInfo *dataConfig = rxDataNode();

    if (pl->IsParameterList())
    {
        int no_of_parts = pl->parameter_list->Size();

        raxIterator keysetIterator;
        raxStart(&keysetIterator, &s->keyset);
        raxSeek(&keysetIterator, "^", NULL, 0);
        rxString kw_key = rxStringNew("@key");
        rxString kw_tuple = rxStringNew("@tuple");
        while (raxNext(&keysetIterator))
        {
            rxString key = rxStringNewLen((const char*)keysetIterator.key, keysetIterator.key_len);

            void *obj = rxFindKey(0, key);
            rxString tuple = rxStringEmpty();
            switch (rxGetObjectType(obj))
            {
            case rxOBJ_HASH:
                tuple = rxHashAsJson(key, obj);
                break;
            case OBJ_TRIPLET:
            {
                Graph_Triplet *t = (Graph_Triplet *)rxGetContainedObject(obj);
                tuple = t->Json(key);
            }
            break;
            default:
                tuple = rxStringNew("{}");
                break;
            }

            rxString *parts = new rxString[no_of_parts];
            int n = 0;
            pl->parameter_list->StartHead();
            FaBlok *p = NULL;
            while ((p = pl->parameter_list->Next()) != NULL)
            {
                if (strcmp(p->setname, kw_key) == 0)
                    parts[n] = key;
                else if (strcmp(p->setname, kw_tuple) == 0)
                    parts[n] = tuple;
                else
                    parts[n] = rxStringNew(p->setname);
                ++n;
            }

            void *stashed_cmd = rxStashCommand2(NULL, parts[0], /*rxStringTrim*/ 1, no_of_parts - 1, (void **)&parts[1]);
            ExecuteRedisCommand(NULL, stashed_cmd, dataConfig->host_reference);
            FreeStash(stashed_cmd);
            rxStringFree(key);
            rxStringFree(tuple);
        }
        rxStringFree(kw_key);
        rxStringFree(kw_tuple);
        raxStop(&keysetIterator);
        FaBlok::Delete(pl);
    }
    else
    {
        ERROR("Incorrect parameters for executeGremlinRedisCommand, must be [<token> | <literal>]...");
    }
    PushResult(s, stack);
}
END_SJIBOLETH_HANDLER(executeGremlinRedisCommand)

GremlinDialect::GremlinDialect()
    : Sjiboleth()
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
        ParserToken *new_cpy = to_cpy->Copy();
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
    this->RegisterSyntax("match", pri500, 1, 1, &executeMatch);
    this->RegisterSyntax("nomatch", 500, 2, 1, &executeNomatch);
    this->RegisterSyntax("incl", 500, 2, 1, &executeGremlinMatchInExclude);
    this->RegisterSyntax("include", 500, 2, 1, &executeGremlinMatchInExclude);
    this->RegisterSyntax("excl", 500, 2, 1, &executeGremlinMatchInExclude);
    this->RegisterSyntax("exclude", 500, 2, 1, &executeGremlinMatchInExclude);
    this->RegisterSyntax("as", 500, 2, 1, &executeGremlinAs);
    this->RegisterSyntax("has", 500, 2, 1, &executeGremlinHas);
    this->RegisterSyntax("hasNot", 500, 2, 1, &executeGremlinHas);
    this->RegisterSyntax("missing", 500, 2, 1, &executeGremlinHas);
    this->RegisterSyntax("hasLabel", 500, 2, 1, &executeGremlinHasLabel);
    this->RegisterSyntax("missingLabel", 500, 2, 1, &executeGremlinHasLabel);
    this->RegisterSyntax("by", 500, 2, 1, &executeGremlinGroupby);
    this->RegisterSyntax("out", 500, 2, 1, &executeGremlinOut);
    this->RegisterSyntax("in", 500, 2, 1, &executeGremlinIn);
    this->RegisterSyntax("inout", 500, 2, 1, &executeGremlinInOut);
    this->RegisterSyntax("hasOut", 500, 2, 1, &executeGremlinHasOut);
    this->RegisterSyntax("hasIn", 500, 2, 1, &executeGremlinHasIn);
    this->RegisterSyntax("hasInout", 500, 2, 1, &executeGremlinHasInOut);
    this->RegisterSyntax("addEdge", 500, 2, 1, &executeGremlinAddEdge);
    this->RegisterSyntax("addE", 500, 2, 1, &executeGremlinAddEdge);
    this->RegisterSyntax("property", 500, 2, 1, &executeGremlinAddProperty);
    this->RegisterSyntax("addVertex", 500, 2, 1, &executeGremlinAddVertex);
    this->RegisterSyntax("addV", 500, 2, 1, &executeGremlinAddVertex);
    this->RegisterSyntax("to", 500, 2, 1, &executeGremlinLinkVertex);
    this->RegisterSyntax("object", 500, 2, 1, &executeGremlinLinkVertex, &GremlinScopeCheck);
    this->RegisterSyntax("from", 500, 2, 1, &executeGremlinLinkVertex);
    this->RegisterSyntax("subject", 500, 2, 1, &executeGremlinLinkVertex, &GremlinScopeCheck);
    this->RegisterSyntax("eq", 500, 2, 1, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax("ne", 500, 2, 1, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax("gt", 500, 2, 1, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax("ge", 500, 2, 1, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax("lt", 500, 2, 1, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax("LT", 500, 2, 1, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax("le", 500, 2, 1, &executeGremlinComparePropertyToValue);

    this->RegisterSyntax("-", 30, 2, 1, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax("+", 30, 2, 1, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax("==", 20, 2, 1, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax(">=", 20, 2, 1, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax("<=", 20, 2, 1, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax("<", 20, 2, 1, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax(">", 20, 2, 1, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax("!=", 20, 2, 1, &executeGremlinComparePropertyToValue);

    this->RegisterSyntax("between", 500, 2, 1, &executeGremlinComparePropertyToRangeValue);
    this->RegisterSyntax("contains", 500, 2, 1, &executeGremlinComparePropertyToValue);
    this->RegisterSyntax("redis", 500, 2, 1, &executeGremlinRedisCommand);
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

        if (*key == '^')
            continue;

        int stringmatch = rxStringCharCount(key, ':') >= 2;
        if (allkeys || on_matched == stringmatch)
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

// static FaBlok *getAllKeysByField(int dbNo, const char *regex, int on_matched, rxString field, rxString value)
// {
//     list *patterns = listCreate();
//     listAddNodeTail(patterns, value);
//     rxString keyset_name = rxStringFormat("%s == %s", field, value);
//     FaBlok *b = getAllKeysByField(dbNo, regex, on_matched, field, patterns, keyset_name);
//     listRelease(patterns);
//     return b;
// }

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
int AddMemberToKeysetForMatch(int db, unsigned char *vstr, size_t vlen, FaBlok *kd, Graph_Leg *terminal, short filterOnly)
{
    // look up target key
    int segments = 0;
    int link_segments = 0;
    rxString some_str = rxStringNewLen((char *)vstr, vlen);
    rxString *parts = rxStringSplitLen(some_str, vlen, "|", 1, &segments);
    rxStringFree(some_str);

    if (filterOnly /*&& terminal->de*/)
    {
        Graph_Leg *leader = terminal;
        while (leader->origin)
        {
            leader = leader->origin;
        }
        if (leader != NULL && leader != terminal)
        {
            void *tobj = Graph_Triplet::NewForMatch(db, terminal, leader->key, NULL);
            Graph_Triplet::Link(tobj, kd);
            rxString tk = rxStringFormat(":%s:%s:", leader->key, terminal->key);
            kd->AddKey(tk, tobj);
            rxStringFree(tk);
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
    void *tobj = Graph_Triplet::New(db, terminal, rxStringEmpty(), rxStringEmpty());
    if (segments == 1)
    {
        rxString member = rxStringNewLen((const char *)vstr, vlen);
        if (!tobj)
            if ((tobj = rxFindHashKey(db, member)) == NULL)
                return 0;
        Graph_Triplet::Link(tobj, kd);
        kd->AddKey(member, tobj);
        return 1;
    }

    rxString link = rxStringNew(parts[1] + 1);
    rxString *link_parts = rxStringSplitLen(link, strlen(link), ":", 1, &link_segments);
    if (link_segments == 1)
    {
        if (!tobj)
            if ((tobj = rxFindHashKey(db, link)) == NULL)
                return 0;
        kd->AddKey(link, tobj);
        return 1;
    }
    rxString edge_key = rxStringFormat("^%s", link);
    void *members = rxFindSetKey(db, edge_key);
    if (members != NULL)
    {
        void *si = NULL;
        rxString member;
        int64_t l;
        while (rxScanSetMembers(members, &si, (char **)&member, &l) != NULL)
        {
            int msegments = 0;
            rxString *mparts = rxStringSplitLen(member, strlen(member), "|", 1, &msegments);
            void *mobj = kd->LookupKey(link /*terminal->key*/);
            if (mobj == NULL)
            {
                mobj = Graph_Triplet::New(db, terminal, mparts[1], rxStringTrim(parts[1], "^"));
            }
            Graph_Triplet::Link(mobj, kd);
            kd->AddKey(link /*terminal->key*/, mobj);
            rxStringFreeSplitRes(mparts, msegments);
        }
    }
    else
    {

        if (*parts[2] == 'S' || *(parts[2] + 1) == 'S')
            link = rxStringNew(link_parts[2]);
        if (*parts[2] == 'O' || *(parts[2] + 1) == 'O')
            link = rxStringNew(link_parts[1]);

        if (!tobj)
            if ((tobj = rxFindHashKey(db, link)) == NULL)
                return 0;
        kd->AddKey(link, tobj);
    }
    rxStringFreeSplitRes(parts, segments);
    rxStringFreeSplitRes(link_parts, link_segments);
    return 1;
}

int getEdgeDirection(rxString flowy)
{
    if (strcmp(flowy, Graph_Subject_to_Edge) == 0)
        return GRAPH_TRAVERSE_OUT;
    if (strcmp(flowy, Graph_Edge_to_Object) == 0)
        return GRAPH_TRAVERSE_OUT;
    if (strcmp(flowy, Graph_Edge_to_Subject) == 0)
        return GRAPH_TRAVERSE_IN;
    if (strcmp(flowy, Graph_Object_to_Edge) == 0)
        return GRAPH_TRAVERSE_IN;
    return GRAPH_TRAVERSE_INOUT;
}

int matchEdges(int db, Graph_Leg *leg, list *patterns, FaBlok *kd, GraphStack<Graph_Leg> *bsf_q, rax *touches, int traverse_direction, short filter_only, rax *terminators, rax *includes, rax *excludes)
{
    if (terminators)
    {
        if (raxContains(terminators, leg->key))
        {
            AddMemberToKeysetForMatch(db, (unsigned char *)leg->key, strlen(leg->key), kd, leg, filter_only);
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

    void *si = NULL;
    rxString elesds;
    int64_t intobj;
    void *m;
    while ((m = rxScanSetMembers(zobj, &si, (char **)&elesds, &intobj)) != NULL)
    {
        int segments = 0;
        rxString *parts = rxStringSplitLen(elesds, strlen(elesds), "|", 1, &segments);
        double weight = atof(parts[3]);
        rxString link = rxStringNew(parts[1] + 1);
        int link_segments = 0;
        rxString *link_parts = rxStringSplitLen(link, strlen(link), ":", 1, &link_segments);
        // Does the link match the  pattern?
        if (segments >= 3)
        {
            void *excluded = (excludes) ? raxFind(excludes, (UCHAR *)parts[0], strlen(parts[0])) : raxNotFound;
            if (excluded != raxNotFound)
            {
                rxStringFreeSplitRes(parts, segments);
                continue;
            }
            // Only traverse edges in the requested direction
            if (traverse_direction != GRAPH_TRAVERSE_INOUT && getEdgeDirection(parts[2]) != traverse_direction)
            {
                rxStringFreeSplitRes(parts, segments);
                rxStringFreeSplitRes(link_parts, link_segments);
                rxStringFree(link);
                continue;
            }
        }
        int doesMatchOneOfThePatterns = 0;
        void *included = (includes) ? raxFind(includes, (UCHAR *)parts[0], strlen(parts[0])) : raxNotFound;
        if (included != raxNotFound)
            doesMatchOneOfThePatterns = 1;
        if (doesMatchOneOfThePatterns == 0 && patterns != NULL)
        {
            listIter *li = listGetIterator(patterns, 0);
            listNode *ln;
            while ((ln = listNext(li)) && doesMatchOneOfThePatterns == 0)
            {
                rxString pattern = (rxString)ln->value;

                doesMatchOneOfThePatterns = (rxStringMatchLen(pattern, strlen(pattern), (const char *)parts[0], strlen(parts[0]), 1));
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
            numkeys += AddMemberToKeysetForMatch(db, (unsigned char *)elesds, strlen(elesds), kd, leg, filter_only);
        }
        else if (patterns == NULL)
        {
            // Is the subject or object of the requested type?

            Graph_Leg *next = Graph_Leg::Add(link, weight, leg, bsf_q, touches);
            if (next)
            {
                next->obj = rxFindKey(db, link);
            }
        }
        rxStringFreeSplitRes(parts, segments);
        rxStringFreeSplitRes(link_parts, link_segments);
        rxStringFree(link);
    }
    rxStringFree(key);
    return numkeys;
}

int breadthFirstSearch(FaBlok *leaders, list *patterns, FaBlok *kd, int traverse_direction, short filter_only, rax *terminators, rax *includes, rax *excludes)
{
    int numkeys = 0;

    if (raxSize(&leaders->keyset) <= 0)
        leaders->LoadKey(0, leaders->setname);

    if (leaders && raxSize(&leaders->keyset) > 0)
    {
        GraphStack<Graph_Leg> bsf_q;
        GraphStack<Graph_Leg> bsf_c;
        rax *touches = raxNew();
        // Seed the search
        raxIterator leadersIterator;
        raxStart(&leadersIterator, &leaders->keyset);
        raxSeek(&leadersIterator, "^", NULL, 0);
        while (raxNext(&leadersIterator))
        {
            rxString key = rxStringNewLen((const char *)leadersIterator.key, leadersIterator.key_len);
            Graph_Leg *leg = Graph_Leg::Add(key, 0.0, &bsf_q);
            leg->start = leg;
            leg->obj = leadersIterator.data;
        }
        raxStop(&leadersIterator);

        int db = 0;
        while (bsf_q.HasEntries())
        {
            Graph_Leg *leg = bsf_q.Pop();
            numkeys += matchEdges(db, leg, patterns, kd, &bsf_q, touches, traverse_direction, filter_only, terminators, includes, excludes);
            bsf_c.Enqueue(leg);
        }

        while (bsf_c.HasEntries())
        {
            Graph_Leg *leg = bsf_c.Dequeue();
            delete leg;
        }
        raxFree(touches);
    }
    return numkeys;
}
