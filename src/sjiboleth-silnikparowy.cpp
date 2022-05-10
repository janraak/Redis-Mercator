

#ifdef __cplusplus
extern "C"
{
#endif
#include <cstdlib>

#include "string.h"

#include "../../deps/hiredis/hiredis.h"

#include "rax.h"
#include "rxSuiteHelpers.h"
    extern raxNode *raxNewNode(size_t children, int datafield);

#ifdef __cplusplus
}
#endif

#include "sjiboleth-fablok.hpp"
#include "sjiboleth-graph.hpp"
#include "client-pool.hpp"


rax *SilNikParowy::Execute(ParsedExpression *e, SilNikParowy_Kontekst *stack, const char *key)
{
    FaBlok *v = FaBlok::Get((char *)key, KeysetDescriptor_TYPE_KEYSET);
    v->AsTemp();
    v->LoadKey(0, v->setname);
    stack->Push(v);
    rax *result = this->Execute(e, stack);
    // delete v;
    return result;
}

rax *SilNikParowy::Execute(ParsedExpression *e, SilNikParowy_Kontekst *stack)
{
    e->expression->StartHead();
    ParserToken *t;
    FaBlok *kd;
    while ((t = e->expression->Next()) != NULL)
    {
        switch (t->TokenType())
        {
        case _operand:
            kd = FaBlok::Get(t->Token(), KeysetDescriptor_TYPE_SINGULAR);
            stack->Push(kd);
            break;
        case _literal:
            kd = FaBlok::Get(t->Token(), KeysetDescriptor_TYPE_SINGULAR);
            stack->Push(kd);
            break;
        case _operator:
            if (t->HasExecutor())
            {
                if (t->Execute(t, stack) == C_ERR)
                {
                    goto end_of_loop;
                }
            }
            else if (t->Priority() > priImmediate)
            {
                char error[256];
                snprintf(error, sizeof(error), "%d Invalid operation %s\n", t->Priority(), t->Token());
                e->AddError(error);
            }
            break;
        default:
            break;
        }
    }
end_of_loop:
    sds error;
    rax *rx = NULL;
    if (listLength(e->errors) > 0)
    {
        error = sdsempty();
        listNode *ln;
        listIter *li = listGetIterator(e->errors, AL_START_HEAD);
        while((ln = listNext(li)) != NULL){
            if(strlen((char *)ln->value) > 0){
                error = sdscat(error, (char *)ln->value);
                error = sdscat(error, "\n");
            }
        }
        if(sdslen(error) > 0){
            return NULL;
        }
    }


    {
        ParserToken *t = NULL;
        FaBlok *r = NULL;
        switch (stack->Size())
        {
        case 0:
            t = e->lastInstruction();
            if (t->CompareToken(sdsnew("=")) != 0)
            {
                e->AddError(sdsnew("Invalid expression, no result yielded!"));
            }
            break;
        case 1:
            r = stack->Pop();
            if (r->IsValueType(KeysetDescriptor_TYPE_MONITORED_SET))
                stack->RetainResult();
            if ((r->IsValueType(KeysetDescriptor_TYPE_KEYSET) || r->IsValueType(KeysetDescriptor_TYPE_SINGULAR) )&& !r->HasKeySet())
            {
                stack->FetchKeySet(r);
            }
            e->final_result_value_type = r->ValueType();
            rx = r->AsRax();
            return rx;
        case 2:
        default:
        {
            r = stack->Pop();
            if (r->IsValueType(KeysetDescriptor_TYPE_MONITORED_SET))
                stack->RetainResult();
            FaBlok *checkV = stack->Pop();
            FaBlok *memo = (FaBlok *)stack->Recall("V");
            if (memo != NULL && memo != checkV)
            {
                error = sdsnew("Invalid expression, ");
                error = sdscatfmt(error, "%i  results yielded!\n", stack->Size());
                e->AddError(error);
                return NULL;
            }
            if ((r->IsValueType(KeysetDescriptor_TYPE_KEYSET) || r->IsValueType(KeysetDescriptor_TYPE_SINGULAR))
            && !r->HasKeySet())
            {
                r->FetchKeySet(stack->host, stack->port, sdsempty(), r->setname, sdsempty());
                // stack->Push(r);
            }
            if (r->IsValueType(KeysetDescriptor_TYPE_MONITORED_SET))
                stack->RetainResult();
            e->final_result_value_type = r->ValueType();
            rx = r->AsRax();
            return rx;
        }
        break;
        // default:
        //     error = sdsnew("Invalid expression, ");
        //     error = sdscatfmt(error, "%i  results yielded!\n", stack->Size());
        //     e->AddError(error);
        //     this->ClearStack();
        //     break;
        }
    }
    return NULL;
}

SilNikParowy::SilNikParowy()
{
}

SilNikParowy::~SilNikParowy()
{
}
