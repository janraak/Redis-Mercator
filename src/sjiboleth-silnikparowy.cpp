#ifndef __sjiboleth_silnikparowy_CPP__
#define __sjiboleth_silnikparowy_CPP__

#ifdef __cplusplus
extern "C"
{
#endif
#include <cstdlib>

#include "string.h"
#include "sdsWrapper.h"
#include "../../deps/hiredis/hiredis.h"

#include "rax.h"
#include "rxSuiteHelpers.h"
    extern raxNode *raxNewNode(size_t children, int datafield);

#ifdef __cplusplus
}
#endif

#include "client-pool.hpp"
#include "sjiboleth-fablok.hpp"
#include "sjiboleth-graph.hpp"

rax *SilNikParowy::Execute(ParsedExpression *e, SilNikParowy_Kontekst *stack, const char *key)
{
    FaBlok *v = FaBlok::Get((char *)key, KeysetDescriptor_TYPE_KEYSET);
    v->InitKeyset(true);
    v->AsTemp();
    // if(v->size <= 0 )
    v->LoadKey(0, v->setname);
    stack->Push(v);
    SilNikParowy::Preload(e, stack);
    rax *result = SilNikParowy::Execute(e, stack);
    // delete v;
    return result;
}

void SilNikParowy::Preload(ParsedExpression *e, SilNikParowy_Kontekst *)
{
    e->expression->StartHead();
    ParserToken *t;
    while ((t = e->expression->Next()) != NULL)
    {
        if (strlen(t->Token()) > 0)
        {
            switch (t->TokenType())
            {
            case _operand:
                FaBlok::Get(t->Token(), KeysetDescriptor_TYPE_SINGULAR);
                break;
            case _literal:
                FaBlok::Get(t->Token(), KeysetDescriptor_TYPE_SINGULAR);
                break;
            default:
                break;
            }
        }
        else
        {
            rxServerLogRaw(rxLL_WARNING,
                           rxStringNew("Odd token"));
        }
    }
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
            kd->IsValid();
            stack->Push(kd);
            break;
        case _literal:
            kd = FaBlok::Get(t->Token(), KeysetDescriptor_TYPE_SINGULAR);
            kd->IsValid();
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
                char msg[256];
                snprintf(msg, sizeof(msg), "%d Invalid operation %s\n", t->Priority(), t->Token());
                e->AddError(msg);
            }
            break;
        default:
            break;
        }
    }
end_of_loop:
    printf("\n");
    rxString error;
    rax *rx = NULL;
    if (listLength(e->errors) > 0)
    {
        error = rxStringEmpty();
        listNode *ln;
        listIter *li = listGetIterator(e->errors, AL_START_HEAD);
        while ((ln = listNext(li)) != NULL)
        {
            if (strlen((char *)ln->value) > 0)
            {
                printf("ERROR: %s\n", e->ToString());
                printf("ERROR: %s\n", (char *)ln->value);
                error = rxStringFormat("%s%s", error, (char *)ln->value);
                error = rxStringFormat("%s%s", error, "\n");
            }
        }
        if (strlen(error) > 0)
        {
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
            if (t != NULL && t->CompareToken(rxStringNew("=")) != 0)
            {
                e->AddError(rxStringNew("Invalid expression, no result yielded!"));
            }
            break;
        case 1:
            r = stack->Pop();
            if (r->IsValueType(KeysetDescriptor_TYPE_MONITORED_SET))
                stack->RetainResult();
            if ((r->IsValueType(KeysetDescriptor_TYPE_KEYSET) || r->IsValueType(KeysetDescriptor_TYPE_SINGULAR)) && !r->HasKeySet())
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
                error = rxStringNew("Invalid expression, ");
                error = rxStringFormat("%s %d  results yielded!\n", error, stack->Size());
                e->AddError(error);
                return NULL;
            }
            if ((r->IsValueType(KeysetDescriptor_TYPE_KEYSET) || r->IsValueType(KeysetDescriptor_TYPE_SINGULAR)) && !r->HasKeySet())
            {
                r->FetchKeySet(stack->serviceConfig, rxStringEmpty(), r->setname, rxStringEmpty());
                // stack->Push(r);
            }
            if (r->IsValueType(KeysetDescriptor_TYPE_MONITORED_SET))
                stack->RetainResult();
            e->final_result_value_type = r->ValueType();
            rx = r->AsRax();

            while (stack->HasEntries())
            {
                stack->Pop();
            }

            return rx;
        }
        break;
            // default:
            //     error = rxStringNew("Invalid expression, ");
            //     error = rxStringFormat("%s %d  results yielded!\n", error, stack->Size());
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
#endif