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

// void *SilNikParowy::CurrentException = NULL;

// struct sigaction SilNikParowy::new_action;
// struct sigaction SilNikParowy::old_action;
//    jmp_buf SilNikParowy::env_buffer;

// __sighandler_t *SilNikParowy::UpstreamExceptionHandler = NULL;
// __sighandler_t *SilNikParowy::CurrentExceptionHandler = NULL;

// void SilNikParowy::ExceptionHandler(int signal, siginfo_t *info, void *secret)
// {
//     printf("ExceptionHandler \: %d\n", signal);
//     SilNikParowy::CurrentException = info;
//     longjmp(SilNikParowy::env_buffer, "SilNikParowy::ExceptionHandler");    
// }

// bool SilNikParowy::ExceptionHandlerState = false;

// bool SilNikParowy::IsExceptionHandlerActive() { return false; }

// bool SilNikParowy::ExceptionHandler_Activate()
// {
//     if (SilNikParowy::CurrentExceptionHandler == NULL)
//     {
//         SilNikParowy::CurrentExceptionHandler = (__sighandler_t *)&SilNikParowy::ExceptionHandler;
//         new_action.sa_handler = SilNikParowy::ExceptionHandler;
//         sigemptyset(&SilNikParowy::new_action.sa_mask);
//         sigaddset(&SilNikParowy::old_action.sa_mask, SIGSEGV);
//         sigaddset(&SilNikParowy::old_action.sa_mask, SIGINT);
//         sigaddset(&SilNikParowy::old_action.sa_mask, SIGFPE);
//         sigaddset(&SilNikParowy::old_action.sa_mask, SIGTERM);
//         SilNikParowy::new_action.sa_flags = 0;
//         // SilNikParowy::new_action.sa_flags = SA_NODEFER | SA_RESETHAND | SA_SIGINFO;
//         sigaction(SIGSEGV, &SilNikParowy::new_action, &SilNikParowy::old_action);

//         sigaction(SIGFPE, &SilNikParowy::new_action, NULL);
//         sigaction(SIGTERM, &SilNikParowy::new_action, NULL);
//         sigaction(SIGINT, &SilNikParowy::new_action, NULL);
//     }
//     SilNikParowy::ExceptionHandlerState = true;
//     return SilNikParowy::ExceptionHandlerState;
// }
// bool SilNikParowy::ExceptionHandler_Deactivate()
// {
//     SilNikParowy::ExceptionHandlerState = false;
//     return SilNikParowy::ExceptionHandlerState;
// }
// void *SilNikParowy::GetException()
// {
//     return SilNikParowy::CurrentException;
// }

rax *SilNikParowy::Execute(ParsedExpression *e, SilNikParowy_Kontekst *stack)
{
    // SilNikParowy::ExceptionHandler_Activate();
    e->expression->StartHead();
    ParserToken *t;
    FaBlok *kd;
    // setjmp( SilNikParowy::env_buffer );
    // if(SilNikParowy::GetException() == NULL){
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
                if (t->Execute(stack) == C_ERR)
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
    // }
end_of_loop:
    // SilNikParowy::ExceptionHandler_Deactivate();
    // rxServerLog(rxLL_NOTICE, "\n");
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
                rxServerLog(rxLL_NOTICE, "ERROR: %s\n", e->ToString());
                rxServerLog(rxLL_NOTICE, "ERROR: %s\n", (char *)ln->value);
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
            // FaBlok *checkV = stack->Pop();
            // FaBlok *memo = (FaBlok *)stack->Recall("V");
            // if (memo != NULL && memo != checkV)
            // {
            //     error = rxStringNew("Invalid expression, ");
            //     error = rxStringFormat("%s %d  results yielded!\n", error, stack->Size());
            //     e->AddError(error);
            //     return NULL;
            // }
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