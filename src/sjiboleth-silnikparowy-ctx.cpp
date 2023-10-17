#ifndef __sjiboleth_silnikparowy_ctx_CPP__
#define __sjiboleth_silnikparowy_ctx_CPP__


#ifdef __cplusplus
extern "C"
{
#endif
#include <cstdlib>

#include "string.h"

#include "../../deps/hiredis/hiredis.h"

#include "../../src/rax.h"
#include "rxSuiteHelpers.h"
    extern raxNode *raxNewNode(size_t children, int datafield);

#ifdef __cplusplus
}
#endif

#include "sjiboleth-fablok.hpp"
#include "sjiboleth-graph.hpp"

rax *SilNikParowy_Kontekst::Execute(ParsedExpression *e)
{
    SilNikParowy *engine = e->GetEngine();
    if(engine == NULL)
        rxModulePanic((char *)"Engine expected");
    this->expression = e;
    // engine->Preload(e, this);
    return engine->Execute(e, this);
}

rax *SilNikParowy_Kontekst::Execute(ParsedExpression *e, const char *key)
{
    SilNikParowy *engine = e->GetEngine();
    if(engine == NULL)
        rxModulePanic((char *)"Engine expected");
    this->expression = e;
    return engine->Execute(e, this, key);
}


void *SilNikParowy_Kontekst::Execute(ParsedExpression *e, void *data)
{
    SilNikParowy *engine = e->GetEngine();
    if(engine == NULL)
        rxModulePanic((char *)"Engine expected");
    this->expression = e;
    return engine->Execute(e, this, data);
}

list *SilNikParowy_Kontekst::Errors()
{
    return this->expression->errors;
}

void SilNikParowy_Kontekst::AddError(rxString msg)
{
    if(this->expression)
        this->expression->AddError(msg);
}

void SilNikParowy_Kontekst::Reset()
{
    FaBlok::ClearCache();
}

SilNikParowy_Kontekst::SilNikParowy_Kontekst()
{
    this->serviceConfig = NULL;
    this->module_contex = NULL;
    this->memoization = raxNew();
    this->can_delete_result = 653974783;
    this->fieldSelector = NULL;
    this->sortSelector = NULL;
}

void SilNikParowy_Kontekst::RetainResult()
{
    this->can_delete_result = 722765;
}

bool SilNikParowy_Kontekst::CanDeleteResult()
{
    return this->can_delete_result == 653974783;
}

SilNikParowy_Kontekst::SilNikParowy_Kontekst(redisNodeInfo* server)
    : SilNikParowy_Kontekst()
{
    this->serviceConfig = server;
}

SilNikParowy_Kontekst::SilNikParowy_Kontekst(redisNodeInfo* server, RedisModuleCtx *module_contex)
    : SilNikParowy_Kontekst(server)
{
    this->module_contex = module_contex;
}

SilNikParowy_Kontekst::~SilNikParowy_Kontekst()
{
    if (this->memoization == NULL)
        rxServerLog(rxLL_NOTICE, "this->Memoization == NULL\n");
    FaBlok::DeleteAllTempDescriptors();
    raxFree(this->memoization);
    this->memoization = NULL;
    if (this->fieldSelector != NULL)
        delete this->fieldSelector;
    if (this->sortSelector != NULL)
        delete this->sortSelector;
}

int SilNikParowy_Kontekst::FetchKeySet(FaBlok *out, FaBlok *left, FaBlok *right, ParserToken *token)
{
    if (out->HasKeySet())
        return out->size;
    return out->FetchKeySet(this->serviceConfig, left->setname, right->setname, (char *)token->Token());
}

int SilNikParowy_Kontekst::FetchKeySet(FaBlok *out)
{
    if (out->HasKeySet())
        return out->size;
    return out->FetchKeySet(this->serviceConfig, out->AsSds());
}

void SilNikParowy_Kontekst::ClearStack()
{
    FaBlok::DeleteAllTempDescriptors();
}

void SilNikParowy_Kontekst::DumpStack()
{
    this->StartHead();
    FaBlok *kd;
    int n = 0;
    while ((kd = this->Next()) != NULL)
    {
        rxServerLog(rxLL_NOTICE, "%2d\t%p temp=%d type=%d size=%d %s", n++, kd, kd->is_temp, kd->value_type, kd->size, kd->setname);
        if (kd->IsParameterList())
        {
            GraphStack<FaBlok> *pl = kd->parameter_list;
            pl->StartHead();
            FaBlok *p;
            int pn = 0;
            while ((p = pl->Next()) != NULL)
            {
                rxServerLog(rxLL_NOTICE, ".%2d\t%p temp=%d type=%d size=%d %s", pn++, p, p->is_temp, p->value_type, p->size, p->setname);
            }
        }
    }
    this->Stop();
}

FaBlok *SilNikParowy_Kontekst::GetOperationPair(char const  *operation, int load_left_and_or_right)
{
    FaBlok *l = this->Pop();
    if (((load_left_and_or_right & QE_LOAD_LEFT_ONLY) && l->size == 0))
    {
        rxString lk = rxStringNew(l->setname);
        l->FetchKeySet(this->serviceConfig, lk);
        rxStringFree(lk);
    }

    FaBlok *r = this->Pop();
    if (((load_left_and_or_right & QE_LOAD_RIGHT_ONLY)) && r->size == 0)
    {
        rxString rk = rxStringNew(r->setname);
        r->FetchKeySet(this->serviceConfig, rk);
        rxStringFree(rk);
    }

    int do_swap = ((load_left_and_or_right && QE_SWAP_LARGEST_FIRST) && (raxSize(l->keyset) < raxSize(r->keyset))) || ((load_left_and_or_right && QE_SWAP_SMALLEST_FIRST) && (raxSize(l->keyset) > raxSize(r->keyset)));
    if (do_swap)
    {
        FaBlok *swap = l;
        l = r;
        r = swap;
    }

    rxString keyset_name = rxStringFormat("%s %s %s", l->setname, operation, r->setname);
    FaBlok *kd = FaBlok::Get(keyset_name, KeysetDescriptor_TYPE_KEYSET);
    rxStringFree(keyset_name);
    kd->left = l;
    kd->right = r;
    kd->start = ustime();

    if (load_left_and_or_right && QE_CREATE_SET)
    {
        kd->ValueType(KeysetDescriptor_TYPE_KEYSET);
    }
    return kd;
}

bool SilNikParowy_Kontekst::IsMemoized(char const *field)
{
    if(this->memoization == NULL)
        return false;
    void *m = raxFind(this->memoization, (UCHAR *)field, strlen(field));
    return (m != raxNotFound);
}

void SilNikParowy_Kontekst::Memoize(char const *field, /*T*/ void *value)
{
    if(this->memoization == NULL)
        this->memoization = raxNew();
    raxInsert(this->memoization, (UCHAR *)field, strlen(field), value, NULL);
}

void SilNikParowy_Kontekst::ClearMemoizations()
{
    if (this->memoization != NULL)
    {
        raxIterator clearanceIterator;
        raxStart(&clearanceIterator, this->memoization);
        while (raxNext(&clearanceIterator))
        {
            raxRemove(this->memoization, clearanceIterator.data, strlen(clearanceIterator.data), NULL);
            raxSeek(&clearanceIterator, "^", NULL, 0);
        }
        raxStop(&clearanceIterator);
    }
}

/*template<typename T>T */ void *SilNikParowy_Kontekst::Recall(char const *field)
{
    if(this->memoization == NULL)
        return NULL;
    /*T*/ void *m = raxFind(this->memoization, (UCHAR *)field, strlen(field));

    if (m != raxNotFound)
        return m;
    return NULL;
}

/*template<typename T>T */ void *SilNikParowy_Kontekst::Forget(char const *field)
{
    if(this->memoization == NULL)
        return NULL;
    void *m = NULL;
    raxRemove(this->memoization, (UCHAR *)field, strlen(field), &m);

    if (m != raxNotFound)
        return m;
    return NULL;
}

// template class RedisClientPool<SilNikParowy_Kontekst>;

#endif