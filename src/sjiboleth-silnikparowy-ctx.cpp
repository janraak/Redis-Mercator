

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

rax *SilNikParowy_Kontekst::Execute(ParsedExpression *e)
{
    SilNikParowy *engine = e->GetEngine();
    if(engine == NULL)
        rxModulePanic((char *)"Engine expected");
    this->expression = e;
    engine->Preload(e, this);
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

list *SilNikParowy_Kontekst::Errors()
{
    return this->expression->errors;
}

void SilNikParowy_Kontekst::AddError(sds msg)
{
    return this->expression->AddError(msg);
}

void SilNikParowy_Kontekst::Reset()
{
    FaBlok::DeleteAllTempDescriptors();
}

SilNikParowy_Kontekst::SilNikParowy_Kontekst()
{
    this->host = sdsempty();
    this->port = 6379;
    this->module_contex = NULL;
    this->Memoization = raxNew();
    this->can_delete_result = 653974783;
}

void SilNikParowy_Kontekst::RetainResult()
{
    this->can_delete_result = 2069722765;
}

bool SilNikParowy_Kontekst::CanDeleteResult()
{
    return this->can_delete_result == 653974783;
}

SilNikParowy_Kontekst::SilNikParowy_Kontekst(char *h, int port, RedisModuleCtx *module_contex)
    : SilNikParowy_Kontekst()
{
    this->host = sdsnew(h);
    this->port = port;
    this->module_contex = module_contex;
}

SilNikParowy_Kontekst::~SilNikParowy_Kontekst()
{
    FaBlok::DeleteAllTempDescriptors();
    raxFree(this->Memoization);
}

int SilNikParowy_Kontekst::FetchKeySet(FaBlok *out, FaBlok *left, FaBlok *right, ParserToken *token)
{
    if (out->HasKeySet())
        return out->size;
    return out->FetchKeySet(this->host, this->port, left->setname, right->setname, (char *)token->Token());
}

int SilNikParowy_Kontekst::FetchKeySet(FaBlok *out)
{
    if (out->HasKeySet())
        return out->size;
    return out->FetchKeySet(this->host, this->port, out->AsSds());
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
        printf("%2d 0x%10x temp=%d type=%d size=%d %s\n", n++, (POINTER)kd, kd->is_temp, kd->value_type, kd->size, kd->setname);
    }
    this->Stop();
}

FaBlok *SilNikParowy_Kontekst::GetOperationPair(sds operation, int load_left_and_or_right)
{
    FaBlok *l = this->Pop();
    if (((load_left_and_or_right & QE_LOAD_LEFT_ONLY) && l->size == 0))
    {
        sds lk = sdsdup(l->setname);
        l->FetchKeySet(this->host, this->port, lk);
        sdsfree(lk);
    }

    FaBlok *r = this->Pop();
    if (((load_left_and_or_right & QE_LOAD_RIGHT_ONLY)) && r->size == 0)
    {
        sds rk = sdsdup(r->setname);
        r->FetchKeySet(this->host, this->port, rk);
        sdsfree(rk);
    }

    int do_swap = ((load_left_and_or_right && QE_SWAP_LARGEST_FIRST) && (raxSize(&l->keyset) < raxSize(&r->keyset))) || ((load_left_and_or_right && QE_SWAP_SMALLEST_FIRST) && (raxSize(&l->keyset) > raxSize(&r->keyset)));
    if (do_swap)
    {
        FaBlok *swap = l;
        l = r;
        r = swap;
    }

    sds keyset_name = sdscatfmt(sdsempty(), "%s %s %s", l->setname, operation, r->setname);
    FaBlok *kd = FaBlok::Get(keyset_name, KeysetDescriptor_TYPE_KEYSET);
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
    void *m = raxFind(this->Memoization, (UCHAR *)field, strlen(field));
    return (m != raxNotFound);
}

void SilNikParowy_Kontekst::Memoize(char const *field, /*T*/ void *value)
{
    raxInsert(this->Memoization, (UCHAR *)field, strlen(field), value, NULL);
}

/*template<typename T>T */ void *SilNikParowy_Kontekst::Recall(char const *field)
{
    /*T*/ void *m = raxFind(this->Memoization, (UCHAR *)field, strlen(field));

    if (m != raxNotFound)
        return m;
    return NULL;
}

/*template<typename T>T */ void *SilNikParowy_Kontekst::Forget(char const *field)
{
    void *m = NULL;
    raxRemove(this->Memoization, (UCHAR *)field, strlen(field), &m);

    if (m != raxNotFound)
        return m;
    return NULL;
}
