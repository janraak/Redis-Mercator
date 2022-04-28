

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

FaBlok *FaBlok::Get(const char *sn)
{
    FaBlok *f = (FaBlok *)raxFind(FaBlok::Registry, (UCHAR *)sn, strlen(sn));
    if (f == raxNotFound)
    {
        return NULL;
    }
    f->IsValid();
    return f;
}

bool FaBlok::IsValid()
{
    if ((sdslen(this->setname) > 0 && this->size != raxSize(&this->keyset)) || !(this->marked_for_deletion == 653974783 || this->marked_for_deletion == 2069722765))
    {
        printf("FaBlok for %s may be corrupted fab: 0x%x reused:%d sz: %u set:0x%x sz: %lld  marked_for_deletion:%d temp:%d\n",
               this->setname,
               (POINTER)this,
               this->reuse_count,
               this->size,
               (POINTER) & this->keyset,
               raxSize(&this->keyset),
               this->marked_for_deletion,
               this->is_temp);
        return false;
    }
    return true;
}

FaBlok *FaBlok::Get(const char *sn, UCHAR value_type)
{
    FaBlok *f = (FaBlok *)raxFind(FaBlok::Registry, (UCHAR *)sn, strlen(sn));
    if (f == raxNotFound)
    {
        FaBlok *n = new FaBlok(sdsdup(sdsnew(sn)), value_type);
        n->AsTemp();
        raxInsert(FaBlok::Registry, (UCHAR *)n->setname, sdslen(n->setname), n, NULL);
        n->IsValid();
        return n;
    }
    else
    {
        f->reuse_count++;
        f->IsValid();
    }
    return f;
}

FaBlok *FaBlok::Delete(FaBlok *d)
{
    if (d->IsValueType(KeysetDescriptor_TYPE_MONITORED_SET))
        return NULL;
    d->marked_for_deletion = 2069722765;
    return NULL;
}

FaBlok *FaBlok::Rename(sds setname)
{
    auto *standing = raxFind(FaBlok::Registry, (UCHAR *)this->setname, sdslen(this->setname));
    if(standing == this){
        void *old;
        raxRemove(FaBlok::Registry, (UCHAR *)this->setname, sdslen(this->setname), &old);
    }
    this->setname = setname;
    raxInsert(FaBlok::Registry, (UCHAR *)this->setname, sdslen(this->setname), this, NULL);
    return this;
}

void FaBlok::LoadKey(int dbNo, sds key)
{
    void *zobj = rxFindKey(dbNo, key);
    if (zobj == NULL)
        return;
    if (rxGetObjectType(zobj) == rxOBJ_HASH)
    {
        this->InsertKey((UCHAR *)key, sdslen(key), zobj);
        this->size = raxSize(&this->keyset);
    }
}

int FaBlok::AsTemp()
{
    if (!this->IsValueType(KeysetDescriptor_TYPE_MONITORED_SET))
    {
        this->is_temp = 2069722765;
        // this->marked_for_deletion = 2069722765;
    }
    return (this->is_temp == 2069722765);
}

FaBlok::FaBlok()
{
    this->setname = sdsempty();
    this->marked_for_deletion = 653974783;
    this->ValueType(KeysetDescriptor_TYPE_UNKNOWN);
    this->is_temp = 653974783;
    this->is_on_stack = 0;
    // EMBEDDED RAX INITIALIZATION
    this->InitKeyset(true);
    //
    this->vertices_or_edges = GENERIC_SET;
    this->reuse_count = 0;
    this->size = 0;
    this->creationTime = ustime();
    this->latency = 0;
    this->dirty = 0;
    this->left = NULL;
    this->right = NULL;
    this->parameter_list = NULL;
}

void FaBlok::InitKeyset(bool withRootNode)
{
    // EMBEDDED RAX INITIALIZATION
    this->keyset.numele = 0;
    this->keyset.numnodes = 1;
    this->keyset.head = ( withRootNode == true ) ? raxNewNode(0, 0): NULL;
}

FaBlok *FaBlok::Attach(rax *r)
{
    this->keyset.numele = r->numele;
    this->keyset.numnodes = r->numnodes;
    this->keyset.head = r->head;
    return this;
}

rax *FaBlok::AsRax()
{
    if (raxSize(&this->keyset) == 0)
        return NULL;
    rax *r = (rax *)malloc(sizeof(rax));
    if (r == NULL)
        return NULL;
    r->numele = this->keyset.numele;
    r->numnodes = this->keyset.numnodes;
    r->head = this->keyset.head;
    this->keyset.head = NULL;
    return r;
}

FaBlok::~FaBlok()
{
if(this->keyset.head != NULL){
    rax *tmp = (rax *)malloc(sizeof(rax));
    tmp->head = this->keyset.head;
    tmp->numele = this->keyset.numele;
    tmp->numnodes = this->keyset.numnodes;
    FreeResults(tmp);
    this->InitKeyset(false);
}

    sdsfree(this->setname);
}
FaBlok::FaBlok(sds sn, UCHAR value_type)
    : FaBlok()
{
    this->setname = sn;
    this->ValueType(value_type);
}

rax *SilNikParowy::Execute(ParsedExpression *e, const char *key)
{
    FaBlok *v = FaBlok::Get((char *)key, KeysetDescriptor_TYPE_KEYSET);
    v->AsTemp();
    v->LoadKey(0, v->setname);
    stack->Push(v);
    rax *result = this->Execute(e);
    // delete v;
    return result;
}

bool FaBlok::HasKeySet()
{
    return raxSize(&this->keyset) > 0;
}

list *SilNikParowy::Errors(ParsedExpression *e)
{
    return e->errors;
}

rax *SilNikParowy::Execute(ParsedExpression *e)
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
                if (t->Execute(this, e, t, stack) == C_ERR)
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
    DumpStack();
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
            this->ClearStack();
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
                this->RetainResult();
            if ((r->IsValueType(KeysetDescriptor_TYPE_KEYSET) || r->IsValueType(KeysetDescriptor_TYPE_SINGULAR) )&& !r->HasKeySet())
            {
                r->FetchKeySet(this->host, this->port, sdsempty(), r->setname, sdsempty());
                // stack->Push(r);
            }
            e->final_result_value_type = r->ValueType();
            rx = r->AsRax();
            return rx;
        case 2:
        default:
        {
            r = stack->Pop();
            if (r->IsValueType(KeysetDescriptor_TYPE_MONITORED_SET))
                this->RetainResult();
            FaBlok *checkV = stack->Pop();
            FaBlok *memo = (FaBlok *)this->Recall("V");
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
                r->FetchKeySet(this->host, this->port, sdsempty(), r->setname, sdsempty());
                // stack->Push(r);
            }
            if (r->IsValueType(KeysetDescriptor_TYPE_MONITORED_SET))
                this->RetainResult();
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

void SilNikParowy::Reset(){
    FaBlok::DeleteAllTempDescriptors();
}

rax *FaBlok::Registry = raxNew();

SilNikParowy::SilNikParowy()
{
    this->stack = new GraphStack<FaBlok>();
    this->host = sdsempty();
    this->port = 6379;
    this->module_contex = NULL;
    this->Memoization = raxNew();
    this->can_delete_result = 653974783;
}

void SilNikParowy::RetainResult()
{
    this->can_delete_result = 2069722765;
}

bool SilNikParowy::CanDeleteResult()
{
    return this->can_delete_result == 653974783;
}

SilNikParowy::SilNikParowy(char *h, int port, RedisModuleCtx *module_contex)
    : SilNikParowy()
{
    this->host = sdsnew(h);
    this->port = port;
    this->module_contex = module_contex;
}

SilNikParowy::~SilNikParowy()
{
    raxFree(this->Memoization);
    // RedisClientPool::Release(this->index_context);
    delete this->stack;
}

UCHAR FaBlok::ValueType()
{
    return this->value_type;
}

void FaBlok::pushIndexEntries(redisReply *reply)
{
    if (!reply)
        return;
    if (reply->type == REDIS_REPLY_ARRAY)
    {
        this->ValueType(KeysetDescriptor_TYPE_KEYSET);

        // 1) member
        // 2) score
        for (size_t j = 0; j < reply->elements; j += 2)
        {
            sds index_entry = sdsnew(reply->element[j]->str);

            int segments = 0;
            sds *parts = sdssplitlen(index_entry, sdslen(index_entry), "\t", 1, &segments);
            void *o = rxCreateStringObject(index_entry, sdslen(index_entry));
            this->InsertKey((UCHAR *)sdsdup(parts[0]), sdslen(parts[0]), o);
            sdsfreesplitres(parts, segments);
        }
        this->size = raxSize(&this->keyset);
    }
    freeReplyObject(reply);
}

sds FaBlok::AsSds()
{
    return this->setname;
}

int FaBlok::FetchKeySet(sds host, int port, sds rh)
{
    redisContext *index_context = RedisClientPool::Acquire(host, port);
    if (!index_context)
        return C_ERR;
    long long start = ustime();
    try
    {
        redisReply *rcc = (redisReply *)redisCommand(index_context, "rxfetch %s", rh);
        this->latency = ustime() - start;
        this->pushIndexEntries(rcc);
    }
    catch (...)
    {
        printf("#550#RXFETCH %s\n", rh);
        printf("#............ FetchKeyset ... %lld ... \n", this->latency);
    }
    RedisClientPool::Release(index_context);
    return C_OK;
}

int FaBlok::FetchKeySet(sds h, int port, sds lh, sds rh, sds cmp)
{
    auto *index_context = RedisClientPool::Acquire(h, port);
    if (index_context == NULL)
        return C_ERR;
    redisReply *rcc;
    long long start = ustime();
    if (isdigit(*lh))
    {
        rcc = (redisReply *)redisCommand(index_context, "RXFETCH %s %s %s", rh, lh, cmp);
    }
    else if( *lh)
    {
        rcc = (redisReply *)redisCommand(index_context, "RXFETCH %s %s ", rh, lh);
    }
    else
    {
        rcc = (redisReply *)redisCommand(index_context, "RXFETCH %s", rh);
    }
    this->latency = ustime() - start;
    this->pushIndexEntries(rcc);
    RedisClientPool::Release(index_context);
    return C_OK;
}

int SilNikParowy::FetchKeySet(FaBlok *out, FaBlok *left, FaBlok *right, ParserToken *token)
{
    if(out->HasKeySet())
        return out->size;
    return out->FetchKeySet(this->host, this->port, left->setname, right->setname, (char *)token->Token());
}

void FaBlok::DeleteAllTempDescriptors()
{
    raxIterator ri;
    raxStart(&ri, FaBlok::Registry);
    raxSeek(&ri, "^", NULL, 0);
    while (raxNext(&ri))
    {
       FaBlok *kd = (FaBlok *)ri.data;
        //printf("#100# DeleteAllTempDescriptors for %s vt:%d t:%d fab: 0x%x reused:%d sz: %u set:0x%x sz: %lld  \n",

            //    kd->setname,
            //    kd->ValueType(),
            //    kd->is_temp,
            //    (POINTER)kd,
            //    kd->reuse_count,
            //    kd->size,
            //    (POINTER) & kd->keyset,
            //    &kd->keyset != NULL ? raxSize(&kd->keyset) : 0);

        if (kd->marked_for_deletion == 2069722765 || kd->is_temp == 2069722765)
        {
            FaBlok *old;
            raxRemove(FaBlok::Registry, ri.key, ri.key_len, (void **)&old);
            delete kd;
            raxSeek(&ri, ">", ri.key, ri.key_len);
        }
    }
    raxStop(&ri);
}

void FaBlok::ClearCache()
{
    raxIterator ri;
    raxStart(&ri, FaBlok::Registry);
    raxSeek(&ri, "^", NULL, 0);
    while (raxNext(&ri))
    {
        FaBlok *kd = (FaBlok *)ri.data;
        if (!kd->ValueType(KeysetDescriptor_TYPE_MONITORED_SET))
        {
            FaBlok *old;
            raxRemove(FaBlok::Registry, ri.key, ri.key_len, (void **)&old);
            delete kd;
            raxSeek(&ri, ">", ri.key, ri.key_len);
        }
    }
    raxStop(&ri);
}

bool FaBlok::IsParameterList()
{
    return this->parameter_list != NULL;
}

void SilNikParowy::ClearStack()
{
}

void SilNikParowy::DumpStack()
{
    return;
    stack->StartHead();
    FaBlok *kd;
    int n = 0;
    while ((kd = stack->Next()) != NULL)
    {
        printf("%2d 0x%10x temp=%d type=%d size=%d %s\n", n++, (POINTER)kd, kd->is_temp, kd->value_type, kd->size, kd->setname);
    }
    stack->Stop();
}

FaBlok *SilNikParowy::GetOperationPair(sds operation, GraphStack<FaBlok> *stack, int load_left_and_or_right)
{
    FaBlok *l = stack->Pop();
    if ((load_left_and_or_right & QE_LOAD_LEFT_ONLY))
    {
        sds lk = sdsdup(l->setname);
        l->FetchKeySet(this->host, this->port, lk);
        sdsfree(lk);
    }

    FaBlok *r = stack->Pop();
    if ((load_left_and_or_right & QE_LOAD_RIGHT_ONLY))
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

FaBlok *FaBlok::Right()
{
    return this->right;
}

FaBlok *FaBlok::Left()
{
    return this->left;
}

FaBlok *FaBlok::Copy(sds set_name, int value_type, RaxCopyCallProc *fnCallback, void **privData)
{
    FaBlok *out = FaBlok::Get(set_name ? set_name : this->setname, value_type);
    out->Open();
    out->AsTemp();
    if (out->HasKeySet())
    {
        rax *tmp = (rax *)malloc(sizeof(rax));
        tmp->head = this->keyset.head;
        tmp->numele = this->keyset.numele;
        tmp->numnodes = this->keyset.numnodes;
        FreeResults(tmp);
        this->InitKeyset(true);
    }

    if (raxFind(FaBlok::Registry, (UCHAR *)set_name, strlen(set_name)) == raxNotFound)
    {
        raxInsert(FaBlok::Registry, (UCHAR *)out->setname, sdslen(out->setname), out, NULL);
    }

    if (this->HasKeySet())
    {
        raxIterator ri;
        raxStart(&ri, &this->keyset);
        raxSeek(&ri, "^", NULL, 0);
        while (raxNext(&ri))
        {
            if (fnCallback == NULL || fnCallback(ri.key, ri.key_len, ri.data, privData))
            {
                void *old_data = NULL;
                raxTryInsert(&out->keyset, ri.key, ri.key_len, ri.data, &old_data);
            }
        }
        raxStop(&ri);
    }
    out->Close();
    return out;
}

int FaBlok::CopyTo(FaBlok *out)
{
    raxIterator ri;
    int c = 0;
    raxStart(&ri, &this->keyset);
    raxSeek(&ri, "^", NULL, 0);
    while (raxNext(&ri))
    {
        out->InsertKey(ri.key, ri.key_len, ri.data);
        ++c;
    }
    raxStop(&ri);
    out->size = raxSize(&out->keyset);
    return c;
}

int FaBlok::MergeInto(FaBlok *out)
{
    raxIterator riIn;
    int c = 0;
    raxStart(&riIn, &this->keyset);
    raxSeek(&riIn, "^", NULL, 0);
    raxIterator riOut;
    raxStart(&riOut, &out->keyset);
    raxSeek(&riOut, "^", NULL, 0);
    while (raxNext(&riIn))
    {
        raxSeek(&riOut, ">=", riIn.key, riIn.key_len);
        if (raxCompare(&riOut, "==", riIn.key, riIn.key_len) != 1)
        {
            out->InsertKey(riIn.key, riIn.key_len, riIn.data);
            ++c;
        }
        else
            raxPrev(&riOut);
    }
    raxStop(&riIn);
    out->size = raxSize(&out->keyset);
    return c;
}

void FaBlok::PushResult(GraphStack<FaBlok> *stack)
{
    if (&this->keyset != NULL)
    {
        this->size = raxSize(&this->keyset);
    }
    this->latency = ustime() - this->start;
    stack->Push(this);
    // serverLog(LL_NOTICE, "#putKeysetDescriptor2# 0x%x nm=%s reuse=%d", (POINTER) kd, kd->setname, kd->reuse_count);
}

FaBlok *FaBlok::Open()
{
    this->start = ustime();
    if (this->marked_for_deletion == 2069722765)
    {
        printf("Opening a deleted FABLOK!  for %s fab: 0x%x reused:%d sz: %u set:0x%x sz: %lld  \n",

               this->setname,
               (POINTER)this,
               this->reuse_count,
               this->size,
               (POINTER) & this->keyset,
               &this->keyset != NULL ? raxSize(&this->keyset) : 0);
    }
    this->IsValid();
    return this;
}
FaBlok *FaBlok::Close()
{
    if (&this->keyset != NULL)
    {
        this->size = raxSize(&this->keyset);
    }
    this->latency += ustime() - this->start;
    this->IsValid();
    return this;
}

int FaBlok::MergeFrom(FaBlok *left, FaBlok *right)
{
    raxIterator riLeft;
    int c = 0;
    raxStart(&riLeft, &left->keyset);
    raxSeek(&riLeft, "^", NULL, 0);
    raxIterator riRight;
    raxStart(&riRight, &right->keyset);
    raxSeek(&riRight, "^", NULL, 0);

    raxNext(&riLeft);
    raxNext(&riRight);
    while (!(raxEOF(&riLeft) || raxEOF(&riRight)))
    {
        if (raxCompare(&riRight, "==", riLeft.key, riLeft.key_len) == 1)
        {
            this->InsertKey(riLeft.key, riLeft.key_len, riLeft.data);
            raxNext(&riLeft);
            raxNext(&riLeft);
            ++c;
        }
        else if (raxCompare(&riRight, ">", riLeft.key, riLeft.key_len) == 1)
        {
            raxNext(&riLeft);
        }
        else if (raxCompare(&riLeft, ">", riRight.key, riRight.key_len) == 1)
        {
            raxNext(&riRight);
        }
        else
            printf("#140# FaBlok::MergeFrom %s -> %s\n", riLeft.key, riRight.key);
    }
    raxStop(&riLeft);
    raxStop(&riRight);
    this->size = raxSize(&this->keyset);
    return c;
}

int FaBlok::MergeDisjunct(FaBlok *left, FaBlok *right)
{
    raxIterator riLeft;
    int c = 0;
    raxStart(&riLeft, &left->keyset);
    raxSeek(&riLeft, "^", NULL, 0);
    raxIterator riRight;
    raxStart(&riRight, &right->keyset);
    raxSeek(&riRight, "^", NULL, 0);

    raxNext(&riLeft);
    raxNext(&riRight);
    while (!(raxEOF(&riLeft) || raxEOF(&riRight)))
    {
        if (raxCompare(&riRight, "==", riLeft.key, riLeft.key_len) == 1)
        {
            raxNext(&riLeft);
            raxNext(&riLeft);
        }
        else if (raxCompare(&riRight, ">", riLeft.key, riLeft.key_len) == 1)
        {
            this->InsertKey(riLeft.key, riLeft.key_len, riLeft.data);
            ++c;
            raxNext(&riLeft);
        }
        else if (raxCompare(&riLeft, ">", riRight.key, riRight.key_len) == 1)
        {
            this->InsertKey(riRight.key, riRight.key_len, riRight.data);
            ++c;
            raxNext(&riRight);
        }
        else
            printf("#140# FaBlok::MergeDisjunct %s -> %s\n", riLeft.key, riRight.key);
    }
    raxStop(&riLeft);
    this->size = raxSize(&this->keyset);
    return c;
}

int FaBlok::CopyNotIn(FaBlok *left, FaBlok *right)
{
    raxIterator riLeft;
    int c = 0;
    raxStart(&riLeft, &left->keyset);
    raxSeek(&riLeft, "^", NULL, 0);
    raxIterator riRight;
    raxStart(&riRight, &right->keyset);
    raxSeek(&riRight, "^", NULL, 0);

    raxNext(&riLeft);
    raxNext(&riRight);
    while (!(raxEOF(&riLeft) || raxEOF(&riRight)))
    {
        if (raxCompare(&riRight, "==", riLeft.key, riLeft.key_len) == 1)
        {
            raxNext(&riLeft);
            raxNext(&riLeft);
        }
        else if (raxCompare(&riRight, ">", riLeft.key, riLeft.key_len) == 1)
        {
            this->InsertKey(riLeft.key, riLeft.key_len, riLeft.data);
            ++c;
            raxNext(&riLeft);
        }
        else if (raxCompare(&riLeft, ">", riRight.key, riRight.key_len) == 1)
        {
            raxNext(&riRight);
        }
    }
    raxStop(&riLeft);
    this->size = raxSize(&this->keyset);
    return c;
}

bool SilNikParowy::IsMemoized(char const *field)
{
    void *m = raxFind(this->Memoization, (UCHAR *)field, strlen(field));
    return (m != raxNotFound);
}

void SilNikParowy::Memoize(char const *field, /*T*/ void *value)
{
    raxInsert(this->Memoization, (UCHAR *)field, strlen(field), value, NULL);
}

/*template<typename T>T */ void *SilNikParowy::Recall(char const *field)
{
    /*T*/ void *m = raxFind(this->Memoization, (UCHAR *)field, strlen(field));

    if (m != raxNotFound)
        return m;
    return NULL;
}

/*template<typename T>T */ void *SilNikParowy::Forget(char const *field)
{
    void *m = NULL;
    raxRemove(this->Memoization, (UCHAR *)field, strlen(field), &m);

    if (m != raxNotFound)
        return m;
    return NULL;
}

void FaBlok::AddKey(sds key, void *obj)
{
    this->Open();
    this->InsertKey(key, obj);
    this->Close();
}

void FaBlok::InsertKey(sds key, void *obj)
{
    void *old = NULL;
    if(rxGetObjectType(obj) == rxOBJ_TRIPLET){
        auto *g = (Graph_Triplet *)rxGetContainedObject(obj);
        g->IncrRefCnt();
    }
    raxInsert(&this->keyset, (UCHAR *)sdsdup(key), sdslen(key), obj, &old);
}

void FaBlok::InsertKey(unsigned char *s, size_t len, void *obj)
{
    sds key = sdsnewlen(s, len);
    if(rxGetObjectType(obj) == rxOBJ_TRIPLET){
        auto *g = (Graph_Triplet *)rxGetContainedObject(obj);
        g->IncrRefCnt();
    }
    this->InsertKey(key, obj);
    sdsfree(key);
}

void *FaBlok::RemoveKey(sds key)
{
    void *old;
    raxRemove(&this->keyset, (UCHAR *)key, sdslen(key), &old);
    return old;
}

void *FaBlok::RemoveKey(unsigned char *s, size_t len)
{
    sds key = sdsnewlen(s, len);
    void *old = this->RemoveKey(key);
    sdsfree(key);
    return old;
}

void *FaBlok::LookupKey(sds key)
{
    void *m = raxFind(&this->keyset, (UCHAR *)key, sdslen(key));
    if (m == raxNotFound)
        return NULL;
    return m;
}

bool FaBlok::IsValueType(int mask)
{
    return ((this->value_type & mask) != 0);
}

int FaBlok::ValueType(int value_type)
{
    int old = this->value_type;
    this->value_type = value_type;
    return old;
}

sds FaBlok::GetCacheReport(){
    // TODO
    return sdsempty();
}