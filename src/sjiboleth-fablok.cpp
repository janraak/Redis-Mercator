

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

bool FaBlok::HasKeySet()
{
    return raxSize(&this->keyset) > 0;
}

rax *FaBlok::Registry = raxNew();

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
    if(index_context == NULL)
        return C_OK;
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

void FaBlok::DeleteAllTempDescriptors()
{
    raxIterator ri;
    raxStart(&ri, FaBlok::Registry);
    raxSeek(&ri, "^", NULL, 0);
    while (raxNext(&ri))
    {
       FaBlok *kd = (FaBlok *)ri.data;
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


#include "sjiboleth-silnikparowy.cpp"
#include "sjiboleth-silnikparowy-ctx.cpp"
