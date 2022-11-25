

#ifdef __cplusplus
extern "C"
{
#endif
#include <cstdlib>

#include "../../deps/hiredis/hiredis.h"
#include "string.h"
#include "sdsWrapper.h"
#include <pthread.h>
#include <unistd.h>

#include "rax.h"
#include "rxSuiteHelpers.h"
    extern raxNode *raxNewNode(size_t children, int datafield);

#ifdef __cplusplus
}
#endif

#include "client-pool.hpp"
#include "sjiboleth-fablok.hpp"
#include "sjiboleth-graph.hpp"

static char RUMBLE_STRIP1[] = "AUTHOR:JAN RAAK.";
static char RUMBLE_STRIP2[] = "2022SHORELINE,WA";

rax *FaBlok::Get_Thread_Registry()
{
    pthread_t id = pthread_self();
    rax *local_registry = (rax *)raxFind(FaBlok::Registry, (UCHAR *)&id, sizeof(id));
    if(local_registry == raxNotFound){
        local_registry = raxNew();
        raxInsert(FaBlok::Registry, (UCHAR *)&id, sizeof(id), local_registry, NULL);
    }
    return local_registry;
}

void FaBlok::Free_Thread_Registry()
{
    pthread_t id = pthread_self();
    raxRemove(FaBlok::Registry, (UCHAR *)&id, sizeof(id), NULL);
}

FaBlok *FaBlok::Get(const char *sn)
{
    FaBlok *f = (FaBlok *)raxFind(FaBlok::Get_Thread_Registry(), (UCHAR *)sn, strlen(sn));
    if (f == raxNotFound)
    {
        return NULL;
    }
    f->IsValid();
    return f;
}

bool FaBlok::IsValid()
{
    if (memcmp(this->rumble_strip1, RUMBLE_STRIP1, sizeof(this->rumble_strip1)) != 0 || memcmp(this->rumble_strip2, RUMBLE_STRIP2, sizeof(this->rumble_strip2)) != 0)
    {
        printf("Rumble in the Jungle");
        return false;
    }
    if ((strlen(this->setname) > 0 && this->size != raxSize(&this->keyset)) || !(this->marked_for_deletion == 653974783 || this->marked_for_deletion == 2069722765))
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
    FaBlok *f = (FaBlok *)raxFind(FaBlok::Get_Thread_Registry(), (UCHAR *)sn, strlen(sn));
    if (f == raxNotFound)
    {
        FaBlok *n = FaBlok::New(sn, value_type);
        n->AsTemp();
        raxInsert(FaBlok::Get_Thread_Registry(), (UCHAR *)sn, strlen(sn), n, NULL);
        n->IsValid();
        return n;
    }
    else
    {
        f->reuse_count++;
        f->IsValid();
    }
    if (memcmp(f->rumble_strip1, RUMBLE_STRIP1, sizeof(f->rumble_strip1)) != 0 || memcmp(f->rumble_strip2, RUMBLE_STRIP2, sizeof(f->rumble_strip2)) != 0)
    {
        printf("Rumble in the Jungle");
    }
    return f;
}

FaBlok *FaBlok::Delete(FaBlok *d)
{
    if (memcmp(d->rumble_strip1, RUMBLE_STRIP1, sizeof(d->rumble_strip1)) != 0 || memcmp(d->rumble_strip2, RUMBLE_STRIP2, sizeof(d->rumble_strip2)) != 0)
    {
        printf("Rumble in the Jungle");
    }
    // if (d->IsValueType(KeysetDescriptor_TYPE_MONITORED_SET))
    //     return NULL;
    d->marked_for_deletion = 2069722765;
    return NULL;
}

FaBlok *FaBlok::Rename(const char *setname)
{
    auto *standing = raxFind(FaBlok::Get_Thread_Registry(), (UCHAR *)this->setname, strlen(this->setname));
    if (standing == this)
    {
        void *old;
        raxRemove(FaBlok::Get_Thread_Registry(), (UCHAR *)this->setname, strlen(this->setname), &old);
    }
    this->setname = setname;
    raxInsert(FaBlok::Get_Thread_Registry(), (UCHAR *)this->setname, strlen(this->setname), this, NULL);
    return this;
}

void FaBlok::LoadKey(int dbNo, const char *k)
{
    rxString key = rxStringNew(k);
    void *zobj = rxFindKey(dbNo, key);
    if (zobj == NULL)
    {
        rxStringFree(key);
        return;
    }
    if (rxGetObjectType(zobj) == rxOBJ_HASH)
    {
        this->InsertKey((UCHAR *)k, strlen(key), zobj);
        this->size = raxSize(&this->keyset);
    }
    rxStringFree(key);
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
    this->Init();
}

FaBlok *FaBlok::Init()
{
    this->pid = getpid();
    this->thread_id = pthread_self();

    this->setname = rxStringEmpty();
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
    memcpy(this->rumble_strip1, RUMBLE_STRIP1, sizeof(this->rumble_strip1));
    memcpy(this->rumble_strip2, RUMBLE_STRIP2, sizeof(this->rumble_strip2));
    return this;
}

void FaBlok::InitKeyset(bool withRootNode)
{
    // EMBEDDED RAX INITIALIZATION
    this->keyset.numele = 0;
    this->keyset.numnodes = 1;
    this->keyset.head = (withRootNode == true) ? raxNewNode(0, 0) : NULL;
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
    this->IsValid();
    if (this->value_type == 0xff)
        return;
    this->value_type = 0xff;
    if (this->thread_id != pthread_self())
        printf("Cross thread alloc/free!\n");
    if (this->keyset.head != NULL)
    {
        this->IsValid();
        if (this->size > 0)
        {
            raxIterator ri;
            raxStart(&ri, &this->keyset);
            raxSeek(&ri, "^", NULL, 0);
            while (raxNext(&ri))
            {
                void *old;
                raxRemove(FaBlok::Get_Thread_Registry(), ri.key, ri.key_len, &old);
                raxSeek(&ri, ">", ri.key, ri.key_len);
            }
            raxStop(&ri);
        }
        this->InitKeyset(false);
    }
    this->value_type = 0xff;
}
FaBlok::FaBlok(rxString sn, UCHAR value_type)
    : FaBlok()
{
    this->setname = sn;
    this->ValueType(value_type);
}

FaBlok *FaBlok::New(const char *sn, UCHAR value_type)
{
    int l = strlen(sn);
    void *fabSpace = rxMemAlloc(sizeof(FaBlok) + l + 1);
    char *s = (char *)fabSpace + sizeof(FaBlok);
    strncpy(s, sn, l);
    s[l] = 0x00;
    auto *fab = ((FaBlok *)fabSpace)->Init();
    fab->setname = s;
    fab->value_type = value_type;
    return fab;
}

bool FaBlok::HasKeySet()
{
    return raxSize(&this->keyset) > 0;
}

rax *FaBlok::Registry = raxNew();

UCHAR FaBlok::ValueType()
{
    if (memcmp(this->rumble_strip1, RUMBLE_STRIP1, sizeof(this->rumble_strip1)) != 0 || memcmp(this->rumble_strip2, RUMBLE_STRIP2, sizeof(this->rumble_strip2)) != 0)
    {
        printf("Rumble in the Jungle");
    }
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
        for (size_t j = 0; j < reply->elements; j++)
        {
            redisReply *row = reply->element[j];
            redisReply *item = row->element[0];
            rxString index_entry = rxStringNew(item->str);

            int segments = 0;
            rxString *parts = rxStringSplitLen(index_entry, strlen(index_entry), "\t", 1, &segments);
            if (segments > 0)
            {
                void *o = rxCreateStringObject(index_entry, strlen(index_entry));
                this->InsertKey((UCHAR *)parts[0], strlen(parts[0]), o);
            }
            rxStringFreeSplitRes(parts, segments);
        }
        this->size = raxSize(&this->keyset);
    }
    freeReplyObject(reply);
}

rxString FaBlok::AsSds()
{
    if (memcmp(this->rumble_strip1, RUMBLE_STRIP1, sizeof(this->rumble_strip1)) != 0 || memcmp(this->rumble_strip2, RUMBLE_STRIP2, sizeof(this->rumble_strip2)) != 0)
    {
        printf("Rumble in the Jungle");
    }
    return rxStringNew(this->setname);
}

int FaBlok::FetchKeySet(redisNodeInfo *serviceConfig, const char *rh)
{
    redisContext *index_context = RedisClientPool<redisContext>::Acquire(serviceConfig->host_reference);
    if (!index_context)
        return C_ERR;
    long long start = ustime();
    try
    {
        redisReply *rcc;
        int segments = 0;
        rxString *parts = rxStringSplitLen(rh, strlen(rh), " ", 1, &segments);
        switch (segments)
        {
        case 2:
            rcc = (redisReply *)redisCommand(index_context, "rxfetch %s %s", parts[0], parts[1]);
            break;
        case 3:
            rcc = (redisReply *)redisCommand(index_context, "rxfetch %s %s %s", parts[0], parts[2], parts[0]);
            break;
        default:
            rcc = (redisReply *)redisCommand(index_context, "rxfetch %s", rh);
            break;
        }
        rxStringFreeSplitRes(parts, segments);
        this->latency = ustime() - start;
        this->pushIndexEntries(rcc);
    }
    catch (...)
    {
        printf("#550#RXFETCH %s\n", rh);
        printf("#............ FetchKeyset ... %lld ... \n", this->latency);
    }
    RedisClientPool<redisContext>::Release(index_context);
    return C_OK;
}

int FaBlok::FetchKeySet(redisNodeInfo *serviceConfig, const char *lh, const char *rh, rxString cmp)
{
    auto *index_context = RedisClientPool<redisContext>::Acquire(serviceConfig->host_reference);
    if (index_context == NULL)
        return C_OK;
    if (index_context == NULL)
        return C_ERR;
    redisReply *rcc;
    long long start = ustime();
    if (isdigit(*rh))
    {
        rcc = (redisReply *)redisCommand(index_context, "RXFETCH %s %s %s", rh, lh, cmp);
    }
    else if (*lh)
    {
        rcc = (redisReply *)redisCommand(index_context, "RXFETCH %s %s ", rh, lh);
    }
    else
    {
        rcc = (redisReply *)redisCommand(index_context, "RXFETCH %s", rh);
    }
    this->latency = ustime() - start;
    this->pushIndexEntries(rcc);
    RedisClientPool<redisContext>::Release(index_context);
    return C_OK;
}


void FreeFaBlok(void *o){
            rxMemFree(o);
}

void FaBlok::DeleteAllTempDescriptors()
{
    raxFreeWithCallback(FaBlok::Get_Thread_Registry(), FreeFaBlok);
    FaBlok::Free_Thread_Registry();
}

void FaBlok::ClearCache()
{
    raxIterator ri;
    raxStart(&ri, FaBlok::Get_Thread_Registry());
    raxSeek(&ri, "^", NULL, 0);
    while (raxNext(&ri))
    {
        FaBlok *kd = (FaBlok *)ri.data;
        // if (!kd->ValueType(KeysetDescriptor_TYPE_MONITORED_SET))
        {
            FaBlok *old;
            raxRemove(FaBlok::Get_Thread_Registry(), ri.key, ri.key_len, (void **)&old);
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

FaBlok *FaBlok::Copy(rxString set_name, int value_type, RaxCopyCallProc *fnCallback, void **privData)
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

    if (raxFind(FaBlok::Get_Thread_Registry(), (UCHAR *)set_name, strlen(set_name)) == raxNotFound)
    {
        raxInsert(FaBlok::Get_Thread_Registry(), (UCHAR *)out->setname, strlen(out->setname), out, NULL);
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
    // rxServerLog(LL_NOTICE, "#putKeysetDescriptor2# 0x%x nm=%s reuse=%d", (POINTER) kd, kd->setname, kd->reuse_count);
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

void FaBlok::AddKey(const char *key, void *obj)
{
    this->Open();
    this->InsertKey(key, obj);
    this->Close();
}

void FaBlok::InsertKey(const char *key, void *obj)
{
    void *old = raxFind(&this->keyset, (UCHAR *)key, strlen(key));
    if (old != raxNotFound)
        return;

    old = NULL;
    if (rxGetObjectType(obj) == rxOBJ_TRIPLET)
    {
        auto *g = (Graph_Triplet *)rxGetContainedObject(obj);
        g->IncrRefCnt();
    }
    raxInsert(&this->keyset, (UCHAR *)key, strlen(key), obj, &old);
}

void FaBlok::InsertKey(unsigned char *s, size_t len, void *obj)
{
    rxString key = rxStringNewLen((const char *)s, len);
    if (rxGetObjectType(obj) == rxOBJ_TRIPLET)
    {
        auto *g = (Graph_Triplet *)rxGetContainedObject(obj);
        g->IncrRefCnt();
    }
    this->InsertKey(key, obj);
    rxStringFree(key);
}

void *FaBlok::RemoveKey(const char *key)
{
    void *old;
    raxRemove(&this->keyset, (UCHAR *)key, strlen(key), &old);
    return old;
}

void *FaBlok::RemoveKey(unsigned char *s, size_t len)
{
    rxString key = rxStringNewLen((const char*)s, len);
    void *old = this->RemoveKey(key);
    rxStringFree(key);
    return old;
}

void *FaBlok::LookupKey(const char *key)
{
    void *m = raxFind(&this->keyset, (UCHAR *)key, strlen(key));
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

rxString FaBlok::GetCacheReport()
{
    // TODO
    return rxStringEmpty();
}

#include "sjiboleth-silnikparowy-ctx.cpp"
#include "sjiboleth-silnikparowy.cpp"
