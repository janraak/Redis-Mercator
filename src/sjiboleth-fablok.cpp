#include "../../src/version.h"

#ifdef __cplusplus
extern "C"
{
#endif
#include <cstdlib>

#include "../../deps/hiredis/hiredis.h"

#include "sdsWrapper.h"

#if REDIS_VERSION_NUM < 0x00060200
#define SDSEMPTY sdsempty
#endif
#if REDIS_VERSION_NUM >= 0x00060200
#define SDSEMPTY hi_sdsempty
#endif


#include "string.h"
#include <pthread.h>
#include <unistd.h>

#include "../../src/rax.h"
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

#include "tls.hpp"
extern void *_allocateRax(void *parm);

rax *FaBlok::Get_FaBlok_Registry()
{
    rax *registry = tls_get<rax *>((const char *)"FaBlokCache", _allocateRax, NULL);
    return registry;
}

void FaBlok::Free_Thread_Registry()
{
    // pthread_t tid = pthread_self();
    // char id[64];
    // snprintf(id, sizeof(id), "fab:%lx", tid);
    // void *old = NULL;
    // ;
    // raxRemove(FaBlok::FaBlokRegistry, (UCHAR *)&id, sizeof(id), &old);
}

void FaBlok::Cache(FaBlok *b)
{
    return;
    rax *r = FaBlok::Get_FaBlok_Registry();
    if (raxFind(r, (UCHAR *)b->setname, strlen(b->setname)) == raxNotFound)
    {
       // rxServerLog(rxLL_NOTICE, "FaBlok::Cache fab=%p ks=%p for %s", b, b->keyset, b->setname);
        raxInsert(r, (UCHAR *)b->setname, strlen(b->setname), b, NULL);
    }
}

void FaBlok::Uncache(const char *sn)
{
    rax *r = FaBlok::Get_FaBlok_Registry();
    auto *standing = raxFind(r, (UCHAR *)sn, strlen(sn));
    if (standing != raxNotFound)
    {
       // rxServerLog(rxLL_NOTICE, "FaBlok::Rename FOUND OLD setname:%s", sn);
        void *old;
        raxRemove(r, (UCHAR *)sn, strlen(sn), &old);
    }
}

void FaBlok::Uncache(FaBlok *b)
{
    FaBlok::Uncache(b->setname);
}

FaBlok *FaBlok::Get(const char *sn)
{
   // rxServerLog(rxLL_NOTICE, "FaBlok::Get setname:%s", sn);
    FaBlok *f = (FaBlok *)raxFind(FaBlok::Get_FaBlok_Registry(), (UCHAR *)sn, strlen(sn));
    if (f == raxNotFound)
    {
        return NULL;
    }
   // rxServerLog(rxLL_NOTICE, "FaBlok::Get FOUND setname:%s", sn);
    f->IsValid();
    return f;
}

bool FaBlok::IsValid()
{
    // // if (!(((void *)this - sizeof(rax)) == (void *)this->keyset))
    // // {
    // //     rxServerLog(rxLL_NOTICE, "Possible memory corruption fablok=%p instead of %p rax=%p instead of %p",
    // //                 this,
    // //                 ((void *)this->keyset) + sizeof(rax),
    // //                 this->keyset,
    // //                 ((void *)this) - sizeof(rax));
    // //     return false;
    // // }

    // // if (!(((void *)this + sizeof(FaBlok)) == (void *)this->setname))
    // // {
    // //     rxServerLog(rxLL_NOTICE, "Possible memory corruption fablok=%p instead of %p setname=%p instead of %p",
    // //                 this,
    // //                 ((void *)this->setname) - sizeof(FaBlok)),
    // //         this->setname,
    // //         ((void *)this + sizeof(FaBlok));
    // //     return false;
    // // }

    // if (memcmp(this->rumble_strip1, RUMBLE_STRIP1, sizeof(this->rumble_strip1)) != 0 || memcmp(this->rumble_strip2, RUMBLE_STRIP2, sizeof(this->rumble_strip2)) != 0)
    // {
    //     rxServerLogRaw(rxLL_NOTICE, "Rumble in the Jungle");
    //     return false;
    // }
    // if ((strlen(this->setname) > 0 /*&& this->size != raxSize(this->keyset)*/) || !(this->marked_for_deletion == 653974783 || this->marked_for_deletion == 722765))
    // {
    //    rxServerLog(rxLL_NOTICE, "FaBlok for %s may be corrupted fab: reused:%ld sz: %lu marked_for_deletion:%ld temp:%ld\n",
    //                 this->setname,
    //                 this->reuse_count,
    //                 this->size,
    //                 this->marked_for_deletion,
    //                 this->is_temp);
    //     return false;
    // }
    return true;
}

FaBlok *FaBlok::Get(const char *sn, UCHAR value_type)
{
   // rxServerLog(rxLL_NOTICE, "FaBlok::Get setname:%s for type:%d", sn, value_type);
    FaBlok *f = (FaBlok *)raxFind(FaBlok::Get_FaBlok_Registry(), (UCHAR *)sn, strlen(sn));
    if (f == raxNotFound)
    {
       // rxServerLog(rxLL_NOTICE, "FaBlok::Get NOT FOUND setname:%s for type:%d", sn, value_type);
        FaBlok *n = FaBlok::New(sn, value_type);
        n->AsTemp();
        if (value_type != KeysetDescriptor_TYPE_SINGULAR)
        {
            FaBlok::Cache(n);
        }
        n->IsValid();
        return n;
    }
    else
    {
       // rxServerLog(rxLL_NOTICE, "FaBlok::Get FOUND setname:%s for type:%d", sn, value_type);
        f->reuse_count++;
        f->IsValid();
    }
    if (memcmp(f->rumble_strip1, RUMBLE_STRIP1, sizeof(f->rumble_strip1)) != 0 || memcmp(f->rumble_strip2, RUMBLE_STRIP2, sizeof(f->rumble_strip2)) != 0)
    {
        rxServerLogRaw(rxLL_NOTICE, "Rumble in the Jungle");
    }
    return f;
}

FaBlok *FaBlok::Rename(const char *setname)
{
   // rxServerLog(rxLL_NOTICE, "FaBlok::Rename OLD setname:%s", this->setname);
    Uncache(setname);
    Uncache(this);
    this->setname = setname;
    Cache(this);
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
        this->size = raxSize(this->keyset);
    }
    rxStringFree(key);
}

int FaBlok::AsTemp()
{
    if (!this->IsValueType(KeysetDescriptor_TYPE_MONITORED_SET))
    {
        this->is_temp = 722765;
        // this->marked_for_deletion = 722765;
    }
    return (this->is_temp == 722765);
}

FaBlok::FaBlok()
{
    this->Init(NULL);
}

FaBlok *FaBlok::Init(void *r)
{
    this->claims = 0;
    this->pid = gettid();
    this->thread_id = pthread_self();

    this->setname = rxStringEmpty();
    this->marked_for_deletion = 653974783;
    this->ValueType(KeysetDescriptor_TYPE_UNKNOWN);
    this->is_temp = 653974783;
    this->is_on_stack = 0;
    // EMBEDDED RAX INITIALIZATION
    if (r)
        this->InitKeyset(r, true);

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
    this->objectExpression = NULL;
    return this;
}

void FaBlok::ObjectExpression(ParserToken *expr)
{
    this->objectExpression = expr;
}

ParserToken *FaBlok::ObjectExpression()
{
    return this->objectExpression;
}

void FaBlok::InitKeyset(void *r, bool withRootNode)
{
    if (r == NULL)
        this->keyset = raxNew();
    else
    {
        // EMBEDDED RAX INITIALIZATION
        this->keyset = (rax *)r;
        this->keyset->numele = 0;
        this->keyset->numnodes = 1;
        this->keyset->head = (withRootNode == true) ? raxNewNode(0, 0) : NULL;
    }
   // rxServerLog(rxLL_NOTICE, "InitKeyset fab=%p ks=%p for %s", this, this->keyset, this->setname);
}

rax *FaBlok::AsRax()
{
    return this->keyset;
}

FaBlok::~FaBlok()
{
   // rxServerLog(rxLL_NOTICE, "~FaBlok fab=%p ks=%p for %s", this, this->keyset, this->setname);
    this->IsValid();
    if (this->value_type == 0xff)
        return;
    this->value_type = 0xff;
    if (this->thread_id != pthread_self())
        rxServerLogRaw(rxLL_NOTICE, "Cross thread alloc/free!\n");

    this->IsValid();
    // if (this->size > 0)
    // {
    //     rxRaxFree(this->keyset);
    // }

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
    size_t sz = sizeof(rax) + sizeof(FaBlok) + l + 1;
    void *fabSpace = rxMemAlloc(sz);

    char *s = (char *)fabSpace + sizeof(rax) + sizeof(FaBlok);
    strncpy(s, sn, l);
    s[l] = 0x00;
    auto *fab = ((FaBlok *)(fabSpace + sizeof(rax)))->Init(fabSpace);
    fab->setname = s;
    fab->value_type = value_type;

    return fab;
}


FaBlok *FaBlok::Delete(FaBlok *b)
{
   // rxServerLog(rxLL_NOTICE, "FaBlok::Delete fab=%p ks=%p for %s", b, b->keyset, b->setname);
    if (memcmp(b->rumble_strip1, RUMBLE_STRIP1, sizeof(b->rumble_strip1)) != 0 || memcmp(b->rumble_strip2, RUMBLE_STRIP2, sizeof(b->rumble_strip2)) != 0)
    {
        rxServerLogRaw(rxLL_NOTICE, "Rumble in the Jungle");
    }
    b->marked_for_deletion = 722765;
    if(b->parameter_list){
        // b->parameter_list->StartHead();
        // while(b->parameter_list->HasEntries()){
        //     auto p = b->parameter_list->Pop();
        //     Delete(p);
        // }
        delete b->parameter_list;
        b->parameter_list = NULL;
    }
    b->ClearKeys();
    if(raxSize(b->keyset))
        rxRaxFreeWithCallback(b->keyset, NULL);
    return NULL;
}

bool FaBlok::HasKeySet()
{
    return raxSize(this->keyset) > 0;
}

UCHAR FaBlok::ValueType()
{
    if (memcmp(this->rumble_strip1, RUMBLE_STRIP1, sizeof(this->rumble_strip1)) != 0 || memcmp(this->rumble_strip2, RUMBLE_STRIP2, sizeof(this->rumble_strip2)) != 0)
    {
        rxServerLogRaw(rxLL_NOTICE, "Rumble in the Jungle");
    }
    return this->value_type;
}

int FaBlok::pushIndexEntries(redisReply *reply)
{
    if (!reply)
        return C_ERR;
    if (reply->type == REDIS_REPLY_ARRAY)
    {
        this->ValueType(KeysetDescriptor_TYPE_KEYSET);

        // 1) member
        // 2) score
        for (size_t j = 0; j < reply->elements; j++)
        {
            redisReply *row = reply->element[j];
            redisReply *item = row->element[0];
            double score = (row->elements > 1) ? atof(row->element[1]->str) : 1.0;
            void *o = rxIndexEntry::NewAsRobj(item->str, score);
            auto *ie = (rxIndexEntry *)rxGetContainedObject(o);
            this->InsertKey((UCHAR *)ie->key, strlen(ie->key), o);
        }
        this->size = raxSize(this->keyset);
        freeReplyObject(reply);
        return C_OK;
    }
    freeReplyObject(reply);
    return C_ERR;
}

rxString FaBlok::AsSds()
{
    if (memcmp(this->rumble_strip1, RUMBLE_STRIP1, sizeof(this->rumble_strip1)) != 0 || memcmp(this->rumble_strip2, RUMBLE_STRIP2, sizeof(this->rumble_strip2)) != 0)
    {
        rxServerLogRaw(rxLL_NOTICE, "Rumble in the Jungle");
    }
    return rxStringNew(this->setname);
}

int FaBlok::FetchKeySet(redisNodeInfo *serviceConfig, const char *rh)
{
    int rc = C_ERR;
    if(serviceConfig->is_local != 0)
        return rc;
    redisContext *index_context = RedisClientPool<redisContext>::Acquire(serviceConfig->host_reference, "_CLIENT", "FaBlok::FetchKeySet");
    if (!index_context)
        return C_ERR;
    if ((size_t)index_context->obuf == (size_t)-1)
        index_context->obuf = SDSEMPTY();
    long long start = ustime();
    try
    {
        redisReply *rcc = NULL;
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
        rc = this->pushIndexEntries(rcc);
    }
    catch (...)
    {
        rxServerLog(rxLL_NOTICE, "#550#RXFETCH %s\n", rh);
        rxServerLog(rxLL_NOTICE, "#............ FetchKeyset ... %lld ... \n", this->latency);
    }
    RedisClientPool<redisContext>::Release(index_context, "FaBlok::FetchKeySet");
    return rc;
}

int FaBlok::FetchKeySet(redisNodeInfo *serviceConfig, const char *lh, const char *rh, rxString cmp)
{
    if(serviceConfig->is_local != 0)
        return C_ERR;
    auto *index_context = RedisClientPool<redisContext>::Acquire(serviceConfig->host_reference, "_CLIENT", "FaBlok::FetchKeySet2");
    if (index_context == NULL)
        return C_OK;
    if ((size_t)index_context->obuf == (size_t)-1)
        index_context->obuf = SDSEMPTY();
    redisReply *rcc = NULL;
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
    int rc = this->pushIndexEntries(rcc);
    RedisClientPool<redisContext>::Release(index_context, "FaBlok::FetchKeySet2");
    return rc;
}

void FreeFaBlok(void *o)
{
    FaBlok *kd = (FaBlok *)o;
    if (kd->IsValid())
        rxMemFree(kd->keyset);
}

void FaBlok::DeleteAllTempDescriptors()
{
    FaBlok::ClearCache();
    // raxFreeWithCallback(FaBlok::Get_FaBlok_Registry(), FreeFaBlok);
    // FaBlok::Free_Thread_Registry();
}

void FreeFaBlokConditionally(void *o)
{
    FaBlok *kd = (FaBlok *)o;
    // TODO: Add conditions!

    if (kd->IsValid())
        rxMemFree(kd->keyset);
}

void FaBlok::ClearCache()
{
    // TODO: Cache Management
    //  rax *r = FaBlok::Get_FaBlok_Registry();
    //  if (raxSize(r) > 0){
    //      raxFreeWithCallback(r, FreeFaBlokConditionally);
    //      if (raxSize(r) == 0)
    //          FaBlok::Free_Thread_Registry();
    //  }
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

FaBlok *FaBlok::Copy(rxString set_name, int value_type, RaxCopyCallProc *fnCallback, void *privData)
{
    FaBlok *out = FaBlok::Get(set_name ? set_name : this->setname, value_type);
    out->Open();
    out->AsTemp();
    if (out->HasKeySet())
    {
        // TODO: delete all standing entries
        // rxRaxFree(this->keyset);
       // rxServerLog(rxLL_NOTICE, "FaBlok::Copy Purge before copy fab=%p ks=%p for %s", this, this->keyset, this->setname);
    }

    FaBlok::Cache(out);

    if (this->HasKeySet())
    {
       // rxServerLog(rxLL_NOTICE, "FaBlok::Copy fab=%p ks=%p for %s", this, this->keyset, this->setname);
        raxIterator ri;
        raxStart(&ri, this->keyset);
        raxSeek(&ri, "^", NULL, 0);
        while (raxNext(&ri))
        {
            rxString k = rxStringNewLen((const char*)ri.key, ri.key_len);
            void *v = rxFindKey(0, k);
            // if(v != ri.data){
            //     rxServerLogRaw(rxLL_NOTICE, "Changed object");
            // }
            // rxServerLog(rxLL_NOTICE, "%s\ncallback on %x -> %x : %s",k , ri.data, rxGetContainedObject(ri.data), (char *)rxGetContainedObject(ri.data));
            // if(v)
            //     rxServerLog(rxLL_NOTICE, "standing on %x -> %x : %s", v, rxGetContainedObject(v), (char *)rxGetContainedObject(v));
            //     else
            //     rxServerLog(rxLL_NOTICE, "standing on %x -> ODD", v);
            rxStringFree(k);
            auto result = (fnCallback == NULL) ? true : fnCallback(ri.key, ri.key_len, v, privData);
            if (result != 0)
            {
                void *old_data = NULL;
                raxTryInsert(out->keyset, ri.key, ri.key_len, v, &old_data);
            }
        }
        raxStop(&ri);
    }
    out->Close();
    return out;
}
FaBlok *FaBlok::CopyAnd(FaBlok *that){
    char set_name[512];
    snprintf(set_name, sizeof(set_name), "AND %s :: %s", this->setname, that->setname);
    FaBlok *out = FaBlok::New(set_name, VERTEX_SET);   

    out->MergeFrom(this, that);
    return out;
}

FaBlok *FaBlok::Copy(rax *r)
{
    // rxServerLog(rxLL_NOTICE, "FaBlok::Copy fab=%p ks=%p for %s", this, this->keyset, this->setname);
    raxIterator ri;
    raxStart(&ri, r);
    raxSeek(&ri, "^", NULL, 0);
    while (raxNext(&ri))
    {
        this->size += raxInsert(this->keyset, ri.key, ri.key_len, ri.data, NULL);
    }
    raxStop(&ri);
    return this;
}

FaBlok *FaBlok::Copy(rxString set_name, int value_type)
{
    return Copy(set_name, value_type, (RaxCopyCallProc *)NULL, NULL);
}

FaBlok *FaBlok::CopyKeys(rxString set_name, int value_type, RaxCopyCallProc *, void *privData)
{
    int is_object = *((bool *)privData);

    FaBlok *out = FaBlok::Get(set_name ? set_name : this->setname, value_type);
    out->Open();
    out->AsTemp();
    if (out->HasKeySet())
    {
        // TODO: delete all standing entries
        // rxRaxFree(this->keyset);
       // rxServerLog(rxLL_NOTICE, "FaBlok::Copy Purge before copy fab=%p ks=%p for %s", this, this->keyset, this->setname);
    }

    FaBlok::Cache(out);

    if (this->HasKeySet())
    {
        // rxServerLog(rxLL_NOTICE, "FaBlok::Copy fab=%p ks=%p for %s", this, this->keyset, this->setname);
        raxIterator ri;
        raxStart(&ri, this->keyset);
        raxSeek(&ri, "^", NULL, 0);
        while (raxNext(&ri))
        {
            rxString k = rxStringNewLen((const char *)ri.key, ri.key_len);
            char *colons[4] = {NULL, NULL, NULL, NULL};
            colons[0] = strchr((char *)k, ':');
            if (colons[0])
                colons[1] = strchr(colons[0] + 1, ':');
            if (colons[1])
                colons[2] = strchr(colons[1] + 1, ':');
            if (colons[2])
                colons[3] = strchr(colons[2] + 1, ':');

            rxString new_key;
            if(colons[0] == NULL){
                new_key = rxStringNewLen((const char *)ri.key, ri.key_len);
            }else             if (is_object == 0 /* Get subject */)
            {
                new_key = rxStringNewLen(colons[0] + 1, colons[1] - colons[0] - 1);
            }
            else /* Get object */
            {
                new_key = rxStringNew(colons[1] + 1);
            }

            void *v = rxFindKey(0, new_key);
            rxStringFree(k);
                void *old_data = NULL;
                raxTryInsert(out->keyset, (UCHAR*)new_key, strlen(new_key), v, &old_data);
            rxStringFree(new_key);
        }
        raxStop(&ri);
    }
    out->Close();
    return out;
}

FaBlok *FaBlok::Copy(rxString set_name, int value_type, RaxFilterCallProc *fnCallback, void *privData)
{
    FaBlok *out = FaBlok::Get(set_name ? set_name : this->setname, value_type);
    out->Open();
    out->AsTemp();
    if (out->HasKeySet())
    {
        // TODO: delete all standing entries
        // rxRaxFree(this->keyset);
       // rxServerLog(rxLL_NOTICE, "FaBlok::Copy Purge before copy fab=%p ks=%p for %s", this, this->keyset, this->setname);
    }

    FaBlok::Cache(out);

    if (this->HasKeySet())
    {
       // rxServerLog(rxLL_NOTICE, "FaBlok::Copy fab=%p ks=%p for %s", this, this->keyset, this->setname);
        raxIterator ri;
        raxStart(&ri, this->keyset);
        raxSeek(&ri, "^", NULL, 0);
        while (raxNext(&ri))
        {
            rxString k = rxStringNewLen((const char*)ri.key, ri.key_len);
            void *v = rxFindKey(0, k);
            // if(v != ri.data){
            //     rxServerLogRaw(rxLL_NOTICE, "Changed object");
            // }
            // rxServerLog(rxLL_NOTICE, "%s\ncallback on %x -> %x : %s",k , ri.data, rxGetContainedObject(ri.data), (char *)rxGetContainedObject(ri.data));
            // if(v)
            //     rxServerLog(rxLL_NOTICE, "standing on %x -> %x : %s", v, rxGetContainedObject(v), (char *)rxGetContainedObject(v));
            //     else
            //     rxServerLog(rxLL_NOTICE, "standing on %x -> ODD", v);
            rxStringFree(k);
            auto result = (fnCallback == NULL) ? true : fnCallback(ri.key, ri.key_len, v, privData);
            if (result != 0)
            {
                void *old_data = NULL;
                raxTryInsert(out->keyset, ri.key, ri.key_len, v, &old_data);
            }
        }
        raxStop(&ri);
    }
    out->Close();
    return out;
}

FaBlok *FaBlok::CopyKeys(rxString set_name, int value_type, RaxFilterCallProc *, void *privData)
{
    int is_object = *((bool *)privData);

    FaBlok *out = FaBlok::Get(set_name ? set_name : this->setname, value_type);
    out->Open();
    out->AsTemp();
    if (out->HasKeySet())
    {
        // TODO: delete all standing entries
        // rxRaxFree(this->keyset);
       // rxServerLog(rxLL_NOTICE, "FaBlok::Copy Purge before copy fab=%p ks=%p for %s", this, this->keyset, this->setname);
    }

    FaBlok::Cache(out);

    if (this->HasKeySet())
    {
        // rxServerLog(rxLL_NOTICE, "FaBlok::Copy fab=%p ks=%p for %s", this, this->keyset, this->setname);
        raxIterator ri;
        raxStart(&ri, this->keyset);
        raxSeek(&ri, "^", NULL, 0);
        while (raxNext(&ri))
        {
            rxString k = rxStringNewLen((const char *)ri.key, ri.key_len);
            char *colons[4] = {NULL, NULL, NULL, NULL};
            colons[0] = strchr((char *)k, ':');
            if (colons[0])
                colons[1] = strchr(colons[0] + 1, ':');
            if (colons[1])
                colons[2] = strchr(colons[1] + 1, ':');
            if (colons[2])
                colons[3] = strchr(colons[2] + 1, ':');

            rxString new_key;
            if(colons[0] == NULL){
                new_key = rxStringNewLen((const char *)ri.key, ri.key_len);
            }else             if (is_object == 0 /* Get subject */)
            {
                new_key = rxStringNewLen(colons[0] + 1, colons[1] - colons[0] - 1);
            }
            else /* Get object */
            {
                new_key = rxStringNew(colons[1] + 1);
            }

            void *v = rxFindKey(0, new_key);
            rxStringFree(k);
                void *old_data = NULL;
                raxTryInsert(out->keyset, (UCHAR*)new_key, strlen(new_key), v, &old_data);
            rxStringFree(new_key);
        }
        raxStop(&ri);
    }
    out->Close();
    return out;
}

FaBlok *FaBlok::CopyKeys(rxString set_name, int value_type, void *privData)
{
    return CopyKeys(set_name, value_type, (RaxFilterCallProc *) NULL, privData);
}

int FaBlok::CopyTo(FaBlok *out)
{
    raxIterator ri;
    int c = 0;
    raxStart(&ri, this->keyset);
    raxSeek(&ri, "^", NULL, 0);
    while (raxNext(&ri))
    {
        out->InsertKey(ri.key, ri.key_len, ri.data);
        ++c;
    }
    raxStop(&ri);
    out->size = raxSize(out->keyset);
    return c;
}

int FaBlok::MergeInto(FaBlok *out)
{
    if(!out)
        return 0;
    raxIterator riIn;
    int c = 0;
    raxStart(&riIn, this->keyset);
    raxSeek(&riIn, "^", NULL, 0);
    raxIterator riOut;
    raxStart(&riOut, out->keyset);
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
        {
            if (rxGetObjectType(riIn.data) == rxOBJ_INDEX_ENTRY && rxGetObjectType(riIn.data) == rxGetObjectType(riOut.data))
            {
                rxIndexEntry *in = (rxIndexEntry *)rxGetContainedObject(riIn.data);
                rxIndexEntry *out = (rxIndexEntry *)rxGetContainedObject(riOut.data);
                out->key_score += in->key_score;
            }
            raxPrev(&riOut);
        }
    }
    raxStop(&riIn);
    out->size = raxSize(out->keyset);
    return c;
}

void FaBlok::PushResult(GraphStack<FaBlok> *stack)
{
    if (this->keyset != NULL)
    {
        this->size = raxSize(this->keyset);
    }
    this->latency = ustime() - this->start;
    stack->Push(this);
}

FaBlok *FaBlok::Open()
{
    this->start = ustime();
    if (this->marked_for_deletion == 722765)
    {
        rxServerLog(rxLL_NOTICE, "Opening a deleted FABLOK!  for %s fab: reused:%ld sz: %lu    \n",
                    this->setname,
                    this->reuse_count,
                    this->size);
    }
    this->IsValid();
    return this;
}
FaBlok *FaBlok::Close()
{
    if (this->keyset != NULL)
    {
        this->size = raxSize(this->keyset);
    }
    this->latency += ustime() - this->start;
    this->IsValid();
    return this;
}

int FaBlok::MergeFrom(FaBlok *left, FaBlok *right)
{
    raxIterator riLeft;
    int c = 0;
    raxStart(&riLeft, left->keyset);
    raxSeek(&riLeft, "^", NULL, 0);
    raxIterator riRight;
    raxStart(&riRight, right->keyset);
    raxSeek(&riRight, "^", NULL, 0);

    raxNext(&riLeft);
    raxNext(&riRight);
    while (!(raxEOF(&riLeft) || raxEOF(&riRight)))
    {
        if (raxCompare(&riRight, "==", riLeft.key, riLeft.key_len) == 1)
        {
            if (rxGetObjectType(riLeft.data) == rxOBJ_INDEX_ENTRY && rxGetObjectType(riLeft.data) == rxGetObjectType(riRight.data))
            {
                rxIndexEntry *left = (rxIndexEntry *)rxGetContainedObject(riLeft.data);
                rxIndexEntry *right = (rxIndexEntry *)rxGetContainedObject(riRight.data);
                left->key_score += right->key_score;
            }
            this->InsertKey(riLeft.key, riLeft.key_len, riLeft.data);
            raxNext(&riLeft);
            raxNext(&riRight);
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
            rxServerLog(rxLL_NOTICE, "#140# FaBlok::MergeFrom %s -> %s\n", riLeft.key, riRight.key);
    }
    raxStop(&riLeft);
    raxStop(&riRight);
    this->size = raxSize(this->keyset);
    return c;
}

int FaBlok::MergeDisjunct(FaBlok *left, FaBlok *right)
{
    raxIterator riLeft;
    int c = 0;
    raxStart(&riLeft, left->keyset);
    raxSeek(&riLeft, "^", NULL, 0);
    raxIterator riRight;
    raxStart(&riRight, right->keyset);
    raxSeek(&riRight, "^", NULL, 0);

    raxNext(&riLeft);
    raxNext(&riRight);
    while (!(raxEOF(&riLeft) || raxEOF(&riRight)))
    {
        if (raxCompare(&riRight, "==", riLeft.key, riLeft.key_len) == 1)
        {
            raxNext(&riLeft);
            raxNext(&riRight);
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
            rxServerLog(rxLL_NOTICE, "#140# FaBlok::MergeDisjunct %s -> %s\n", riLeft.key, riRight.key);
    }
    while (!raxEOF(&riLeft))
    {
            this->InsertKey(riLeft.key, riLeft.key_len, riLeft.data);
            ++c;
            raxNext(&riLeft);
    }
    while (!raxEOF(&riRight))
    {
            this->InsertKey(riRight.key, riRight.key_len, riRight.data);
            ++c;
            raxNext(&riRight);
    }
    raxStop(&riLeft);
    raxStop(&riRight);
    this->size = raxSize(this->keyset);
    return c;
}

// int CopyNotInOnRax(FaBlok *out, rax *left, rax *right)
// {
//     raxIterator riLeft;
//     int c = 0;
//     raxStart(&riLeft, left);
//     raxSeek(&riLeft, "^", NULL, 0);
//     raxIterator riRight;
//     raxStart(&riRight, right);
//     raxSeek(&riRight, "^", NULL, 0);

//     raxNext(&riLeft);
//     raxNext(&riRight);
//     while (!(raxEOF(&riLeft) || raxEOF(&riRight)))
//     {
//         if (raxCompare(&riRight, "==", riLeft.key, riLeft.key_len) == 1)
//         {
//             raxNext(&riLeft);
//             raxNext(&riRight);
//         }
//         else if (raxCompare(&riRight, ">", riLeft.key, riLeft.key_len) == 1)
//         {
//             out->InsertKey(riLeft.key, riLeft.key_len, riLeft.data);
//             ++c;
//             raxNext(&riLeft);
//         }
//         else if (raxCompare(&riLeft, ">", riRight.key, riRight.key_len) == 1)
//         {
//             raxNext(&riRight);
//         }
//     }
//     raxStop(&riLeft);
//     return c;
// }

int FaBlok::CopyNotIn(rax *left, rax *right)
{
    bool deleteRight = false;
    if (!right)
    {
        right = raxNew();
        deleteRight = true;
    }
    raxIterator riLeft;
    int c = 0;
    raxStart(&riLeft, left);
    raxSeek(&riLeft, "^", NULL, 0);
    raxIterator riRight;
    raxStart(&riRight, right);
    raxSeek(&riRight, "^", NULL, 0);

    raxNext(&riLeft);
    raxNext(&riRight);
    while (!(raxEOF(&riLeft) || raxEOF(&riRight)))
    {
        if (raxCompare(&riRight, "==", riLeft.key, riLeft.key_len) == 1)
        {
            raxNext(&riLeft);
            raxNext(&riRight);
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
    if (deleteRight)
    {
        raxFree(right);
    }
    return c;
}

int FaBlok::CopyNotIn(FaBlok *left, FaBlok *right)
{
    return this->CopyNotIn(left->keyset, right->keyset);
}

void FaBlok::AddKey(const char *key, void *obj)
{
    this->Open();
    this->InsertKey(key, obj);
    this->Close();
}

void FaBlok::InsertKey(const char *key, void *obj)
{
    //rxServerLog(rxLL_NOTICE, "FaBlok::InsertKey setname:%s key:%s obj:%p", this->setname, key, obj);
    void *old = raxFind(this->keyset, (UCHAR *)key, strlen(key));
    if (old != raxNotFound)
    {
        //rxServerLog(rxLL_NOTICE, "FaBlok::InsertKey FOUND setname:%s key:%s obj:%p", this->setname, key, obj);
        return;
    }
    old = NULL;
    if (rxGetObjectType(obj) == rxOBJ_TRIPLET)
    {
        auto *g = (Graph_Triplet *)rxGetContainedObject(obj);
        g->IncrRefCnt();
    }
    //rxServerLog(rxLL_NOTICE, "FaBlok::InsertKey fab=%p ks=%p for %s key:%s", this, this->keyset, this->setname, key);
    raxInsert(this->keyset, (UCHAR *)key, strlen(key), obj, &old);
    this->size++;
}

void FaBlok::InsertKey(unsigned char *s, size_t len, void *obj)
{
    rxString key = rxStringNewLen((const char *)s, len);
    if (rxGetObjectType(obj) == rxOBJ_TRIPLET)
    {
        auto *g = (Graph_Triplet *)rxGetContainedObject(obj);
        g->IncrRefCnt();
    }
    //rxServerLog(rxLL_NOTICE, "FaBlok::InsertKey from string fab=%p ks=%p for %s key:%s", this, this->keyset, this->setname, key);
    this->InsertKey(key, obj);
    rxStringFree(key);
}

void *FaBlok::RemoveKey(const char *key)
{
    void *old;
   // rxServerLog(rxLL_NOTICE, "FaBlok::RemoveKey fab=%p ks=%p for %s key:%s", this, this->keyset, this->setname, key);
    raxRemove(this->keyset, (UCHAR *)key, strlen(key), &old);
    return old;
}

void *FaBlok::RemoveKey(unsigned char *s, size_t len)
{
    rxString key = rxStringNewLen((const char *)s, len);
   // rxServerLog(rxLL_NOTICE, "FaBlok::RemoveKey bytes fab=%p ks=%p for %s key:%s", this, this->keyset, this->setname, key);
    void *old = this->RemoveKey(key);
    rxStringFree(key);
    return old;
}

#include <semaphore.h> 

void *FaBlok::ClearKeys()
{
    static bool lock_initialized = false;
    static mtx_t lock;
    if (!lock_initialized)
    {
        mtx_init(&lock, mtx_plain);
        lock_initialized = true;
    }

    mtx_lock(&lock);
    raxIterator setIterator;
    raxStart(&setIterator, this->keyset);
    raxSeek(&setIterator, "^", NULL, 0);
    while (raxNext(&setIterator))
    {
        raxRemove(this->keyset, setIterator.key, setIterator.key_len, NULL);
        raxStop(&setIterator);
        raxStart(&setIterator, this->keyset);
        raxSeek(&setIterator, "^", NULL, 0);
    }
    raxStop(&setIterator);
    this->size = 0;
    mtx_unlock(&lock);
    return this;
}

void *FaBlok::LookupKey(const char *key)
{
   // rxServerLog(rxLL_NOTICE, "FaBlok::LookupKey fab=%p ks=%p for %s key:%s", this, this->keyset, this->setname, key);
    void *m = raxFind(this->keyset, (UCHAR *)key, strlen(key));
    if (m == raxNotFound)
        return NULL;
   // rxServerLog(rxLL_NOTICE, "FaBlok::LookupKey FOUNDfab=%p ks=%p for %s key:%s", this, this->keyset, this->setname, key);
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



template<> void GraphStack<FaBlok>::PopAndDeleteValue(){
    FaBlok *e = this->Pop();
    // FaBlok::Delete(e);
    // // delete e;
}
