#ifndef __RXINDEX_H__
#define __RXINDEX_H__

#ifdef __cplusplus
extern "C"
{
#endif

#include "rax.h"
#include "util.h"
#include "zmalloc.h"

#ifdef __cplusplus
}
#endif

#include "sjiboleth-fablok.hpp"

class Mercator_Index{
    public:
        rax *keys_in_transaction;
        // TODO rax *value_attribute_index;
        // TODO rax *attribute_value_index;


    Mercator_Index(){
        this->keys_in_transaction = raxNew();
    }

    ~Mercator_Index(){
        raxFree(this->keys_in_transaction);
    }

    void Open_Key(sds key, int dbNo){
        if(raxFind(this->keys_in_transaction, (UCHAR *)key, sdslen(key)) != raxNotFound){
            this->Rollback_Key(key, dbNo);
        }
        sds objectIndexkey = sdscatfmt(sdsempty(), "_ox_:%s", key);
        void *obj = rxRemoveKeyRetainValue(dbNo, objectIndexkey);
        raxInsert(this->keys_in_transaction, (UCHAR *)key, sdslen(key), obj, &obj);
        sdsfree(objectIndexkey);
    }

    void Commit_Key(sds key, int dbNo){
        rxUNUSED(dbNo);
        void *obj;
        raxRemove(this->keys_in_transaction, (UCHAR *)key, sdslen(key), &obj);
        rxCommitKeyRetainValue(dbNo, sdsdup(key), obj);
    }

    void Rollback_Key(sds key, int dbNo){
        void *obj;
        sds objectIndexkey = sdscatfmt(sdsempty(), "_ox_:%s", key);
        raxRemove(this->keys_in_transaction, (UCHAR *)key, sdslen(key), &obj);
        rxRestoreKeyRetainValue(dbNo, objectIndexkey, obj);
        sdsfree(objectIndexkey);
    }
};
#endif