#ifndef __RULE_H__
#define __RULE_H__

#ifdef __cplusplus
extern "C"
{
#endif

#include "rax.h"

#define LL_DEBUG 0
#define LL_VERBOSE 1
#define LL_NOTICE 2
#define LL_WARNING 3
#define LL_RAW (1<<10) /* Modifier to log without timestamp */


#ifdef __GNUC__
void serverLog(int level, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
#else
void serverLog(int level, const char *fmt, ...);
#endif
#include "rxSuite.h"
#include "adlist.h"
#include "parser.h"
#include "queryEngine.h"
#ifdef __cplusplus
}
#endif

#include "sjiboleth-fablok.hpp"

const char *GREMLIN_DIALECT = "gremlin";
const char *QUERY_DIALECT = "query";

// template <typename T>
class BusinessRule
{
public:
    static rax *Registry;
    // Parser *rule;
    ParsedExpression *rule = NULL;
    bool isvalid;
    FaBlok *set;
    sds setName;
    long long apply_count;
    long long apply_skipped_count;
    long long apply_hit_count;
    long long apply_miss_count;

    static GremlinDialect *RuleParser;

    static  BusinessRule *Retain(BusinessRule *br){
        if(BusinessRule::Registry == NULL)
            BusinessRule::Registry = raxNew();
        BusinessRule *old;
        raxTryInsert(BusinessRule::Registry, (UCHAR *)br->setName, sdslen(br->setName), br, (void **)&old);
        return old;
    }

    static  BusinessRule *Forget(BusinessRule *br){
        BusinessRule *old = (BusinessRule *)raxFind(BusinessRule::Registry, (UCHAR *)br->setName, sdslen(br->setName));
        if(old != raxNotFound)
        {
            raxRemove(BusinessRule::Registry, (UCHAR *)br->setName, sdslen(br->setName), (void **)&old);
        }
        return old;
    }

    static int WriteList(RedisModuleCtx *ctx)
    {
        RedisModule_ReplyWithArray(ctx, raxSize(BusinessRule::Registry));
        raxIterator ri;

        raxStart(&ri, BusinessRule::Registry);
        raxSeek(&ri, "^", NULL, 0);
        while (raxNext(&ri))
        {
            sds ruleName = sdsnewlen(ri.key, ri.key_len);
            RedisModule_ReplyWithArray(ctx, 14);
            BusinessRule *br = (BusinessRule *)ri.data;
            RedisModule_ReplyWithStringBuffer(ctx, (char *)"name", 4);
            RedisModule_ReplyWithStringBuffer(ctx, (char *)ruleName, strlen((char *)ruleName));
            sds rpn = br->ParsedToString();
            RedisModule_ReplyWithStringBuffer(ctx, (char *)"rule", 4);
            RedisModule_ReplyWithStringBuffer(ctx, (char *)rpn, strlen((char *)rpn));
            sdsfree(rpn);
            RedisModule_ReplyWithStringBuffer(ctx, (char *)"no of keys", 10);
            RedisModule_ReplyWithLongLong(ctx, raxSize(&br->set->keyset));
            RedisModule_ReplyWithStringBuffer(ctx, (char *)"no of applies", 13);
            RedisModule_ReplyWithLongLong(ctx, br->apply_count);
            RedisModule_ReplyWithStringBuffer(ctx, (char *)"no of skips", 11);
            RedisModule_ReplyWithLongLong(ctx, br->apply_skipped_count);
            RedisModule_ReplyWithStringBuffer(ctx, (char *)"no of hits", 10);
            RedisModule_ReplyWithLongLong(ctx, br->apply_hit_count);
            RedisModule_ReplyWithStringBuffer(ctx, (char *)"no of misses", 12);
            RedisModule_ReplyWithLongLong(ctx, br->apply_miss_count);
        }
        raxStop(&ri);
        return REDISMODULE_OK;
    }

    static sds ApplyAll(sds key)
    {
        sds response = sdsempty();
        raxIterator ri;

        raxStart(&ri, BusinessRule::Registry);
        raxSeek(&ri, "^", NULL, 0);
        while (raxNext(&ri))
        {
            sds ruleName = sdsnewlen(ri.key, ri.key_len);
            BusinessRule *br = (BusinessRule *)ri.data;

            bool result = br->Apply(key);
            response = sdscatfmt(response, "%s -> %i\r", ruleName, result);
        }
        raxStop(&ri);
        return response;
    }

    BusinessRule()
    {
        this->rule = NULL;
        this->isvalid = false;
        this->set = NULL;
        this->setName = sdsempty();
        this->apply_count = 0;
        this->apply_skipped_count = 0;
        this->apply_hit_count = 0;
        this->apply_miss_count = 0;
    };

    BusinessRule(const char *setName, const char *query)
    :BusinessRule()
    {
        this->setName = sdsnew(strdup(setName));
        this->rule = this->RuleParser->Parse(query);
        // this->isvalid = (simulateQuery(this->rule, 1) == 1 ? true : false);
        // optimizeQuery(this->rule);
        this->set = FaBlok::Get(this->setName, KeysetDescriptor_TYPE_MONITORED_SET);
        this->set->is_temp = KeysetDescriptor_TYPE_MONITORED_SET;
        // /**/ serverlog(LL_NOTICE, "monitored this: x%x set:x%x %s d:x%x", (POINTER)this, (POINTER)this->set, this->set->setName, (POINTER)&this->set->keyset);

        // /**/ serverlog(LL_NOTICE, "parsed rule: %s %ld tokens", query, this->rule->rpn->len);
        // for (long unsigned int j = 0; j < this->rule->rpn->len; j++)
        // {
        //     Token *t = getTokenAt(this->rule->rpn, j);
        //     // /**/ serverlog(LL_NOTICE, "step: %ld %d %s", j, t->token_type, t->token);
        // }
        // /**/ serverlog(LL_NOTICE, "=====");
    };

    ~BusinessRule()
    {
        // /**/ serverlog(LL_NOTICE, "~BusinessRule rule: x%x set:x%x %s d:x%x", (POINTER)this, (POINTER)this->set, this->set->setName, (POINTER)&this->set->keyset);
        if (this->rule)
        {
            sdsfree(this->setName);
            this->setName = NULL;
            delete this->rule;
            this->rule = NULL;
            this->isvalid = false;
        }
    };

    bool Apply(const char *key)
    {
        this->set->Open();
        sds k = sdsnew(key);
        int stringmatch = sdscharcount(k, ':');
        sdsfree(k);
        if(stringmatch >= 2){
            this->apply_skipped_count++;
            return false;
        }
        sds rpn = this->rule->ToString();
        this->apply_count++;
        auto *executor = new SilNikParowy((char*)"192.168.1.182", 6379, NULL);
        rax *set = executor->Execute(this->rule, key);

        if (set != NULL && raxSize(set) != 0)
        {
            this->apply_hit_count++;
            raxIterator iter;
            raxStart(&iter, set);
            raxSeek(&iter, "^", NULL, 0);
            while (raxNext(&iter))
            {
                sds mkey = sdsnewlen(iter.key, iter.key_len);
                this->set->AddKey(mkey, iter.data);
                sdsfree(mkey);
            }
            raxStop(&iter);
            raxFree(set);
            this->set->Close();
            sdsfree(rpn);
            executor->Reset();
            delete executor;
            return true;
        }
        this->apply_miss_count++;
        this->set->RemoveKey(k);
        if (set != NULL)
            raxFree(set);
        this->set->Close();
        sdsfree(rpn);
        executor->Reset();
        delete executor;
        return false;
    }

    sds ParsedToString(){
        return this->rule->ToString();
    }
};

GremlinDialect *BusinessRule::RuleParser = new GremlinDialect();
rax *BusinessRule::Registry = raxNew();
#endif