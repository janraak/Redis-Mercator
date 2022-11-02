#ifndef __RULE_H__
#define __RULE_H__

#ifdef __cplusplus
extern "C"
{
#endif

#include "rax.h"

#ifndef LL_RAW

#define LL_DEBUG 0
#define LL_VERBOSE 1
#define LL_NOTICE 2
#define LL_WARNING 3
#define LL_RAW (1 << 10) /* Modifier to log without timestamp */

#endif

#ifdef __GNUC__
    void serverLog(int level, const char *fmt, ...)
        __attribute__((format(printf, 2, 3)));
#else
void serverLog(int level, const char *fmt, ...);
#endif
#include "../../src/adlist.h"
// #include "parser.h"
// #include "queryEngine.h"
#include "rxSuite.h"
#ifdef __cplusplus
}
#endif
#undef eTokenType_TYPE
#include "sjiboleth.h"

#include "sjiboleth-fablok.hpp"
#include "sjiboleth.hpp"

const char *GREMLIN_DIALECT = "gremlin";
const char *QUERY_DIALECT = "query";

#include "client-pool.hpp"
#include "graphstackentry.hpp"

typedef struct indexerThreadRX_LOADSCRIPT
{
    sds index_address;
    int index_port;
    int is_local;
    int database_no;
} IndexerInfo;

// template <typename T>
class BusinessRule
{
public:
    static rax *Registry;
    // Parser *rule;
    ParsedExpression *rule = NULL;
    bool isvalid;
    sds setName;
    long long apply_count;
    long long apply_skipped_count;
    long long apply_hit_count;
    long long apply_miss_count;
    IndexerInfo *index_info;

    static GremlinDialect *RuleParser;

    static BusinessRule *Retain(BusinessRule *br)
    {
        if (BusinessRule::Registry == NULL)
            BusinessRule::Registry = raxNew();
        BusinessRule *old;
        raxTryInsert(BusinessRule::Registry, (UCHAR *)br->setName, sdslen(br->setName), br, (void **)&old);
        return old;
    }

    static BusinessRule *Forget(BusinessRule *br)
    {
        BusinessRule *old = (BusinessRule *)raxFind(BusinessRule::Registry, (UCHAR *)br->setName, sdslen(br->setName));
        if (old != raxNotFound)
        {
            raxRemove(BusinessRule::Registry, (UCHAR *)br->setName, sdslen(br->setName), (void **)&old);
        }
        return old;
    }


    static void FreeBusinessRule(void *o)
    {
        auto *t = (BusinessRule *)o;
        delete t;
    }

    static void ForgetAll()
    {
        if(BusinessRule::Registry == NULL)
            return;
        // raxStart(&ri, BusinessRule::Registry);
        // raxSeek(&ri, "^", NULL, 0);
        // while (raxNext(&ri))
        // {
        //     sds ruleName = sdsnewlen(ri.key, ri.key_len);
        //     BusinessRule *br = (BusinessRule *)ri.data;

        //     rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(), "Applying rules: %s to: %s\n", ruleName, key));
        //     bool result = br->Apply(key);
        //     rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(), "Applied rules: %s to: %s -> %d\n", ruleName, key, result));
        //     response = sdscatfmt(response, "%s -> %i\r", ruleName, result);
        // }
        // raxStop(&ri);

        raxFreeWithCallback(BusinessRule::Registry, FreeBusinessRule);
        BusinessRule::Registry = NULL;
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
            RedisModule_ReplyWithArray(ctx, 12);
            BusinessRule *br = (BusinessRule *)ri.data;
            RedisModule_ReplyWithStringBuffer(ctx, (char *)"name", 4);
            RedisModule_ReplyWithStringBuffer(ctx, (char *)ruleName, strlen((char *)ruleName));
            sds rpn = br->ParsedToString();
            RedisModule_ReplyWithStringBuffer(ctx, (char *)"rule", 4);
            RedisModule_ReplyWithStringBuffer(ctx, (char *)rpn, strlen((char *)rpn));
            sdsfree(rpn);
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

            rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(), "Applying rules: %s to: %s\n", ruleName, key));
            bool result = br->Apply(key);
            rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(), "Applied rules: %s to: %s -> %d\n", ruleName, key, result));
            response = sdscatfmt(response, "%s -> %i\r", ruleName, result);
        }
        raxStop(&ri);
        return response;
    }

    BusinessRule()
    {
        this->rule = NULL;
        this->isvalid = false;
        this->setName = sdsempty();
        this->apply_count = 0;
        this->apply_skipped_count = 0;
        this->apply_hit_count = 0;
        this->apply_miss_count = 0;
        this->index_info = NULL;
    };

    BusinessRule(const char *setName, const char *query, IndexerInfo &index_info)
        : BusinessRule()
    {
        this->index_info = &index_info;
        this->setName = sdsnew(strdup(setName));
        rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(), "Rule (1): %s as: %s\n", this->setName, query));
        this->rule = this->RuleParser->Parse(query);
        rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(), "Rule (2): %s as: %s\n", this->setName, this->rule->ToString()));

        // this->isvalid = (simulateQuery(this->rule, 1) == 1 ? true : false);
        // optimizeQuery(this->rule);
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
        if (this->rule)
        {
            BusinessRule *old;
            raxRemove(BusinessRule::Registry, (UCHAR *)this->setName, sdslen(this->setName), (void **)&old);
            sdsfree(this->setName);
            this->setName = NULL;
            delete this->rule;
            this->rule = NULL;
            this->isvalid = false;
        }
    };

    static BusinessRule *Find(sds name)
    {
        auto *br = (BusinessRule *)raxFind(BusinessRule::Registry, (UCHAR *)name, sdslen(name));
        if (br != raxNotFound)
            return br;
        return NULL;
    };

    bool Apply(const char *key)
    {
        sds k = sdsnew(key);
        int stringmatch = sdscharcount(k, ':');
        void *obj = rxFindKey(0, k);
        sdsfree(k);
        if (stringmatch >= 2)
        {
            this->apply_skipped_count++;
            return false;
        }
        // sds rpn = this->rule->ToString();
        this->apply_count++;
        FaBlok::ClearCache();
        auto *executor = new SilNikParowy_Kontekst((char *)index_info->index_address, index_info->index_port, NULL);
        rax *set = executor->Execute(this->rule, key);
        FaBlok::ClearCache();
        rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(), "Rule: %s as: %s\n", this->setName, this->rule->ToString()));
        rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(), "Applying rule: %s to: %s =>%lld\n", this->setName, key, set ? raxSize(set) : -1));

        const char *objT = "?";
        if (obj != NULL)
            switch (rxGetObjectType(obj))
            {
            case rxOBJ_STRING:
                objT = "S";
                break;
            case rxOBJ_HASH:
                objT = "H";
                break;
            case rxOBJ_LIST:
                objT = "L";
                break;
            }
        if (set != NULL && raxSize(set) != 0)
        {
            this->apply_hit_count++;
            if (index_info->is_local != 0)
            {
                sds t = sdsnew(objT);
                sds f = sdsnew("rule");
                sds one = sdsnew("1");
                void *stashed_cmd = rxStashCommand(NULL, "RXADD", 5, key, t, f, this->setName, one);
                ExecuteRedisCommand(NULL, stashed_cmd);
                FreeStash(stashed_cmd);
                sdsfree(t);
                sdsfree(f);
                sdsfree(one);
            }
            else
            {
                auto *redis_node = RedisClientPool<redisContext>::Acquire(index_info->index_address, index_info->index_port);
                if (redis_node != NULL)
                {
                    sds cmd = sdscatfmt(sdsempty(), "RXADD %s %s RULE %s 1", key, objT, this->setName);
                    redisReply *rcc = (redisReply *)redisCommand(redis_node, cmd);
                    rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(), " %s: %s:%d --> %s\n", cmd, index_info->index_address, index_info->index_port, rcc->str));
                    freeReplyObject(rcc);
                    sdsfree(cmd);
                    RedisClientPool<redisContext>::Release(redis_node);
                }
            }
            return true;
        }
        this->apply_miss_count++;
        if (index_info->is_local != 0)
        {
            sds t = sdsnew(objT);
            sds f = sdsnew("rule");
            sds one = sdsnew("1");
            void *stashed_cmd = rxStashCommand(NULL, "RXDEL", 4, key, t, f, this->setName);
            ExecuteRedisCommand(NULL, stashed_cmd);
            FreeStash(stashed_cmd);
            sdsfree(t);
            sdsfree(f);
            sdsfree(one);
        }
        else
        {
            auto *redis_node = RedisClientPool<redisContext>::Acquire(index_info->index_address, index_info->index_port);
            if (redis_node != NULL)
            {
                sds cmd = sdscatfmt(sdsempty(), "RXDEL %s %s RULE %s", key, objT, this->setName);
                redisReply *rcc = (redisReply *)redisCommand(redis_node, cmd);
                rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(), " %s: %s:%d --> %s\n", cmd, index_info->index_address, index_info->index_port, rcc->str));
                freeReplyObject(rcc);
                sdsfree(cmd);
                RedisClientPool<redisContext>::Release(redis_node);
            }
        }
        return false;
    }

    sds ParsedToString()
    {
        return this->rule->ToString();
    }
};

GremlinDialect *BusinessRule::RuleParser = new GremlinDialect();
rax *BusinessRule::Registry = raxNew();
#endif