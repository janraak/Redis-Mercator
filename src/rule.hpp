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

// template <typename T>
class BusinessRule
{
public:
    static rax *Registry;
    static bool RegistryLock;
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
        while (BusinessRule::RegistryLock)
        {
            sched_yield();
        }
        BusinessRule *old;
        raxTryInsert(BusinessRule::Registry, (UCHAR *)br->setName, sdslen(br->setName), br, (void **)&old);
        return old;
    }

    static BusinessRule *Forget(BusinessRule *br)
    {
        if (BusinessRule::Registry == NULL)
            return NULL;
        while (BusinessRule::RegistryLock)
        {
            sched_yield();
        }
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
        rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(),"# ForgetAll # 000 #"));
        if (BusinessRule::Registry == NULL)
            return;
        rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(),"# ForgetAll # 010 #"));
        raxIterator ri;
        raxStart(&ri, BusinessRule::Registry);
        raxSeek(&ri, "^", NULL, 0);
        while (raxNext(&ri))
        {
            sds ruleName = sdsnewlen(ri.key, ri.key_len);
            rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(),"# ForgetAll # 100 # rule: %s", ruleName));
            sdsfree(ruleName);
            BusinessRule *br = (BusinessRule *)ri.data;
            while(BusinessRule::RegistryLock){
                sched_yield();
            }
            BusinessRule::Forget(br);
            raxSeek(&ri, "^", NULL, 0);
        }
        raxStop(&ri);
        BusinessRule::Registry = NULL;
        rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(),"# ForgetAll # 800 # BusinessRule::Registry isnull: %x", (POINTER)BusinessRule::Registry));
    }

    static int WriteList(RedisModuleCtx *ctx)
    {
        RedisModule_ReplyWithArray(ctx, raxSize(BusinessRule::Registry));
        raxIterator ri;

        if (BusinessRule::Registry != NULL)
        {
            BusinessRule::RegistryLock = true;

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
            BusinessRule::RegistryLock = false;
        }else
            RedisModule_ReplyWithSimpleString(ctx, "No rules defined");

        return REDISMODULE_OK;
    }

    static sds ApplyAll(sds key)
    {

        if (BusinessRule::Registry == NULL)
            return sdsempty();
        BusinessRule::RegistryLock = true;
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
        BusinessRule::RegistryLock = false;
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
        this->setName = sdsnew(setName);
        rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(), "Rule (1): %s as: %s\n", this->setName, query));
        this->rule = this->RuleParser->Parse(query);
        rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(), "Rule (2): %s as: %s\n", this->setName, this->rule->ToString()));
    };

    ~BusinessRule()
    {
        if (this->rule)
        {
            BusinessRule *old;
            while(BusinessRule::RegistryLock){
                sched_yield();
            }
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
        if(BusinessRule::Registry == NULL)
            return NULL;
        while(BusinessRule::RegistryLock){
            sched_yield();
        }
        auto *br = (BusinessRule *)raxFind(BusinessRule::Registry, (UCHAR *)name, sdslen(name));
        if (br != raxNotFound)
            return br;
        return NULL;
    };

    bool Apply(const char *key)
    {
        redisNodeInfo *index_config = rxIndexNode();
        redisNodeInfo *data_config = rxDataNode();
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
        if(data_config->executor == NULL)
            data_config->executor = (void *)new SilNikParowy_Kontekst(data_config, NULL);
        rax *set = ((SilNikParowy_Kontekst *)data_config->executor)->Execute(this->rule, key);
        FaBlok::ClearCache();

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
            if (index_config->is_local != 0)
            {
                sds t = sdsnew(objT);
                sds f = sdsnew("rule");
                sds one = sdsnew("1");
                void *stashed_cmd = rxStashCommand(NULL, "RXADD", 5, key, t, f, this->setName, one);
                ExecuteRedisCommand(NULL, stashed_cmd, index_config->host_reference);
                FreeStash(stashed_cmd);
                sdsfree(t);
                sdsfree(f);
                sdsfree(one);
            }
            else
            {
                auto *redis_node = RedisClientPool<redisContext>::Acquire(index_config->host_reference);
                if (redis_node != NULL)
                {
                    sds cmd = sdscatfmt(sdsempty(), "RXADD %s %s RULE %s 1", key, objT, this->setName);
                    redisReply *rcc = (redisReply *)redisCommand(redis_node, cmd);
                    rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(), " %s: %s --> %s\n", cmd, index_config->host_reference, rcc->str));
                    freeReplyObject(rcc);
                    sdsfree(cmd);
                    RedisClientPool<redisContext>::Release(redis_node);
                }
            }
            return true;
        }
        this->apply_miss_count++;
        if (index_config->is_local != 0)
        {
            sds t = sdsnew(objT);
            sds f = sdsnew("rule");
            sds one = sdsnew("1");
            void *stashed_cmd = rxStashCommand(NULL, "RXDEL", 4, key, t, f, this->setName);
            ExecuteRedisCommand(NULL, stashed_cmd, index_config->host_reference);
            FreeStash(stashed_cmd);
            sdsfree(t);
            sdsfree(f);
            sdsfree(one);
        }
        else
        {
            auto *redis_node = RedisClientPool<redisContext>::Acquire(index_config->host_reference);
            if (redis_node != NULL)
            {
                sds cmd = sdscatfmt(sdsempty(), "RXDEL %s %s RULE %s", key, objT, this->setName);
                redisReply *rcc = (redisReply *)redisCommand(redis_node, cmd);
                rxServerLogRaw(rxLL_WARNING, sdscatprintf(sdsempty(), " %s: %s --> %s\n", cmd, index_config->host_reference, rcc->str));
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
bool BusinessRule::RegistryLock = false;
#endif