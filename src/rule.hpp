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
    void rxServerLog(int level, const char *fmt, ...)
        __attribute__((format(printf, 2, 3)));
#else
void rxServerLog(int level, const char *fmt, ...);
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
    static rax *RuleRegistry;
    static bool RegistryLock;
    // Parser *rule;
    ParsedExpression *rule = NULL;
    bool isvalid;
    rxString setName;
    long long apply_count;
    long long apply_skipped_count;
    long long apply_hit_count;
    long long apply_miss_count;

    static GremlinDialect *RuleParser;

    static BusinessRule *Retain(BusinessRule *br)
    {
        if (BusinessRule::RuleRegistry == NULL)
            BusinessRule::RuleRegistry = raxNew();
        while (BusinessRule::RegistryLock)
        {
            sched_yield();
        }
        BusinessRule *old;
        raxTryInsert(BusinessRule::RuleRegistry, (UCHAR *)br->setName, strlen(br->setName), br, (void **)&old);
        return old;
    }

    static BusinessRule *Forget(BusinessRule *br)
    {
        if (BusinessRule::RuleRegistry == NULL)
            return NULL;
        while (BusinessRule::RegistryLock)
        {
            sched_yield();
        }
        BusinessRule *old = (BusinessRule *)raxFind(BusinessRule::RuleRegistry, (UCHAR *)br->setName, strlen(br->setName));
        if (old != raxNotFound)
        {
            raxRemove(BusinessRule::RuleRegistry, (UCHAR *)br->setName, strlen(br->setName), (void **)&old);
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
        if (BusinessRule::RuleRegistry == NULL)
            return;
        raxIterator ri;
        raxStart(&ri, BusinessRule::RuleRegistry);
        raxSeek(&ri, "^", NULL, 0);
        while (raxNext(&ri))
        {
            rxString ruleName = rxStringNewLen((const char*)ri.key, ri.key_len);
            rxStringFree(ruleName);
            BusinessRule *br = (BusinessRule *)ri.data;
            while(BusinessRule::RegistryLock){
                sched_yield();
            }
            BusinessRule::Forget(br);
            raxSeek(&ri, "^", NULL, 0);
        }
        raxStop(&ri);
        BusinessRule::RuleRegistry = NULL;
    }

    static int WriteList(RedisModuleCtx *ctx)
    {
        if (BusinessRule::RuleRegistry != NULL)
        {
            RedisModule_ReplyWithArray(ctx, raxSize(BusinessRule::RuleRegistry));
            raxIterator ri;
            BusinessRule::RegistryLock = true;

            raxStart(&ri, BusinessRule::RuleRegistry);
            raxSeek(&ri, "^", NULL, 0);
            while (raxNext(&ri))
            {
                rxString ruleName = rxStringNewLen((const char*)ri.key, ri.key_len);
                RedisModule_ReplyWithArray(ctx, 12);
                BusinessRule *br = (BusinessRule *)ri.data;
                RedisModule_ReplyWithStringBuffer(ctx, (char *)"name", 4);
                RedisModule_ReplyWithStringBuffer(ctx, (char *)ruleName, strlen((char *)ruleName));
                rxString rpn = br->ParsedToString();
                RedisModule_ReplyWithStringBuffer(ctx, (char *)"rule", 4);
                RedisModule_ReplyWithStringBuffer(ctx, (char *)rpn, strlen((char *)rpn));
                rxStringFree(rpn);
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

    static rxString ApplyAll(rxString key)
    {

        if (BusinessRule::RuleRegistry == NULL)
            return rxStringEmpty();
        BusinessRule::RegistryLock = true;
        rxString response = rxStringEmpty();
        raxIterator ri;

        raxStart(&ri, BusinessRule::RuleRegistry);
        raxSeek(&ri, "^", NULL, 0);
        while (raxNext(&ri))
        {
            rxString ruleName = rxStringNewLen((const char*)ri.key, ri.key_len);
            BusinessRule *br = (BusinessRule *)ri.data;

            rxServerLogRaw(rxLL_WARNING, rxStringFormat( "Applying rules: %s to: %s\n", ruleName, key));
            bool result = br->Apply(key);
            rxServerLogRaw(rxLL_WARNING, rxStringFormat( "Applied rules: %s to: %s -> %d\n", ruleName, key, result));
            response = rxStringFormat("%s%s -> %d\r", response, ruleName, result);
        }
        raxStop(&ri);
        BusinessRule::RegistryLock = false;
        return response;
    }

    BusinessRule()
    {
        this->rule = NULL;
        this->isvalid = false;
        this->setName = rxStringEmpty();
        this->apply_count = 0;
        this->apply_skipped_count = 0;
        this->apply_hit_count = 0;
        this->apply_miss_count = 0;
    };

    BusinessRule(const char *setName, const char *query)
        : BusinessRule()
    {
        this->setName = rxStringNew(setName);
        rxServerLogRaw(rxLL_WARNING, rxStringFormat( "Rule (1): %s as: %s\n", this->setName, query));
        this->rule = this->RuleParser->Parse(query);
        rxServerLogRaw(rxLL_WARNING, rxStringFormat( "Rule (2): %s as: %s\n", this->setName, this->rule->ToString()));
    };

    ~BusinessRule()
    {
        if (this->rule)
        {
            BusinessRule *old;
            while(BusinessRule::RegistryLock){
                sched_yield();
            }
            raxRemove(BusinessRule::RuleRegistry, (UCHAR *)this->setName, strlen(this->setName), (void **)&old);
            rxStringFree(this->setName);
            this->setName = NULL;
            delete this->rule;
            this->rule = NULL;
            this->isvalid = false;
        }
    };

    static BusinessRule *Find(rxString name)
    {
        if(BusinessRule::RuleRegistry == NULL)
            return NULL;
        while(BusinessRule::RegistryLock){
            sched_yield();
        }
        auto *br = (BusinessRule *)raxFind(BusinessRule::RuleRegistry, (UCHAR *)name, strlen(name));
        if (br != raxNotFound)
            return br;
        return NULL;
    };

    bool Apply(const char *key)
    {
        redisNodeInfo *index_config = rxIndexNode();
        redisNodeInfo *data_config = rxDataNode();
        rxString k = rxStringNew(key);
        int stringmatch = rxStringCharCount(k, ':');
        void *obj = rxFindKey(0, k);
        rxStringFree(k);
        if (stringmatch >= 2)
        {
            this->apply_skipped_count++;
            return false;
        }
        // rxString rpn = this->rule->ToString();
        this->apply_count++;
        FaBlok::ClearCache();
        if(data_config->executor == NULL)
            data_config->executor = (void *)new SilNikParowy_Kontekst(data_config, NULL);
        rax *set = ((SilNikParowy_Kontekst *)data_config->executor)->Execute(this->rule, key);
        ((SilNikParowy_Kontekst *)data_config->executor)->ClearMemoizations();
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
                rxString t = rxStringNew(objT);
                rxString f = rxStringNew("rule");
                rxString one = rxStringNew("1");
                void *stashed_cmd = rxStashCommand(NULL, "RXADD", 5, key, t, f, this->setName, one);
                ExecuteRedisCommand(NULL, stashed_cmd, index_config->host_reference);
                FreeStash(stashed_cmd);
                rxStringFree(t);
                rxStringFree(f);
                rxStringFree(one);
            }
            else
            {
                auto *redis_node = RedisClientPool<redisContext>::Acquire(index_config->host_reference, "_CLIENT", "RULE::Apply");
                if (redis_node != NULL)
                {
                    rxString cmd = rxStringFormat("RXADD %s %s RULE %s 1", key, objT, this->setName);
                    redisReply *rcc = (redisReply *)redisCommand(redis_node, cmd);
                    freeReplyObject(rcc);
                    rxStringFree(cmd);
                    RedisClientPool<redisContext>::Release(redis_node, "RULE::Apply");
                }
            }
            return true;
        }
        this->apply_miss_count++;
        if (index_config->is_local != 0)
        {
            rxString t = rxStringNew(objT);
            rxString f = rxStringNew("rule");
            rxString one = rxStringNew("1");
            void *stashed_cmd = rxStashCommand(NULL, "RXDEL", 4, key, t, f, this->setName);
            ExecuteRedisCommand(NULL, stashed_cmd, index_config->host_reference);
            FreeStash(stashed_cmd);
            rxStringFree(t);
            rxStringFree(f);
            rxStringFree(one);
        }
        else
        {
            auto *redis_node = RedisClientPool<redisContext>::Acquire(index_config->host_reference, "_CLIENT", "RULE::Apply");
            if (redis_node != NULL)
            {
                rxString cmd = rxStringFormat("RXDEL %s %s RULE %s", key, objT, this->setName);
                redisReply *rcc = (redisReply *)redisCommand(redis_node, cmd);
                freeReplyObject(rcc);
                rxStringFree(cmd);
                RedisClientPool<redisContext>::Release(redis_node, "RULE::Apply");
            }
        }
        return false;
    }

    rxString ParsedToString()
    {
        return this->rule->ToString();
    }
};

GremlinDialect *BusinessRule::RuleParser = new GremlinDialect();
rax *BusinessRule::RuleRegistry = raxNew();
bool BusinessRule::RegistryLock = false;
#endif
