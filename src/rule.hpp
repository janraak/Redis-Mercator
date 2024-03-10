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
#include "indexIntercepts.h"
#include "rxSuite.h"

#ifdef __cplusplus
}
#endif
#undef eTokenType_TYPE
#include "sjiboleth.h"

#include "sjiboleth-fablok.hpp"
#include "sjiboleth.hpp"
#include "tls.hpp"

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
    bool IsFinal = false;
    // Parser *rule;
    ParsedExpression *if_this = NULL;
    ParsedExpression *then_that = NULL;
    ParsedExpression *else_that = NULL;

    char *then_apply = NULL;
    BusinessRule *then_rule = NULL;
    char *else_apply = NULL;
    BusinessRule *else_rule = NULL;

    bool isvalid;
    bool isDeclaration;
    rxString setName;
    long long apply_count;
    long long apply_skipped_count;
    long long apply_if_count;
    long long apply_hit_count;
    long long apply_else_count;
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
            rxString ruleName = rxStringNewLen((const char *)ri.key, ri.key_len);
            rxStringFree(ruleName);
            BusinessRule *br = (BusinessRule *)ri.data;
            while (BusinessRule::RegistryLock)
            {
                sched_yield();
            }
            BusinessRule::Forget(br);
            raxSeek(&ri, "^", NULL, 0);
        }
        raxStop(&ri);
        BusinessRule::RuleRegistry = NULL;
    }

    static int ResetCounters(RedisModuleCtx *ctx)
    {
        if (BusinessRule::RuleRegistry != NULL)
        {
            raxIterator ri;
            BusinessRule::RegistryLock = true;

            raxStart(&ri, BusinessRule::RuleRegistry);
            raxSeek(&ri, "^", NULL, 0);
            while (raxNext(&ri))
            {
                BusinessRule *br = (BusinessRule *)ri.data;
                br->apply_count = 0;
                br->apply_skipped_count = 0;
                br->apply_if_count = 0;
                br->apply_hit_count = 0;
                br->apply_else_count = 0;
                br->apply_miss_count = 0;
            }
            raxStop(&ri);
            BusinessRule::RegistryLock = false;
        }
        RedisModule_ReplyWithSimpleString(ctx, "Ok");
        return REDISMODULE_OK;
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
                rxString ruleName = rxStringNewLen((const char *)ri.key, ri.key_len);
                BusinessRule *br = (BusinessRule *)ri.data;
                RedisModule_ReplyWithArray(ctx, 14 + (br->if_this ? 2 : 0) + (br->else_that ? 2 : 0));
                RedisModule_ReplyWithStringBuffer(ctx, (char *)"name", 4);
                RedisModule_ReplyWithStringBuffer(ctx, (char *)ruleName, strlen((char *)ruleName));
                if (br->if_this)
                {
                    rxString rpn = br->ParsedIfToString();
                    RedisModule_ReplyWithSimpleString(ctx, (char *)"if_this");
                    RedisModule_ReplyWithStringBuffer(ctx, (char *)rpn, strlen((char *)rpn));
                    rxStringFree(rpn);
                }
                rxString rpn = br->ParsedToString();
                RedisModule_ReplyWithSimpleString(ctx, (char *)"then_that");
                RedisModule_ReplyWithStringBuffer(ctx, (char *)rpn, strlen((char *)rpn));
                rxStringFree(rpn);
                if (br->else_that)
                {
                    rxString rpn = br->ParsedElseToString();
                    RedisModule_ReplyWithSimpleString(ctx, (char *)"else_that");
                    RedisModule_ReplyWithStringBuffer(ctx, (char *)rpn, strlen((char *)rpn));
                    rxStringFree(rpn);
                }
                RedisModule_ReplyWithStringBuffer(ctx, (char *)"no of applies", 13);
                RedisModule_ReplyWithLongLong(ctx, br->apply_count);
                RedisModule_ReplyWithStringBuffer(ctx, (char *)"no of skips", 11);
                RedisModule_ReplyWithLongLong(ctx, br->apply_skipped_count);
                RedisModule_ReplyWithStringBuffer(ctx, (char *)"no of ifs", 10);
                RedisModule_ReplyWithLongLong(ctx, br->apply_if_count);
                RedisModule_ReplyWithStringBuffer(ctx, (char *)"no of hits", 10);
                RedisModule_ReplyWithLongLong(ctx, br->apply_hit_count);
                RedisModule_ReplyWithStringBuffer(ctx, (char *)"no of else hits", 10);
                RedisModule_ReplyWithLongLong(ctx, br->apply_else_count);
                RedisModule_ReplyWithStringBuffer(ctx, (char *)"no of misses", 12);
                RedisModule_ReplyWithLongLong(ctx, br->apply_miss_count);
            }
            raxStop(&ri);
            BusinessRule::RegistryLock = false;
        }
        else
            RedisModule_ReplyWithSimpleString(ctx, "No rules defined");

        return REDISMODULE_OK;
    }

    static rxString ApplyAll(rxString key, SilNikParowy_Kontekst *stack)
    {
        if (rxStringCharCount(key, ':') >= 2)
        {
            return NULL;
        }

        if (BusinessRule::RuleRegistry == NULL)
            return rxStringEmpty();
        BusinessRule::RegistryLock = true;
        rxString response = rxStringEmpty();
        raxIterator ri;

        raxStart(&ri, BusinessRule::RuleRegistry);
        raxSeek(&ri, "^", NULL, 0);
        while (raxNext(&ri))
        {
            rxString ruleName = rxStringNewLen((const char *)ri.key, ri.key_len);

            BusinessRule *br = (BusinessRule *)ri.data;

            rxServerLogRaw(rxLL_DEBUG, rxStringFormat("Applying rules: %s to: %s\n", ruleName, key));
            bool result = br->Apply(key, stack);
            rxServerLogRaw(rxLL_DEBUG, rxStringFormat("Applied rules: %s to: %s -> %d\n", ruleName, key, result));
            response = rxStringFormat("%s%s -> %d\r", response, ruleName, result);
            if(result && br->IsFinal)
                break;
        }
        raxStop(&ri);
        BusinessRule::RegistryLock = false;
        return response;
    }

    static rxString ApplyAll(FaBlok *triggers, SilNikParowy_Kontekst *stack)
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
            BusinessRule *br = (BusinessRule *)ri.data;
            br->Apply(triggers, stack);
        }
        raxStop(&ri);
        BusinessRule::RegistryLock = false;
        return response;
    }

    static void Touched(rxString key)
    {
        auto index_info = (indexerThread *)GetIndex_info();

        if (index_info == NULL || index_info->index_update_respone_queue == NULL)
            return;
        rxStashCommand(index_info->index_rxRuleApply_request_queue, "RULE.APPLY", 1, key);
    }

    static int QueuedTouchesCount()
    {
        auto index_info = (indexerThread *)GetIndex_info();

        if (index_info == NULL || index_info->index_update_respone_queue == NULL)
            return 0;
        return lengthSimpleQueue(index_info->index_rxRuleApply_request_queue);
    }

    BusinessRule()
    {
        this->if_this = NULL;
        this->then_that = NULL;
        this->then_apply = NULL;
        this->then_rule = NULL;
        this->else_that = NULL;
        this->else_apply = NULL;
        this->else_rule = NULL;
        this->IsFinal = false;
        this->isvalid = false;
        this->isDeclaration = false;
        this->setName = rxStringEmpty();
        this->apply_count = 0;
        this->apply_skipped_count = 0;
        this->apply_if_count = 0;
        this->apply_hit_count = 0;
        this->apply_else_count = 0;
        this->apply_miss_count = 0;
    };

    BusinessRule(const char *setName, bool isFinal, const char *if_this, const char *then_that, const char *then_apply, const char *else_apply, const char *else_that)
        : BusinessRule()
    {
        this->setName = rxStringNew(setName);
        this->IsFinal = isFinal;
        if (if_this && strlen(if_this) > 0)
        {
            rxServerLogRaw(rxLL_DEBUG, rxStringFormat("Rule (1) IF: %s as: %s\n", this->setName, if_this));
            this->if_this = this->RuleParser->Parse(if_this);
            rxServerLogRaw(rxLL_DEBUG, rxStringFormat("Rule (2) IF: %s as: %s\n", this->setName, this->if_this->ToString()));
        }
        if (then_apply)
        {
            rxServerLogRaw(rxLL_DEBUG, rxStringFormat("Rule (1) THEN: %s as: apply%s\n", this->setName, then_apply));
            this->then_apply = (char *)then_apply;
        }
        else
        {
            rxServerLogRaw(rxLL_DEBUG, rxStringFormat("Rule (1): %s as: %s\n", this->setName, then_that));
            this->then_that = this->RuleParser->Parse(then_that);
            rxServerLogRaw(rxLL_DEBUG, rxStringFormat("Rule (2): %s as: %s\n", this->setName, this->then_that->ToString()));
        }
        if (else_apply)
        {
            rxServerLogRaw(rxLL_DEBUG, rxStringFormat("Rule (1) ELSE: %s as: apply %s\n", this->setName, else_apply));
            this->else_apply = (char *)else_apply;
        }
        else if (else_that && strlen(else_that) > 0)
        {
            rxServerLogRaw(rxLL_DEBUG, rxStringFormat("Rule (1) ELSE: %s as: %s\n", this->setName, else_that));
            this->else_that = this->RuleParser->Parse(else_that);
            rxServerLogRaw(rxLL_DEBUG, rxStringFormat("Rule (2) ELSE: %s as: %s\n", this->setName, this->else_that->ToString()));
        }
    };

    ~BusinessRule()
    {
        if (this->then_that)
        {
            BusinessRule *old;
            while (BusinessRule::RegistryLock)
            {
                sched_yield();
            }
            raxRemove(BusinessRule::RuleRegistry, (UCHAR *)this->setName, strlen(this->setName), (void **)&old);
            rxStringFree(this->setName);
            this->setName = NULL;
            if (this->if_this)
            {
                delete this->if_this;
                this->if_this = NULL;
            }
            delete this->then_that;
            this->then_that = NULL;
            if (this->else_that)
            {
                delete this->else_that;
                this->else_that = NULL;
            }
            this->isvalid = false;
        }
    };

    static BusinessRule *Find(rxString name)
    {
        if (BusinessRule::RuleRegistry == NULL)
            return NULL;
        while (BusinessRule::RegistryLock)
        {
            sched_yield();
        }
        auto *br = (BusinessRule *)raxFind(BusinessRule::RuleRegistry, (UCHAR *)name, strlen(name));
        if (br != raxNotFound)
            return br;
        return NULL;
    };

    bool Apply(const char *key, SilNikParowy_Kontekst *kontekst)
    {
        void *obj = rxFindKey(0, key);

        this->apply_count++;
        FaBlok::ClearCache();
        rax *set = NULL;
        if (this->if_this)
        {
            kontekst->Reset();
            set = kontekst->Execute(this->if_this, key);
            if (set && raxSize(set) > 0)
                set = kontekst->Execute(this->then_that, key);
            else if (this->else_that)
                set = kontekst->Execute(this->else_that, key);
        }
        else
            set = kontekst->Execute(this->then_that, key);
        kontekst->ClearMemoizations();
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

        redisNodeInfo *index_config = rxIndexNode();
        if (set != NULL && raxSize(set) != 0)
        {
            this->apply_hit_count++;
            if (index_config->is_local != 0)
            {
                rxString t = rxStringNew(objT);
                rxString f = rxStringNew("RULE");
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
            rxString f = rxStringNew("RULE");
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

    bool Apply(FaBlok *triggers, SilNikParowy_Kontekst *kontekst)
    {
        bool result = false;
        this->apply_count += triggers->size;
        FaBlok::ClearCache();
        rax *if_set = NULL;
        rax *then_set = NULL;
        rax *else_set = NULL;
        if (this->if_this)
        {
            if_set = kontekst->ExecuteWithSet(this->if_this, triggers);
            if(if_set && raxSize(if_set) > 0){
                FaBlok *matches = FaBlok::New("MATCHED", KeysetDescriptor_TYPE_GREMLIN_VERTEX_SET);
                matches->Copy(if_set);
                then_set = kontekst->ExecuteWithSet(this->then_that, matches);
                result = then_set && raxSize(then_set) > 0;
                FaBlok::Delete(matches);
                FaBlok *not_set = FaBlok::New("NOT branch", KeysetDescriptor_TYPE_GREMLIN_VERTEX_SET);
                not_set->CopyNotIn(triggers->keyset, then_set);
                FaBlok::Delete(not_set);
                if (this->else_that)
                {
                    else_set = kontekst->ExecuteWithSet(this->else_that, not_set);
                    result = raxSize(else_set) > 0;
                }
            }
        }
        else
            then_set = kontekst->ExecuteWithSet(this->then_that, triggers);
        kontekst->ClearMemoizations();
        FaBlok::ClearCache();

        // const char *objT = "?";
        // if (obj != NULL)
        //     switch (rxGetObjectType(obj))
        //     {
        //     case rxOBJ_STRING:
        //         objT = "S";
        //         break;
        //     case rxOBJ_HASH:
        //         objT = "H";
        //         break;
        //     case rxOBJ_LIST:
        //         objT = "L";
        //         break;
        //     }

        // redisNodeInfo *index_config = rxIndexNode();
        // if (set != NULL && raxSize(set) != 0)
        // {
        //     this->apply_hit_count++;
        //     if (index_config->is_local != 0)
        //     {
        //         rxString t = rxStringNew(objT);
        //         rxString f = rxStringNew("RULE");
        //         rxString one = rxStringNew("1");
        //         void *stashed_cmd = rxStashCommand(NULL, "RXADD", 5, key, t, f, this->setName, one);
        //         ExecuteRedisCommand(NULL, stashed_cmd, index_config->host_reference);
        //         FreeStash(stashed_cmd);
        //         rxStringFree(t);
        //         rxStringFree(f);
        //         rxStringFree(one);
        //     }
        //     else
        //     {
        //         auto *redis_node = RedisClientPool<redisContext>::Acquire(index_config->host_reference, "_CLIENT", "RULE::Apply");
        //         if (redis_node != NULL)
        //         {
        //             rxString cmd = rxStringFormat("RXADD %s %s RULE %s 1", key, objT, this->setName);
        //             redisReply *rcc = (redisReply *)redisCommand(redis_node, cmd);
        //             freeReplyObject(rcc);
        //             rxStringFree(cmd);
        //             RedisClientPool<redisContext>::Release(redis_node, "RULE::Apply");
        //         }
        //     }
        //     return true;
        // }
        // this->apply_miss_count++;
        // if (index_config->is_local != 0)
        // {
        //     rxString t = rxStringNew(objT);
        //     rxString f = rxStringNew("RULE");
        //     rxString one = rxStringNew("1");
        //     void *stashed_cmd = rxStashCommand(NULL, "RXDEL", 4, key, t, f, this->setName);
        //     ExecuteRedisCommand(NULL, stashed_cmd, index_config->host_reference);
        //     FreeStash(stashed_cmd);
        //     rxStringFree(t);
        //     rxStringFree(f);
        //     rxStringFree(one);
        // }
        // else
        // {
        //     auto *redis_node = RedisClientPool<redisContext>::Acquire(index_config->host_reference, "_CLIENT", "RULE::Apply");
        //     if (redis_node != NULL)
        //     {
        //         rxString cmd = rxStringFormat("RXDEL %s %s RULE %s", key, objT, this->setName);
        //         redisReply *rcc = (redisReply *)redisCommand(redis_node, cmd);
        //         freeReplyObject(rcc);
        //         rxStringFree(cmd);
        //         RedisClientPool<redisContext>::Release(redis_node, "RULE::Apply");
        //     }
        // }
        // return false;
        if(if_set)
            raxFree(if_set);
        if(then_set)
            raxFree(then_set);
        if(else_set)
            raxFree(else_set);
        return result;
    }

    rxString ParsedIfToString()
    {
        return this->if_this->ToString();
    }

    rxString ParsedToString()
    {
        return this->then_that->ToString();
    }

    rxString ParsedElseToString()
    {
        return this->else_that->ToString();
    }
};

GremlinDialect *BusinessRule::RuleParser = (GremlinDialect *)Sjiboleth::Get("GremlinDialect");
rax *BusinessRule::RuleRegistry = raxNew();
bool BusinessRule::RegistryLock = false;
#endif
