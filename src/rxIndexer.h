/* rxIndexer -- An example of modules dictionary API
 *
 * This module implements a full text index on string and hash keyes.
 *
 * -----------------------------------------------------------------------------
 *
 * Copyright (c) 2021, Jan Raak <jan dotraak dot nl at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __RXINDEXER_H__
#define __RXINDEXER_H__

#define REDISMODULE_EXPERIMENTAL_API
#include "redismodule.h"
#include "/usr/include/arm-linux-gnueabihf/bits/types/siginfo_t.h"
#include <sched.h>
#include <signal.h>

#include "adlist.h"
#include "../../src/dict.h"
#include "server.h"
#include <ctype.h>
#include <locale.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <simpleQueue.h>
#include "rxSuite.h"

void tokenizeValue(sds objectKey, sds keyType, sds fieldName, sds fieldValue);

int stringCaseCompare(void *privdata, const void *key1, const void *key2)
{
    rxUNUSED(privdata);
    int rc = strcmp((const char *)key1, (const char *)key2);
    serverLog(LL_NOTICE, "----------------> |%s|::|%s| == %i", (const sds)key1, (const sds)key2, rc);
    return rc;
}

// const void *stringDuplicate(void *privdata, const void *key)
// {
//     rxUNUSED(privdata);
//     const void *rc = strdup((const char *)key);
//     serverLog(LL_NOTICE, "----------------> |%s| => %s", (const sds)key, rc);
//     return rc;
// }

uint64_t stringHash(const void *key)
{
    uint64_t rc = dictGenHashFunction(key, strlen((char *)key));
    serverLog(LL_NOTICE, "----------------> |%s| => %lli", (const sds)key, rc);
    return rc;
}

void keyDestructor(void *privdata, void *val)
{
    rxUNUSED(privdata);
    rxUNUSED(val);
    // zfree(val);
}

void valueDestructor(void *privdata, void *val)
{
    rxUNUSED(privdata);
    rxUNUSED(val);
    // if (val)
    //     zfree(val);
}

/* Command table. sds string -> command struct pointer. */
dictType valueDictType = {
    stringHash,        /* hash function */
    NULL,   /* key dup */
    NULL,              /* val dup */
    stringCaseCompare, /* key compare */
    dictSdsDestructor,     /* key destructor */
    valueDestructor    /* val destructor */
};


// /* Db->dict, keys are sds strings, vals are Redis objects. */
extern dictType tokenDictType;// = {
//     dictSdsHash,                /* hash function */
//     NULL,                       /* key dup */
//     NULL,                       /* val dup */
//     dictSdsKeyCompare,          /* key compare */
//     dictSdsDestructor,          /* key destructor */
//     NULL   /* val destructor */
// };
/* Threads. */
#endif