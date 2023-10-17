#include <stdlib.h>
#include <string.h>

#include "../../src/sds.h"
#include "ctype.h"
#include "stddef.h"
#include "stdint.h"
#include "stdlib.h"
#include "../../src/util.h"
#include <stdarg.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include "../../src/zmalloc.h"
#include "../../src/rax.h"
#define REDISMODULE_EXPERIMENTAL_API
#include "../../src/redismodule.h"

extern void serverLogHexDump(int level, char *descr, void *value, size_t len);

#include "sdsWrapper.h"

rxString rxStringNew(const char *s)
{
    return sdsnew(s);
}
rxString rxStringNewLen(const char *s, int l)
{
    return sdsnewlen(s, l);
}

rxString rxStringEmpty()
{
    return sdsempty();
}

rxString rxStringDup(rxString s)
{
    return sdsdup((sds)s);
}

void rxStringFree(rxString s)
{
    if(s)
        sdsfree((sds)s);
}

rxString *rxStringSplitLen(const char *s, ssize_t len, const char *sep, int seplen, int *count)
{
    return (const char **)sdssplitlen(s, len, sep, seplen, count);
}

rxString *rxStringFreeSplitRes(rxString *tokens, int count)
{
    if(tokens)
        sdsfreesplitres((sds *)tokens, count);
    return NULL;
}

rxString rxStringMapChars(rxString s, const char *from, const char *to, size_t setlen)
{
    return sdsmapchars((sds)s, from, to, setlen);
}

rxString rxStringLenMapChars(rxString s, size_t len, const char *from, const char *to, size_t setlen){
    return (const char*)memmapchars((char *)s, len, from, to, setlen);
}

rxString rxStringFormat(const char *fmt, ...)
{
    sds s = sdsempty();
    va_list ap;
    char *t;
    va_start(ap, fmt);
    t = sdscatvprintf(s, fmt, ap);
    va_end(ap);
    return t;
}

rxString rxStringAppend(const char *current, const char *extra, char sep)
{
    rxString appended = rxStringFormat("%s%c%s", current, sep, extra);
    rxStringFree(current);
    return appended;
}

rxString rxStringTrim(rxString s, const char *cset)
{
    return sdstrim((sds)s, cset);
}

rxString rxSdsAttachlen(void *address, const void *init, size_t initlen, size_t *total_size)
{
    struct sdshdr32 *s_preamble = (struct sdshdr32 *)address;
    char *s;
    char type = SDS_TYPE_32;
    int hdrlen = sizeof(struct sdshdr32);

    s = (char *)((void *)s_preamble + hdrlen);
    memcpy(s, init, initlen);
    s[initlen] = 0x00;

    s_preamble->len = initlen;
    s_preamble->alloc = initlen;
    s_preamble->flags = type;
    *total_size = hdrlen + initlen + 1;
    return (rxString)s;
}

void *rxStringGetSdsHdr(void *address, int sz)
{
    return (struct sdshdr32 *)(address + sz);
}

int rxStringGetSdsHdrSize()
{
    return sizeof(struct sdshdr32);
}

int rxStringCharCount(const char *s, char c)
{
    int tally = 0;
    char *t = (char *)s;
    while (*t)
    {
        if (*t == toupper(c))
            ++tally;
        ++t;
    }
    return tally;
}

/* Apply tolower() to every character of the string 's'. */
void rxStringToLower(const char *cs)
{
    char *s = (char *)cs;
    int len = strlen(s), j;
    for (j = 0; j < len; j++)
        s[j] = tolower(s[j]);
}

/* Apply toupper() to every character of the string 's'. */
void rxStringToUpper(const char *cs)
{
    char *s = (char *)cs;
    int len = strlen(s), j;
    for (j = 0; j < len; j++)
        s[j] = toupper(s[j]);
}

int rxStringMatchLen(const char *p, int plen, const char *s, int slen, int nocase)
{
    return stringmatchlen(p, plen, s, slen, nocase);
}

int rxStringMatch(const char *p, const char *s, int nocase)
{
    return stringmatch(p, s, nocase);
}

extern void serverLogRaw(int level, const char *msg);
void rxServerLogRaw(int level, const char *msg)
{
    serverLogRaw(level, msg);
}

extern void _serverAssert(const char *estr, const char *file, int line);
void rxServerAssert(const char *estr, const char *file, int line)
{
    _serverAssert(estr, file, line);
}

void serverLog(int level, const char *fmt, ...)
{
    va_list ap;
    char msg[2048];

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    serverLogRaw(level, msg);
}

void rxServerLog(int level, const char *fmt, ...)
{
    va_list ap;
    char msg[2048];

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);
    serverLogRaw(level, msg);
}

void rxServerLogHexDump(int level, void *value, size_t len, const char *fmt, ...)
{
    va_list ap;
    char msg[2048];

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    serverLogHexDump(level, msg, value, len);
}

#ifdef RXDEBUG
static rax *debugAllocated = NULL;
static rax *debugReleased = NULL;
#endif

void *rxMemAlloc(size_t size)
{
#ifdef RXDEBUG
    if (!debugAllocated)
    {
        debugAllocated = raxNew();
        debugReleased = raxNew();
    }
#endif
    void *ptr = zmalloc(size);
#ifdef RXDEBUG
    raxInsert(debugAllocated, (unsigned char *)&ptr, sizeof(void *), size, NULL);
#endif
    return ptr;
}

extern void rxMemFreeSession(void *ptr);

void rxMemFree(void *ptr)
{
    rxMemFreeSession(ptr);
}


void rxMemFreeX(void *ptr)
{
#ifdef RXDEBUG
    void *sz = raxFind(debugAllocated, (unsigned char *)&ptr, sizeof(void *));
    if (sz == raxNotFound)
    {
        rxServerLogHexDump(rxLL_NOTICE, ptr, 64, "rxMemFree NOT FOUND %p", ptr);
    }
    else
    {
        sz = raxFind(debugReleased, (unsigned char *)&ptr, sizeof(void *));
        if (sz != raxNotFound)
        {
            rxServerLogHexDump(rxLL_NOTICE, ptr, 64, "rxMemFree DOUBLE FREE %p", ptr);
        }
        else
        {
            raxRemove(debugAllocated, (unsigned char *)&ptr, sizeof(void *), NULL);
            raxInsert(debugReleased, (unsigned char *)&ptr, sizeof(void *), sz, NULL);
        }
    }
#endif
    zfree(ptr);
}

size_t rxMemAllocSize(void *ptr)
{
#ifdef RXDEBUG
    void *sz = raxFind(debugAllocated, (unsigned char *)&ptr, sizeof(void *));
    if (sz == raxNotFound)
    {
        rxServerLogHexDump(rxLL_NOTICE, ptr, 64, "rxMemAllocSize NOT FOUND %p", ptr);
        sz = raxFind(debugReleased, (unsigned char *)&ptr, sizeof(void *));
        if (sz != raxNotFound)
        {
            rxServerLogHexDump(rxLL_NOTICE, ptr, 64, "rxMemAllocSize FREED %p", ptr);
        }
    }
#endif
    return zmalloc_size(ptr);
}

const char *rxStringBuildRedisCommand(int argc, rxRedisModuleString **argv){
    int commandline_length = 1;
        size_t arg_len;
        const char *s;
    for(int n = 0 ; n < argc; ++n ){
       s = RedisModule_StringPtrLen((RedisModuleString*)argv[n], &arg_len);
        commandline_length += 3 + arg_len;
    }

    char *cmd = (char*)rxMemAlloc(commandline_length);
    memset(cmd, 0x00, commandline_length);
    s = RedisModule_StringPtrLen((RedisModuleString*)argv[0], &arg_len);
    strcpy(cmd, s);
    for (int n = 1; n < argc; ++n)
    {
        s = RedisModule_StringPtrLen((RedisModuleString*)argv[n], &arg_len);
        if (strchr(s, ' ')||strchr(s, '%'))
        {
            strcat(cmd, " \"");
            strcat(cmd, s);
            strcat(cmd, "\"");
        }
        else
        {
            strcat(cmd, " ");
            strcat(cmd, s);
        }
    }
    return (const char *)cmd;
}