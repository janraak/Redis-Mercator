#ifndef __GRAPHSTACKENTRY_ABBREVIATED_HPP__
#define __GRAPHSTACKENTRY_ABBREVIATED_HPP__


#include <cstdarg>
#include <typeinfo>
#define WRAPPER 
#include "simpleQueue.hpp"


#define STASH_STRING 1
#define STASH_ROBJ 2


#define GET_ARGUMENTS_FROM_STASH(s)              \
    int argc = *((int *)s);                      \
    void **argv = (void **)(s + sizeof(void *));

/*
 * A redis command is stashed in a single allocated block of memory.
 * The block can be easily release with a single free.
 *
 * format:
 *          int argc;
 *          robj *argvP[argc]
 *          rxString *argvS[argc]
 *          char *argvT[argc]
 *
 * struct{
 *     int   argc;
 *     robj * argv[*],
 *     robj  argvP[*],
 *     rxString   argvS[*],
 *     char * argvT[*],
 *
 *     argv[0]->argvP[0]->argvS[0]->argvT[0]
 * 
 *   --------------     ---------------------     ---------------------     --------------------- 
 *   | argc | [0] | ... | [n] | NULL | r[0] | ... | r[n]  | s[0] | ... | s[n] | t[0] | ... | t[n]   
 *   ----------|---     ---^-------------|---     ---^--------|---     ----------^------------|--
 *             |           |             |           |        |                  |
 *             .-----------.             .-----------.        .------------------.
 * }
 *
 */
void *rxStashCommand(SimpleQueue *ctx, const char *command, int argc, ...)
{
    void *stash = NULL;

    std::va_list args;
    va_start(args, argc);

    va_list cpyArgs;
    va_copy(cpyArgs, args);
    size_t total_string_size = 1 + strlen(command);
    int preamble_len = rxStringGetSdsHdrSize();
    for (int j = 0; j < argc; j++)
    {
        rxString result = va_arg(cpyArgs, rxString);
        int len = strlen(result);
        total_string_size += preamble_len + len + 1;
    }
    size_t total_pointer_size = (argc + 3) * sizeof(void *);
    size_t total_robj_size = (argc + 1) * rxSizeofRobj();
    size_t total_sds_size = (argc + 1) * preamble_len;
    size_t total_stash_size = (argc + 2) * sizeof(void *) 
                            + total_pointer_size 
                            + total_robj_size 
                            + total_sds_size 
                            + total_string_size + 1;
    stash = rxMemAlloc(total_stash_size);
    memset(stash, 0xff, total_stash_size);

    *((int *)stash) = argc + 1;

    void **argv = (void **)(stash + sizeof(void *));
    void *robj_space = ((void *)stash + total_pointer_size);
    struct sdshdr32 *sds_space = (struct sdshdr32 *)((void *)robj_space + total_robj_size);
    // char *string_space = (char *)((void *)sds_space + total_sds_size);

    size_t l = strlen(command);
    size_t total_size;
    // strcpy(string_space, command);
    rxString s = rxSdsAttachlen(sds_space, command, l, &total_size);
    argv[0] = rxSetStringObject(robj_space, (void *)s);
    sds_space = (struct sdshdr32 *)((char *)sds_space + total_size);
    robj_space = robj_space + rxSizeofRobj();

    va_copy(cpyArgs, args);
    int j = 0;
    for (; j < argc; j++)
    {

        rxString result = va_arg(cpyArgs, rxString);
        l = strlen(result);

        s = rxSdsAttachlen(sds_space, result, l, &total_size);
        argv[j + 1] = rxSetStringObject(robj_space, (void *)s);

        robj_space = robj_space + rxSizeofRobj();
        sds_space = (struct sdshdr32 *)((char *)sds_space + total_size);
    }
    argv[j + 1] = NULL;

    va_end(args);

    if(ctx)
        enqueueSimpleQueue(ctx, stash);
    return stash;
}

void *rxStashCommand2(SimpleQueue *ctx, const char *command, int argt, int argc, void **args)
{
    void *stash = NULL;

    size_t total_string_size = command == NULL ? 0 : 1 + strlen(command);
    int preamble_len = rxStringGetSdsHdrSize();
    for (int j = 0; j < argc; j++)
    {
        rxString result = (( argt == STASH_STRING ) ? (rxString)((rxString *)args[j]) : (rxString)rxGetContainedObject(args[j]));
        total_string_size += preamble_len + strlen(result) + 1;
    }
    size_t total_pointer_size = (argc + 3) * sizeof(void *);
    size_t total_robj_size = (argc + 1) * rxSizeofRobj();
    size_t total_sds_size = (argc + 1) * preamble_len;
    size_t total_stash_size = (argc + 2) * sizeof(void *) 
                            + total_pointer_size 
                            + total_robj_size 
                            + total_sds_size 
                            + total_string_size + 1;
    stash = rxMemAlloc(total_stash_size);
    memset(stash, 0xff, total_stash_size);

    *((int *)stash) = argc + (command == NULL ? 0 : 1);

    void **argv = (void **)(stash + sizeof(void *));
    void *robj_space = ((void *)stash + total_pointer_size);
    void *sds_space = rxStringGetSdsHdr(robj_space, total_robj_size);
    // char *string_space = (char *)((void *)sds_space + total_sds_size);

    int j = 0;
    int k = 0;
    size_t total_size = 0;
    size_t l;
    rxString s;
    if (command != NULL)
    {
        l = strlen(command);
        // strcpy(string_space, command);
        s = rxSdsAttachlen(sds_space, command, l, &total_size);
        argv[0] = rxSetStringObject(robj_space, (void *)s);
    }
    else
        k = 1;
    sds_space = rxStringGetSdsHdr(sds_space, total_size);
    robj_space = robj_space + rxSizeofRobj();
    for (; j < argc; j++)
    {

        rxString result = ( argt == STASH_STRING ) ? (rxString)args[j] : (rxString)rxGetContainedObject(args[j]);
        l = strlen(result);

        s = rxSdsAttachlen(sds_space, result, l, &total_size);
        argv[j - k + 1] = rxSetStringObject(robj_space, (void *)s);

        robj_space = robj_space + rxSizeofRobj();
        sds_space = rxStringGetSdsHdr(sds_space, total_size);
    }
    argv[j - k + 1] = NULL;

    if(ctx != NULL)
        enqueueSimpleQueue(ctx, (char **)stash);
    return stash;
}


#endif