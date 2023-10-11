
#ifndef __GRAPHDB_H__
#define __GRAPHDB_H__

#define REDISMODULE_EXPERIMENTAL_API
#ifdef __cplusplus
extern "C" {
#endif
#include "redismodule.h"

// #include "adlist.h"
// #include "dict.h"
// #include "sdsWrapper.h"
#include <ctype.h>
#include <locale.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef __cplusplus
    extern const char *get_deeper(const char *rootKey, size_t rootKey_len, size_t max_graph_length);
}
#endif

#define UNUSED(x) (void)(x)
#endif