#include "tls.hpp"

#ifdef __cplusplus
extern "C"
{
#endif

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>
#include <string.h>
#include "stdlib.h"

#include "rxSuite.h"
#include "rax.h"
#include "rxSuiteHelpers.h"

#ifdef __cplusplus
}
#endif
#include <thread>
#include <mutex>

static rax *TLS_Registry = NULL;
static std::mutex TLS_Registry_Mutex;

template <typename T>
T tls_get(const char *key, allocatorProc *allocator, void *parms)
{
   const std::lock_guard<std::mutex> lock(TLS_Registry_Mutex);
    if (TLS_Registry == NULL)
        TLS_Registry = raxNew();

    pthread_t tid = pthread_self();
    char id[256];
    snprintf(id, sizeof(id), "%s:%lx", key, tid);
    auto *data = raxFind(TLS_Registry, (UCHAR *)&id, strlen(id));
    if (data == raxNotFound)
    {
        data = allocator(parms);
        raxInsert(TLS_Registry, (UCHAR *)&id, strlen(id), data, NULL);
        raxShow(TLS_Registry);
    }
    return (T)data;
}

template <typename T>
void tls_forget(const char *key){
    if(TLS_Registry == NULL)
        return;
    pthread_t tid = pthread_self();
    char id[256];
    snprintf(id, sizeof(id), "%s:%lx", key, tid);
    raxRemove(TLS_Registry, (UCHAR *)&id, strlen(id), NULL);
}

template<rax> rax *tls_get(const char *key, allocatorProc *allocator, void *parms);
template<rax> rax *tls_forget(const char *key);

class SilNikParowy_Kontekst;

template<class SilNikParowy_Kontekst> SilNikParowy_Kontekst *tls_get(const char *key, allocatorProc *allocator, void *parms);
template<class SilNikParowy_Kontekst> SilNikParowy_Kontekst *tls_forget(const char *key);

// struct _SessionMemory_;

// template<struct _SessionMemory_> _SessionMemory_ *tls_get(const char *key, allocatorProc *allocator, void *parms);
// template<struct _SessionMemory_> _SessionMemory_ *tls_forget(const char *key);
