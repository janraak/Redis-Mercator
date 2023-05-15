#ifndef __TLS_H__
#define __TLS_H__

typedef void *allocatorProc(void *parms);

template <typename T>
T tls_get(const char *key, allocatorProc *allocator, void *parms);

template <typename T>
T tls_forget(const char *key);

#endif
