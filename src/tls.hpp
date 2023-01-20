#ifndef __TLS_H__
#define __TLS_H__

typedef void *allocatorProc();

template <typename T>
T tls_get(const char *key, allocatorProc *allocator);

template <typename T>
T tls_forget(const char *key);

#endif
