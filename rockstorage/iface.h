
#include <stddef.h>
#include <string.h>
#include <stdlib.h>

void c_init(const char* name, size_t namelen);
char* queue_get(const char *key, size_t keylen, size_t *valuelen);
void queue_delete(const char *key, size_t keylen);
void queue_set(const char *key, size_t keylen, const char *value, size_t valuelen);
void queue_it_delete(void* state);
void queue_it_next(void* state, const char *pfx, size_t pfxlen, char** key, size_t* keylen, char** value, size_t* valuelen);
void queue_it_start(void** state, const char *pfx, size_t pfxlen, char** key, size_t* keylen, char** value, size_t* valuelen);
size_t queue_delete_prefix(const char *pfx, size_t pfxlen);
