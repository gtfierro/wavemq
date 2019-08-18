
#include <stddef.h>
#include <string.h>
#include <stdlib.h>

void c_init(const char* name, size_t namelen, size_t spinning_metal);
void close_db();
char* queue_get(const char *key, size_t keylen, size_t *valuelen);
void queue_delete(const char *key, size_t keylen, char** err, size_t* errlen);
void queue_set(const char *key, size_t keylen, const char *value, size_t valuelen, char** err, size_t* errlen);
void queue_it_delete(void* state);
void queue_it_next(void* state, const char *pfx, size_t pfxlen, char** key, size_t* keylen, char** value, size_t* valuelen);
void queue_it_start(void** state, const char *pfx, size_t pfxlen, char** key, size_t* keylen, char** value, size_t* valuelen);
size_t queue_delete_prefix(const char *pfx, size_t pfxlen);

void queue_wb_start(void** state);
void queue_wb_set(void* state, char* key, size_t keylen, char* value, size_t valuelen);
void queue_wb_done(void* state);
void queue_wb_delete(void* state);
