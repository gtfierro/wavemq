
#include <stddef.h>
#include <string.h>
#include <stdlib.h>

void c_init(const char* name, size_t namelen, size_t spinning_metal);
void close_db();
char* db_get(int col, const char *key, size_t keylen, size_t *valuelen);
void db_delete(int col, const char *key, size_t keylen, char** err, size_t* errlen);
void db_set(int col, const char *key, size_t keylen, const char *value, size_t valuelen, char** err, size_t* errlen);
void db_it_delete(void* state);
void db_it_next(void* state, const char *pfx, size_t pfxlen, char** key, size_t* keylen, char** value, size_t* valuelen);
void db_it_start(int col, void** state, const char *pfx, size_t pfxlen, char** key, size_t* keylen, char** value, size_t* valuelen);
size_t db_delete_prefix(int col, const char *pfx, size_t pfxlen);

void db_wb(void** state);
void db_wb_set(int col, void* state, const char *key, size_t keylen, const char *value, size_t valuelen);
void db_wb_commit(int col, void* state, char** error, size_t* errorlen);

//void queue_wb_start(void** state);
//void queue_wb_set(void* state, char* key, size_t keylen, char* value, size_t valuelen);
//void queue_wb_done(void* state);
//void queue_wb_delete(void* state);
