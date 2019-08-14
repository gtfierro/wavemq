#include <cstdio>
#include <string>
#include <iostream>
#include "rocksdb/db.h"

#include <stdlib.h>

using namespace rocksdb;
using std::cerr;
using std::endl;

static DB* db;
static ColumnFamilyHandle* cf_queue;
static WriteOptions write_opts;
static ReadOptions read_opts;

extern "C" {
    #include "iface.h"

    void init(std::string dbname) {
        std::vector<ColumnFamilyDescriptor> cfs;
        std::vector<ColumnFamilyHandle*> handles;

        Options opts;
        opts.IncreaseParallelism();
        opts.OptimizeLevelStyleCompaction();
        opts.create_if_missing = true;
        opts.create_missing_column_families = true;

        cfs.push_back(ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions()));
        cfs.push_back(ColumnFamilyDescriptor("CF_QUEUE", ColumnFamilyOptions()));

        Status s = DB::Open(opts, dbname, cfs, &handles, &db);
        if (!s.ok()) {
            cerr << s.ToString() << endl;
        }
        assert(s.ok());
        cf_queue = handles[1];
    }

    void queue_it_start(void** state, const char *pfx, size_t pfxlen, char** key, size_t* keylen, char** value, size_t* valuelen) {
        ReadOptions ro;
        //ro.total_order_seek=true;
        auto it = db->NewIterator(ro, cf_queue);
        it->Seek(Slice(pfx, pfxlen));
        *state = it;
        if (!it->Valid() || !it->key().starts_with(Slice(pfx, pfxlen))) {
            *keylen = 0;
            *valuelen = 0;
        } else {
            //Slice ks = Slice((char*)it->key().data(), it->key().size());
            //Slice vs = Slice((char*)it->value().data(), it->value().size());
            //cerr << "iter key " << ks.ToString() << " and val " << vs.ToString() << endl;
            *key = (char*) it->key().data();
            *keylen = it->key().size();
            *value = (char*) it->value().data();
            *valuelen = it->value().size();
        }
    }

    void queue_it_next(void* state, const char *pfx, size_t pfxlen, char** key, size_t* keylen, char** value, size_t* valuelen) {
        Iterator *it = (Iterator*) state;
        it->Next();
        if (!it->Valid() || !it->key().starts_with(Slice(pfx, pfxlen))) {
            *keylen = 0;
            *valuelen = 0;
        } else {
            *key = (char*) it->key().data();
            *keylen = it->key().size();
            *value = (char*) it->value().data();
            *valuelen = it->value().size();
        }
    }

    void queue_it_delete(void* state) {
        delete (Iterator*)state;
    }

    void queue_delete_prefix(const char *pfx, size_t pfxlen) {
        auto it = db->NewIterator(ReadOptions());
        Slice prefix = Slice(pfx, pfxlen);
        for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it.Next()) {
            db->Delete(WriteOptions(), it->key());
        }
    }

    void queue_set(const char *key, size_t keylen, const char *value, size_t valuelen) {
        Status s = db->Put(write_opts, cf_queue, Slice(key, keylen), Slice(value, valuelen));
        assert(s.ok());
    }

    void queue_delete(const char *key, size_t keylen) {
        Status s = db->Delete(write_opts, cf_queue, Slice(key, keylen));
        assert(s.ok());
    }

    char* queue_get(const char *key, size_t keylen, size_t *valuelen) {
        std::string value;
        char *rv;
        Status s = db->Get(read_opts, cf_queue, Slice(key, keylen), &value);
        if (s.IsNotFound()) return NULL;
        assert(s.ok());
        rv = (char*) malloc(value.length());
        *valuelen = value.length();
        memcpy(rv, value.data(), value.length());
        return rv;
    }

    void c_init(const char* name, size_t namelen) {
        std::string dbname = std::string(name, namelen);
        init(dbname);
    }
}
