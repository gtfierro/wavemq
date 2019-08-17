#include <cstdio>
#include <string>
#include <iostream>
#include "rocksdb/db.h"
//#include "rocksdb/utilities/transaction.h"
//#include "rocksdb/utilities/transaction_db.h"

#include <stdlib.h>

using namespace rocksdb;
using std::cerr;
using std::endl;

static DB* db;
//static TransactionDB* db;
static ColumnFamilyHandle* cf_queue;
static WriteOptions write_opts;
static ReadOptions read_opts;
//TransactionOptions txn_opts;
static std::vector<ColumnFamilyHandle*> handles;

extern "C" {
    #include "iface.h"

    void init(std::string dbname, size_t spinning_metal) {
        std::vector<ColumnFamilyDescriptor> cfs;

        //write_opts.sync = true;

        Options opts;
        opts.IncreaseParallelism();
        opts.OptimizeLevelStyleCompaction();
        opts.create_if_missing = true;
        opts.create_missing_column_families = true;
        opts.enable_pipelined_write=true;
       // TransactionDBOptions txn_db_options;
        cerr << 1;

        if (spinning_metal > 0) {
            cerr << "Optimizing for spinning metal" << endl;
            // for spinning metal
            opts.compaction_readahead_size = 4 * 1024 * 1024;
            opts.skip_stats_update_on_db_open = true;
        }

        cerr << 2;
        cfs.push_back(ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions()));
        cfs.push_back(ColumnFamilyDescriptor("CF_QUEUE", ColumnFamilyOptions()));
        cerr << 3;

        //Status s = TransactionDB::Open(opts, txn_db_options, dbname, cfs, &handles, &db);
        Status s = DB::Open(opts, dbname, cfs, &handles, &db);
        cerr << 4;
        if (!s.ok()) {
            cerr << "Open DB: " << s.ToString() << endl;
        }
        cerr << 5;
        assert(s.ok());
        cerr << 6;
        cf_queue = handles[1];
        cerr << 7;
    }


    void queue_wb_start(void** state) {
        //Transaction* txn = db->BeginTransaction(write_opts, txn_opts);
        //*state = txn;
        //cerr << "txn start" << endl;
        //
        //batch.Put(key2, value);
        //s = db->Write(rocksdb::WriteOptions(), &batch);
    }

    void queue_wb_set(void* state, char* key, size_t keylen, char* value, size_t valuelen) {
        //Transaction *txn = (Transaction*) state;
        //Status s = txn->Put(cf_queue, Slice(key, keylen), Slice(value, valuelen));
        //if (!s.ok()) {
        //    cerr << "Bulk set: " << s.ToString() << endl;
        //}
        //assert(s.ok());
    }

    void queue_wb_done(void* state) {
        //Transaction *txn = (Transaction*) state;
        //Status s = txn->Commit();
        //if (!s.ok()) {
        //    cerr << "Bulk write: " << s.ToString() << endl;
        //}
        //assert(s.ok());
        //delete txn;
    }

    void queue_wb_delete(void* state) {
        // cerr << "txn done" << endl;
        // Transaction *txn = (Transaction*) state;
        // txn->Rollback();
        // delete txn;
    }

    void queue_it_start(void** state, const char *pfx, size_t pfxlen, char** key, size_t* keylen, char** value, size_t* valuelen) {
        ReadOptions ro;
        cerr << "start iterator" << endl;
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
        cerr << "delete iterator" << endl;
    }

    size_t queue_delete_prefix(const char *pfx, size_t pfxlen) {
        size_t _count = 0;
        auto it = db->NewIterator(ReadOptions());
        Slice prefix = Slice(pfx, pfxlen);
        for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
              db->Delete(write_opts, it->key());
              _count++;
        }
        return _count;
    }

    void queue_set(const char *key, size_t keylen, const char *value, size_t valuelen) {
        Status s = db->Put(write_opts, cf_queue, Slice(key, keylen), Slice(value, valuelen));
        if (!s.ok()) {
            cerr << "Queue Set: " << s.ToString() << endl;
        }
        assert(s.ok());
    }

    void queue_delete(const char *key, size_t keylen) {
        Status s = db->Delete(write_opts, cf_queue, Slice(key, keylen));
        if (!s.ok()) {
            cerr << "Queue Delete: " << s.ToString() << endl;
        }
        assert(s.ok());
    }

    char* queue_get(const char *key, size_t keylen, size_t *valuelen) {
        std::string value;
        char *rv;
        Status s = db->Get(read_opts, cf_queue, Slice(key, keylen), &value);
        if (s.IsNotFound()) return NULL;
        if (!s.ok()) {
            cerr << "queue get: " << s.ToString() << endl;
        }
        assert(s.ok());
        rv = (char*) malloc(value.length());
        *valuelen = value.length();
        memcpy(rv, value.data(), value.length());
        return rv;
    }

    void c_init(const char* name, size_t namelen, size_t spinning_metal) {
        std::string dbname = std::string(name, namelen);
        init(dbname, spinning_metal);
    }

    void close() {
        //delete cf_queue;
        delete db;
    }
}
