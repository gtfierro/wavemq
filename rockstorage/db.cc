#include <cstdio>
#include <string>
#include <iostream>
#include "rocksdb/db.h"
#include <rocksdb/table.h>
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"

#include <stdlib.h>

using namespace rocksdb;
using std::cerr;
using std::endl;

//static DB* db;
static OptimisticTransactionDB* db;
//static TransactionDB* db;
static WriteOptions write_opts;
static ReadOptions read_opts;
//TransactionOptions txn_opts;
static std::vector<ColumnFamilyHandle*> handles;

TableFactory *makeSpinningMetalTableFactory() {
    auto block_opts = BlockBasedTableOptions{};
    block_opts.cache_index_and_filter_blocks = true;
    return NewBlockBasedTableFactory(block_opts);
}

extern "C" {
    #include "iface.h"

    void init(std::string dbname, size_t spinning_metal) {
        std::vector<ColumnFamilyDescriptor> cfs;

        Options opts;
        opts.create_if_missing = true;
        opts.create_missing_column_families = true;
        opts.OptimizeLevelStyleCompaction();
        //opts.enable_pipelined_write=true;

        ColumnFamilyOptions cf_options;

        if (spinning_metal > 0) {
            // suggestsions from https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#difference-of-spinning-disk
            cerr << "Optimizing for spinning metal" << endl;
            // for spinning metal
            opts.IncreaseParallelism(4);
            opts.compaction_readahead_size = 8 * 1024 * 1024;
            opts.optimize_filters_for_hits = true;
            opts.skip_stats_update_on_db_open = true;
            opts.new_table_reader_for_compaction_inputs=true;

            cf_options.table_factory.reset(makeSpinningMetalTableFactory());
        } else {
            opts.IncreaseParallelism();
        }

        cfs.push_back(ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
        cfs.push_back(ColumnFamilyDescriptor("CF_QUEUE", cf_options));
        cfs.push_back(ColumnFamilyDescriptor("CF_PERSIST", cf_options));

        Status s = OptimisticTransactionDB::Open(opts, dbname, cfs, &handles, &db);//cfs, &handles, &db);
        cerr << 4;
        if (!s.ok()) {
            cerr << "Open DB: " << s.ToString() << endl;
        }
        cerr << 5;
        assert(s.ok());
        printf("(good?) db ptr: %p\n", (void*)db);
        cerr << 6;
        cerr << 7;
    }

    void db_it_start(int col, void** state, const char *pfx, size_t pfxlen, char** key, size_t* keylen, char** value, size_t* valuelen) {
        ReadOptions ro;
        cerr << "start iterator" << endl;
        //ro.total_order_seek=true;
        auto it = db->NewIterator(ro, handles[col]);
        it->Seek(Slice(pfx, pfxlen));
        *state = it;
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

    void db_it_next(void* state, const char *pfx, size_t pfxlen, char** key, size_t* keylen, char** value, size_t* valuelen) {
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

    void db_it_delete(void* state) {
        delete (Iterator*)state;
        cerr << "delete iterator" << endl;
    }

    size_t db_delete_prefix(int col, const char *pfx, size_t pfxlen) {
        size_t _count = 0;
        int tries = 10;
        while (tries > 0) {
            Transaction* txn = db->BeginTransaction(write_opts);
            assert(txn);
            auto it = txn->GetIterator(ReadOptions(), handles[col]);
            Slice prefix = Slice(pfx, pfxlen);
            for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
                  Status s = txn->Delete(handles[col], it->key());
                  assert(s.ok());
                  _count++;
            }
            delete it;
            Status s = txn->Commit();
            if (!s.ok()) {
                cerr << "delete pfx failed: " << s.ToString() << " but retrying..." << endl;
                tries --;
                txn->Rollback();   
                delete txn;
                continue;
            }
            assert(s.ok());
            cerr << "deleted " << _count << " entries for prefix" << endl;
            delete txn;
            return _count;
        }
        return _count;
    }

    void db_set(int col, const char *key, size_t keylen, const char *value, size_t valuelen, char** error, size_t* errorlen) {
        Status s = db->Put(write_opts, handles[col], Slice(key, keylen), Slice(value, valuelen));
        if (!s.ok()) {
            printf("db ptr: %p\n", (void*) db);
            cerr << "Queue Set: " << s.ToString() << endl;
            // copy error value out
            auto e = s.ToString();
            *error = (char*) malloc(e.size());
            *errorlen = e.size();
            memcpy(*error, e.data(), e.size());
        } else {
            *errorlen = 0;
        }
        assert(s.ok());
    }

    void db_delete(int col, const char *key, size_t keylen, char** error, size_t* errorlen) {
        Transaction* txn = db->BeginTransaction(write_opts);
        assert(txn);
        Status s = txn->Delete(handles[col], Slice(key, keylen));
        if (!s.ok()) {
            cerr << "Queue Delete: " << s.ToString() << endl;
            auto e = s.ToString();
            *error = (char*) malloc(e.size());
            *errorlen = e.size();
            memcpy(*error, e.data(), e.size());
            delete txn;
            return;
        }
        s = txn->Commit();
        if (!s.ok()) {
            auto e = s.ToString();
            *error = (char*) malloc(e.size());
            *errorlen = e.size();
            memcpy(*error, e.data(), e.size());
            delete txn;
            return;
        }
        assert(s.ok());
        *errorlen = 0;
        delete txn;
    }

    char* db_get(int col, const char *key, size_t keylen, size_t *valuelen) {
        std::string value;
        char *rv;
        Status s = db->Get(read_opts, handles[col], Slice(key, keylen), &value);
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

    void close_db() {
        //delete cf_queue;
        printf("DELETING DB (rocksdb)\n");
        delete db;
    }
}
