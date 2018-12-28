#pragma once
#include <uuid/uuid.h>
#include <savitar.hpp>
#include "rocksdb/db.h"

namespace rocksdb {

class PDB : public PersistentObject {
public:
    PDB(uuid_t id, Options& options, string& db_path) :
        PersistentObject(id) {
        void *t = alloc->alloc(sizeof(DB));
        DB *obj = (DB *)t;
        db = new(obj) DB();

        Status status = DB::Open(options, db_path, &db);
    }

    Status Put(const WriteOptions &options, const Slice& key,
            const Slice& value) {
        Savitar_thread_notify(5, this, PutDefaultColumnFamily,
                options, key, value);
        Status st = db->Put(options, key, value);
        // DEBUG
        Savitar_thread_wait(this, this->log);
        // DEBUG
        return st;
    }

    // <compiler>
    static PersistentObject *BaseFactory(uuid_t id) {
        ObjectAlloc *alloc = GlobalAlloc::getInstance()->newAllocator(id);
        void *temp = alloc->alloc(sizeof(PDB));
        PDB *obj = (PDB *)temp;
        PDB *object = new (obj) PDB(id);
        return object;
    }

    static PersistentObject *RecoveryFactory(NVManager *m, CatalogEntry *e) {
        return BaseFactory(e->uuid);
    }

    static PDB *Factory(uuid_t id) {
        NVManager &manager = NVManager::getInstance();
        manager.lock();
        PDB *obj = (PDB *)manager.findRecovered(id);
        if (obj == NULL) {
            obj = static_cast<PDB *>(BaseFactory(id));
            manager.createNew(classID(), obj);
        }
        manager.unlock();
        return obj;
    }

    uint64_t Log(uint64_t tag, uint64_t *args) {
        int vector_size = 0;
        ArgVector vector[4]; // Max arguments of the class

        switch (tag) {
            case PutDefaultColumnFamily:
                {
                // TODO
                vector[0].addr = &tag;
                vector[0].len = sizeof(tag);
                // WriteOptions : options
                vector[1].addr = (void *)args[0];
                vector[1].len = sizeof(WriteOptions);
                // Slice : key
                Slice *key = dynamic_cast<Slice *>(args[1]);
                vector[2].addr = (void *)key->data();
                vector[2].len = key->size();
                // Slice : value
                Slice *value = dynamic_cast<Slice *>(args[2]);
                vector[3].addr = (void *)value->data();
                vector[3].len = value->size();
                vector_size = 4;
                }
                break;
            case PutColumnFamilyHandle:
            case DeleteDefaultColumnFamily:
            case DeleteColumnFamilyHandle:
            case SingleDeleteDefaultColumnFamily:
            case SingleDeleteColumnFamilyHandle:
            case DeleteRangeColumnFamilyHandle:
            case MergeDefaultColumnFamily:
            case MergeColumnFamilyHandle:
            case WriteBatch:
                assert(false); // Not supported
                break;
            default:
                assert(false);
                break;
        }

        return AppendLog(vector, vector_size);
    }

    size_t Play(uint64_t tag, uint64_t *args, bool dry) {
        size_t bytes_processed = 0;
        switch (tag) {
            case PutDefaultColumnFamily:
                {
                char *ptr = (char *)args;
                WriteOptions *options = (WriteOptions *)ptr;
                ptr += sizeof(WriteOptions);
                const char *key = ptr;
                const char *value = ptr + strlen(key);
                if (!dry) db->Put(*options, key, value);
                bytes_processed = sizeof(WriteOptions) +
                    strlen(key) + strlen(value) + 2;
                }
                break;
            case PutColumnFamilyHandle:
            case DeleteDefaultColumnFamily:
            case DeleteColumnFamilyHandle:
            case SingleDeleteDefaultColumnFamily:
            case SingleDeleteColumnFamilyHandle:
            case DeleteRangeColumnFamilyHandle:
            case MergeDefaultColumnFamily:
            case MergeColumnFamilyHandle:
            case WriteBatch:
                {
                PRINT("Tag not supported: %zu\n", tag);
                assert(false); // Not supported
                }
                break;
            default:
                {
                PRINT("Unknown tag: %zu\n", tag);
                assert(false);
                }
                break;
        }
        return bytes_processed;
    }

    static uint64_t classID() { return __COUNTER__; }
    // </compiler>

private:
    DB *db = NULL;
    // <compiler>
    enum MethodTags {
        PutDefaultColumnFamily = 1,
        PutColumnFamilyHandle = 2,
        DeleteDefaultColumnFamily = 3,
        DeleteColumnFamilyHandle = 4,
        SingleDeleteDefaultColumnFamily = 5,
        SingleDeleteColumnFamilyHandle = 6,
        DeleteRangeColumnFamilyHandle = 7,
        MergeDefaultColumnFamily = 8,
        MergeColumnFamilyHandle = 9,
        WriteBatch = 10,
        // TODO support for other methods
    };
    // </compiler>
};

} // namespace rocksdb
