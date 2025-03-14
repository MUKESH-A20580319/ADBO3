#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "tables.h"
#include <stdlib.h>
#include <string.h>

// Struct for managing table metadata
typedef struct RM_TableMgmtData {
    BM_BufferPool bufferPool;
    int numTuples;
} RM_TableMgmtData;

// Struct for scan management
typedef struct RM_ScanMgmtData {
    int currentPage;
    int currentSlot;
    Expr *condition;
} RM_ScanMgmtData;

// Initializes the Record Manager
RC initRecordManager(void *mgmtData) {
    initStorageManager();
    return RC_OK;
}

// Shuts down the Record Manager
RC shutdownRecordManager() {
    return RC_OK;
}

// Creates a table
RC createTable(char *name, Schema *schema) {
    createPageFile(name);
    SM_FileHandle fh;
    openPageFile(name, &fh);
    ensureCapacity(1, &fh);
    closePageFile(&fh);
    return RC_OK;
}

// Opens a table
RC openTable(RM_TableData *rel, char *name) {
    RM_TableMgmtData *mgmtData = (RM_TableMgmtData *)malloc(sizeof(RM_TableMgmtData));
    initBufferPool(&mgmtData->bufferPool, name, 3, RS_FIFO, NULL);
    rel->mgmtData = mgmtData;
    rel->name = name;
    return RC_OK;
}

// Closes a table
RC closeTable(RM_TableData *rel) {
    RM_TableMgmtData *mgmtData = (RM_TableMgmtData *)rel->mgmtData;
    shutdownBufferPool(&mgmtData->bufferPool);
    free(mgmtData);
    return RC_OK;
}

// Deletes a table
RC deleteTable(char *name) {
    destroyPageFile(name);
    return RC_OK;
}

// Returns the number of tuples
int getNumTuples(RM_TableData *rel) {
    RM_TableMgmtData *mgmtData = (RM_TableMgmtData *)rel->mgmtData;
    return mgmtData->numTuples;
}

// Inserts a record
RC insertRecord(RM_TableData *rel, Record *record) {
    RM_TableMgmtData *mgmtData = (RM_TableMgmtData *)rel->mgmtData;
    mgmtData->numTuples++;
    return RC_OK;
}

// Deletes a record
RC deleteRecord(RM_TableData *rel, RID id) {
    return RC_OK;
}

// Updates a record
RC updateRecord(RM_TableData *rel, Record *record) {
    return RC_OK;
}

// Retrieves a record
RC getRecord(RM_TableData *rel, RID id, Record *record) {
    return RC_OK;
}

// Starts a scan
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
    scan->mgmtData = malloc(sizeof(RM_ScanMgmtData));
    return RC_OK;
}

// Retrieves the next record in a scan
RC next(RM_ScanHandle *scan, Record *record) {
    return RC_RM_NO_MORE_TUPLES;
}

// Closes a scan
RC closeScan(RM_ScanHandle *scan) {
    free(scan->mgmtData);
    return RC_OK;
}

// Creates a schema
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
    Schema *schema = (Schema *)malloc(sizeof(Schema));
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keyAttrs = keys;
    schema->keySize = keySize;
    return schema;
}

// Frees a schema
RC freeSchema(Schema *schema) {
    free(schema);
    return RC_OK;
}

// Creates a record
RC createRecord(Record **record, Schema *schema) {
    *record = (Record *)malloc(sizeof(Record));
    (*record)->data = (char *)malloc(getRecordSize(schema));
    return RC_OK;
}

// Frees a record
RC freeRecord(Record *record) {
    free(record->data);
    free(record);
    return RC_OK;
}

// Gets an attribute
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
    return RC_OK;
}

// Sets an attribute
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
    return RC_OK;
}

// Gets the record size
int getRecordSize(Schema *schema) {
    int size = 0;
    for (int i = 0; i < schema->numAttr; i++) {
        switch (schema->dataTypes[i]) {
            case DT_INT: size += sizeof(int); break;
            case DT_FLOAT: size += sizeof(float); break;
            case DT_BOOL: size += sizeof(bool); break;
            case DT_STRING: size += schema->typeLength[i]; break;
        }
    }
    return size;
}
