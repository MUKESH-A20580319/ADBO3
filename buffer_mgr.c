#define _POSIX_C_SOURCE 200809L

/* buffer_mgr.c - Implementation of the Buffer Manager for Assignment 2 */

#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "dt.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

/* Internal structure representing a single frame in the buffer pool */
typedef struct BM_Frame {
    int pageNum;      // The page number stored in this frame (NO_PAGE if empty)
    char *data;       // Pointer to the page content (allocated PAGE_SIZE bytes)
    int fixCount;     // Number of clients currently pinning this page
    bool dirty;       // TRUE if the page has been modified
    int loadTime;     // Time when the page was loaded (for FIFO)
    int lastUsed;     // Last access time (for LRU)
} BM_Frame;

/* Internal management structure for the entire buffer pool */
typedef struct BM_MgmtData {
    BM_Frame *frames;         // Array of frames
    int numFrames;            // Number of frames (same as bm->numPages)
    int readIO;               // Count of page reads from disk
    int writeIO;              // Count of page writes to disk
    int time;                 // Global time counter for replacement decisions
    SM_FileHandle fileHandle; // Storage manager file handle for the page file
} BM_MgmtData;

/* 
 * initBufferPool: Creates a new buffer pool with the given number of pages and replacement strategy.
 * It allocates the frames, initializes them as empty, opens the page file using the storage manager,
 * and sets IO counters to zero.
 */
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData) {
    if (bm == NULL || pageFileName == NULL || numPages <= 0) {
         return RC_FILE_HANDLE_NOT_INIT;
    }
    bm->pageFile = strdup(pageFileName);
    bm->numPages = numPages;
    bm->strategy = strategy;
    
    BM_MgmtData *mgmt = (BM_MgmtData *) malloc(sizeof(BM_MgmtData));
    if (!mgmt) return RC_WRITE_FAILED;
    
    mgmt->numFrames = numPages;
    mgmt->readIO = 0;
    mgmt->writeIO = 0;
    mgmt->time = 0;
    
    mgmt->frames = (BM_Frame *) malloc(sizeof(BM_Frame) * numPages);
    if (!mgmt->frames) {
         free(mgmt);
         return RC_WRITE_FAILED;
    }
    
    for (int i = 0; i < numPages; i++) {
         mgmt->frames[i].pageNum = NO_PAGE;
         mgmt->frames[i].data = (char *) malloc(PAGE_SIZE);
         if (!mgmt->frames[i].data) {
             for (int j = 0; j < i; j++) {
                free(mgmt->frames[j].data);
             }
             free(mgmt->frames);
             free(mgmt);
             return RC_WRITE_FAILED;
         }
         memset(mgmt->frames[i].data, 0, PAGE_SIZE);
         mgmt->frames[i].fixCount = 0;
         mgmt->frames[i].dirty = false;
         mgmt->frames[i].loadTime = 0;
         mgmt->frames[i].lastUsed = 0;
    }
    
    RC rc = openPageFile((char *)pageFileName, &mgmt->fileHandle);
    if (rc != RC_OK) {
         for (int i = 0; i < numPages; i++) {
             free(mgmt->frames[i].data);
         }
         free(mgmt->frames);
         free(mgmt);
         return rc;
    }
    
    bm->mgmtData = mgmt;
    return RC_OK;
}

/* 
 * shutdownBufferPool: Flushes any dirty pages (if needed), checks that no pages are pinned,
 * frees all allocated memory for frames and mgmtData, closes the page file, and clears mgmtData.
 */
RC shutdownBufferPool(BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) {
         return RC_FILE_HANDLE_NOT_INIT;
    }
    BM_MgmtData *mgmt = (BM_MgmtData *) bm->mgmtData;
    
    for (int i = 0; i < mgmt->numFrames; i++) {
         if (mgmt->frames[i].fixCount > 0) {
              printf("Error: Attempting to shutdown buffer pool with pinned pages.\n");
              return RC_IM_NO_MORE_ENTRIES;
         }
    }
    
    forceFlushPool(bm);
    
    for (int i = 0; i < mgmt->numFrames; i++) {
         free(mgmt->frames[i].data);
    }
    free(mgmt->frames);
    
    RC rc = closePageFile(&mgmt->fileHandle);
    if (rc != RC_OK) {
         free(mgmt);
         bm->mgmtData = NULL;
         return rc;
    }
    
    free(mgmt);
    free(bm->pageFile);
    bm->mgmtData = NULL;
    return RC_OK;
}

/* 
 * forceFlushPool: Writes back all dirty pages (that have fixCount 0) to disk.
 * It iterates through all frames and, if a frame is dirty, writes its content back using writeBlock.
 */
RC forceFlushPool(BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) {
         return RC_FILE_HANDLE_NOT_INIT;
    }
    BM_MgmtData *mgmt = (BM_MgmtData *) bm->mgmtData;
    
    for (int i = 0; i < mgmt->numFrames; i++) {
         if (mgmt->frames[i].dirty && mgmt->frames[i].fixCount == 0) {
              RC rc = writeBlock(mgmt->frames[i].pageNum, &mgmt->fileHandle, mgmt->frames[i].data);
              if (rc != RC_OK) return rc;
              mgmt->frames[i].dirty = false;
              mgmt->writeIO++;
         }
    }
    return RC_OK;
}

/* 
 * markDirty: Marks the page corresponding to the given page handle as dirty.
 */
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
    if (bm == NULL || bm->mgmtData == NULL || page == NULL) {
         return RC_FILE_HANDLE_NOT_INIT;
    }
    BM_MgmtData *mgmt = (BM_MgmtData *) bm->mgmtData;
    for (int i = 0; i < mgmt->numFrames; i++) {
         if (mgmt->frames[i].pageNum == page->pageNum) {
              mgmt->frames[i].dirty = true;
              return RC_OK;
         }
    }
    printf("Error: markDirty: Page not found in buffer pool.\n");
    return RC_IM_KEY_NOT_FOUND;
}

/* 
 * unpinPage: Decrements the fix count of the page in the buffer pool.
 * Returns an error if the page is not found or is not pinned.
 */
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    if (bm == NULL || bm->mgmtData == NULL || page == NULL) {
         return RC_FILE_HANDLE_NOT_INIT;
    }
    BM_MgmtData *mgmt = (BM_MgmtData *) bm->mgmtData;
    for (int i = 0; i < mgmt->numFrames; i++) {
         if (mgmt->frames[i].pageNum == page->pageNum) {
              if (mgmt->frames[i].fixCount <= 0) {
                   printf("Error: unpinPage: Page fix count is already 0.\n");
                   return RC_IM_NO_MORE_ENTRIES;
              }
              mgmt->frames[i].fixCount--;
              return RC_OK;
         }
    }
    printf("Error: unpinPage: Page not found in buffer pool.\n");
    return RC_IM_KEY_NOT_FOUND;
}

/* 
 * forcePage: Immediately writes the contents of the page (if dirty) back to disk.
 */
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    if (bm == NULL || bm->mgmtData == NULL || page == NULL) {
         return RC_FILE_HANDLE_NOT_INIT;
    }
    BM_MgmtData *mgmt = (BM_MgmtData *) bm->mgmtData;
    for (int i = 0; i < mgmt->numFrames; i++) {
         if (mgmt->frames[i].pageNum == page->pageNum) {
              RC rc = writeBlock(mgmt->frames[i].pageNum, &mgmt->fileHandle, mgmt->frames[i].data);
              if (rc != RC_OK) return rc;
              mgmt->frames[i].dirty = false;
              mgmt->writeIO++;
              return RC_OK;
         }
    }
    printf("Error: forcePage: Page not found in buffer pool.\n");
    return RC_IM_KEY_NOT_FOUND;
}

/* 
 * pinPage: Brings the requested page into the buffer pool (if not already present) and pins it.
 * If the page is not in memory, an available (or victim) frame is chosen using the replacement strategy.
 * If the victim is dirty, it is written back to disk before the new page is read.
 */
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
    if (bm == NULL || bm->mgmtData == NULL || page == NULL) {
         return RC_FILE_HANDLE_NOT_INIT;
    }
    if (pageNum < 0) {
         printf("Error: pinPage: Negative page number.\n");
         return RC_READ_NON_EXISTING_PAGE;
    }
    
    BM_MgmtData *mgmt = (BM_MgmtData *) bm->mgmtData;
    mgmt->time++; // update global time

    // Ensure the file has enough pages for the requested page.
    if (pageNum >= mgmt->fileHandle.totalNumPages) {
         RC rc = ensureCapacity(pageNum + 1, &mgmt->fileHandle);
         if (rc != RC_OK) return rc;
    }
    
    // Check if the requested page is already in the pool.
    for (int i = 0; i < mgmt->numFrames; i++) {
         if (mgmt->frames[i].pageNum == pageNum) {
              mgmt->frames[i].fixCount++;
              mgmt->frames[i].lastUsed = mgmt->time;
              page->pageNum = pageNum;
              page->data = mgmt->frames[i].data;
              return RC_OK;
         }
    }
    
    // Look for an empty frame.
    int victim = -1;
    for (int i = 0; i < mgmt->numFrames; i++) {
         if (mgmt->frames[i].pageNum == NO_PAGE) {
              victim = i;
              break;
         }
    }
    if (victim == -1) {
         // No empty frame found; select a victim among frames with fixCount 0.
         int minMetric = -1;
         for (int i = 0; i < mgmt->numFrames; i++) {
              if (mgmt->frames[i].fixCount == 0) {
                   int metric = 0;
                   switch(bm->strategy) {
                        case RS_FIFO:
                             metric = mgmt->frames[i].loadTime;
                             break;
                        case RS_LRU:
                        case RS_LRU_K:
                             metric = mgmt->frames[i].lastUsed;
                             break;
                        default:
                             metric = mgmt->frames[i].loadTime;
                             break;
                   }
                   if (minMetric == -1 || metric < minMetric) {
                        minMetric = metric;
                        victim = i;
                   }
              }
         }
    }
    if (victim == -1) {
         printf("Error: pinPage: No available frame to evict (all pages are pinned).\n");
         return RC_IM_NO_MORE_ENTRIES;
    }
    
    /* Evict victim frame if it is not empty */
    if (mgmt->frames[victim].pageNum != NO_PAGE) {
         if (mgmt->frames[victim].fixCount != 0) {
              printf("Error: pinPage: Selected victim frame is pinned.\n");
              return RC_IM_NO_MORE_ENTRIES;
         }
         if (mgmt->frames[victim].dirty) {
              RC rc = writeBlock(mgmt->frames[victim].pageNum, &mgmt->fileHandle, mgmt->frames[victim].data);
              if (rc != RC_OK) return rc;
              mgmt->frames[victim].dirty = false;
              mgmt->writeIO++;
         }
    }
    
    /* Read the requested page from disk into the victim frame */
    RC rc = readBlock(pageNum, &mgmt->fileHandle, mgmt->frames[victim].data);
    if (rc != RC_OK) return rc;
    mgmt->readIO++;
    
    mgmt->frames[victim].pageNum = pageNum;
    mgmt->frames[victim].fixCount = 1; // page is now pinned
    mgmt->frames[victim].dirty = false;
    mgmt->frames[victim].loadTime = mgmt->time;
    mgmt->frames[victim].lastUsed = mgmt->time;
    
    page->pageNum = pageNum;
    page->data = mgmt->frames[victim].data;
    return RC_OK;
}

/* 
 * getFrameContents: Returns an array (of size numPages) with the page numbers stored in each frame.
 * An empty frame is represented by NO_PAGE.
 */
PageNumber *getFrameContents (BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) return NULL;
    BM_MgmtData *mgmt = (BM_MgmtData *) bm->mgmtData;
    PageNumber *contents = (PageNumber *) malloc(sizeof(PageNumber) * mgmt->numFrames);
    for (int i = 0; i < mgmt->numFrames; i++) {
         contents[i] = mgmt->frames[i].pageNum;
    }
    return contents;
}

/* 
 * getDirtyFlags: Returns an array (of size numPages) of bools indicating whether each frame is dirty.
 */
bool *getDirtyFlags (BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) return NULL;
    BM_MgmtData *mgmt = (BM_MgmtData *) bm->mgmtData;
    bool *flags = (bool *) malloc(sizeof(bool) * mgmt->numFrames);
    for (int i = 0; i < mgmt->numFrames; i++) {
         flags[i] = mgmt->frames[i].dirty;
    }
    return flags;
}

/* 
 * getFixCounts: Returns an array (of size numPages) with the fix counts for each frame.
 */
int *getFixCounts (BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) return NULL;
    BM_MgmtData *mgmt = (BM_MgmtData *) bm->mgmtData;
    int *fixCounts = (int *) malloc(sizeof(int) * mgmt->numFrames);
    for (int i = 0; i < mgmt->numFrames; i++) {
         fixCounts[i] = mgmt->frames[i].fixCount;
    }
    return fixCounts;
}

/* 
 * getNumReadIO: Returns the number of pages read from disk.
 */
int getNumReadIO (BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) return -1;
    BM_MgmtData *mgmt = (BM_MgmtData *) bm->mgmtData;
    return mgmt->readIO;
}

/* 
 * getNumWriteIO: Returns the number of pages written to disk.
 */
int getNumWriteIO (BM_BufferPool *const bm) {
    if (bm == NULL || bm->mgmtData == NULL) return -1;
    BM_MgmtData *mgmt = (BM_MgmtData *) bm->mgmtData;
    return mgmt->writeIO;
}
