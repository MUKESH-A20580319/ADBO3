#include "storage_mgr.h"
#include "dberror.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdint.h>
#define _POSIX_C_SOURCE 200809L

#define FILE_PERMISSIONS S_IRUSR | S_IWUSR

// Initializes the storage system
void initStorageManager(void) {
    printf("Storage Manager initialized successfully.\n");
    printf("Ready to manage page files and handle operations.\n");
}

// Helper function to check if a file path is valid
static RC validateFilePath(const char *filePath) {
    if (filePath == NULL) {
        printf("Error: Invalid file path.\n");
        return RC_FILE_NOT_FOUND;
    }
    return RC_OK;
}

// Helper function to check if a file exists
static int fileExists(const char *filePath) {
    return access(filePath, F_OK) == 0;
}

// Helper function to delete a file from storage
static RC deleteFile(const char *filePath) {
    if (unlink(filePath) == 0) {
        return RC_OK;
    } else {
        perror("Error deleting file");
        return RC_FILE_NOT_FOUND;
    }
}

// Logs file operations for debugging
static void logFileOperation(const char *operation, const char *filePath) {
    printf("LOG: %s operation performed on file: %s\n", operation, filePath);
}

// Deletes a file from storage with additional helper functions
RC destroyPageFile(char *filePath) {
    // Validate file path
    if (validateFilePath(filePath) != RC_OK) {
        return RC_FILE_NOT_FOUND;
    }
    
    // Log operation
    logFileOperation("DELETE", filePath);
    
    // Check if file exists before deletion
    if (!fileExists(filePath)) {
        printf("Error: File does not exist.\n");
        return RC_FILE_NOT_FOUND;
    }
    
    // Attempt to delete the file
    return deleteFile(filePath);
}

// Creates a new page file and initializes it with an empty page
RC createPageFile(char *filePath) {
    if (validateFilePath(filePath) != RC_OK) {
        return RC_FILE_NOT_FOUND;
    }
    logFileOperation("CREATE", filePath);

    int fd = open(filePath, O_RDWR | O_CREAT | O_TRUNC, FILE_PERMISSIONS);
    if (fd == -1) {
        perror("Error creating file");
        return RC_FILE_NOT_FOUND;
    }

    SM_PageHandle emptyBuffer = (SM_PageHandle)malloc(PAGE_SIZE);
    if (!emptyBuffer) {
        close(fd);
        printf("Error: Memory allocation failed.\n");
        return RC_WRITE_FAILED;
    }

    memset(emptyBuffer, '\0', PAGE_SIZE);
    ssize_t bytesWritten = write(fd, emptyBuffer, PAGE_SIZE);
    free(emptyBuffer);
    close(fd);

    return (bytesWritten == PAGE_SIZE) ? RC_OK : RC_WRITE_FAILED;
}

// Opens an existing file and sets up the file handle
RC openPageFile(char *filePath, SM_FileHandle *fileHandle) {
    if (validateFilePath(filePath) != RC_OK || fileHandle == NULL) {
        return RC_FILE_NOT_FOUND;
    }
    logFileOperation("OPEN", filePath);

    int fd = open(filePath, O_RDWR);
    if (fd == -1) {
        perror("Error opening file");
        return RC_FILE_NOT_FOUND;
    }

    struct stat fileStats;
    if (fstat(fd, &fileStats) != 0) {
        close(fd);
        printf("Error: Unable to retrieve file information.\n");
        return RC_FILE_NOT_FOUND;
    }

    fileHandle->totalNumPages = fileStats.st_size / PAGE_SIZE;
    fileHandle->curPagePos = 0;
    fileHandle->fileName = strdup(filePath);
    fileHandle->mgmtInfo = (void *)(intptr_t)fd;
    return RC_OK;
}

// Closes an open file and releases resources
RC closePageFile(SM_FileHandle *fileHandle) {
    if (!fileHandle || !fileHandle->mgmtInfo) {
        printf("Error: File handle is not initialized.\n");
        return RC_FILE_HANDLE_NOT_INIT;
    }
    logFileOperation("CLOSE", fileHandle->fileName);

    close((int)(intptr_t)fileHandle->mgmtInfo);
    free(fileHandle->fileName);
    fileHandle->mgmtInfo = NULL;
    return RC_OK;
}

// Reads a specific page into memory
RC readBlock(int pageIndex, SM_FileHandle *fileHandle, SM_PageHandle buffer) {
    if (!fileHandle || !buffer || pageIndex < 0 || pageIndex >= fileHandle->totalNumPages) {
        printf("Error: Invalid parameters for reading block.\n");
        return RC_READ_NON_EXISTING_PAGE;
    }
    logFileOperation("READ", fileHandle->fileName);

    int fd = (int)(intptr_t)fileHandle->mgmtInfo;
    off_t seekPos = lseek(fd, pageIndex * PAGE_SIZE, SEEK_SET);
    if (seekPos == -1) {
        perror("Error seeking file");
        return RC_READ_NON_EXISTING_PAGE;
    }

    ssize_t bytesRead = read(fd, buffer, PAGE_SIZE);
    return (bytesRead == PAGE_SIZE) ? RC_OK : RC_READ_NON_EXISTING_PAGE;
}

// Reads the first block of a file
RC readFirstBlock(SM_FileHandle *fileHandle, SM_PageHandle buffer) {
    return readBlock(0, fileHandle, buffer);
}

// Writes a page to a specific block
RC writeBlock(int pageIndex, SM_FileHandle *fileHandle, SM_PageHandle buffer) {
    if (!fileHandle || !buffer || pageIndex < 0 || pageIndex >= fileHandle->totalNumPages) {
        printf("Error: Invalid parameters for writing block.\n");
        return RC_WRITE_FAILED;
    }
    logFileOperation("WRITE", fileHandle->fileName);

    int fd = (int)(intptr_t)fileHandle->mgmtInfo;
    if (lseek(fd, pageIndex * PAGE_SIZE, SEEK_SET) == -1) {
        perror("Error seeking file");
        return RC_WRITE_FAILED;
    }
    ssize_t bytesWritten = write(fd, buffer, PAGE_SIZE);

    return (bytesWritten == PAGE_SIZE) ? RC_OK : RC_WRITE_FAILED;
}

// Writes to the first block of a file
RC writeFirstBlock(SM_FileHandle *fileHandle, SM_PageHandle buffer) {
    return writeBlock(0, fileHandle, buffer);
}

// Writes to the current block of a file
RC writeCurrentBlock(SM_FileHandle *fileHandle, SM_PageHandle buffer) {
    return writeBlock(fileHandle->curPagePos, fileHandle, buffer);
}

// Ensures a file contains at least the specified number of pages
RC ensureCapacity(int requiredPages, SM_FileHandle *fileHandle) {
    if (!fileHandle) {
        printf("Error: Invalid file handle.\n");
        return RC_FILE_HANDLE_NOT_INIT;
    }

    int additionalPagesNeeded = requiredPages - fileHandle->totalNumPages;
    while (additionalPagesNeeded > 0) {
        SM_PageHandle emptyPage = (SM_PageHandle)malloc(PAGE_SIZE);
        if (!emptyPage) {
            printf("Error: Memory allocation failed while ensuring capacity.\n");
            return RC_WRITE_FAILED;
        }
        memset(emptyPage, '\0', PAGE_SIZE);
        int fd = (int)(intptr_t)fileHandle->mgmtInfo;
        lseek(fd, 0, SEEK_END);

        ssize_t bytesWritten = write(fd, emptyPage, PAGE_SIZE);
        free(emptyPage);

        if (bytesWritten != PAGE_SIZE) {
            printf("Error: Could not append new page.\n");
            return RC_WRITE_FAILED;
        }
        fileHandle->totalNumPages++;
        additionalPagesNeeded--;
    }
    return RC_OK;
}
