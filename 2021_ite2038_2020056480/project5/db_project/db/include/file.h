#ifndef FILE_H
#define FILE_H 

#include "page.h"

// Open existing database file or create one if not existed.
int64_t file_open_table_file(const char* path);

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int64_t table_id);

// Free an on-disk page to the free page list
void file_free_page(int64_t table_id, pagenum_t pagenum);

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int64_t table_id, pagenum_t pagenum, page_t* dest);

// Write an in-memory page(src) to the on-disk page
void file_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src);

// Close the database file
void file_close_table_file();

void file_read_header_page(int64_t table_id, page_t* dest);
void file_write_header_page(int64_t table_id, page_t* src);

int is_alloc(int64_t table_id, pagenum_t pagenum);
pagenum_t get_next_free_page(int64_t table_id, pagenum_t target_page);

#endif