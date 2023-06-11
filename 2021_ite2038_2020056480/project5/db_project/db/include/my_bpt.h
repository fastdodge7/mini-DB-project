#ifndef MY_BPT_H
#define MY_BPT_H

#include "page.h"
#include "page_manager.h"
#include "buffer.h"
#include "trx.h"

int64_t open_table(char* pathname);

int db_insert(int64_t table_id, int64_t key, char* value, uint16_t val_size);

int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size, int trx_id);

int db_update(int64_t table_id, int64_t key, char* values, 
              uint16_t new_val_size, uint16_t* old_val_size, int trx_id);

int db_delete(int64_t table_id, int64_t key);

int init_db(int num_buf);

int shutdown_db();


pagenum_t find_root_pagenum(int64_t table_id);
pagenum_t find_leaf( pagenum_t root, int64_t table_id, int key, bool verbose );

#endif