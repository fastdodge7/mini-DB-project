#ifndef PAGE_H
#define PAGE_H

#include <stdint.h>

using namespace std;

typedef uint64_t pagenum_t;
typedef struct page_t{
    //pagenum_t freepage;
    //char space[1024 * 4 - 8];
    char space[1024 * 4];
}page_t;

typedef struct header_page_t{
    pagenum_t free_page_num;
    uint64_t num_of_pages;
    pagenum_t root_page_num;
    char space[1024 * 4 -24];
}header_page_t;

typedef struct internal_page_t{
    pagenum_t parent_page_num;
    int is_leaf = 0;
    int num_of_key;
    char temp_space[128 - 16 - 8];
    pagenum_t left_most_pagenum;
    char key_value_space[4*1024 - 128];
}internal_page_t;

typedef struct leaf_page_t{
    pagenum_t parent_page_num;
    int is_leaf = 1;
    int num_of_key;
    char temp_space[128 - 16 - 16];
    int64_t amount_of_freespace;
    pagenum_t right_sibling_pagenum;
    char record_space[4*1024 - 128];
}leaf_page_t;



#endif