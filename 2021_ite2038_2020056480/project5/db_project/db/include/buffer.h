#ifndef BUFFER_H
#define BUFFER_H

#include "page.h"
#include "file.h"
#include "trx.h"
#include <iostream>
#include <map>
#include <unordered_map>
#include <cstring>

using namespace std;

const int IS_LRU_HEADER = -99;
const int IS_LRU_TAIL = -90;
const int IS_NOT_IN_LRU_LIST = -999;
const int IS_HEADER_PAGE = -9999;

typedef class buffer_frame{
public:
    page_t p_frame_;
    int64_t table_id_;
    pagenum_t pagenum_;
    bool is_dirty_;
    int64_t is_pinned_;

    pthread_mutex_t page_latch;

    int next_of_LRU_; // dobuly linked list 형태? 헤더가 하나 필요할 듯. -> 헤더 페이지!
    int prev_of_LRU_;
    bool is_activated_; // 사용되는 프레임인지 나타내줌.
    bool is_header_; // 헤더인지 나타내줌.

    int index_;

    buffer_frame() : is_dirty_(false), is_pinned_(0), is_activated_(false), is_header_(false), index_(-1)
    {
        pthread_mutex_init(&page_latch, NULL); 
    }
    buffer_frame(int64_t table_id, pagenum_t pagenum, int index) 
    : table_id_(table_id), pagenum_(pagenum), is_dirty_(false), is_pinned_(0), is_activated_(true), is_header_(false), index_(index)
    {
        if(pagenum == 0)
        {
            file_read_header_page(table_id, &p_frame_);
            is_header_ = true;
            next_of_LRU_ = IS_HEADER_PAGE;
            prev_of_LRU_ = IS_HEADER_PAGE;
        }
        else if(pagenum > 0)
        {
            file_read_page(table_id, pagenum, &p_frame_);
        }
        else
        {
            cerr << "Error on buffer frame constructor : page number must be non-negative. (header == 0)\n";
            cerr << "Input pagenum : " << pagenum << "\n";
            exit(EXIT_FAILURE);
        }
        pthread_mutex_init(&page_latch, NULL);
    }
    ~buffer_frame()
    {
        //pthread_mutex_destroy(&page_latch);
        // 버퍼 레이어 자체를 셧다운할 때 뮤텍스를 풀어야 할 듯.
    }
    
    void flush()
    {
        file_write_page(table_id_, pagenum_, &p_frame_);
    }

    void set_data(int64_t table_id, pagenum_t pagenum, int index) 
    {
        table_id_ = table_id;
        pagenum_ = pagenum;
        is_dirty_ = false;
        is_pinned_ = 0;
        is_activated_ = true;
        is_header_ = false;
        index_ = index;
        if(pagenum == 0)
        {
            is_header_ = true;
            next_of_LRU_ = IS_HEADER_PAGE;
            prev_of_LRU_ = IS_HEADER_PAGE;
        }
        else if(pagenum < 0)
        {
            cerr << "Error on buffer frame constructor : page number must be non-negative. (header == 0)\n";
            cerr << "Input pagenum : " << pagenum << "\n";
            exit(EXIT_FAILURE);
        }
    }
    void set_page_data(int64_t table_id, pagenum_t pagenum)
    {
        if(pagenum == 0)
        {
            file_read_header_page(table_id, &p_frame_);
        }
        else if(pagenum > 0)
        {
            file_read_page(table_id, pagenum, &p_frame_);
        }
        else
        {
            cerr << "Error on buffer frame constructor : page number must be non-negative. (header == 0)\n";
            cerr << "Input pagenum : " << pagenum << "\n";
            exit(EXIT_FAILURE);
        }
    }
}buffer_frame;

int buffer_init(int num_buf);
int64_t buffer_open_table_file(char* path);
pagenum_t buffer_alloc_page(int64_t table_id); // 버퍼에 페이지를 요청하는 함수.

page_t* buffer_read_page(int64_t table_id, pagenum_t pagenum);
void buffer_write_page(int64_t table_id, pagenum_t pagenum, page_t* src);

void buffer_free_page(int64_t table_id, pagenum_t pagenum); // 버퍼에 사용한 페이지를 반환하는 함수 (unpin 등의 작업)
int buffer_flush();

//void buffer_return(int64_t table_id, pagenum_t pagenum);
void buffer_return(page_t* page);
int buffer_shutdown();

//void buffer_set_dirty(int64_t table_id, pagenum_t pagenum);
void buffer_set_dirty(page_t* page);

#endif