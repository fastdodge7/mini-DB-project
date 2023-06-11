#ifndef __TRX_H__
#define __TRX_H__

#include "page.h"
#include <pthread.h>
#include <vector>
#include <iostream>

namespace std{
template<>
struct hash<pair<int64_t, pagenum_t>>{
    template <class T1, class T2>
    size_t operator()(const pair<T1, T2>& p) const
    {
        auto hash1 = hash<T1>{}(p.first);
        auto hash2 = hash<T2>{}(p.second);
        return hash1 ^ (hash2<<1);
    }
};
}

typedef struct lock_t lock_t;
typedef struct lock_header_t lock_header_t;
typedef struct transaction_t transaction_t;

typedef struct lock_header_t {
    lock_header_t() : table_id(-1), record_id(-1) { 
        pthread_mutex_init(&list_mutex, NULL); 
        head = nullptr;
        tail = nullptr;
    }

    lock_header_t(int64_t table_id_, pagenum_t page_id_) : table_id(table_id_), page_id(page_id_){
        pthread_mutex_init(&list_mutex, NULL);
        head = nullptr;
        tail = nullptr;
    }
    ~lock_header_t()  { pthread_mutex_destroy(&list_mutex); }

    bool operator== (const lock_header_t& rhs) const
    {
        if(table_id == rhs.table_id && record_id == rhs.record_id) return true;
        else return false;
    }

    int64_t table_id;
    pagenum_t page_id;
    int64_t record_id;
    lock_t *head;
    lock_t *tail;
    pthread_mutex_t list_mutex;

}lock_header_t;

typedef struct lock_t {
    lock_t(lock_header_t *_lst, int64_t record_id_, int lock_mode_, int owner_trx_id_) 
    : lst(_lst), record_id(record_id_), lock_mode(lock_mode_), owner_trx_id(owner_trx_id_)
    { 
        im_done = new pthread_cond_t;
        pthread_cond_init(im_done, NULL); 
        prev = nullptr;
        next = nullptr;
        next_of_trx_list = nullptr;
    }
    void cond_var_destroy()
    {
        pthread_cond_destroy(im_done); 
        delete im_done;
    }
      
    lock_t *prev;
    lock_t *next;
    lock_header_t *lst;
    pthread_cond_t *im_done;

    //p5
    int lock_mode;
    int64_t record_id;
    int owner_trx_id;
    lock_t *next_of_trx_list;
    pthread_cond_t *am_waiting_for;
}lock_t;

typedef struct transaction_t{
    transaction_t(){}
    transaction_t(int trx_id_) : trx_id(trx_id_), trx_lock_head(nullptr), i_waiting_for(-1), im_sleep(false), trx_lock_tail(nullptr)
    {
        position_key = vector<int64_t>();
        values = vector<char*>();
    }
    // 둘다 뮤텍스를 걸지 않음에 주의
    void lock_add(lock_t* new_lock)
    {
        if(trx_lock_head == trx_lock_tail && trx_lock_head == nullptr) // trx 락 리스트가 비어있는 경우
        {
            trx_lock_head = trx_lock_tail = new_lock;
            new_lock->next_of_trx_list = nullptr;
        }
        else if( //trx 락 리스트가 비어있지 않은 경우
                trx_lock_head != nullptr &&
                trx_lock_tail != nullptr
                )
        {
            trx_lock_tail->next_of_trx_list = new_lock;
            trx_lock_tail = new_lock;
            new_lock->next_of_trx_list = nullptr;
        }
        else
        {
            cerr << "Trx lock list is smashed!\n";
            exit(EXIT_FAILURE);
        } 
    }

    void lock_release(lock_t* target_lock)
    {
        // if(target_lock != trx_lock_head)
        // {
        //     cerr << "헤더부터 릴리즈 하는게 좋을 것 같은데..?\n";
        //     return;
        // }
        if(trx_lock_head == trx_lock_tail && trx_lock_head == nullptr)
            cerr << "Trx lock list is empty now!\n";
        
        else if(trx_lock_head == trx_lock_tail) // 락 리스트에 하나만 있는 경우
        {
            trx_lock_head = nullptr;
            trx_lock_tail = nullptr;
        }
        else if(trx_lock_head != trx_lock_tail && (trx_lock_head != nullptr && trx_lock_tail != nullptr)) // 두개 이상 있는 경우
        {
            if(target_lock == trx_lock_head)
            {
                trx_lock_head = target_lock->next_of_trx_list;
            }
            else
            {
                if(target_lock == trx_lock_tail) trx_lock_tail = find_prev(target_lock);
                find_prev(target_lock)->next_of_trx_list = target_lock->next_of_trx_list;
            }
        }
            

        else
            cerr << "Trx lock list is smashed!\n";
    }
    lock_t* find_prev(lock_t* target_lock)
    {
        lock_t* iter = trx_lock_head;
        while(iter->next_of_trx_list != target_lock)
        {
            iter = iter->next_of_trx_list;
        }
        return iter;
    }

    int trx_id;
    int i_waiting_for;
    bool im_sleep;
    vector<int64_t> position_table_id;
    vector<int64_t> position_key;
    vector<char*> values;
    lock_t* trx_lock_head;
    lock_t* trx_lock_tail;
}transaction_t;

const int S_LOCK = 0;
const int X_LOCK = 1;


/* APIs for lock table */
int init_lock_table();
lock_t *lock_acquire(int64_t table_id,pagenum_t page_id, int64_t key, int trx_id, int lock_mode);
int lock_release(lock_t* lock_obj);

int trx_begin();
int trx_commit(int trx_id);
int trx_abort(int trx_id);
int trace_graph(int start_trx);


transaction_t* get_trx(int trx_id);
void trx_latch_lock();
void trx_latch_unlock();

#endif /* __TRX_H__ */
