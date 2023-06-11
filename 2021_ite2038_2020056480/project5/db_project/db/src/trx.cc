#include "trx.h"
#include "my_bpt.h"
#include <iostream>
#include <unordered_map>
#include <vector>
#include <pthread.h>

using namespace std;
/* Structures for Lock */

/*
Todo : 버퍼 프레임 뮤텍스를 rwlock으로 바꿔보기
*/


unordered_map<pair<int64_t, pagenum_t>, lock_header_t> lock_table; // collision 등을 알아서 잘 처리해 주겠지!
pthread_mutex_t lock_table_mutex = PTHREAD_MUTEX_INITIALIZER;


/* Structures for Lock  END*/


/* Structures for TRX */


int TRX_ID_GEN = 0;
unordered_map<int,transaction_t*> trx_table;
transaction_t* get_trx(int trx_id) { return trx_table[trx_id]; }


pthread_mutex_t trx_latch = PTHREAD_MUTEX_INITIALIZER;
void trx_latch_lock() {pthread_mutex_lock(&trx_latch);}
void trx_latch_unlock() {pthread_mutex_unlock(&trx_latch);}
//pthread_rwlock_t trx_latch = PTHREAD_RWLOCK_INITIALIZER;

/* Structures for TRX END */


/* Helper functions declarations start*/

// 두 락 모드가 다르면 true, 두 락이 모두 X lock이면 true, 나머지는 false.
bool is_conflict(int left_lock_mode, int right_lock_mode); 

// 현재 lock waiting list에서 지금 lock obj의 락 모드와 충돌하는 락이 있으면 
lock_t* find_conflict_lock(lock_header_t *lst, lock_t* cur_lock);
lock_t* find_conflict_lock(lock_header_t *lst, int64_t record_id, int lock_mode,int trx_id);
// 내부적으로 뮤텍스를 잡지 않음에 주의.

lock_t* find_common_slock(lock_t* cur_lock);

void set_waiting(lock_t* releasing_lock);

/* Helper functions declarations end*/




int init_lock_table() {
    lock_table = unordered_map<pair<int64_t,pagenum_t>, lock_header_t>();
    return 0;
}


/**
 * @brief 
 * 무엇을 하는가?
 * 락 삽입
 * 트랜잭션 락 리스트 갱신
 * 데드락 탐지
 */
lock_t* lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int lock_mode) // 같은 자료에서 넣었다 뺐다 하는 경우?
{ 
    /*    Locking mutex of lock_table    */
    if(pthread_mutex_lock(&lock_table_mutex) < 0)
    {
        cerr << "Error in getting locking table mutex\n";
        return nullptr;
    }
    if(lock_table.find({table_id, page_id}) == lock_table.end())
        lock_table.insert( {{table_id, page_id}, lock_header_t(table_id, page_id)} );
    
    lock_header_t* lst = &lock_table[{table_id, page_id}];
    pthread_mutex_unlock(&lock_table_mutex);
    /*    Unlocking mutex of lock_table     */
    pthread_mutex_lock(&trx_latch); // rwlock으로 걸어서 문제가 발생할 수도 있음.
    auto iter = trx_table.find(trx_id);
    if(iter == trx_table.end()) // error handling
    {
        cerr << "Wrong transaction ID : " << trx_id << "\n";
        pthread_mutex_unlock(&trx_latch);
        return nullptr;
    }

    if(pthread_mutex_lock(&lst->list_mutex) < 0) // Locking mutex of lock_header.
    {
        cerr << "Error in locking header mutex\n";
        pthread_mutex_unlock(&lock_table_mutex);
        return nullptr;
    }

    lock_t* iterator = lst->head;
    while(iterator != nullptr)
    {
        if( iterator->owner_trx_id == trx_id && iterator->record_id == key)
        {
            if(iterator->lock_mode == S_LOCK && lock_mode == X_LOCK)
            {
                //case #1 : 만난 S락 뒤에, 나랑 충돌하는 락이 아무것도 없음. -> 만난 락을 promotion 하고, 앞에 S락 없으면 그걸 리턴, 있으면 기다림
                //case #2 : 만난 S락 뒤에, 내가 걸어놓은 X락이 존재함. -> 해당 X락을 리턴함.
                //case #3 : 만난 S락 뒤에, 다른애가 걸어놓은 나랑 충돌하는 락이 존재함 -> 전체 while문 탈출.
                lock_t* temp = iterator->next;
                bool flag = false;
                while(temp != nullptr)
                {   
                    if(temp->record_id == key) // 충돌이 존재한다.
                    {
                        if(temp->owner_trx_id == trx_id && temp->lock_mode == X_LOCK)
                        {
                            pthread_mutex_unlock(&trx_latch);
                            pthread_mutex_unlock(&lst->list_mutex);
                            return temp; //case #2
                        }
                        else if(temp->owner_trx_id != trx_id) //case #3
                        {
                            flag = true;
                            break;
                        }
                    }
                    temp = temp->next;
                }
                if(flag) break;
                //cerr << "Lock Promotion : " << iterator->record_id << " / TrxID : " << iterator->owner_trx_id << "\n";
                lock_t* to_wait = find_common_slock(iterator);

                
                if(to_wait != nullptr)
                    break;
                
                else
                    iterator->lock_mode = X_LOCK;
               
            }
            pthread_mutex_unlock(&trx_latch);
            pthread_mutex_unlock(&lst->list_mutex);
            return iterator;
        }
        iterator = iterator->next;
    }

    
    /* trx table locking part */
    
    transaction_t* owner_trx = iter->second;
    // new Lock obj creation
    lock_t* new_lock = new lock_t(lst, key, lock_mode, owner_trx->trx_id);
    /* TRX lock add */
    owner_trx->lock_add(new_lock);

    // append a new lock obj at end of lock waiting list
    if(lst->head == nullptr && lst->tail == nullptr) // waiting list가 없는 경우 -> 여기에 나중에 implicit locking 관련 수정 들어갈듯?
    {
        new_lock->prev = nullptr;
        new_lock->next = nullptr;
        lst->head = new_lock;
        lst->tail = new_lock;
    }
    else if(lst->head != nullptr && lst->tail != nullptr)
    {
        new_lock->prev = lst->tail;
        new_lock->next = nullptr;
        lst->tail->next = new_lock; // 왜 세그폴트?
        lst->tail = new_lock;
    }
    else // Error
    {
        cerr << "Fatal Error : head / tail pointer of lock_header is broken!\n;";
        pthread_mutex_unlock(&lst->list_mutex); 
        pthread_mutex_unlock(&trx_latch);
        return nullptr;
    }//if end

    // cerr << "Lock Acquired : " << new_lock->record_id 
    //      << " / Mode : " << new_lock->lock_mode << " / TrxID : " << new_lock->owner_trx_id
    //      << " ---->  waiting : ";
    lock_t *prev_conflict_lock = find_conflict_lock(lst, new_lock);
    

    if(prev_conflict_lock == nullptr) // 내가 처음인 경우 -> 내 앞에 충돌하는 락 오브젝트가 없는 경우
    {
        // 고려할것 -> implicit lock에 대한 lock promotion
        // next of trx list 관리
        //cerr << "nothing\n";


        pthread_mutex_unlock(&lst->list_mutex); // Unlocking mutex of lock_header.
        pthread_mutex_unlock(&trx_latch);
        return new_lock;
    }
    else
    {
        //deadlock detection
        //cerr << "TrxID : " << prev_conflict_lock->owner_trx_id << " / Mode : " << prev_conflict_lock->lock_mode << "\n";
        //lock_t* first_conflicting_lock = find_waiting_trx(new_lock);
        if(prev_conflict_lock != nullptr)
        {
            owner_trx->i_waiting_for = prev_conflict_lock->owner_trx_id;
            if(trace_graph(owner_trx->trx_id) == -1)
            {
                //cerr << "#" << owner_trx->trx_id << " Transaction should be abort!\n";
                pthread_mutex_unlock(&lst->list_mutex);
                trx_abort(owner_trx->trx_id);
                pthread_mutex_unlock(&trx_latch);
                return nullptr;
            }
        }
        new_lock->am_waiting_for = prev_conflict_lock->im_done;
        pthread_mutex_unlock(&trx_latch);
        pthread_cond_wait(prev_conflict_lock->im_done, &lst->list_mutex);
        pthread_mutex_unlock(&lst->list_mutex); 
        /**
         *                           list mutex 굳이 unlock하는 이유 
         * 다른 트랜잭션이 commit 혹은 abort 하는 과정에서 이 락을 릴리즈 해서 얘가 깨어난 경우
         * 얘가 리스트 락을 안놓으면 commit 혹은 abort 하는 트랜잭션이 trx_latch를 잡고 있는데,
         * 그 과정에서 이 페이지에 있는 다른 락을 해제해야 하는 경우, 지금 이 락이 들고있는 list mutex를
         * 잡아오지 못함. 따라서 commit / abort가 완전히 끝난 후 conflict를 체크하기 위해 풀어야함.
         */

        // 앞에서 시그널 받고 깨어남
        pthread_mutex_lock(&trx_latch);
        pthread_mutex_lock(&lst->list_mutex); 
        // lock_t* check = new_lock->prev;
        lock_t* check = lst->head;
        while(check != nullptr && check != new_lock) // 깨어난 뒤 해야할 일 : 내 앞쪽 락들을 스캔하면서 또 기다려야 할 애가 있는지 확인해야함.
        {
            // 찾았다 세그폴트
            if(check->record_id == new_lock->record_id && is_conflict(check->lock_mode, new_lock->lock_mode))
            {
                if(check->owner_trx_id != new_lock->owner_trx_id)
                {   // 나랑 충돌하는 또 다른 락이 존재하는 경우
                    owner_trx->i_waiting_for = check->owner_trx_id;
                    if(trace_graph(owner_trx->trx_id) == -1)
                    {   // deadlock 탐지
                        //cerr << "#" << owner_trx->trx_id << " Transaction should be abort!\n";
                        pthread_mutex_unlock(&lst->list_mutex);
                        trx_abort(owner_trx->trx_id);
                        pthread_mutex_unlock(&trx_latch);
                        return nullptr;
                    }
                    // 데드락이 아니면 다시 기다림.
                    new_lock->am_waiting_for = check->im_done;
                    pthread_mutex_unlock(&lst->list_mutex);
                    pthread_cond_wait(check->im_done, &trx_latch);
                    pthread_mutex_lock(&lst->list_mutex);
                }
                else
                {   //S - X 상태인 경우 -> 그냥 lock promotion
                    break;
                }
                check = lst->head;
                continue;
            }
            check = check->next;
        }
         
        owner_trx->i_waiting_for = -1;
        pthread_mutex_unlock(&lst->list_mutex); 
        pthread_mutex_unlock(&trx_latch);
        
        return new_lock;
    }
}





int lock_release(lock_t* lock_obj) 
{
    if(pthread_mutex_lock(&lock_obj->lst->list_mutex) < 0) // Locking mutex of lock_header.
    {
        cerr << "Error in locking header mutex\n";
        return -1;
    } 
    /* Finding trx END */

    /**
     * @brief lock release 가능 조건의 수정이 필요함
     * waiting list의 맨 앞 요소만 지울 수 있게 설정해 놨다.
     * -> 이게 이제는 Abort 기능이 추가되면서 acquire 하지 않은 lock obj도 release할 수 있어야 한다.
     * -> common shared Slock이 존재하면 내 다음 common shared Slock의 conditional variable을 swap해야 한다.
     */

    if(lock_obj->prev == nullptr && lock_obj == lock_obj->lst->head)
    {   // 현재 오브젝트가 락 리스트의 맨 앞에 위치하는 경우
        if(lock_obj->next == nullptr) // 현재 오브젝트가 유일한 오브젝트인 경우.
        {
            lock_obj->lst->head = nullptr;
            lock_obj->lst->tail = nullptr;
        }
        else
        {
            lock_obj->lst->head = lock_obj->next;
            lock_obj->next->prev = nullptr;
        }
    }
    else if(lock_obj->prev != nullptr && lock_obj != lock_obj->lst->head)
    {
        // 현재 오브젝트가 리스트 중간, 혹은 마지막에 존재하는 경우
        if(lock_obj->next == nullptr)  // 리스트 마지막 원소일 경우
        {
            lock_obj->lst->tail = lock_obj->prev;
            lock_obj->prev->next = nullptr;
        }
        else
        {
            lock_obj->prev->next = lock_obj->next;
            lock_obj->next->prev = lock_obj->prev;
        }
    }
    else
    {
        cerr << "Fatal Error : You attempted to release the lock object in waiting list.\n";
        cerr << "lock table tid : " <<lock_obj->lst->table_id 
             << " / rid : " <<lock_obj->lst->record_id << "\n"; 
        pthread_mutex_unlock(&lock_obj->lst->list_mutex);
        return -1;
    }

    set_waiting(lock_obj);
    pthread_cond_broadcast(lock_obj->im_done);
    pthread_mutex_unlock(&lock_obj->lst->list_mutex);   // Unlocking mutex of lock_header.

    pthread_cond_destroy(lock_obj->im_done);
    delete lock_obj->im_done;
    delete lock_obj;
    return 0;
}
/*
/
우선, pthread_cond_wait 을 이용해서 쓰레드를 재우는 기능을 구현해야 하는것은 명백해 보인다.
그럼, 얘는 wait을 때리기 전에 뮤텍스 하나를 잡아줘야 한다.
단순히 헤더의 뮤텍스를 잡는건 어려워보임. 여러 락 오브젝트가 하나의 뮤텍스에 락 대기를 걸면 순차적으로 준다는 보장이 없음.

중요한 포인트 2개
- 쓰레드 하나가 뮤텍스를 잡고 있을 때, 어떻게 다른 쓰레드를 재울것인가? -> 즉, 다른 쓰레드가 엑세스하려는 자원에 대한 뮤텍스를 잡고 있으면,
lock_acquire가 끝나지 않고 기다려줘야 한다.

- 뮤텍스를 잡고 있던 쓰레드가 뮤텍스를 놓았을 때, 어떻게 '순차적으로' 뮤텍스를 넘겨준다는 것을 보장할 것인가.


현재까지 발견한 오류들
1. release 과정에서 head와 tail 포인터가 망가져 있는 경우가 있음. (else 부분)
2. 지가 걸어놓은 뮤텍스를 지가 못가져옴 -> 락테이블 뮤텍스를 잡고, 그 안에서 리스트 뮤텍스를 잡아와야 하는데 리스트 뮤텍스를 못가져옴
3. 주석 달아놓은 부분에서 세그폴트 에러가 남. 아마 nullptr 참조 오류인듯.

*/

/* Transaction function definition START */

int trx_begin() // trx table을 탐색하는 과정에서도 lock을 걸 필요가 있을 듯.
{
    pthread_mutex_lock(&trx_latch);
    transaction_t* new_trx = new transaction_t(++TRX_ID_GEN);
    trx_table.insert({new_trx->trx_id, new_trx});
    pthread_mutex_unlock(&trx_latch);
    return new_trx->trx_id;
}
int trx_commit(int trx_id)
{
    pthread_mutex_lock(&trx_latch);
    //pthread_mutex_lock(&lock_table_mutex);
    // for(auto it : trx_table)
    // {
    //     cerr << "Before: Trx #" << it.second->trx_id << " is now waiting #" << it.second->i_waiting_for << "\n";
    // }

    auto pos = trx_table.find(trx_id);
    if(pos == trx_table.end()) 
    {
        pthread_mutex_unlock(&trx_latch);
        return 0;
    }

    for(auto i : pos->second->values)
        delete[] i;
    
    lock_t* iter = pos->second->trx_lock_head;
    while(iter != nullptr)
    {
        pos->second->lock_release(iter);
        lock_t* temp = iter->next_of_trx_list;
        lock_release(iter);
        iter = temp;
    }
    // for(auto it : trx_table)
    // {
    //     if(it.second->i_waiting_for == trx_id)
    //     {
    //         it.second->i_waiting_for = -1;
    //     }
    // }
    /**
     * 
     * 락 해제 등의 코드들 필요.
     * 
     */
    delete pos->second;
    trx_table.erase(pos);
    // cerr << "TRX #" << trx_id << " was commited\n";

    // for(auto it : trx_table)
    // {
    //     cerr << "After: Trx #" << it.second->trx_id << " is now waiting #" << it.second->i_waiting_for << "\n";
    // }

    //pthread_mutex_unlock(&lock_table_mutex);
    pthread_mutex_unlock(&trx_latch);
    return trx_id;
}



int trx_abort(int trx_id)
{
    //pthread_mutex_lock(&lock_table_mutex);
    auto pos = trx_table.find(trx_id);
    if(pos == trx_table.end()) return 0;

    pos->second->i_waiting_for = -1;

    transaction_t* target_trx = pos->second;

    /*   모든 변경사항을 되돌림    */
    for(int i = (target_trx->position_key).size() - 1; i >= 0; i--)
    {
        pagenum_t root = find_root_pagenum(target_trx->position_table_id[i]);
        pagenum_t target = find_leaf(root, target_trx->position_table_id[i], target_trx->position_key[i], false);
        Leaf_page_manager target_page = Leaf_page_manager(target, target_trx->position_table_id[i]);
        for(int k = 0; k < target_page.get_num_of_key(); k++)
        {
            if(target_trx->position_key[i] == target_page.get_key(k))
            {
                Leaf_page_manager::Slot ts = target_page.get_slot(k);
                short bin;
                target_page.update_record_at(k, target_trx->values[i], ts.size, &bin);
                break;
            }
        } 
        target_page.flush(-1);
    }


    for(auto i : pos->second->values)
        delete[] i;
    
    lock_t* iter = pos->second->trx_lock_head;
    // for(auto it : trx_table)
    // {
    //     if(it.second->i_waiting_for == trx_id)
    //     {
    //         it.second->i_waiting_for = -1;
    //     }
    // }
    // for(auto it : trx_table)
    // {
    //     cerr << "Before : Trx #" << it.second->trx_id << " is now waiting #" << it.second->i_waiting_for << "\n";
    // }
    while(iter != nullptr)
    {
        pos->second->lock_release(iter);
        lock_t* temp = iter->next_of_trx_list;
        lock_release(iter);
        iter = temp;
    }

    delete pos->second;
    trx_table.erase(pos);
    // for(auto it : trx_table)
    // {
    //     cerr << " After : Trx #" << it.second->trx_id << " is now waiting #" << it.second->i_waiting_for << "\n";
    // }
    //pthread_mutex_unlock(&lock_table_mutex);
    return trx_id;
}



/* Transaction function definition END */

/**
 * @brief Transaction 2PL 구현 방안
 * 트랜잭션 오브젝트에서 자기가 걸어놓은 락을 전부다 기록해 놓는다. X, S 락을 구분할지에 대한 여부는 유보.
 * 릴리즈 하고자 하는 락이 
 */



/* Helper function definition START */
bool is_conflict(int left_lock_mode, int right_lock_mode)
{
    if(left_lock_mode < 0 || right_lock_mode < 0 || left_lock_mode > 1 || right_lock_mode > 1) // 나중에 지우면 될 듯
    {
        cerr << "Lock Mode Error! : "<< left_lock_mode <<", "<< right_lock_mode <<"\n";
        exit(EXIT_FAILURE);
    }
    if(left_lock_mode != right_lock_mode) return true;
    if(left_lock_mode == X_LOCK) return true;
    return false;
}
lock_t* find_conflict_lock(lock_header_t *lst, int64_t record_id, int lock_mode,int trx_id)
{
    // lock_t* iterator = cur_lock->prev;
    lock_t* iterator = lst->head;
    while(iterator != nullptr)
    {
        if((iterator->record_id == record_id && is_conflict(lock_mode, iterator->lock_mode))
            && iterator->owner_trx_id != trx_id)
        {
            return iterator;
        }
        else
        {
            iterator = iterator->next;
            continue;
        }
    }
    return nullptr;
}

lock_t* find_conflict_lock(lock_header_t *lst, lock_t* cur_lock)
{
    // lock_t* iterator = cur_lock->prev;
    lock_t* iterator = lst->head;
    while(iterator != nullptr && iterator != cur_lock)
    {
        if((iterator->record_id == cur_lock->record_id && is_conflict(cur_lock->lock_mode, iterator->lock_mode))
            && iterator->owner_trx_id != cur_lock->owner_trx_id)
        {
            return iterator;
        }
        else
        {
            iterator = iterator->next;
            continue;
        }
    }
    return nullptr;
}


void set_waiting(lock_t* releasing_lock)
{
    lock_t* iter = releasing_lock->next;
    while(iter != nullptr)
    {
        if(iter->am_waiting_for == releasing_lock->im_done)
        {
            //쟤가 만약에 다른 애를 기다리고 있다면?
            if(trx_table[iter->owner_trx_id]->i_waiting_for != releasing_lock->owner_trx_id)
            {
                cerr << "Something weired Thing happened\n";
                cerr << "TRX #" << releasing_lock->owner_trx_id << " tried to wake up TRX #"  << iter->owner_trx_id 
                     << ", but it didn't wait " << "TRX #" <<  releasing_lock->owner_trx_id << "\n";
            }
            else
            {
                trx_table[iter->owner_trx_id]->i_waiting_for = -1;
            }
        }
        iter = iter->next;
    }
}

lock_t* find_waiting_trx(lock_t* cur_lock)
{
    lock_t* iterator = cur_lock->lst->tail->prev;
    while(iterator != nullptr)
    {
        if((iterator->record_id == cur_lock->record_id 
            && is_conflict(cur_lock->lock_mode, iterator->lock_mode))
            && iterator->owner_trx_id != cur_lock->owner_trx_id
            )
        {
            return iterator; // 수정해줘야함
        }
            
        else
        {
            iterator = iterator->prev;
            continue;
        }
    }
    return nullptr;
}


lock_t* find_common_slock(lock_t* cur_lock)
{
    lock_t* iterator = cur_lock->prev;
    while(iterator != nullptr)
    {
        if( iterator->record_id == cur_lock->record_id && 
            iterator->lock_mode == S_LOCK &&
            cur_lock->lock_mode == S_LOCK)
        {
            return iterator;
        }
        else if( iterator->record_id == cur_lock->record_id && 
            iterator->lock_mode == X_LOCK &&
            cur_lock->lock_mode == S_LOCK)
        {
            break;
        }
        else
        {
            iterator = iterator->prev;
            continue;
        }
    }
    return nullptr;
}

int trace_graph(int start_trx)
{
    int iterator = start_trx;

    while(1)//여기
    {
        if(trx_table.find(iterator) == trx_table.end())
        {
            cerr << "error! : Start [" << start_trx << "]" << "Target [" << iterator << "]" <<"\n";
            return 0;
        }
        if(trx_table[iterator]->i_waiting_for == -1) break;
        if(trx_table[iterator]->i_waiting_for == start_trx)
            return -1; //deadlock
        
        iterator = trx_table[iterator]->i_waiting_for;
    }
    return 0;
}
// 날 기다리는 모든 trx에 대해서 waiting for 재설정이 필요할듯


/* Helper function definition END */


/*

<동일 레코드에 대해>

S <-> S <-> S <-> X <-> S <-> S <-> X
와 같은 락 테이블이 걸려있다고 하자.
현재 구현에서는 맨 처음 X락은 바로 앞의 S락의 조건변수를 받으면 acquire된다.
그런데 실제 스펙은 앞의 3개의 락이 모두 해제된 다음 X가 acquire되어야 한다.
이는 S 락을 해제할 때, 자신이 리스트의 헤드가 아니라면 자신의 앞 락 중에서 
동일 레코드, 동일 모드로 잡고있는 락 오브젝트와 condition variable을 swap하여 구현할 수 있을 것 같다.

*/