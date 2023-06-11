#include "buffer.h"
#include <unordered_set>
struct hash_pair {
    template <class T1, class T2>
    size_t operator()(const pair<T1, T2>& p) const
    {
        auto hash1 = hash<T1>{}(p.first);
        auto hash2 = hash<T2>{}(p.second);
        return hash1 ^ hash2;
    }
};

struct table_page_pair{
    int64_t table_id;
    pagenum_t pagenum;
};

/* Latch start */
pthread_mutex_t buffer_manager_latch = PTHREAD_MUTEX_INITIALIZER;
/* Latch end*/

buffer_frame* main_buffer;
unordered_map<pair<int64_t, pagenum_t>, int, hash_pair> indexer;
//map<pair<int64_t, pagenum_t>, int> indexer;

unordered_set<int> holes;

int NUM_BUF;
int LRU_list_head = -1;
int LRU_list_tail = -2;

int find_hole_index2()
{
    for(int i = 0; i < NUM_BUF; i++) // 리니어 서치
    {
        if(main_buffer[i].is_activated_)
            continue;
        else
            return i;
    }

    return -1; // 버퍼에 빈 공간이 없음.
}
int find_hole_index()
{
    if(holes.size() == 0) return -1;
    else
    {
        auto iter = holes.begin();
        int hole = *iter;
        holes.erase(iter);
        return hole;
    }
    return -1; // 버퍼에 빈 공간이 없음.
}
void buffer_set_dirty(int64_t table_id, pagenum_t pagenum)
{
    auto index_pair = indexer.find({table_id, pagenum});
    if(index_pair == indexer.end())
    {
        cerr << "Error on setting dirty : non exist frame\n";
        return;
    }
    int index = index_pair->second;
    main_buffer[index].is_dirty_ = true;
}
void buffer_set_dirty(page_t* page)
{
    int index = ((buffer_frame*)page)->index_;
    main_buffer[index].is_dirty_ = true;
}

int buffer_init(int num_buf)
{
    NUM_BUF = num_buf;
    try
    {
        main_buffer = new buffer_frame[NUM_BUF];
    }
    catch( const std::bad_alloc& e )
    {
        return -1;
    }
    for(int i = 0; i < num_buf; i++)
    {
        holes.insert(i);
    }
    return 0;
}
int64_t buffer_open_table_file(char* path)
{
    int64_t new_table_id = file_open_table_file(path);
    int index = find_hole_index();
    if(index < 0)
    {
        index = buffer_flush();
    }
    //cout << "index : " << index << "\n";
    indexer.insert({{new_table_id, 0}, index});
    main_buffer[index] = buffer_frame(new_table_id, 0, index);
    main_buffer[index].next_of_LRU_ = IS_HEADER_PAGE;
    main_buffer[index].prev_of_LRU_ = IS_HEADER_PAGE;
    return new_table_id;
}

// 여기에 뮤텍스 걸어주는 과정이 필요할듯

page_t* buffer_read_page(int64_t table_id, pagenum_t pagenum) // main read // 헤더에 대한 처리가 빠져서 세그폴트남.
{   // 디스크에서 뭘 읽어오는 과정 자체는 일단 차치하고, LRU 리스트만
    pthread_mutex_lock(&buffer_manager_latch);
    auto index_pair = indexer.find({table_id, pagenum}); // 이 앞에 버퍼 래치 걸기
    if(index_pair == indexer.end()) // 디스크에서 페이지를 읽어와야 하는 경우 -> cache miss
    {
        int hole = find_hole_index();
        int index = (hole == -1 ? LRU_list_head : hole);
        if(pthread_mutex_trylock(&main_buffer[index].page_latch) == EBUSY)
        {
            pthread_mutex_unlock(&buffer_manager_latch);
                // page latch 획득
            pthread_mutex_lock(&main_buffer[index].page_latch);
            pthread_mutex_lock(&buffer_manager_latch);
        }
        index = (hole == -1 ? buffer_flush() : index);
        
        // 여기서 뮤텍스를 지워도 괜찮은가?
        main_buffer[index].set_data(table_id, pagenum, index);
        
        main_buffer[index].is_pinned_++;
        main_buffer[index].next_of_LRU_ = main_buffer[index].prev_of_LRU_ = IS_NOT_IN_LRU_LIST;
        indexer.insert( {{table_id, pagenum}, index} );
        pthread_mutex_unlock(&buffer_manager_latch);
        main_buffer[index].set_page_data(table_id, pagenum);
        
        return &main_buffer[index].p_frame_;
    }

    else    // cache hit
    {
        int index = index_pair->second;
         // 페이지의 락을 푸는 과정에서도 LRU 리스트의 수정이 가해지므로, 락을 풀 때에도 매니저 latch를 잡아야 함.
        if(pthread_mutex_trylock(&main_buffer[index].page_latch) == EBUSY)
        {
            pthread_mutex_unlock(&buffer_manager_latch);
            pthread_mutex_lock(&main_buffer[index].page_latch);
            pthread_mutex_lock(&buffer_manager_latch);
        }
        
        if(main_buffer[index].prev_of_LRU_ == IS_NOT_IN_LRU_LIST && main_buffer[index].next_of_LRU_ == IS_NOT_IN_LRU_LIST)
        {   // 이미 LRU 리스트에서 빠져있는 경우
            main_buffer[index].is_pinned_++;
        }
        else if(main_buffer[index].prev_of_LRU_ == IS_HEADER_PAGE && main_buffer[index].next_of_LRU_ == IS_HEADER_PAGE)
        {
            main_buffer[index].is_pinned_++;
        }
        else    // LRU 리스트에 있는 경우
        {
            main_buffer[index].is_pinned_++;
            if(main_buffer[index].prev_of_LRU_ == IS_LRU_HEADER && main_buffer[index].next_of_LRU_ == IS_LRU_TAIL)
            {   // buffer에 단 하나의 페이지만 있는 경우
                LRU_list_head = -1;
                LRU_list_tail = -2;
                main_buffer[index].next_of_LRU_ = main_buffer[index].prev_of_LRU_ = IS_NOT_IN_LRU_LIST;
            }
            else if(main_buffer[index].prev_of_LRU_ == IS_LRU_HEADER)
            {
                LRU_list_head = main_buffer[index].next_of_LRU_;
                main_buffer[LRU_list_head].prev_of_LRU_ = IS_LRU_HEADER;
                main_buffer[index].next_of_LRU_ = main_buffer[index].prev_of_LRU_ = IS_NOT_IN_LRU_LIST;
            }
            else if(main_buffer[index].next_of_LRU_ == IS_LRU_TAIL)
            {
                LRU_list_tail = main_buffer[index].prev_of_LRU_;
                main_buffer[LRU_list_tail].next_of_LRU_ = IS_LRU_TAIL;
                main_buffer[index].next_of_LRU_ = main_buffer[index].prev_of_LRU_ = IS_NOT_IN_LRU_LIST;
            }
            else
            {
                int left = main_buffer[index].prev_of_LRU_;
                int right = main_buffer[index].next_of_LRU_;
                main_buffer[left].next_of_LRU_ = right;
                main_buffer[right].prev_of_LRU_ = left;
                main_buffer[index].next_of_LRU_ = main_buffer[index].prev_of_LRU_ = IS_NOT_IN_LRU_LIST;
            }    
        }
        pthread_mutex_unlock(&buffer_manager_latch);  
        return &main_buffer[index].p_frame_;
    }
}

// 여기에 뮤텍스 걸어주는 과정이 필요할듯

void buffer_write_page(int64_t table_id, pagenum_t pagenum, page_t* src) // main write
{
    auto index_pair = indexer.find({table_id, pagenum});
    if(index_pair == indexer.end()) // 디스크에서 페이지를 읽어와야 하는 경우 -> cache miss
    {
        int index = find_hole_index();
        if(index == -1) // 버퍼가 꽉 찬 경우
        {
            index = buffer_flush(); // 버퍼가 꽉 차있으면?
        }
        main_buffer[index] = buffer_frame(table_id, pagenum, index);
        main_buffer[index].is_pinned_++;
        main_buffer[index].next_of_LRU_ = main_buffer[index].prev_of_LRU_ = IS_NOT_IN_LRU_LIST;
        indexer.insert( {{table_id, pagenum}, index} );
        memmove(&main_buffer[index].p_frame_, src, sizeof(page_t));
        main_buffer[index].is_dirty_ = true;
    }
    
    else    // cache hit
    {
        int index = index_pair->second;
        if(main_buffer[index].prev_of_LRU_ == IS_NOT_IN_LRU_LIST && main_buffer[index].next_of_LRU_ == IS_NOT_IN_LRU_LIST)
        {   // 이미 LRU 리스트에서 빠져있는 경우
            main_buffer[index].is_pinned_++;
            memmove(&main_buffer[index].p_frame_, src, sizeof(page_t));
            main_buffer[index].is_dirty_ = true;
        }
        else if(main_buffer[index].prev_of_LRU_ == IS_HEADER_PAGE && main_buffer[index].next_of_LRU_ == IS_HEADER_PAGE)
        {
            main_buffer[index].is_pinned_++;
            memmove(&main_buffer[index].p_frame_, src, sizeof(page_t));
            main_buffer[index].is_dirty_ = true;
        }
        else    // LRU 리스트에 있는 경우
        {
            main_buffer[index].is_pinned_++;
            if(main_buffer[index].prev_of_LRU_ == IS_LRU_HEADER && main_buffer[index].next_of_LRU_ == IS_LRU_TAIL)
            {   // buffer에 단 하나의 페이지만 있는 경우
                LRU_list_head = -1;
                LRU_list_tail = -2;
                main_buffer[index].next_of_LRU_ = main_buffer[index].prev_of_LRU_ = IS_NOT_IN_LRU_LIST;
            }
            else if(main_buffer[index].prev_of_LRU_ == IS_LRU_HEADER) // 현재 페이지가 LRU 헤더인 경우
            {
                LRU_list_head = main_buffer[index].next_of_LRU_;
                main_buffer[LRU_list_head].prev_of_LRU_ = IS_LRU_HEADER;
                main_buffer[index].next_of_LRU_ = main_buffer[index].prev_of_LRU_ = IS_NOT_IN_LRU_LIST;
            }
            else if(main_buffer[index].next_of_LRU_ == IS_LRU_TAIL)     // 현재 페이지가 LRU tail인 경우
            {
                LRU_list_tail = main_buffer[index].prev_of_LRU_;
                main_buffer[LRU_list_tail].next_of_LRU_ = IS_LRU_TAIL;
                main_buffer[index].next_of_LRU_ = main_buffer[index].prev_of_LRU_ = IS_NOT_IN_LRU_LIST;
            }
            else
            {
                int left = main_buffer[index].prev_of_LRU_;
                int right = main_buffer[index].next_of_LRU_;
                main_buffer[left].next_of_LRU_ = right;
                main_buffer[right].prev_of_LRU_ = left;
                main_buffer[index].next_of_LRU_ = main_buffer[index].prev_of_LRU_ = IS_NOT_IN_LRU_LIST;
            }
            memmove(&main_buffer[index].p_frame_, src, sizeof(page_t));
            main_buffer[index].is_dirty_ = true;
        }   
    }
}

pagenum_t buffer_alloc_page(int64_t table_id)  // main alloc // 헤더 조정 해야함.
// 버퍼에 페이지를 요청하는 함수. 얘가 victim 이런거 다 해야함.
// 얘는 이미 alloc된 페이지더라도 버퍼가 가득 차는 등의 이유에 의해서 다시 요청이 들어올 수 있음.
// 따라서 항상 file_alloc을 수행해서는 안된다.
{
    page_t* header = buffer_read_page(table_id, 0);
    file_write_header_page(table_id, header);

    pagenum_t new_page = file_alloc_page(table_id);
    int index = find_hole_index();
    if(index == -1) // 버퍼가 꽉 찬 경우
    {
        index = buffer_flush();
    }
    main_buffer[index] = buffer_frame(table_id, new_page, index);
    //main_buffer[index].is_pinned_++; -> 지금 당장 뭔가 엑세스하는건 아님.
    main_buffer[index].next_of_LRU_ = IS_LRU_TAIL;
    if(LRU_list_head == -1 && LRU_list_tail == -2)
    {
        main_buffer[index].prev_of_LRU_ = IS_LRU_HEADER;
        main_buffer[index].next_of_LRU_ = IS_LRU_TAIL;
        LRU_list_head = index;
    }
    else
    {
        main_buffer[index].prev_of_LRU_ = LRU_list_tail;
        main_buffer[LRU_list_tail].next_of_LRU_ = index;
    }
    indexer.insert( {{table_id, new_page}, index} );
    LRU_list_tail = index;

    header_page_t temp;
    file_read_header_page(table_id, (page_t*)&temp); // 여기에서 문제 발생함.
    memmove(header, &temp, sizeof(page_t));
    // buffer_return(table_id, 0);
    buffer_return(header);

    return new_page;
}


void buffer_free_page(int64_t table_id, pagenum_t pagenum) // 버퍼에 없는 페이지를 free할 떄도 있다. // 헤더 조정 해야함.
{
    page_t* header = buffer_read_page(table_id, 0);
    file_write_header_page(table_id, header);

    auto index_pair = indexer.find({table_id, pagenum});
    if(index_pair == indexer.end()) // 디스크에서 페이지를 읽어와야 하는 경우 -> cache miss -> 바로 free 하기.
    {
        file_free_page(table_id, pagenum);
        // 이제 안쓰겠다고 free하는 페이지를 굳이 버퍼에 올려다 놓을 필요가 없다는 판단.
    }

    else    // cache hit
    {
        int index = index_pair->second;
        if(main_buffer[index].prev_of_LRU_ == IS_NOT_IN_LRU_LIST && main_buffer[index].next_of_LRU_ == IS_NOT_IN_LRU_LIST)
        {   // 이미 LRU 리스트에서 빠져있는 경우 -> 핀카운트
            if(main_buffer[index].is_pinned_ > 1)
            {
                cerr << "Pin count > 1 / cur pin count : " << main_buffer[index].is_pinned_ << "\n";
                return;
            }
            else
            {
                if(main_buffer[index].is_dirty_)
                {
                    file_write_page(table_id, pagenum, &main_buffer[index].p_frame_);
                }
                file_free_page(table_id, pagenum);
                main_buffer[index].is_activated_ = false;
                holes.insert(index);
                indexer.erase({table_id, pagenum});
            }
        }
        else    // LRU 리스트에 있는 경우
        {
            if(main_buffer[index].prev_of_LRU_ == IS_LRU_HEADER && main_buffer[index].next_of_LRU_ == IS_LRU_TAIL)
            {   // buffer에 단 하나의 페이지만 있는 경우
                LRU_list_head = -1;
                LRU_list_tail = -2;
                main_buffer[index].next_of_LRU_ = main_buffer[index].prev_of_LRU_ = IS_NOT_IN_LRU_LIST;
            }
            else if(main_buffer[index].prev_of_LRU_ == IS_LRU_HEADER) // 현재 페이지가 LRU 헤더인 경우
            {
                LRU_list_head = main_buffer[index].next_of_LRU_;
                main_buffer[LRU_list_head].prev_of_LRU_ = IS_LRU_HEADER;
                main_buffer[index].next_of_LRU_ = main_buffer[index].prev_of_LRU_ = IS_NOT_IN_LRU_LIST;
            }
            else if(main_buffer[index].next_of_LRU_ == IS_LRU_TAIL)     // 현재 페이지가 LRU tail인 경우
            {
                LRU_list_tail = main_buffer[index].prev_of_LRU_;
                main_buffer[LRU_list_tail].next_of_LRU_ = IS_LRU_TAIL;
                main_buffer[index].next_of_LRU_ = main_buffer[index].prev_of_LRU_ = IS_NOT_IN_LRU_LIST;
            }
            else
            {
                int left = main_buffer[index].prev_of_LRU_;
                int right = main_buffer[index].next_of_LRU_;
                main_buffer[left].next_of_LRU_ = right;
                main_buffer[right].prev_of_LRU_ = left;
                main_buffer[index].next_of_LRU_ = main_buffer[index].prev_of_LRU_ = IS_NOT_IN_LRU_LIST;
            }
            file_free_page(table_id, pagenum);
            main_buffer[index].is_activated_ = false;
            holes.insert(index);
            indexer.erase({table_id, pagenum});
        }   
    }

    page_t temp;
    file_read_header_page(table_id, &temp);
    memmove(header, &temp, sizeof(page_t));
    // buffer_return(table_id, 0);
    buffer_return(header);
    return;
}

void buffer_return(page_t* page)
{
    pthread_mutex_lock(&buffer_manager_latch);
    int index = ((buffer_frame*)page)->index_;
    main_buffer[index].is_pinned_--; // 9에서 10으로 넘어갈 때 2번 실행됨 왜?

    if(main_buffer[index].is_header_) 
    {
        pthread_mutex_unlock(&main_buffer[index].page_latch);
        pthread_mutex_unlock(&buffer_manager_latch);
        return;
    }

    if(main_buffer[index].is_pinned_ == 0)
    {
        if(LRU_list_head == -1 && LRU_list_tail == -2)
        {
            main_buffer[index].prev_of_LRU_ = IS_LRU_HEADER;
            main_buffer[index].next_of_LRU_ = IS_LRU_TAIL;
            LRU_list_head = index;
        }
        else
        {
            main_buffer[index].prev_of_LRU_ = LRU_list_tail;
            main_buffer[LRU_list_tail].next_of_LRU_ = index;
            main_buffer[index].next_of_LRU_ = IS_LRU_TAIL;
        }
        LRU_list_tail = index;
    }
    pthread_mutex_unlock(&main_buffer[index].page_latch); // page latch unlock    
    pthread_mutex_unlock(&buffer_manager_latch);
}
void buffer_return(int64_t table_id, pagenum_t pagenum)
{
    pthread_mutex_lock(&buffer_manager_latch);
    auto index_pair = indexer.find({table_id, pagenum});
    if(index_pair == indexer.end()) // 디스크에서 페이지를 읽어와야 하는 경우 -> cache miss -> 바로 free 하기.
    {
        cerr << "Error : It's not in buffer pool\n";
        cerr << "Table_id : " << table_id << " / Pagenum : " << pagenum << "\n";
    }
    else
    {
        int index = index_pair->second;
        main_buffer[index].is_pinned_--; // 9에서 10으로 넘어갈 때 2번 실행됨 왜?

        if(main_buffer[index].is_header_) 
        {
            pthread_mutex_unlock(&main_buffer[index].page_latch);
            pthread_mutex_unlock(&buffer_manager_latch);
            return;
        }

        if(main_buffer[index].is_pinned_ == 0)
        {
            if(LRU_list_head == -1 && LRU_list_tail == -2)
            {
                main_buffer[index].prev_of_LRU_ = IS_LRU_HEADER;
                main_buffer[index].next_of_LRU_ = IS_LRU_TAIL;
                LRU_list_head = index;
            }
            else
            {
                main_buffer[index].prev_of_LRU_ = LRU_list_tail;
                main_buffer[LRU_list_tail].next_of_LRU_ = index;
                main_buffer[index].next_of_LRU_ = IS_LRU_TAIL;
            }
            LRU_list_tail = index;
        }
        pthread_mutex_unlock(&main_buffer[index].page_latch); // page latch unlock
    }
    pthread_mutex_unlock(&buffer_manager_latch);
} 

// 버퍼가 꽉 찼을 때, LRU 리스트의 헤더를 디스크로 내려보낸 뒤 그 자리를 리턴해줌.
// LRU 헤더는 반드시 제대로 설정되어있다고 가정하고 사용하는 함수임.
// 따라서 LRU 헤더에 핀이 꽂혀있는 그런 경우는 없어야 한다.
int buffer_flush()  // 반드시 buffer manager latch를 걸고 호출해야함.
{
    int index = LRU_list_head;

    if(index < 0 || index >= NUM_BUF)
    {
        cerr << "Error : LRU list header index is less than 0 or larger than NUM_BUf\n";
        cerr << "LRU_list_head : " << LRU_list_head << "\n";
        cerr << "NUM_BUF : " << NUM_BUF << "\n";
        return -1;
    }

    if(main_buffer[index].is_pinned_ > 0)
    {
        cerr << "Error on evict page : pin count is large than 0\n";
        cerr << "Pin count : " << main_buffer[index].is_pinned_ << "\n";
        return -1;
    }

    if(main_buffer[index].prev_of_LRU_ != IS_LRU_HEADER)
    {
        cerr << "Error : LRU policy violation\n";
        cerr << "prev_of_LRU : " << main_buffer[index].prev_of_LRU_ << "\n";
        return -1;
    }
    
    int64_t table_id = main_buffer[index].table_id_;
    pagenum_t pagenum = main_buffer[index].pagenum_;

    if(main_buffer[index].is_dirty_) // 더티페이지면 디스크에 쓰고
    {
        if(pagenum == 0)
            file_write_header_page(table_id, &main_buffer[index].p_frame_);
        else
            file_write_page(table_id, pagenum, &main_buffer[index].p_frame_);
    }
    else
    {
        int t = 3;
        t++;
    }    
    main_buffer[index].is_activated_ = false;   // 클린 표시
    //holes.insert(index);
    indexer.erase({table_id, pagenum});         // 인덱서에서 지워주고

    if(LRU_list_head == LRU_list_tail)
    {
        LRU_list_head = -1;
        LRU_list_tail = -2;
    }
    else
    {
        LRU_list_head = main_buffer[index].next_of_LRU_;
        main_buffer[LRU_list_head].prev_of_LRU_ = IS_LRU_HEADER; // LRU리스트 갱신.
    }
    
    return index;
}

int buffer_shutdown()
{
    for(int i = 0; i < NUM_BUF; i++)
    {
        pthread_mutex_destroy(&main_buffer[i].page_latch);
        if(main_buffer[i].is_activated_ == false)
            continue;
        if(main_buffer[i].is_dirty_ == false)
            continue;
        if(main_buffer[i].is_header_)
        {
            file_write_header_page(main_buffer[i].table_id_, &main_buffer[i].p_frame_);
        }
            
        else
            file_write_page(main_buffer[i].table_id_, main_buffer[i].pagenum_, &main_buffer[i].p_frame_);
    }
    file_close_table_file();
    LRU_list_head = -1;
    LRU_list_tail = -2;
    indexer.clear();
    try{
        delete[] main_buffer;
    }catch(std::exception& e){
        return -1;
    }
    return 0;
}
