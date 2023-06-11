#ifndef PAGE_MANAGER_H
#define PAGE_MANAGER_H


#include <iostream>
#include <cstring>
#include <algorithm>
#include <vector>
#include <string>
#include "page.h"
#include "buffer.h"

using namespace std;
const int SLOT_SIZE = 16;
/*
num_of_key * SLOT_SIZE + freespace - record_size = 내가 넣을 레코드의 시작 포지션
*/

class page_manager{
public:
    virtual int64_t get_key(int index) = 0;

    virtual int get_num_of_key() = 0;

    virtual pagenum_t get_src_pagenum() = 0;

    virtual pagenum_t get_src_parent() = 0;
    virtual void set_src_parent(pagenum_t parent) = 0;
    virtual void flush(int64_t table_id) = 0;

};

class Leaf_page_manager : public page_manager{
public:
    //Leaf_page_manager(pagenum_t pagenum, leaf_page_t* src) : pagenum_(pagenum), page_(src){}
    Leaf_page_manager(pagenum_t pagenum, int64_t table_id) : pagenum_(pagenum), table_id_(table_id)
    {
        page_ = (leaf_page_t*)buffer_read_page(table_id, pagenum);
    }
    ~Leaf_page_manager()
    {
        //buffer_return(table_id_, pagenum_);
    }

    int get_num_of_key() {return page_->num_of_key;}
    void set_num_of_key(int num) {page_->num_of_key = num; buffer_set_dirty((page_t*)page_);}

    pagenum_t get_src_pagenum() {return pagenum_;}
    
    pagenum_t get_src_parent() {return page_->parent_page_num;}
    void set_src_parent(pagenum_t parent) {page_->parent_page_num = parent; buffer_set_dirty((page_t*)page_);}

    int64_t get_amount_of_freespace() {return page_->amount_of_freespace;}
    void set_amount_of_freespace(int64_t space) { page_->amount_of_freespace = space; buffer_set_dirty((page_t*)page_);}

    pagenum_t get_right_sibling_pagenum() {return page_->right_sibling_pagenum;}
    void set_right_sibling_pagenum(pagenum_t sibling) { page_->right_sibling_pagenum = sibling; buffer_set_dirty((page_t*)page_);}
    
    // 최대한 배열과 유사하게 만들어주기 위해서 인덱스를 기반으로 한 입출력만 구현함.
    // 슬롯은 최소 32개, 최대 64개.
    struct Slot{
        int64_t key;
        short size;
        short offset;
        int trxid;
    };
    Slot get_slot(int index) // ?
    {
        if(index < 0 || index > page_->num_of_key)
        {
            cerr << "Invalid index. input index : "<< index <<"\n";
            exit(EXIT_FAILURE);
        }
        Slot* position = (Slot*)(page_->record_space + SLOT_SIZE*index);
        Slot val;
        memmove(&val, position, sizeof(Slot));
        return val;
    }
    int64_t get_key(int index) // ?
    {
        return get_slot(index).key;
        // if(index < 0)
        // {
        //     cerr << "Invalid index.\n";
        //     exit(EXIT_FAILURE);
        //     return -1;
        // }
        // Slot* position = ((Slot*)(page_->record_space)) + index;
        // int64_t val;
        // memmove(&val, (int64_t*)position, sizeof(int64_t));
        // return val;
    }

    void move_slot(int from_index, int to_index)
    {
        buffer_set_dirty((page_t*)page_);
        if(from_index < 0)
        {
            cerr << "Invalid index.\n";
            exit(EXIT_FAILURE);
        }
        Slot* position_from = (Slot*)(page_->record_space + SLOT_SIZE*from_index);
        Slot* position_to = (Slot*)(page_->record_space + SLOT_SIZE*to_index);
        memmove(position_to, position_from, sizeof(Slot));
    }

    void get_record_by_index(int index, char* dest)
    {
        if(index < 0 || index >= get_num_of_key())
        {
            cerr << "get_record_by_index() method received"
                 << " invalid index.\n"
                 << "Input index : " << index
                 << "\nCurrent page's num of key : " << get_num_of_key() << "\n";
            exit(EXIT_FAILURE);
        }

        Slot temp_slot = get_slot(index);
        memmove(dest, page_->record_space + temp_slot.offset, sizeof(char) * temp_slot.size);
    }
    void get_record_by_key(int64_t key, char* dest)
    {
        int start = 0;
        int end = get_num_of_key() - 1;
        int index = 0;
        
        while(start <= end)
        {
            int mid = (start + end) / 2;
            int64_t t_key = get_key(mid);
            if(t_key == key) 
            {
                index = mid;
                break;
            }
            else if(t_key > key)
                end = mid - 1;
            
            else 
                start = mid + 1;
        }
        if(start > end) 
        {
            cerr << "Failure on Binary Searching!\n";
            remove("database.db");
            exit(EXIT_FAILURE);
        }
        cout << "key : " << key << "\n index = " << index << "\n";
        get_record_by_index(index, dest);
    }
    string get_record_by_index(int index)
    {
        if(index < 0 || index >= get_num_of_key())
        {
            cerr << "get_record_by_index() method received"
                 << " invalid index.\n"
                 << "Input index : " << index
                 << "\nCurrent page's num of key : " << get_num_of_key() << "\n";
            remove("database.db");
            exit(EXIT_FAILURE);
        }

        Slot temp_slot = get_slot(index);
        char* copy_of_record = new char[temp_slot.size];
        memmove(copy_of_record, page_->record_space + temp_slot.offset, sizeof(char) * temp_slot.size);
        string temp = string(copy_of_record);
        delete copy_of_record;
        return temp;
    }
    string get_record_by_key(int64_t key)
    {
        int start = 0;
        int end = get_num_of_key() - 1;
        int index = 0;
        

        while(start <= end)
        {
            int mid = (start + end) / 2;
            int64_t t_key = get_key(mid);
            if(t_key == key) 
            {
                index = mid;
                break;
            }
            else if(t_key > key)
                end = mid - 1;
            
            else 
                start = mid + 1;
        }
        if(start > end) return string();
        
        return get_record_by_index(index);
    }

    void insert_record_in(int index, int64_t key, char* value, short val_size) // 슬롯 무브는 다른데서 해야 함.
    {                                                                          // 얜 그냥 자리에 넣기만 함.
        buffer_set_dirty((page_t*)page_);
        Slot new_slot = {key, 
                        val_size, 
                        static_cast<short>(page_->num_of_key * SLOT_SIZE + static_cast<short>(page_->amount_of_freespace) - val_size),
                        -1};
        Slot* slot_insertion_position = (Slot*)(page_->record_space + SLOT_SIZE*index);
        memmove(slot_insertion_position, &new_slot, sizeof(Slot));
        memmove(page_->record_space + new_slot.offset, value, sizeof(char) * val_size);
        // 중요, 여기서 freespace의 크기, 키 개수를 건드릴거다.
        page_->amount_of_freespace -= (SLOT_SIZE + val_size);
        page_->num_of_key++;
    }
    void update_record_at(int index, char* new_value, short new_val_size, short* old_size_return)
    {
        ((buffer_frame*)page_)->is_dirty_ = true;
        Slot target_slot = get_slot(index);
        //memmove(back_up, page_->record_space + target_slot.offset, sizeof(char) * target_slot.size);
        memmove(page_->record_space + target_slot.offset, new_value, sizeof(char) * new_val_size);
        memmove(old_size_return, &target_slot.size, sizeof(short));
    }
    void delete_record(int index)
    {
        buffer_set_dirty((page_t*)page_);
        Slot target_slot = get_slot(index);
        std::vector<Slot> vec = std::vector<Slot>();
        for(int i = 0; i < page_->num_of_key; i++)
        {
            if(i == index) continue;
            vec.push_back(get_slot(i));
        }
        std::sort(vec.begin(), vec.end(), compare); // 오름차순으로 정렬
        for(int i = 0; i < vec.size(); i++) // 레코드를 우선 밀어줌. 원래 레코드가 있는 위치보다 뒤쪽에 있는 애들을 밀어줌.
        {
            if(vec[i].offset > target_slot.offset) continue;

            memmove(page_->record_space + vec[i].offset + target_slot.size, 
                    page_->record_space + vec[i].offset, 
                    sizeof(char) * vec[i].size);
            set_offset_by_key(vec[i].key, vec[i].offset + target_slot.size);
        }
        for(int i = index; i < (page_->num_of_key - 1); i++)
            move_slot(i + 1, i);
        
        page_->amount_of_freespace += (SLOT_SIZE + target_slot.size);
        page_->num_of_key--;
    }

    void flush(int64_t table_id)
    {
        //buffer_return(table_id_, pagenum_);
        buffer_return((page_t*)page_);
    }

    char* get_record_space() {return page_->record_space;}
    leaf_page_t* get_page() {return page_;}
private:
    leaf_page_t* page_;
    pagenum_t pagenum_;
    int64_t table_id_;
    void set_offset_by_key(int64_t key, short offset)
    {
        buffer_set_dirty((page_t*)page_);
        for(int i = 0; i < page_->num_of_key; i++)
        {
            int64_t temp_key;
            memmove(&temp_key, page_->record_space + SLOT_SIZE*i, sizeof(int64_t));
            if(temp_key == key)
            {
                memmove(page_->record_space + SLOT_SIZE*i + 10, &offset, sizeof(short));
                return;
            }
        }
        cerr << "No key founded on set_offset_by_key()!\n";
        exit(EXIT_FAILURE);
    }
    bool static compare(Slot a, Slot b) { return a.offset > b.offset; }
};

class Internal_page_manager : public page_manager{
public:
    Internal_page_manager() : pagenum_(-1), page_(nullptr), table_id_(-1) {}
    //Internal_page_manager(pagenum_t pagenum, internal_page_t* src) : pagenum_(pagenum), page_(src){}
    Internal_page_manager(pagenum_t pagenum, int64_t table_id) : pagenum_(pagenum), table_id_(table_id)
    {
        page_ = (internal_page_t*)buffer_read_page(table_id, pagenum);
    }
    ~Internal_page_manager()
    {
        //buffer_return(table_id_, pagenum_);
    }
    int get_num_of_key() {return page_->num_of_key;}
    void set_num_of_key(int num) {page_->num_of_key = num; buffer_set_dirty((page_t*)page_);}

    pagenum_t get_src_pagenum() {return pagenum_;}
    
    pagenum_t get_src_parent() {return page_->parent_page_num;}
    void set_src_parent(pagenum_t parent) {page_->parent_page_num = parent; buffer_set_dirty((page_t*)page_);}
    

    int64_t get_key(int index)
    {
        if(index > page_->num_of_key - 1 || index < 0)
        {
            cerr << "Invalid index on get_key() method.\n";
            cerr << "Pagenum : " << pagenum_ << "\ninput index : " << index << "\n";
            cerr << "Now, Page #" << pagenum_ << " has " << page_->num_of_key << " keys.\n";
            remove("database.db");
            exit(EXIT_FAILURE);
            return -1;
        }
        int64_t key;
        memmove(&key, ((int64_t*)(page_->key_value_space)) + 2*index, sizeof(int64_t));
        return key;
    }

    void set_key(int index, int64_t key)
    {
        memmove( ((int64_t*)(page_->key_value_space)) + 2*index, &key, sizeof(int64_t));
        buffer_set_dirty((page_t*)page_);
    }

    pagenum_t get_child_pagenum(int index)
    {
        pagenum_t pagenum;
        memmove(&pagenum, ((pagenum_t*)(page_->key_value_space)) + 2*index - 1, sizeof(int64_t));
        return pagenum;
    }

    void set_child_pagenum(int index, pagenum_t pagenum)
    {
        memmove( ((pagenum_t*)(page_->key_value_space)) + 2*index - 1, &pagenum, sizeof(pagenum_t));
        buffer_set_dirty((page_t*)page_);
    }

    

    void flush(int64_t table_id)
    {
        //buffer_return(table_id_, pagenum_);
        buffer_return((page_t*)page_);
    }

private:
    internal_page_t* page_; // 이거 버퍼 올려놓으면 포인터로 바꿔야 할듯?
    pagenum_t pagenum_;
    int64_t table_id_;
};

#endif