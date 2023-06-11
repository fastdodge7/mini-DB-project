#include "my_bpt.h"

pagenum_t find_root_pagenum(int64_t table_id);
int cut( int length );
int get_left_index(int64_t table_id, pagenum_t parent, pagenum_t left) ;
int is_leaf(int64_t table_id, pagenum_t page);
pagenum_t find_leaf( pagenum_t root, int64_t table_id, int key, bool verbose );
pagenum_t make_leaf_page( int64_t table_id );
pagenum_t make_internal_page( int64_t table_id );
pagenum_t insert_into_leaf_page(pagenum_t root, int64_t table_id, pagenum_t target_leaf,
                                int64_t key, char* value, short val_size);
pagenum_t insert_into_leaf_after_splitting(pagenum_t root, int64_t table_id, pagenum_t target_leaf, 
                                           int64_t key, char* value, short val_size);

pagenum_t insert_into_internal_page(pagenum_t root, int64_t table_id, pagenum_t n, int left_index, int64_t key, pagenum_t right);

pagenum_t insert_into_internal_page_after_splitting(pagenum_t root, int64_t table_id, pagenum_t old_node, 
                                                    int left_index, int key, pagenum_t right);

pagenum_t insert_into_parent(pagenum_t root, int64_t table_id, pagenum_t left, int key, pagenum_t right);
pagenum_t start_new_tree(int64_t table_id, int64_t key, char* value, short val_size);
pagenum_t insert_into_new_root(int64_t table_id, pagenum_t left, int64_t key, pagenum_t right);
pagenum_t insert( pagenum_t root, int64_t table_id,  int64_t key, char* value, short val_size );

int get_neighbor_index( int64_t table_id, pagenum_t page );
pagenum_t remove_entry_from_node(int64_t table_id, pagenum_t n, int key, pagenum_t pointer);
pagenum_t adjust_root(int64_t table_id, pagenum_t root);
pagenum_t coalesce_nodes(pagenum_t root, int64_t table_id,  pagenum_t n, pagenum_t neighbor, int neighbor_index, int k_prime);
pagenum_t redistribute_nodes(pagenum_t root,int64_t table_id, pagenum_t n, pagenum_t neighbor, int neighbor_index, 
                             int k_prime_index, int64_t k_prime);
pagenum_t delete_entry( pagenum_t root, int64_t table_id, pagenum_t n, int64_t key, pagenum_t pointer );
pagenum_t db_delete_with_root(pagenum_t root, int64_t table_id, int64_t key);


unordered_map<pair<int64_t, pagenum_t>, int> is_leaf_hash;

int order = 249; // d = 124
const int DEFAULT_PAGE_FREE_SPACE = 3968;
int number_of_opened_table = 0;

int64_t open_table(char* pathname)
{
    if(number_of_opened_table < 20)
        return buffer_open_table_file(pathname);
    else
    {
        cerr << "Current the number of talbes : " << number_of_opened_table << "\n Table limit exceeded.\n";
        return -1;
    }
}

int db_insert(int64_t table_id, int64_t key, char* value, uint16_t val_size)
{
    pagenum_t root = find_root_pagenum(table_id);
    if(root == 0) // 빈 트리에는 바로 insert.
    {
        root = insert(root, table_id, key, value, val_size);
        return 0;
    }
    pagenum_t target = find_leaf(root, table_id, key, false);
    Leaf_page_manager target_page = Leaf_page_manager(target, table_id);
    for(int i = 0; i < target_page.get_num_of_key(); i++)
    {
        if(key == target_page.get_key(i))
            return -1;
    }
    target_page.flush(table_id);
    root = insert(root, table_id, key, value, val_size);
    
    return 0;
}

// int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size)
// {
//     pagenum_t root = find_root_pagenum(table_id);
//     if(root == 0) // 빈 트리에는 바로 insert.
//         return -1;
    
//     pagenum_t target = find_leaf(root, table_id, key, false);
//     Leaf_page_manager target_page = Leaf_page_manager(target, table_id);


//     for(int i = 0; i < target_page.get_num_of_key(); i++)
//     {
//         if(key == target_page.get_key(i))
//         {
//             target_page.get_record_by_index(i, ret_val);
//             short t = target_page.get_slot(i).size;
//             memmove(val_size, &t, sizeof(short));
//             target_page.flush(table_id);
//             return 0;
//         }
//     }
//     target_page.flush(table_id);
//     return -1;
// }

int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size, int trx_id)
{
    pagenum_t root = find_root_pagenum(table_id);
    if(root == 0) // 빈 트리에는 바로 insert.
        return -1;
    
    pagenum_t target = find_leaf(root, table_id, key, false);
    Leaf_page_manager target_page = Leaf_page_manager(target, table_id);

    int start = 0;
    int end = target_page.get_num_of_key() - 1;
    int mid;
    int64_t value;

    while(start <= end)
    {
        mid = (start + end) / 2;
        value = target_page.get_key(mid);
        if(key < value)
            end = mid - 1;
        else if(value < key)
            start = mid + 1;
        else
            break;
    }
    if(start > end)
    {
        target_page.flush(table_id);
        return -1;
    }
    else
    {
        int i = mid;
        pthread_mutex_unlock(&((buffer_frame*)target_page.get_page())->page_latch);
        lock_t* lock_obj = lock_acquire(table_id, target_page.get_src_pagenum(), key, trx_id, S_LOCK);
        if(lock_obj == nullptr)
        {
            target_page.flush(table_id);
            return -1;
        }
        //pthread_mutex_lock(&);
        pthread_mutex_lock(&((buffer_frame*)target_page.get_page())->page_latch);
        target_page.get_record_by_index(i, ret_val);
        short t = target_page.get_slot(i).size;
        memmove(val_size, &t, sizeof(short));
        target_page.flush(table_id);
        return 0;
    }
}

int db_update(int64_t table_id, int64_t key, char* value, uint16_t new_val_size, uint16_t* old_val_size, int trx_id)
{
    pagenum_t root = find_root_pagenum(table_id);
    if(root == 0) // 빈 트리에는 바로 insert.
        return -1;
    
    pagenum_t target = find_leaf(root, table_id, key, false);
    Leaf_page_manager target_page = Leaf_page_manager(target, table_id);

    int start = 0;
    int end = target_page.get_num_of_key() - 1;
    int mid;
    int64_t tvalue;

    while(start <= end)
    {
        mid = (start + end) / 2;
        tvalue = target_page.get_key(mid);
        if(key < tvalue)
            end = mid - 1;
        else if(tvalue < key)
            start = mid + 1;
        else
            break;
    }
    if(start > end)
    {
        target_page.flush(table_id);
        return -1;
    }
    else
    {
        int i = mid;
        pthread_mutex_unlock(&(((buffer_frame*)(target_page.get_page()))->page_latch));

        lock_t* lock_obj = lock_acquire(table_id, target_page.get_src_pagenum(), key, trx_id, X_LOCK);
        if(lock_obj == nullptr)
        {
            target_page.flush(table_id);
            return -1;
        }
        trx_latch_lock();
            
        pthread_mutex_lock(&(((buffer_frame*)(target_page.get_page()))->page_latch));
            
        short t_size = target_page.get_slot(i).size;
        char* back_up_val = new char[t_size];
        int64_t pos = key;
        target_page.get_record_by_index(i, back_up_val);
            
        transaction_t* trx = get_trx(trx_id);
        trx->position_table_id.push_back(table_id);
        trx->position_key.push_back(pos);
        trx->values.push_back(back_up_val);
        trx_latch_unlock();
        memmove(old_val_size, &t_size, sizeof(short));

        target_page.update_record_at(i, value, (short)new_val_size, (short*)old_val_size);
        target_page.flush(table_id);
        return 0;
    }
}

int db_delete(int64_t table_id, int64_t key)
{
    pagenum_t root = find_root_pagenum(table_id);
    if(root == 0) // 빈 트리에는 바로 insert.
        return -1;
    pagenum_t target = find_leaf(root, table_id, key, false);
    Leaf_page_manager target_page = Leaf_page_manager(target, table_id);
    for(int i = 0; i < target_page.get_num_of_key(); i++)
    {
        if(key == target_page.get_key(i))
        {
            target_page.flush(table_id);
            db_delete_with_root(root, table_id, key);
            return 0;
        }
    }
    target_page.flush(table_id);
    return -1;
}

int init_db(int num_buf)
{
    int flag = buffer_init(num_buf);
    return flag;
}

int shutdown_db()
{
    int flag = buffer_shutdown();
    return flag;
}

pagenum_t find_root_pagenum(int64_t table_id)
{
    page_t* temp = buffer_read_page(table_id, 0);
    pagenum_t val;
    memmove(&val, temp->space + 16, sizeof(pagenum_t));
    //buffer_return(table_id, 0);
    buffer_return(temp);
    return val;
}







int cut( int length ) {
    if (length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}


int get_left_index(int64_t table_id, pagenum_t parent, pagenum_t left) 
{
    Internal_page_manager wparent_page = Internal_page_manager(parent, table_id);

    int left_index = 0;
    while (left_index <= wparent_page.get_num_of_key() && 
            wparent_page.get_child_pagenum(left_index) != left)
        left_index++;
    wparent_page.flush(table_id);
    return left_index; // 부모 노드의 포인터 중 left를 가리키는 포인터의 인덱스 혹은 부모 노드의 맨 오른쪽 키 바로 다음거.
}
int is_leaf(int64_t table_id, pagenum_t page)
{
    int flag;
    // page_t* temp = buffer_read_page(table_id, page);
    // memmove(&flag, temp->space + 8, sizeof(int));
    // buffer_return(table_id, page);
    auto iter = is_leaf_hash.find({table_id, page});
    if(iter == is_leaf_hash.end())
    {
        page_t* temp = buffer_read_page(table_id, page);
        memmove(&flag, temp->space + 8, sizeof(int));
        //buffer_return(table_id, page);
        buffer_return(temp);
        is_leaf_hash.insert({{table_id, page}, flag});
    }
    else
        flag = (iter->second);
    
    
    return flag;
}

/* 트랜잭션 오브젝트에서 락 걸어 놓은 것들을 다 관리하면서 마지막에 쫙 풀어주는 그런 과정이 필요할듯? */
/**
 * @brief  query = 35
 *       0    1    2    3    4    5
 *      10   20   30   40   50   60
 *      mid = 30 -> start = 40
 *      mid = 50 -> end = 40
 *      mid = 40 -> end = 30 
 * 
 * 
 */

pagenum_t find_leaf( pagenum_t root, int64_t table_id, int key, bool verbose ) {
    int i = 0;
    pagenum_t c_pn = root;
    Internal_page_manager wc;


    if (root == 0) {
        if (verbose) 
            printf("Empty tree.\n");
        return root;
    }


    while (!(is_leaf(table_id, c_pn) == 1)) {
        wc = Internal_page_manager(c_pn, table_id);
        
        int start = 0;
        int end = wc.get_num_of_key() - 1;
        int mid;
        int64_t value;
        while(start <= end)
        {
            mid = (start + end) / 2;
            value = wc.get_key(mid);
            if(key < value)
                end = mid - 1;
            
            else if(value < key)
                start = mid + 1;
            
            else
            {
                break;
            }
        }
        i = start > end ? start : mid + 1;
        
        c_pn = wc.get_child_pagenum(i);
        wc.flush(table_id);
    }


    // Leaf_page_manager wleaf = Leaf_page_manager(c_pn, table_id);

    // if (verbose) {
    //     printf("Leaf [");
    //     for (i = 0; i < wleaf.get_num_of_key() - 1; i++)
    //         printf("%d ", static_cast<int>(wleaf.get_key(i)));
    //     printf("%d] -> %d\n", static_cast<int>(wleaf.get_key(i)), static_cast<int>(wleaf.get_src_pagenum()));
    // }
    // wleaf.flush(table_id);
    return c_pn;
}





pagenum_t make_leaf_page( int64_t table_id ) // 내꺼
{
    pagenum_t new_page = buffer_alloc_page(table_id);

    leaf_page_t src;

    src.parent_page_num = 0;
    src.is_leaf = 1;
    src.amount_of_freespace = 4096 - 128;
    src.num_of_key = 0;
    src.right_sibling_pagenum = 0;

    page_t* temp = buffer_read_page(table_id, new_page);
    memmove(temp, (page_t*)&src, sizeof(page_t));
    //buffer_write_page(table_id, new_page, (page_t*)&src);
    //buffer_return(table_id, new_page);
    buffer_return(temp);

    return new_page;
}

pagenum_t make_internal_page( int64_t table_id ) // 내꺼
{
    pagenum_t new_page = buffer_alloc_page(table_id);

    internal_page_t src;

    src.parent_page_num = 0;
    src.is_leaf = 0;
    src.num_of_key = 0;
    src.left_most_pagenum = 0;

    page_t* temp = buffer_read_page(table_id, new_page);
    memmove(temp, (page_t*)&src, sizeof(page_t));
    buffer_return(temp);
    // buffer_write_page(table_id, new_page, (page_t*)&src);
    // buffer_return(table_id, new_page);

    return new_page;
}


pagenum_t insert_into_leaf_page(pagenum_t root, int64_t table_id, pagenum_t target_leaf, int64_t key, char* value, short val_size)
{
    Leaf_page_manager wdest = Leaf_page_manager(target_leaf, table_id); // alloc 되지 않았던 페이지는 read되지 않음.

    int i, insertion_point;

    insertion_point = 0;
    while (insertion_point < wdest.get_num_of_key() && wdest.get_key(insertion_point) < key)
        insertion_point++;
    // 이 시점에서 insertion point는 새로 집어넣을 key 바로 왼쪽 key의 바로 다음을 나타냄.

    for (i = wdest.get_num_of_key(); i > insertion_point; i--) { // insertion point 번째 인덱스를 포함해서 그 이후의
        wdest.move_slot(i-1, i); // 다른 키, 포인터들을 죄다 오른쪽으로 shift.
    }
    wdest.insert_record_in(insertion_point, key, value, val_size);
    wdest.flush(table_id);
    return wdest.get_src_pagenum();
}

pagenum_t insert_into_leaf_after_splitting(pagenum_t root, int64_t table_id, pagenum_t target_leaf, int64_t key, char* value, short val_size) 
{   
    //cout << "Leaf split\n";
    Leaf_page_manager wleaf = Leaf_page_manager(target_leaf, table_id);

    pagenum_t new_leaf_pagenum = make_leaf_page(table_id);
    Leaf_page_manager wnew_leaf = Leaf_page_manager(new_leaf_pagenum, table_id);


    Leaf_page_manager::Slot* temp_keys; //?
    char** temp_pointers; // ?
    int insertion_index, split, new_key, i, j;

    temp_keys = (Leaf_page_manager::Slot *)malloc( (wleaf.get_num_of_key() + 1) * sizeof(Leaf_page_manager::Slot) );
    if (temp_keys == NULL) {
        perror("Temporary keys array.");
        exit(EXIT_FAILURE);
    }

    temp_pointers = (char **)malloc( (wleaf.get_num_of_key() + 1) * sizeof(char *) );
    if (temp_pointers == NULL) {
        perror("Temporary pointers array.");
        exit(EXIT_FAILURE);
    }

    insertion_index = 0;
    while (insertion_index < wleaf.get_num_of_key() &&  wleaf.get_key(insertion_index) < key)
        insertion_index++;

    for (i = 0, j = 0; i < wleaf.get_num_of_key(); i++, j++) {
        if (j == insertion_index) j++;
        temp_keys[j] = wleaf.get_slot(i);
        char* temp_val = (char*)malloc(temp_keys[j].size * sizeof(char));
        memmove(temp_val, (char*)(wleaf.get_record_space()) + temp_keys[j].offset, temp_keys[j].size * sizeof(char));
        temp_pointers[j] = temp_val;
    }


    // 로직 변경, insertion point에 도달한 경우에는 점프함.
    // temp_keys[insertion_index] = key;
    // temp_pointers[insertion_index] = pointer;

    

    //split = cut(order - 1);
    /* cut for leaf*/
    int cut_pos = 0;
    int size = wleaf.get_slot(cut_pos).size + SLOT_SIZE;
    while(cut_pos < wleaf.get_num_of_key() && size <= 1983) // size : cut_pos 까지의 size합.
    {
        cut_pos++;
        size += (wleaf.get_slot(cut_pos).size + SLOT_SIZE);
    }
    cut_pos++;
    split = cut_pos;

    int temp_num_of_key = wleaf.get_num_of_key();

    /* end of cut for leaf*/

    wleaf.set_num_of_key(0);
    wleaf.set_amount_of_freespace(4096-128);

    for (i = 0; i < split; i++) {
        if(i == insertion_index)
            wleaf.insert_record_in(i, key, value, val_size);
        
        else
        {
            wleaf.insert_record_in(i, temp_keys[i].key, temp_pointers[i], temp_keys[i].size);
            
        }
            
    }

    for (i = split, j = 0; i < temp_num_of_key + 1; i++, j++) {
        if(i == insertion_index)
        {
            wnew_leaf.insert_record_in(i - split, key, value, val_size);
        }
        else
        {
            wnew_leaf.insert_record_in(j, temp_keys[i].key, temp_pointers[i], temp_keys[i].size);
        }     
    }

    for (i = 0, j = 0; i < wleaf.get_num_of_key(); i++, j++) { // 메모리 추가 해제해야함. 여기서 버그날수도?
        if (j == insertion_index) j++;
        free(temp_pointers[j]);
    }
    free(temp_pointers);
    free(temp_keys);

    wnew_leaf.set_right_sibling_pagenum(wleaf.get_right_sibling_pagenum());
    wleaf.set_right_sibling_pagenum(wnew_leaf.get_src_pagenum());

    wnew_leaf.set_src_parent(wleaf.get_src_parent());
    new_key = wnew_leaf.get_key(0);

    wnew_leaf.flush(table_id);
    wleaf.flush(table_id);

    return insert_into_parent(root, table_id, wleaf.get_src_pagenum(), new_key, wnew_leaf.get_src_pagenum());
}

pagenum_t insert_into_internal_page(pagenum_t root, int64_t table_id, pagenum_t n, int left_index, int64_t key, pagenum_t right) 
{
    Internal_page_manager wn_page = Internal_page_manager(n, table_id);

    int i;

    for (i = wn_page.get_num_of_key(); i > left_index; i--) {    // left_index에 새로운 키가 들어간다. 
        wn_page.set_child_pagenum(i + 1, wn_page.get_child_pagenum(i)); // 이를 위해 left index위치를 비워주려고 나머지를 shift.
        wn_page.set_key(i, wn_page.get_key(i - 1));
    }
    wn_page.set_child_pagenum(left_index + 1, right);           // split된 이후 생긴 새로운 노드를 또 달아줘야 함.
    wn_page.set_key(left_index, key);
    wn_page.set_num_of_key(wn_page.get_num_of_key() + 1);
    wn_page.flush(table_id);
    return root;
}

pagenum_t insert_into_internal_page_after_splitting(pagenum_t root, int64_t table_id, pagenum_t old_node, 
                                                    int left_index, int key, pagenum_t right) 
{
    //cout << "Internal split\n";
    Internal_page_manager wold_page = Internal_page_manager(old_node, table_id); 

    int i, j, split;  // k_prime은 분할 후에 위쪽 노드로 보내줄 키 값을 말하는거임.
    int64_t k_prime;
    pagenum_t new_node, child;
    int64_t * temp_keys;
    pagenum_t* temp_pointers;
    // 기본적으로 임시 포인터 배열과 키 배열을 만든 뒤에, 이를 다시 원래 노드와 새 노드에 재분배하는 방식으로 구현됨.
    
    temp_pointers = (pagenum_t*)malloc( (order + 1) * sizeof(pagenum_t) ); // 임시 포인터 배열. 값을 하나 더 넣어야 해서 크기가 1 큼.
    if (temp_pointers == NULL) {
        perror("Temporary pointers array for splitting nodes.");
        exit(EXIT_FAILURE);
    }
    temp_keys = (int64_t *)malloc( order * sizeof(int64_t) );               // * 키 배열
    if (temp_keys == NULL) {
        perror("Temporary keys array for splitting nodes.");
        exit(EXIT_FAILURE);
    }

    for (i = 0, j = 0; i < wold_page.get_num_of_key() + 1; i++, j++) {
        if (j == left_index + 1) j++;
        temp_pointers[j] = wold_page.get_child_pagenum(i);  // 기존의 포인터 배열을 temp에 죄다복사.  
    }                                                               // 새로 들어갈 포인터의 자리는 비워둠.

    for (i = 0, j = 0; i < wold_page.get_num_of_key(); i++, j++) {
        if (j == left_index) j++;
        temp_keys[j] = wold_page.get_key(i);
    }

    temp_pointers[left_index + 1] = right;                          // 임시 배열들은 완벽해졌음.
    temp_keys[left_index] = key;

    /* Create the new node and copy
     * half the keys and pointers to the
     * old and half to the new.
     */  
    split = cut(order);                                             // 노드 스플릿의 기준점이 됨. 버그 발생 가능
    new_node = make_internal_page(table_id);                                         // 노드 새로 만들어주고.

    Internal_page_manager wnew_page = Internal_page_manager(new_node, table_id);

    wold_page.set_num_of_key(0);
    for (i = 0; i < split - 1; i++) {       
        wold_page.set_child_pagenum(i, temp_pointers[i]);
        wold_page.set_key(i, temp_keys[i]);
        wold_page.set_num_of_key(wold_page.get_num_of_key() + 1);     
    }
    wold_page.set_child_pagenum(i, temp_pointers[i]);
    k_prime = temp_keys[split - 1]; // split -1 번째 인덱스의 키가 prime key가 된다.
    for (++i, j = 0; i < order; i++, j++) {                         //  1 2 3 4    라는 상황에서 (order = 4)
        wnew_page.set_child_pagenum(j, temp_pointers[i]);           // p p p p p      
        wnew_page.set_key(j, temp_keys[i]);
        wnew_page.set_num_of_key(wnew_page.get_num_of_key() + 1);   //  1  |  2(부모로 보낼 키)  |  3 4    이런 상황이 된 거임.         
                                                                    // p p |                     | p p p
    }                                                               
    wnew_page.set_child_pagenum(j, temp_pointers[i]);
    free(temp_pointers); // temp 다 없애버리자
    free(temp_keys);
    wnew_page.set_src_parent(wold_page.get_src_parent()); // 새 노드의 parent는 기존 노드의 parent와 같아야 한다
    for (i = 0; i <= wnew_page.get_num_of_key(); i++) {     // old 노드의 자식들은 원래 부모가 old노드이지만
                                                    // new node로 옮겨진 child 노드들의 부모는 new node로 바꿔야함.
        child = wnew_page.get_child_pagenum(i);
        Internal_page_manager wchild_page = Internal_page_manager(child, table_id);

        wchild_page.set_src_parent(wnew_page.get_src_pagenum());
        wchild_page.flush(table_id);
    }

    wold_page.flush(table_id);
    wnew_page.flush(table_id);
    /* Insert a new key into the parent of the two
     * nodes resulting from the split, with
     * the old node to the left and the new to the right.
     */

    return insert_into_parent(root, table_id, old_node, k_prime, new_node); // 노드  |  키  | 노드 를 위로 보내줌.
}

pagenum_t insert_into_parent(pagenum_t root, int64_t table_id, pagenum_t left, int key, pagenum_t right) {

    int left_index;
    pagenum_t parent;

    page_t* temp = buffer_read_page(table_id, left); // 기존 리프 노드의 부모를 알아냄
    memmove(&parent, temp->space, sizeof(pagenum_t));
    //buffer_return(table_id, left);
    buffer_return(temp);

    /* Case: new root. */

    if (parent == 0)
    {
        return insert_into_new_root(table_id, left, key, right);
    }
        

    /* Case: leaf or node. (Remainder of
     * function body.)  
     */

    /* Find the parent's pointer to the left 
     * node.
     */

    left_index = get_left_index(table_id, parent, left); // left를 가리키는 포인터가 부모 노드에서 가지는 인덱스.


    /* Simple case: the new key fits into the node. 
     */
    Internal_page_manager wparent_page = Internal_page_manager(parent, table_id);

    if (wparent_page.get_num_of_key() < order - 1)
    {
        wparent_page.flush(table_id);
        return insert_into_internal_page(root, table_id, parent, left_index, key, right); // 부모 노드가 비었으면 걍 넣지.
    }
        

    /* Harder case:  split a node in order 
     * to preserve the B+ tree properties.
     */
    wparent_page.flush(table_id);
    return insert_into_internal_page_after_splitting(root, table_id, parent, left_index, key, right); // 부모도 꽉 찼으면 부모도 스플릿 해야지.
}

pagenum_t start_new_tree(int64_t table_id, int64_t key, char* value, short val_size) {

    pagenum_t root_pn = make_leaf_page(table_id);
    Leaf_page_manager wroot = Leaf_page_manager(root_pn, table_id);

    wroot.insert_record_in(0, key, value, val_size);
    wroot.flush(table_id); // 여기!

    // 헤더페이지 루트 설정
    page_t* header_page = buffer_read_page(table_id, 0);
    memmove(header_page->space + 16, &root_pn, sizeof(pagenum_t));
    ((buffer_frame*)header_page)->is_dirty_ = true;
    //buffer_set_dirty(table_id, 0);
    buffer_return(header_page);
    //buffer_return(table_id, 0);

    return root_pn;
}

pagenum_t insert_into_new_root(int64_t table_id, pagenum_t left, int64_t key, pagenum_t right) {

    pagenum_t root_pn = make_internal_page(table_id);
    Internal_page_manager wroot = Internal_page_manager(root_pn, table_id);

    wroot.set_key(0, key);
    wroot.set_child_pagenum(0, left);
    wroot.set_child_pagenum(1, right);
    wroot.set_num_of_key(wroot.get_num_of_key() + 1);

    page_t* left_page = buffer_read_page(table_id, left); 
    page_t* right_page = buffer_read_page(table_id, right);
    
    

    memmove(left_page->space, &root_pn, sizeof(pagenum_t));
    memmove(right_page->space, &root_pn, sizeof(pagenum_t));
    // dirty bit 올려야함.
    ((buffer_frame*)left_page)->is_dirty_ = true;
    ((buffer_frame*)right_page)->is_dirty_ = true;

    // buffer_return(table_id, left); 
    // buffer_return(table_id, right);
    buffer_return(left_page);
    buffer_return(right_page);

    // 헤더페이지 루트 설정
    page_t* header_page = buffer_read_page(table_id, 0);
    memmove(header_page->space + 16, &root_pn, sizeof(pagenum_t));
    ((buffer_frame*)header_page)->is_dirty_ = true;
    // buffer_return(table_id, 0);
    buffer_return(header_page);

    memmove(right_page->space, &root_pn, sizeof(pagenum_t));

    wroot.flush(table_id);

    return root_pn;
}


pagenum_t insert( pagenum_t root, int64_t table_id,  int64_t key, char* value, short val_size ) {

    //record * pointer;
    //node * leaf;
    pagenum_t leaf;

    /* The current implementation ignores
     * duplicates.
     */

    // if (find(root, key, false) != NULL) // find가 아직 구현이 안됐음.
    //     return root;

    /* Create a new record for the
     * value.
     */


    /* Case: the tree does not exist yet.
     * Start a new tree.
     */
    if(root < 0)
    {
        cerr << "Invalid root page number!\n" << "input : " << root << endl;
        exit(EXIT_FAILURE);
    }

    if (root == 0) 
        return start_new_tree(table_id, key, value, val_size);

    /* Case: the tree already exists.
     * (Rest of function body.)
     */
    leaf = find_leaf(root, table_id, key, false);
    Leaf_page_manager leaf_page = Leaf_page_manager(leaf, table_id);

    /* Case: leaf has room for key and pointer.
     */

    if (leaf_page.get_amount_of_freespace() >= (SLOT_SIZE+val_size)) {
        leaf_page.flush(table_id);
        leaf = insert_into_leaf_page(root, table_id, leaf, key, value, val_size);
        return root;
    }


    /* Case:  leaf must be split.
     */
    leaf_page.flush(table_id);
    return insert_into_leaf_after_splitting(root, table_id, leaf, key, value, val_size);
    
}


/*         DELETION START           */
int get_neighbor_index( int64_t table_id, pagenum_t page ) { // 해당 노드의 왼쪽 sibling 노드가 부모 노드 상에서 몇 인덱스에 위치하는지 리턴함.

    int i;
    page_manager* wpage;
    if(is_leaf(table_id, page)) wpage = new Leaf_page_manager(page, table_id);
    else wpage = new Internal_page_manager(page, table_id);

    /* Return the index of the key to the left
     * of the pointer in the parent pointing
     * to n.  
     * If n is the leftmost child, this means // 제일 왼쪽 노드이면, -1이 리턴됨!
     * return -1.
     */
    pagenum_t parent = wpage->get_src_parent();
    Internal_page_manager wparent = Internal_page_manager(parent, table_id);
    wpage->flush(table_id);
    for (i = 0; i <= wparent.get_num_of_key(); i++)
    {
        if (wparent.get_child_pagenum(i) == page)
        {
            delete wpage;
            wparent.flush(table_id);
            return i - 1;
        }
    }
    wparent.flush(table_id);
    delete wpage;
    // Error state.
    printf("Search for nonexistent pointer to node in parent.\n");
    printf("Page #%#lx\n", page);
    exit(EXIT_FAILURE);
}

pagenum_t remove_entry_from_node(int64_t table_id, pagenum_t n, int key, pagenum_t pointer) {

    int i, num_pointers;
    bool is_leaf_flag = is_leaf(table_id, n);
    page_manager* wn;
    if(is_leaf_flag) wn = new Leaf_page_manager(n, table_id);
    else wn = new Internal_page_manager(n, table_id);

    // Remove the key and shift other keys accordingly.
    i = 0;
    while (wn->get_key(i) != key) // 여기서?
        i++;

    if(is_leaf_flag)
    {
        dynamic_cast<Leaf_page_manager*>(wn)->delete_record(i);
        wn->flush(table_id);
        delete wn;
    }
    //   p0 k1 p1 k2 p2
    else
    {
        Internal_page_manager* internal_wn = dynamic_cast<Internal_page_manager*>(wn);
        for (++i; i < wn->get_num_of_key(); i++) // 해당 키 삭제 후 shift까지 했음.
        {
            internal_wn->set_key(i-1, internal_wn->get_key(i));
        }
        i = 0;
        while (internal_wn->get_child_pagenum(i) != pointer)
            i++;
        
        for (++i; i < wn->get_num_of_key() + 1; i++) // 해당 키 삭제 후 shift까지 했음. -> 레코드 증발(find 수정!)
        {
            internal_wn->set_child_pagenum(i-1, internal_wn->get_child_pagenum(i));
        }
        internal_wn->set_num_of_key(internal_wn->get_num_of_key() - 1);
        internal_wn->flush(table_id);
        delete wn;
    }

    return n;
}

pagenum_t adjust_root(int64_t table_id, pagenum_t root) {  // 루트 노드에서 키 삭제가 일어난 후에 실행되는 함수인듯.

    pagenum_t new_root;
    page_manager* wroot;
    if(is_leaf(table_id, root)) wroot = new Leaf_page_manager(root, table_id);
    else wroot = new Internal_page_manager(root, table_id);
    /* Case: nonempty root.
     * Key and pointer have already been deleted,
     * so nothing to be done.
     */

    if (wroot->get_num_of_key() > 0)
    {
        wroot->flush(table_id);
        return root;
    }
        

    /* Case: empty root. 
     */

    // If it has a child, promote 
    // the first (only) child
    // as the new root.

    if (dynamic_cast<Leaf_page_manager*>(wroot) == nullptr) { // internal node라면
        Internal_page_manager* winternal_root = dynamic_cast<Internal_page_manager*>(wroot);
        if(winternal_root == nullptr)
        {
            cerr << "Wrong pointer on adjusting root\n";
            exit(EXIT_FAILURE);
        }
        new_root = winternal_root->get_child_pagenum(0);
        page_t* nr_page = buffer_read_page(table_id, new_root);
        pagenum_t t = 0;
        memmove(nr_page->space, &t, sizeof(pagenum_t));
        //dirty bit 설정해야함
        ((buffer_frame*)nr_page)->is_dirty_ = true;
        // buffer_return(table_id, new_root);
        buffer_return(nr_page);
        winternal_root->flush(table_id);

        //헤더 루트 재설정
        page_t* temp_header = buffer_read_page(table_id, 0);
        memmove(temp_header->space + 16, &new_root, sizeof(pagenum_t));
        ((buffer_frame*)temp_header)->is_dirty_ = true;
        // buffer_return(table_id, 0);
        buffer_return(temp_header);
        buffer_free_page(table_id, root);
        is_leaf_hash.erase({table_id, root});
    }

    // If it is a leaf (has no children),
    // then the whole tree is empty.

    else
    {
        //헤더 페이지 루트
        pagenum_t zero = 0;
        page_t* temp_header = buffer_read_page(table_id, 0);
        memmove(temp_header->space + 16, &zero, sizeof(pagenum_t));
        ((buffer_frame*)temp_header)->is_dirty_ = true;
        // buffer_return(table_id, 0);
        buffer_return(temp_header);
        new_root = 0;
        wroot->flush(table_id);
        buffer_free_page(table_id, root);
        is_leaf_hash.erase({table_id, root});
    }
        
    
    // cerr << "wroot delete합니다." << endl;
    delete wroot;
    return new_root;
}

pagenum_t coalesce_nodes(pagenum_t root, int64_t table_id,  pagenum_t n, pagenum_t neighbor, int neighbor_index, int k_prime) {
    // 노드 병합
    int i, j, neighbor_insertion_index, n_end;
    pagenum_t tmp;

    bool is_this_leaf = is_leaf(table_id, n);

    //cerr << "노드병합" << endl;
    /* Swap neighbor with node if node is on the
     * extreme left and neighbor is to its right.
     */

    if (neighbor_index == -1) { // left most일 때?
        tmp = n;
        n = neighbor;
        neighbor = tmp;
    }

    /* Starting point in the neighbor for copying
     * keys and pointers from n.
     * Recall that n and neighbor have swapped places
     * in the special case of n being a leftmost child.
     */

    

    /* Case:  nonleaf node.
     * Append k_prime and the following pointer.
     * Append all pointers and keys from the neighbor.
     */

    if (!is_this_leaf) { // internal node인 경우.

        /* Append k_prime.
         */
        Internal_page_manager wneighbor = Internal_page_manager(neighbor, table_id);
        Internal_page_manager wn = Internal_page_manager(n, table_id);
        neighbor_insertion_index = wneighbor.get_num_of_key();  // 이웃 노드에 기존 노드 내용을 복사하려고 하는듯.
                                                                // 이웃 노드 삽입 시작점.

        wneighbor.set_key(neighbor_insertion_index, k_prime); // k prime을 삽입하고

        wneighbor.set_num_of_key(wneighbor.get_num_of_key() + 1);

        // 이게 뭘까
        n_end = wn.get_num_of_key();

        for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) { // 나머지 내용을 옮겨옴.
            wneighbor.set_key(i, wn.get_key(j)); // i, j 유의
            wneighbor.set_child_pagenum(i, wn.get_child_pagenum(j));
            wneighbor.set_num_of_key(wneighbor.get_num_of_key() + 1);
            
        }
        wn.set_num_of_key(wn.get_num_of_key() - n_end);
        /* The number of pointers is always
         * one more than the number of keys.
         */

        wneighbor.set_child_pagenum(i, wn.get_child_pagenum(j));

        /* All children must now point up to the same parent.
         */ // 자식 노드 재설정.

        for (i = 0; i < wneighbor.get_num_of_key() + 1; i++) {
            tmp = wneighbor.get_child_pagenum(i);
            page_t* temp = buffer_read_page(table_id, tmp);
            memmove(temp->space, &neighbor, sizeof(pagenum_t));
            //dirty bit 설정해야함
            ((buffer_frame*)temp)->is_dirty_ = true;
            // buffer_return(table_id, tmp);
            buffer_return(temp);
        }
        wneighbor.flush(table_id);
        wn.flush(table_id);
    }

    /* In a leaf, append the keys and pointers of
     * n to the neighbor.
     * Set the neighbor's last pointer to point to
     * what had been n's right neighbor.
     */

    else {
        Leaf_page_manager wneighbor = Leaf_page_manager(neighbor, table_id);
        Leaf_page_manager wn = Leaf_page_manager(n, table_id);
        neighbor_insertion_index = wneighbor.get_num_of_key();   // 이웃 노드에 기존 노드 내용을 복사하려고 하는듯.
                                                         // 이웃 노드 삽입 시작점.
        for (i = neighbor_insertion_index, j = 0; j < wn.get_num_of_key(); i++, j++) { //레코드랑 키를 그냥 옮기는거임.
            Leaf_page_manager::Slot temp_slot = wn.get_slot(j);
            char* temp_space = new char[temp_slot.size];
            wn.get_record_by_index(j, temp_space);
            //strcpy(temp_space, wn.get_record_by_index(j).c_str());
            wneighbor.insert_record_in(i, temp_slot.key, temp_space, temp_slot.size);
            delete temp_space;
        }
        wneighbor.set_right_sibling_pagenum(wn.get_right_sibling_pagenum());    // right sibling pagenum 바꿔주는거.
        wneighbor.flush(table_id);
        wn.flush(table_id);
    }

    page_manager* pm;
    if(is_leaf(table_id, n)) pm = new Leaf_page_manager(n, table_id);
    else pm = new Internal_page_manager(n, table_id);

    pagenum_t parent = pm->get_src_parent();
    pm->flush(table_id);
    delete pm;
    root = delete_entry(root, table_id, parent, k_prime, n);               // 빈 노드 삭제. 이거 부활시켜야한다.
    buffer_free_page(table_id, n);
    is_leaf_hash.erase({table_id, root});
    return root;
}

pagenum_t redistribute_nodes(pagenum_t root,int64_t table_id, pagenum_t n, pagenum_t neighbor, int neighbor_index, 
        int k_prime_index, int64_t k_prime) {  

    int i;
    pagenum_t tmp;
    //cout << "재분배 일어남\n";
    /* Case: n has a neighbor to the left. 
     * Pull the neighbor's last key-pointer pair over
     * from the neighbor's right end to n's left end.
     */
    if(is_leaf(table_id, n))
    {
        Leaf_page_manager wn = Leaf_page_manager(n, table_id);
        Leaf_page_manager wneighbor = Leaf_page_manager(neighbor, table_id);
        int number_of_distr = 0;
        int free_space = wn.get_amount_of_freespace();
        for(int x = 0; free_space >= 2500; x++)
        {
            if(x >= wneighbor.get_num_of_key())
            {
                cerr << "It should be merged, but redistribution?\n";
                exit(EXIT_FAILURE);
            }
            if(neighbor_index != -1) // 모자란 노드가 왼쪽 끝이 아닌 경우에는 이웃 노드의 끝부터
            {
                free_space -= (wneighbor.get_slot(wneighbor.get_num_of_key() - 1 - x).size + SLOT_SIZE);
                number_of_distr++;
            }
            else
            {
                free_space -= (wneighbor.get_slot(x).size + SLOT_SIZE);
                number_of_distr++;
            }    
        }
        if(neighbor_index != -1) // 리프이면서 왼쪽 끝이 아닌 경우
        {
            for (i = wn.get_num_of_key(); i > 0; i--) 
            {                           // 키 인덱스랑 연동해서 슬롯 다 오른쪽으로 밀어버림. 맨 왼쪽에 슬롯 하나 남음
                wn.move_slot(i - 1, i - 1 + number_of_distr);
            }
            for(int x = 1; x <= number_of_distr; x++)
            {
                //string temp = wneighbor.get_record_by_index(wneighbor.get_num_of_key() -  1);  // 이웃 노드의 끝 레코드를 복사함
                Leaf_page_manager::Slot t_slot = wneighbor.get_slot(wneighbor.get_num_of_key() -  1);

                char* t_string = new char[t_slot.size];
                wneighbor.get_record_by_index(wneighbor.get_num_of_key() -  1, t_string);
                //memmove(t_string, temp.c_str(), sizeof(char) * t_slot.size);                  // 타입을 맞추기 위함.

                wn.insert_record_in(number_of_distr - x, t_slot.key, t_string, t_slot.size); // 이웃 노드의 끝 키, 슬롯을 모자란 노드의 맨 왼쪽에 삽입.
                // 여기서 이미 num of key가 하나 늘어남.
                delete t_string;
                wneighbor.delete_record(wneighbor.get_num_of_key() - 1); // 여기서 num of key가 하나 줄어들었음.
            }
            Internal_page_manager wn_parent = Internal_page_manager(wn.get_src_parent(), table_id); // 모자란 노드의 부모의 키를 재설정해야함.
            wn_parent.set_key(k_prime_index, wn.get_key(0));
            wn_parent.flush(table_id);

            wn.flush(table_id);
            wneighbor.flush(table_id);
        }
        else                    // 리프인데 왼쪽 끝인 경우.
        {
            for(int x = 0; x < number_of_distr; x++)
            {
                //string temp = wneighbor.get_record_by_index(0);  // 이웃 노드의 왼쪽 끝 레코드를 복사
                Leaf_page_manager::Slot t_slot = wneighbor.get_slot(0);

                char* t_string = new char[t_slot.size];
                wneighbor.get_record_by_index(0, t_string);
                //memmove(t_string, temp.c_str(), sizeof(char) * t_slot.size);                  // 타입을 맞추기 위함.

                wn.insert_record_in(wn.get_num_of_key(), t_slot.key, t_string, t_slot.size); // 이웃 노드의 끝 키, 슬롯을 모자란 노드의 맨 왼쪽에 삽입.
                // 여기서 이미 num of key가 하나 늘어남.
                delete t_string;
                wneighbor.delete_record(0); // 여기서 num of key가 하나 줄어들었음.
            }

            Internal_page_manager wn_parent = Internal_page_manager(wn.get_src_parent(), table_id);
            wn_parent.set_key(k_prime_index, wneighbor.get_key(0));
            wn_parent.flush(table_id);

            wn.flush(table_id);
            wneighbor.flush(table_id);
        }
    }
    else
    {
        Internal_page_manager wn = Internal_page_manager(n, table_id);
        Internal_page_manager wneighbor = Internal_page_manager(neighbor, table_id);
        if(neighbor_index != -1) // internal 이면서 왼쪽 끝이 아닌 경우
        {
            wn.set_child_pagenum(wn.get_num_of_key() + 1, wn.get_child_pagenum(wn.get_num_of_key()));
            for (i = wn.get_num_of_key(); i > 0; i--) {                           // 키 인덱스랑 연동해서 키, 포인터 다 오른쪽으로 밀어버림
                wn.set_key(i, wn.get_key(i-1));
                wn.set_child_pagenum(i, wn.get_child_pagenum(i-1));
            }

            wn.set_child_pagenum(0, wneighbor.get_child_pagenum(wneighbor.get_num_of_key()));

            tmp = wn.get_child_pagenum(0);      // 옮겨온 자식 부모 재설정.
            page_manager* tmp_page;
            if(is_leaf(table_id, tmp)) tmp_page = new Leaf_page_manager(tmp, table_id);
            else tmp_page = new Internal_page_manager(tmp, table_id);
            tmp_page->set_src_parent(n);
            tmp_page->flush(table_id); // ?????????????버퍼
            delete tmp_page;

            wneighbor.set_child_pagenum(wneighbor.get_num_of_key(), 0);
            wn.set_key(0, k_prime);            // internal은 b트리랑 구조가 비슷하니 k prime(부모노드에 있던 키값)을 넣음

            Internal_page_manager wn_parent = Internal_page_manager(wn.get_src_parent(), table_id); 
            wn_parent.set_key(k_prime_index, wneighbor.get_key(wneighbor.get_num_of_key() - 1)); // k_prime이 있던 자리에는 이웃 노드의 끝 키가 들어감.
            wn_parent.flush(table_id);

            wn.set_num_of_key(wn.get_num_of_key() + 1);
            wneighbor.set_num_of_key(wneighbor.get_num_of_key() - 1);
            wn.flush(table_id);
            wneighbor.flush(table_id);
        }
        else                    // internal 인데 왼쪽 끝인 경우.
        {
            wn.set_key(wn.get_num_of_key(), k_prime);
            wn.set_child_pagenum(wn.get_num_of_key() + 1, wneighbor.get_child_pagenum(0));

            tmp = wn.get_child_pagenum(wn.get_num_of_key() + 1); // 받아온 자식 부모를 n으로 설정.
            page_manager* tmp_page;
            if(is_leaf(table_id, tmp)) { tmp_page = new Leaf_page_manager(tmp, table_id); }
            else { tmp_page = new Internal_page_manager(tmp, table_id);}
            tmp_page->set_src_parent(n);
            tmp_page->flush(table_id);
            delete tmp_page;
            
            Internal_page_manager wn_parent = Internal_page_manager(wn.get_src_parent(), table_id);
            wn_parent.set_key(k_prime_index, wneighbor.get_key(0));
            wn_parent.flush(table_id);

            for (i = 0; i < wneighbor.get_num_of_key() - 1; i++) {      // 이웃 키들을 전부 왼쪽으로 밀어버림 -> 수정했다!
                wneighbor.set_key(i, wneighbor.get_key(i + 1));
                wneighbor.set_child_pagenum(i, wneighbor.get_child_pagenum(i + 1));
            }
            wneighbor.set_child_pagenum(i, wneighbor.get_child_pagenum(i + 1));
            //wneighbor.set_key(i, wneighbor.get_key(i + 1));

            wn.set_num_of_key(wn.get_num_of_key() + 1);
            wneighbor.set_num_of_key(wneighbor.get_num_of_key() - 1);
            wn.flush(table_id);
            wneighbor.flush(table_id);
        }
    }
    return root;
}

pagenum_t delete_entry( pagenum_t root, int64_t table_id, pagenum_t n, int64_t key, pagenum_t pointer ) {

    int64_t min_keys;
    pagenum_t neighbor;
    int neighbor_index;
    int k_prime_index;
    int64_t k_prime;
    int capacity;

    // Remove key and pointer from node.

    n = remove_entry_from_node(table_id, n, key, pointer);

    /* Case:  deletion from the root. 
     */

    if (n == root) 
        return adjust_root(table_id, root);


    /* Case:  deletion from a node below the root.
     * (Rest of function body.)
     */

    /* Determine minimum allowable size of node,
     * to be preserved after deletion.
     */

    //min_keys = n->is_leaf ? cut(order - 1) : cut(order) - 1; // order = 짝수 -> 리프 minimum이 1 작음.
    

    /* Case:  node stays at or above minimum.
     * (The simple case.)
     */
    if(is_leaf(table_id, n))
    {
        Leaf_page_manager wn = Leaf_page_manager(n, table_id);
        if(wn.get_amount_of_freespace() < 2500)
        {
            wn.flush(table_id);
            return root;
        }
        wn.flush(table_id);
    }
    else
    {
        Internal_page_manager wn = Internal_page_manager(n, table_id);
        if(wn.get_num_of_key() >= 124)
        {
            wn.flush(table_id);
            return root;
        }
        wn.flush(table_id);
    }

    // if (n->num_keys >= min_keys)                    // 이 파트는 수정이 필요함.
    //     return root;                                // 과제에서는 우선 리프인지 아닌지 여부에 따라  borrow/merge 판정을 위한
                                                       // 기준이 상이함.(리프는 프리스페이스로 따짐.)

    /* Case:  node falls below minimum.
     * Either coalescence or redistribution
     * is needed.
     */

    /* Find the appropriate neighbor node with which
     * to coalesce.
     * Also find the key (k_prime) in the parent
     * between the pointer to node n and the pointer
     * to the neighbor.
     */

    neighbor_index = get_neighbor_index( table_id, n );       // 이웃 인덱스 가져오고
    k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;  // prime 인덱스는 n이 left most면 0, 아니면 그냥 이웃 인덱스.

    page_manager* temp;
    if(is_leaf(table_id, n)) temp = new Leaf_page_manager(n, table_id);
    else temp = new Internal_page_manager(n, table_id);
    Internal_page_manager parent = Internal_page_manager(temp->get_src_parent(), table_id);
    parent.flush(table_id);
    temp->flush(table_id);
    delete temp;
    
    k_prime = parent.get_key(k_prime_index);// k_prime은 이웃 (여기)   나 사이에 껴있는 부모의 키 값. left most면 그냥 맨 처음 키.

    neighbor = neighbor_index == -1 ? parent.get_child_pagenum(1) : 
                                      parent.get_child_pagenum(neighbor_index); 
                                      // left most인 경우엔 이웃을 자기 오른쪽으로, 아닌 경우엔 왼쪽으로 설정.


    /* Coalescence. */ // 원칙대로면 아마 죄우를 모두 확인해야 할 거 같기는 한데, 여기선 한쪽만 본다.
    if(is_leaf(table_id, n))
    {
        Leaf_page_manager wneighbor = Leaf_page_manager(neighbor, table_id);
        Leaf_page_manager wn = Leaf_page_manager(n, table_id);
        if(wneighbor.get_amount_of_freespace() >= (DEFAULT_PAGE_FREE_SPACE - wn.get_amount_of_freespace()))
        {
            wneighbor.flush(table_id);
            wn.flush(table_id);
            return coalesce_nodes(root, table_id, n, neighbor, neighbor_index, k_prime);
        }
        wneighbor.flush(table_id);
        wn.flush(table_id);
    }
    else
    {
        Internal_page_manager wneighbor = Internal_page_manager(neighbor, table_id);
        if(wneighbor.get_num_of_key() <= (order - 1)/2)
        {
            wneighbor.flush(table_id);
            return coalesce_nodes(root, table_id, n, neighbor, neighbor_index, k_prime);
        }
        wneighbor.flush(table_id);
    }

    /* Redistribution. */

    return redistribute_nodes(root, table_id, n, neighbor, neighbor_index, k_prime_index, k_prime); // 큰 수정이 필요할듯. 리프는 레코드를 여러개 분배하기도 한다.
}

pagenum_t db_delete_with_root(pagenum_t root, int64_t table_id, int64_t key) {

    pagenum_t key_leaf;
    // record * key_record; 
    
    key_leaf = find_leaf(root, table_id, key, false);
    if (key_leaf != 0) {
        root = delete_entry(root, table_id, key_leaf, key, __LONG_LONG_MAX__);
    }
    return root;
}