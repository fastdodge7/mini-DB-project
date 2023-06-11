#include <gtest/gtest.h>
#include "file.h"
#include "my_bpt.h"
#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <cstdio>
#include <ctime>
#include <algorithm>
#include <vector>
#include <random>
#include <unistd.h>
#include <utility>



using namespace std;

static const std::string CHARACTERS {
	"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"};

static const std::string BASE_TABLE_NAME { "table" };
static constexpr int NUM_TABLES { 1 };

static constexpr int NUM_KEYS { 10000 };

static constexpr int MIN_VAL_SIZE { 95 };
static constexpr int MAX_VAL_SIZE { 95 };

int64_t table_id;
int64_t key;
char* value;
uint16_t sizeT;

char ret_value[MAX_VAL_SIZE];
uint16_t ret_size;

std::vector<std::pair<int64_t, std::string>> key_value_pairs {};
std::vector<std::pair<std::string, int64_t>> tables {};

std::random_device rd;
std::mt19937 gen;
std::uniform_int_distribution<uint16_t> len_dis;
std::uniform_int_distribution<int> char_dis;

std::default_random_engine rng;


const int NUM_OF_THREAD = 20;
const int NUM_BUF = 700;

pthread_t thread_pool[NUM_OF_THREAD];
pagenum_t last;
vector<int> list;
vector<int> trash;

std::mt19937 test_engine;
std::uniform_int_distribution<int> distribution;

pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond1 = PTHREAD_COND_INITIALIZER;
pthread_cond_t cond2 = PTHREAD_COND_INITIALIZER;
pthread_cond_t cond3 = PTHREAD_COND_INITIALIZER;
pthread_cond_t cond4 = PTHREAD_COND_INITIALIZER;



class IndexFileManager : public ::testing::Test{
protected:
	void SetUp() override
	{
        
	}
	void TearDown() override
	{
	}
};

void* find_only_trx(void* argv)
{
    //Argv* arguements = (Argv*)argv;
    char thread_ret_value[MAX_VAL_SIZE];
    uint16_t thread_ret_size = 0;
    auto generator = bind(distribution, test_engine);
    int new_trx_id = trx_begin();

    for(int j = 0; j < tables.size(); j++)
    {
        for(int i = 0; i < 25000; i++) // 랜덤
        {
            int index = generator();
            //cerr << "Find with KEY : " << index+1 << "\n";
            EXPECT_EQ(db_find(j, key_value_pairs[index].first, thread_ret_value, &thread_ret_size, new_trx_id), 0);
            EXPECT_FALSE(key_value_pairs[index].second.size() != thread_ret_size ||
                            key_value_pairs[index].second != std::string(thread_ret_value, thread_ret_size));
        }
    }
    
    // for(int i = 0; i < 1; i++)
    // {
    //     for (const auto& t: tables) { // 순차적으로
    //         for (const auto& kv: key_value_pairs)
    //         {
    //             ret_size = 0;
    //             memset(ret_value, 0x00, MAX_VAL_SIZE);
    //             EXPECT_EQ(db_find(t.second, kv.first, thread_ret_value, &thread_ret_size, new_trx_id), 0);
    //             EXPECT_FALSE(kv.second.size() != thread_ret_size ||
    //                         kv.second != std::string(thread_ret_value, thread_ret_size));
    //         }
    //     }
    // }
    
    // for(int i = 0; i < 10000; i++) // 동일 레코드에 집중적으로
    // {
    //     EXPECT_EQ(db_find(0, 10, thread_ret_value, &thread_ret_size, new_trx_id), 0);
    //     EXPECT_FALSE(key_value_pairs[9].second.size() != thread_ret_size ||
    //                     key_value_pairs[9].second != std::string(thread_ret_value, thread_ret_size));
    // }
    trx_commit(new_trx_id);
    //db_find(arguements->table_id, arguements->key, arguements->ret_val, arguements->val_size, arguements->trx_id);
}

void* update_only_trx(void* argv)
{
    //Argv* arguements = (Argv*)argv;
    char thread_ret_value[MAX_VAL_SIZE] = "updated #0";
    char thread__value[MAX_VAL_SIZE];
    thread_ret_value[9] = *(int*)argv + '0';
    uint16_t thread_ret_size = 0;
    auto generator = bind(distribution, test_engine);
    int new_trx_id = trx_begin();
    // for(int i = 0; i < 1; i ++)
    // {
    //     for (const auto& t: tables) {
    //         for (const auto& kv: key_value_pairs)
    //         {
    //             ret_size = 0;
    //             memset(ret_value, 0x00, MAX_VAL_SIZE);
    //             if(db_update(t.second, kv.first, thread_ret_value,kv.second.size() ,&thread_ret_size, new_trx_id) < 0)
    //             {
    //                 cerr << "Transaction was aborted.\n";
    //                 return nullptr;
    //             }
    //             //EXPECT_EQ(db_update(t.second, kv.first, thread_ret_value,kv.second.size() ,&thread_ret_size, new_trx_id), 0);
    //             if(db_find(t.second, kv.first, thread__value, &thread_ret_size, new_trx_id) < 0)
    //             {
    //                 cerr << "Transaction was aborted.\n";
    //                 return nullptr;
    //             }
    //             //EXPECT_EQ(db_find(t.second, kv.first, thread__value, &thread_ret_size, new_trx_id), 0);
    //             EXPECT_FALSE(kv.second.size() != thread_ret_size);
    //             EXPECT_FALSE(std::string(thread_ret_value) != std::string(thread__value));
    //             //cerr << "Find : \n";
    //         }
    //     }
    // }
    for(int j = 0; j < tables.size(); j++)
    {
        for(int i = 0; i < 30000; i++) // 랜덤
        {
            int index = generator();
            //cerr << "Find with KEY : " << index+1 << "\n";
            if(db_find(j, key_value_pairs[index].first, thread__value, &thread_ret_size, new_trx_id) < 0)
            {
                cerr << "Transaction was aborted.\n";
                return nullptr;
            }
            if(db_update(j, key_value_pairs[index].first, thread_ret_value, key_value_pairs[index].second.size(),&thread_ret_size, new_trx_id) < 0)
            {
                cerr << "Transaction was aborted.\n";
                return nullptr;
            }
            if(db_find(j, key_value_pairs[index].first, thread__value, &thread_ret_size, new_trx_id) < 0)
            {
                cerr << "Transaction was aborted.\n";
                return nullptr;
            }
            EXPECT_FALSE(std::string(thread__value, thread_ret_size) != std::string(thread_ret_value, thread_ret_size));
        }
    }
    // for(int j = 0; j < tables.size(); j++)
    // {
    //     for(int i = (generator() % 5000); i < key_value_pairs.size(); i++) // 랜덤
    //     {
    //         //int index = generator();
    //         ret_size = 0;
    //         memset(ret_value, 0x00, MAX_VAL_SIZE);
    //         int flag = db_update(j, key_value_pairs[i].first, thread_ret_value, key_value_pairs[i].second.size() 
    //                             ,&thread_ret_size, new_trx_id);
    //         if(flag == -1)
    //         {
    //             cerr << "Transaction was aborted.\n";
    //             return nullptr;
    //         }
    //         EXPECT_FALSE(key_value_pairs[i].second.size() != thread_ret_size);
    //         //cerr << thread__value << "\n";
    //         //EXPECT_FALSE(std::string(thread_ret_value) != std::string(thread__value));
    //     }
    // }
    trx_commit(new_trx_id);
    //db_find(arguements->table_id, arguements->key, arguements->ret_val, arguements->val_size, arguements->trx_id);
}

void* for_dead_lock_trx(void* argv)
{
    //Argv* arguements = (Argv*)argv;
    char thread_ret_value[MAX_VAL_SIZE] = "updated #0";
    char thread__value[MAX_VAL_SIZE];
    thread_ret_value[9] = *(int*)argv + '0';
    uint16_t thread_ret_size = 0;
    auto generator = bind(distribution, test_engine);
    int new_trx_id = trx_begin();
    for(int j = 0; j < tables.size(); j++)
    {
        for(int i = key_value_pairs.size() - 1; i >= 0 ; i--) 
        {
            //int index = generator();
            ret_size = 0;
            memset(ret_value, 0x00, MAX_VAL_SIZE);
            int flag = db_update(j, key_value_pairs[i].first, thread_ret_value, key_value_pairs[i].second.size() 
                                ,&thread_ret_size, new_trx_id);
            if(flag == -1)
            {
                cerr << "Transaction was aborted.\n";
                return nullptr;
            }
            //EXPECT_EQ(db_find(j, key_value_pairs[i].first, thread__value, &thread_ret_size, new_trx_id), 0);
            EXPECT_FALSE(key_value_pairs[i].second.size() != thread_ret_size);
            //cerr << thread__value << "\n";
            //EXPECT_FALSE(std::string(thread_ret_value) != std::string(thread__value));
        }
    }
    trx_commit(new_trx_id);
    //db_find(arguements->table_id, arguements->key, arguements->ret_val, arguements->val_size, arguements->trx_id);
}
void* for_dead_lock_trx1(void* argv)
{
    char thread_ret_value[MAX_VAL_SIZE] = "updated #0";
    char thread__value[MAX_VAL_SIZE];
    thread_ret_value[9] = *(int*)argv + '0';
    uint16_t thread_ret_size = 0;
    auto generator = bind(distribution, test_engine);
    int new_trx_id1 = trx_begin();
    int new_trx_id2 = trx_begin();
    int new_trx_id3 = trx_begin();
    int new_trx_id4 = trx_begin();

    db_find(0, 1, thread__value, &thread_ret_size, new_trx_id1);
    db_find(0, 4, thread__value, &thread_ret_size, new_trx_id1);

    db_update(0, 2, thread_ret_value, 95,&thread_ret_size, new_trx_id2);

    db_find(0, 2, thread__value, &thread_ret_size, new_trx_id1);

    // db_find(0, 4, thread__value, &thread_ret_size, new_trx_id3);
    // db_find(0, 3, thread__value, &thread_ret_size, new_trx_id3);

    // db_update(0, 3, thread_ret_value, 95,&thread_ret_size, new_trx_id2);
    // db_update(0, 2, thread_ret_value, 95,&thread_ret_size, new_trx_id4);
    if(db_update(0, 1, thread_ret_value, 95,&thread_ret_size, new_trx_id3) < 0)
    {
        cerr << "transaction aborted\n";
        trx_commit(new_trx_id1);
        trx_commit(new_trx_id2);
        trx_commit(new_trx_id4);
        return nullptr;
    }
    

    
}
void* for_dead_lock_trx2(void* argv)
{
    char thread_ret_value[MAX_VAL_SIZE] = "updated #0";
    char thread__value[MAX_VAL_SIZE];
    thread_ret_value[9] = *(int*)argv + '0';
    uint16_t thread_ret_size = 0;
    auto generator = bind(distribution, test_engine);
    pthread_cond_wait(&cond1, &mtx);
    db_update(0, 2, thread_ret_value, 95,&thread_ret_size, 2);
}
void* for_dead_lock_trx3(void* argv)
{

}
void* for_dead_lock_trx4(void* argv)
{

}

TEST_F(IndexFileManager, InitTestAndDB)
{
    gen = std::mt19937(rd());
    len_dis = std::uniform_int_distribution<uint16_t>(MIN_VAL_SIZE, MAX_VAL_SIZE);
    char_dis = std::uniform_int_distribution<int>(1, CHARACTERS.size());
    rng = std::default_random_engine(rd());
    tables.reserve(NUM_TABLES);
	key_value_pairs.reserve(NUM_KEYS);

    test_engine = std::mt19937(time(NULL));
    distribution = std::uniform_int_distribution<int>(0, NUM_KEYS - 1);
    

    //thread_pool.reserve(NUM_OF_THREAD);

	auto helper_function = [] (auto& gen, auto& cd, auto& size) -> std::string {
		std::string ret_str;
		int index;
		ret_str.reserve(size);

		for (int i = 0; i < size; ++i) {
			index = cd(gen) - 1;
			ret_str += CHARACTERS[index];
		}
		return ret_str;
	};

	for (int i = 0; i < NUM_TABLES; ++i)
		tables.emplace_back(BASE_TABLE_NAME + to_string(i), 0);

	for (int i = 1; i <= NUM_KEYS; ++i) {
		sizeT = len_dis(gen);
		key_value_pairs.emplace_back(i, helper_function(gen, char_dis, sizeT));
	}


    EXPECT_EQ(init_db(NUM_BUF), 0);
    EXPECT_EQ(key_value_pairs.size(), NUM_KEYS) << key_value_pairs.size();
}


TEST_F(IndexFileManager, OpenTables)
{
    for (auto& t : tables) {
            table_id = open_table(const_cast<char*>(t.first.c_str()));
            EXPECT_TRUE(table_id >= 0);
            t.second = table_id;
        }
}

TEST_F(IndexFileManager, Insertion)
{
    for (const auto& t: tables) {
        for (const auto& kv: key_value_pairs) {
            EXPECT_EQ(db_insert(t.second, kv.first, const_cast<char*>(kv.second.c_str()), kv.second.size())
                    ,0);
            //cout << "Insert : "<<kv.second << endl;
        }
    }
    EXPECT_TRUE(shutdown_db() == 0);
    EXPECT_EQ(init_db(NUM_BUF), 0);
    for (auto& t : tables) {
            table_id = open_table(const_cast<char*>(t.first.c_str()));
            EXPECT_TRUE(table_id >= 0);
            t.second = table_id;
        }
    // pagenum_t temp = 0;
    // last = get_next_free_page(0,temp);
    // for(int i = 0; i < 100; i++)
    // {
        
    //     temp = get_next_free_page(0,temp);
    //     cerr << temp << endl;
    // }
    //  cerr << "root : " << find_root_pagenum(0) << endl;
    //  pagenum_t temp = get_next_free_page(0,0);
    // //     list[temp - 1] = 1;
    //      cerr << "NEXT:" <<temp << endl;
    //  cerr << "-------------" << endl;
}
TEST_F(IndexFileManager, Find)
{
    int a[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
    // for(int i = 0; i < NUM_OF_THREAD; i++)
    // {
    //     pthread_create(&thread_pool[i], NULL, find_only_trx, (void*)&a[i]);
    //     //pthread_create(&thread_pool[i + 1], NULL, for_dead_lock_trx, (void*)&a[i + 1]);
    // }
    // for(int i = 0; i < NUM_OF_THREAD; ++i)
    // {
    //     pthread_join(thread_pool[i], NULL);
    //     //pthread_join(thread_pool[i + 1], NULL);
    // }
    for(int i = 0; i < NUM_OF_THREAD; i++)
    {
        pthread_create(&thread_pool[i], NULL, update_only_trx, (void*)&a[i]);
        //pthread_create(&thread_pool[i + 1], NULL, for_dead_lock_trx, (void*)&a[i + 1]);
    }
    for(int i = 0; i < NUM_OF_THREAD; ++i)
    {
        pthread_join(thread_pool[i], NULL);
        //pthread_join(thread_pool[i + 1], NULL);
    }

    // char thread_ret_value[MAX_VAL_SIZE];
    // uint16_t thread_ret_size = 0;
    // auto generator = bind(distribution, test_engine);
    // int new_trx_id = trx_begin();

    // for(int j = 0; j < tables.size(); j++)
    // {
    //     for(int i = 0; i < 24000; i++) // 랜덤
    //     {
    //         int index = generator();
    //         //cerr << "Find with KEY : " << index+1 << "\n";
    //         EXPECT_EQ(db_find(j, key_value_pairs[index].first, thread_ret_value, &thread_ret_size, new_trx_id), 0);
    //         EXPECT_FALSE(key_value_pairs[index].second.size() != thread_ret_size ||
    //                         key_value_pairs[index].second != std::string(thread_ret_value, thread_ret_size));
    //     }
    // }
    // trx_commit(new_trx_id);
}
/*
TEST_F(IndexFileManager, Find)
{
    for (const auto& t: tables) {
        for (const auto& kv: key_value_pairs)
        {
            ret_size = 0;
            memset(ret_value, 0x00, MAX_VAL_SIZE);
            EXPECT_EQ(db_find(t.second, kv.first, ret_value, &ret_size), 0);
            EXPECT_FALSE(kv.second.size() != ret_size ||
                        kv.second != std::string(ret_value, ret_size));
            //cout << "Find : "<< std::string(ret_value, ret_size) << endl;
        }
    }
}
*/
TEST_F(IndexFileManager, Delete)
{
    for (const auto& t: tables) {
        for (const auto& kv: key_value_pairs) 
            EXPECT_EQ(db_delete(t.second, kv.first), 0);
    }
}
/*
TEST_F(IndexFileManager, ReFind)
{
    for (const auto& t: tables) {
        for (const auto& kv: key_value_pairs) {
                ret_size = 0;
                memset(ret_value, 0x00, MAX_VAL_SIZE);
                EXPECT_FALSE(db_find(t.second, kv.first, ret_value, &ret_size) == 0);
        }
    }
}
*/
TEST_F(IndexFileManager, ShutDown)
{
    
    // pagenum_t temp = 0;
    // list = vector<int>(last, -1);
    //     trash = vector<int>();
    // for(int i = 0; i < 100; i++)
    // {
    //     temp = get_next_free_page(0,temp);
    //     list[temp - 1] = 1;
    //     cerr << temp << endl;
    // }
    // for(int i = 0; i < last; i++)
    // {
    //     if(list[i] == -1)
    //     {
    //         cerr << i+1 << " is not freed!\n";
    //         trash.push_back(i+1);
    //     }
    // }
    // cerr << trash.size() << endl;
    short t;
    uint16_t s = 1234;
    memmove(&t, &s, sizeof(short));
    cerr << t << endl;
    EXPECT_TRUE(shutdown_db() == 0);
}
