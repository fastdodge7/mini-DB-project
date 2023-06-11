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

static constexpr int NUM_KEYS { 6000 };

static constexpr int MIN_VAL_SIZE { 50 };
static constexpr int MAX_VAL_SIZE { 112 };

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

class IndexFileManager : public ::testing::Test{
protected:
	void SetUp() override
	{
	}
	void TearDown() override
	{
	}
};


TEST_F(IndexFileManager, InitTestAndDB)
{
    gen = std::mt19937(rd());
    len_dis = std::uniform_int_distribution<uint16_t>(MIN_VAL_SIZE, MAX_VAL_SIZE);
    char_dis = std::uniform_int_distribution<int>(1, CHARACTERS.size());
    rng = std::default_random_engine(rd());
    tables.reserve(NUM_TABLES);
	key_value_pairs.reserve(NUM_KEYS);
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
    EXPECT_EQ(init_db(200), 0);
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
    EXPECT_EQ(init_db(200), 0);
    for (auto& t : tables) {
            table_id = open_table(const_cast<char*>(t.first.c_str()));
            EXPECT_TRUE(table_id >= 0);
            t.second = table_id;
        }
}

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

TEST_F(IndexFileManager, Delete)
{
    for (const auto& t: tables) {
        for (const auto& kv: key_value_pairs) 
            EXPECT_EQ(db_delete(t.second, kv.first), 0);
    }
}

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

TEST_F(IndexFileManager, ShutDown)
{
    EXPECT_TRUE(shutdown_db() == 0);
}
