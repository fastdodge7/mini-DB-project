#include <gtest/gtest.h>
#include "file.h"
#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <cstdio>

using namespace std;

class DataBaseTest : public ::testing::Test{
protected:
	void SetUp() override
	{
		fd = file_open_table_file("Database.db");
	}
	void TearDown() override
	{
		file_close_table_file();
		int flag = remove("Database.db");
		ASSERT_TRUE(flag == 0);
	}

	uint64_t fd;
};


TEST_F(DataBaseTest, File_Have_Correct_Size)
{
	ASSERT_TRUE(fd >= 0);
	ifstream ifs = ifstream("Database.db", ios::in | ios::binary);
	ifs.seekg(0, ios::end);
	ASSERT_EQ(ifs.tellg(), 10*1024*1024) << "current size is " <<ifs.tellg();
	ifs.close();
}

TEST_F(DataBaseTest, File_Have_Correct_Pages)
{
	ASSERT_TRUE(fd >= 0);
	ifstream ifs = ifstream("Database.db", ios::in | ios::binary);
	pagenum_t numOfPage;
	ifs.seekg(8, ios::beg);
	ifs.read((char*)&numOfPage, sizeof(pagenum_t));
	EXPECT_EQ(numOfPage, 10 * 1024 / 4);
	ifs.close();
}

TEST_F(DataBaseTest, PageManagement)
{
	ASSERT_TRUE(fd >= 0);
	pagenum_t page1 = file_alloc_page(fd);
	pagenum_t freed_page = file_alloc_page(fd);
	file_free_page(fd, freed_page);

	// freed_page = freed, page1 = not freed => freed_page must be found && page1 must not be found
	bool is_page1_found = false;
	bool is_freed_page_found = false;
	
	ifstream ifs = ifstream("Database.db", ios::in | ios::binary);

	
	pagenum_t temp;
	ifs.read((char*)&temp, sizeof(pagenum_t));
	while(temp != 0)
	{
		if(temp == page1) is_page1_found = true;
		if(temp == freed_page) is_freed_page_found = true;

		ifs.seekg(temp * sizeof(page_t), ios::beg);
		ifs.read((char*)&temp, sizeof(pagenum_t));
	}

	EXPECT_TRUE((!is_page1_found) && is_freed_page_found);
	file_free_page(fd, page1);
	ifs.close();
}

TEST_F(DataBaseTest, PageIO)
{
	ASSERT_TRUE(fd >= 0);
	pagenum_t page1 = file_alloc_page(fd);
	page_t SomePage;

	string teststring = "";
	for(int i = 0; i < 4 * 1024 - 1; i++)
	{
		teststring += "a";
	}

	strcpy(SomePage.space, teststring.c_str());

	file_write_page(fd, page1, &SomePage);
	page_t readtest;
	file_read_page(fd, page1, &readtest);
	file_free_page(fd, page1);
	EXPECT_EQ(strcmp(SomePage.space, readtest.space), 0);
}

TEST_F(DataBaseTest, PageIO_bigsize)
{
	ASSERT_TRUE(fd >= 0);
	for(int i = 0; i < 3000; i++)
	{
		file_alloc_page(fd);
	}
	pagenum_t page1 = file_alloc_page(fd);
	page_t SomePage;

	string teststring = "";
	for(int i = 0; i < 4 * 1024 - 1; i++)
	{
		teststring += "a";
	}

	strcpy(SomePage.space, teststring.c_str());

	file_write_page(fd, page1, &SomePage);
	page_t readtest;
	file_read_page(fd, page1, &readtest);
	file_free_page(fd, page1);
	EXPECT_EQ(strcmp(SomePage.space, readtest.space), 0);
}