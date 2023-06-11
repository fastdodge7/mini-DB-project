#include "file.h"
#include <iostream>
#include <fcntl.h>
#include <error.h>
#include <unistd.h>
#include <fstream>
#include <map>
#include <string>
#include <vector>
#include <cstring>

using namespace std;

map<string, int64_t> Filename_map_table_id;
map<int64_t, ifstream*> table_id_map_ifstream;
map<int64_t, ofstream*> table_id_map_ofstream;

map<int64_t, vector<bool>*> table_id_map_allocmap;

ifstream* ifs;
ofstream* ofs;

const char* filename = "noname.bin";
const uint64_t DEFAULT_DATABASE_SIZE = 10 * 1024 * 1024; //10mb
const uint64_t DEFAULT_PAGE_SIZE = 4 * 1024; //4kb

// int64_t file_open_database_file(const char* path);
// pagenum_t file_alloc_page(int64_t table_id);
// void file_free_page(int64_t table_id, pagenum_t pagenum);
// void file_read_page(int64_t table_id, pagenum_t pagenum, page_t* dest);
// void file_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src);
// void file_close_table_file();

// page number를 파일 상에서 해당 페이지의 시작 위치로 변환시켜준다.(0에서 시작하는 걸 기준으로함.)
uint64_t page_location_on_file(pagenum_t pagenum); 

// 해당 페이지의 allocation 여부를 업데이트함.
void update_allocation(int64_t table_id, pagenum_t pagenum, bool alloc);

// 해당 페이지의 allocation 여부를 리턴해줌.
int is_alloc(int64_t table_id, pagenum_t pagenum);

// 해당 파일디스크립터와 연결된 db파일이 가지고있는 페이지의 수를 리턴한다.
uint64_t get_num_of_pages(int64_t table_id);

// 해당 파일디스크립터와 연결된 db파일이 가지고있는 페이지의 수를 세팅한다.
void set_num_of_pages(int64_t table_id, uint64_t val);

// 이파일 저파일 오고갈 수 있게 파일디스크립터와 대응되는 db파일을 열고 있는 fstream을 세팅해준다.
void set_fstreams(int64_t table_id);

// 해당 파일디스크립터와 페이지 넘버가 유효한지 확인한다.
bool is_valid_page(int64_t table_id, pagenum_t pagenum);

// allocation map을 초기화한다.
void init_trace_map(int64_t table_id);

// 파일이 늘어날 경우, allocation map도 늘어나야 하므로 늘려준다.
void expand_trace_map(int64_t table_id);

// 해당 페이지의 next free page number를 가져온다. 모든 I/O를 page 단위로 이루어지도록 했다.
pagenum_t get_next_free_page(int64_t table_id, pagenum_t target_page);

// 해당 페이지의 next free page number를 설정한다. 모든 I/O를 page 단위로 이루어지도록 했다.
void set_next_free_page(int64_t table_id, pagenum_t target_page, pagenum_t val);

void set_root_of_header(int64_t table_id, pagenum_t root_pagenum)
{
    page_t temp;
    file_read_header_page(table_id, &temp);
    memmove(temp.space + 16, &root_pagenum, sizeof(pagenum_t));
    file_write_header_page(table_id, &temp);
}


pthread_mutex_t file_mutex = PTHREAD_MUTEX_INITIALIZER;

// Open existing database file or create one if not existed.
int64_t file_open_table_file(const char* path) // 오류들은??
{
    //std::ios::sync_with_stdio(false);
    ifstream temp = ifstream(path);
    if(temp.fail()) // 해당 파일이 존재하지 않을 경우, 새 파일을 만들어 준다.
    {
        ofstream fout = ofstream(path, ios::out | ios::binary);
        if(fout.fail())
        {
            cerr << "Fatal Error : DBMS have some problem with making a new DB file.\n";
            file_close_table_file();
            exit(EXIT_FAILURE);
            return -1; // error
        }
        fout.close();
    
        ifs = new ifstream(path, ios::in | ios::binary);
        ofs = new ofstream(path, ios::out | ios::in | ios::binary);
        // ifs->rdbuf()->pubsetbuf(0,0);
        // ofs->rdbuf()->pubsetbuf(0,0);

        if(ifs->fail() || ofs->fail())
        {
            cerr << "Error : DBMS have some problem with accessing DB file.\n";
            ifs->close(); delete ifs;
            ofs->close(); delete ofs;
            file_close_table_file();
            exit(EXIT_FAILURE);
            return -1; // error
        }
        
        page_t temp;
        pagenum_t freepage;
        freepage = 1;

        for(int i = 0; i < DEFAULT_DATABASE_SIZE / sizeof(page_t) - 1; i++) // 마지막 페이지 제외.
        {
            
            memmove(temp.space, &freepage, sizeof(pagenum_t)); //added
            ofs->write((char*)&temp, sizeof(page_t));
            if(ofs->bad()) { cerr << "Error : std::ofstream::write() failed!\n"; exit(EXIT_FAILURE);}
            freepage++;
            // ofs->flush();
            // sync(); //sync
        }
        freepage = 0;
        memmove(temp.space, &freepage, sizeof(pagenum_t)); //added
        ofs->write((char*)&temp, sizeof(page_t));
        if(ofs->bad()) { cerr << "Error : std::ofstream::write() failed!\n"; exit(EXIT_FAILURE);}
        ofs->flush();
        sync(); //sync
        
        //cout << "Num of Pages(including header) : "<<p << endl;

        Filename_map_table_id.insert({path, Filename_map_table_id.size()});
        table_id_map_ifstream.insert({Filename_map_table_id[path], ifs});
        table_id_map_ofstream.insert({Filename_map_table_id[path], ofs});
        set_num_of_pages(Filename_map_table_id[path], DEFAULT_DATABASE_SIZE / sizeof(page_t)); // 해당 파일이 갖고있는 페이지 수.
        set_root_of_header(Filename_map_table_id[path], 0);
        init_trace_map(Filename_map_table_id[path]);
    }
    else if(Filename_map_table_id.find(path) != Filename_map_table_id.end())
    {
        cerr <<"Table file ( " << path << " ) is already opened!\n";
        return -1;
    }
    else
    {
        ifs = new ifstream(path, ios::in | ios::binary);
        ofs = new ofstream(path, ios::out | ios::in | ios::binary);
        // ifs->rdbuf()->pubsetbuf(0,0);
        // ofs->rdbuf()->pubsetbuf(0,0);

        if(ifs->fail() || ofs->fail())
        {
            cerr << "Error : DBMS have some problem with accessing DB file.\n";
            ifs->close(); delete ifs;
            ofs->close(); delete ofs;
            file_close_table_file();
            exit(EXIT_FAILURE);
            return -1; // error
        }

        Filename_map_table_id.insert({path, Filename_map_table_id.size()});
        table_id_map_ifstream.insert({Filename_map_table_id[path], ifs});
        table_id_map_ofstream.insert({Filename_map_table_id[path], ofs});
        init_trace_map(Filename_map_table_id[path]);
    }
    temp.close();
    return Filename_map_table_id[path];
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int64_t table_id)
{
    if(table_id_map_ifstream.find(table_id) == table_id_map_ifstream.end())
    {
        cerr << "Error : Invalid File Descriptor!\n";
        file_close_table_file();
        exit(EXIT_FAILURE);
        return -1; // error
    }
    set_fstreams(table_id);
    pagenum_t target;
    target = get_next_free_page(table_id, 0);
    if(target == 0)
    {
        // 새로운 페이지를 만들면서 데이터베이스 파일을 늘려야함...
        ofs->seekp(0, ios::end);
        //cout << "Now Position : " << ofs->tellp() << endl;
        page_t temp_page;
        
        pagenum_t freepage = ((uint64_t)ofs->tellp() + 1) / 1024 / 4;
       
        pagenum_t origin = freepage;
        ofs->seekp(0, ios::end);

        for(int i = 0; i < origin; i++)
        {
            if(i == origin - 1) freepage = 0;
            else freepage++;

            memmove(temp_page.space, &freepage, sizeof(pagenum_t));
            ofs->write((char*)&temp_page, sizeof(page_t));
            if(ofs->bad()) { cerr << "Error : std::ofstream::write() failed!\n"; exit(EXIT_FAILURE);}
            // ofs->flush();
            // sync(); //sync
            //cout << "Now Position2 : " << ofs->tellp() << endl;
        }
        set_next_free_page(table_id, 0, origin + 1);
        expand_trace_map(table_id);
        set_num_of_pages(table_id, get_num_of_pages(table_id) * 2);
        update_allocation(table_id, origin, true);
        return origin;
    }
    else // free page가 존재함.
    {
        pagenum_t temp = get_next_free_page(table_id, target);
        update_allocation(table_id, target, true);
        set_next_free_page(table_id, 0, temp);
        return target;
    }
}

// Free an on-disk page to the free page list
void file_free_page(int64_t table_id, pagenum_t pagenum)
{
    if(!is_valid_page(table_id, pagenum)) return;
    set_fstreams(table_id);

    pagenum_t temp_page; //header->next

    if(!is_alloc(table_id, pagenum)) 
    { 
        cerr << "Error : Page #" << pagenum << " is alredy freed.\n";
        return; 
    }
    else
    {
        temp_page = get_next_free_page(table_id, 0);

        set_next_free_page(table_id, 0, pagenum);

        set_next_free_page(table_id, pagenum, temp_page);
        update_allocation(table_id, pagenum, false);
    }
    
}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int64_t table_id, pagenum_t pagenum, page_t* dest)
{
    //if(!is_valid_page(table_id, pagenum)) return;
    pthread_mutex_lock(&file_mutex);
    set_fstreams(table_id);
    
    if(is_alloc(table_id, pagenum))
    {
        ifs->seekg(page_location_on_file(pagenum), ios::beg);
        ifs->read((char*)&(dest->space), sizeof(char) * (4 * 1024));
        if(ifs->bad()) { cerr << "Error : std::ifstream::read() failed!\n"; exit(EXIT_FAILURE);}
        ifs->sync();
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    else
    {
        cerr << "Error : Page #" << pagenum << " is not allocated by Disk Space Manager. \n";
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src)
{
    //if(!is_valid_page(table_id, pagenum)) return;
    pthread_mutex_lock(&file_mutex);
    set_fstreams(table_id);

    if(is_alloc(table_id, pagenum))
    {
        ofs->seekp(page_location_on_file(pagenum), ios::beg);
        ofs->write((char*)&(src->space), sizeof(char) * (4 * 1024));
        if(ofs->bad()) { cerr << "Error : std::ofstream::write() failed!\n"; exit(EXIT_FAILURE);}
        ofs->flush();
        sync(); //sync
        pthread_mutex_unlock(&file_mutex);
        return;
    }
    else
    {
        cerr << "Error : Page #" << pagenum << " is not allocated by Disk Space Manager. \n";
        pthread_mutex_unlock(&file_mutex);
        return;
    }
}

// Stop referencing the database file
void file_close_table_file()
{
    for(auto p : table_id_map_ifstream)
    {
        (p.second)->close();
        delete p.second;
    }
    for(auto p : table_id_map_ofstream)
    {
        (p.second)->close();
        delete p.second;
    }
    for(auto p : table_id_map_allocmap)
    {
        delete p.second;
    }
    table_id_map_ifstream.clear();
    table_id_map_ofstream.clear();
    Filename_map_table_id.clear();
    table_id_map_allocmap.clear();
}


void file_read_header_page(int64_t table_id, page_t* dest)
{
    if(table_id < 0)
    {
        cerr << "INVALID FILEDESCRIPTOR\n" << endl;
        exit(EXIT_FAILURE);
    }
    set_fstreams(table_id);

    ifs->seekg(page_location_on_file(0), ios::beg);
    ifs->read((char*)&(dest->space), sizeof(char) * (4 * 1024));
    if(ifs->bad()) { cerr << "Error : std::ifstream::read() failed!\n"; exit(EXIT_FAILURE);}
    ifs->sync();
    ifs->seekg(0,ios::beg);
    return;
}
void file_write_header_page(int64_t table_id, page_t* src)
{
    if(table_id < 0)
    {
        cerr << "INVALID FILEDESCRIPTOR\n" << endl;
        exit(EXIT_FAILURE);
    }
    set_fstreams(table_id);
    ofs->seekp(page_location_on_file(0), ios::beg);
    ofs->write((char*)&(src->space), sizeof(char) * (4 * 1024));
    if(ofs->bad()) { cerr << "Error : std::ofstream::write() failed!\n"; exit(EXIT_FAILURE);}
    ofs->flush();
    sync(); //sync
    ofs->seekp(0, ios::beg);
    return;
}



// 페이지의 시작부분을 반환해줌.
uint64_t page_location_on_file(pagenum_t pagenum){ return (pagenum) * sizeof(page_t); }

void update_allocation(int64_t table_id, pagenum_t pagenum, bool alloc)
{
    vector<bool>* tracer = table_id_map_allocmap[table_id];
    (*tracer)[pagenum] = alloc;
}

int is_alloc(int64_t table_id, pagenum_t pagenum)
{
    vector<bool>* tracer = table_id_map_allocmap[table_id];
    return (*tracer)[pagenum];
}

uint64_t get_num_of_pages(int64_t table_id)
{
    set_fstreams(table_id);
    ifs->seekg(0, ios::beg);

    pagenum_t num_of_pages;
    page_t t_page;
    
    ifs->read((char*)&t_page, sizeof(page_t));
    if(ifs->bad()) { cerr << "Error : std::ifstream::read() failed!\n"; exit(EXIT_FAILURE);}
    memmove(&num_of_pages, t_page.space + 8, sizeof(pagenum_t));
    ifs->sync();
    ifs->seekg(0, ios::beg);
    return num_of_pages;
}

void set_num_of_pages(int64_t table_id, uint64_t val)
{
    set_fstreams(table_id);

    ifs->seekg(0, ios::beg);
    page_t t_page;    
    ifs->read((char*)&t_page, sizeof(page_t));
    if(ifs->bad()) { cerr << "Error : std::ifstream::read() failed!\n"; exit(EXIT_FAILURE);}
    memmove((char*)(&t_page) + 8, &val, sizeof(pagenum_t));
    ifs->sync();
    ifs->seekg(0, ios::beg);

    ofs->seekp(0, ios::beg);
    ofs->write((char*)&t_page, sizeof(page_t));
    if(ofs->bad()) { cerr << "Error : std::ofstream::write() failed!\n"; exit(EXIT_FAILURE);}
    ofs->seekp(0, ios::beg);
    ofs->flush();
    sync(); //sync
}

void set_fstreams(int64_t table_id)
{
    ifs = table_id_map_ifstream[table_id];
    ofs = table_id_map_ofstream[table_id];
}

bool is_valid_page(int64_t table_id, pagenum_t pagenum)
{
    if(table_id_map_ifstream.find(table_id) == table_id_map_ifstream.end())
    {
        cerr << "Error : Invalid File Descriptor!\n";
        exit(EXIT_FAILURE);
        return false;
    }
    if(pagenum < 1 || get_num_of_pages(table_id) <= pagenum) 
    { 
        cerr    << "Error : Invalid page number! less than 1 or "
                << "bigger than the number of page that current DB file has.\n"
                << "input pagenum : " << pagenum << "\n";
        exit(EXIT_FAILURE);
        return false; 
    }
    else
        return true;
}


void init_trace_map(int64_t table_id)
{
    set_fstreams(table_id);

    pagenum_t num_of_pages;
    ifs->seekg(8,ios::beg);
    ifs->read((char*)&num_of_pages, sizeof(pagenum_t));
    if(ifs->bad()) { cerr << "Error : std::ifstream::read() failed!\n"; exit(EXIT_FAILURE);}

    vector<bool>* tracer = new vector<bool>(num_of_pages, true);
    (*tracer)[0] = false;

    pagenum_t next_page = get_next_free_page(table_id, 0);

    while(next_page != 0)
    {
        (*tracer)[next_page] = false;
        ifs->seekg(page_location_on_file(next_page),ios::beg);
        ifs->read((char*)&next_page, sizeof(pagenum_t));
        if(ifs->bad()) { cerr << "Error : std::ifstream::read() failed!\n"; exit(EXIT_FAILURE);}    
    }

    table_id_map_allocmap.insert({table_id, tracer});
}

pagenum_t get_next_free_page(int64_t table_id, pagenum_t target_page)
{
    set_fstreams(table_id);
    ifs->seekg(page_location_on_file(target_page), ios::beg);

    pagenum_t temp;
    page_t t_page;
    
    ifs->read((char*)&t_page, sizeof(page_t));
    if(ifs->bad()) { cerr << "Error : std::ifstream::read() failed!\n"; exit(EXIT_FAILURE);}

    memmove(&temp, t_page.space, sizeof(pagenum_t));
    ifs->sync();
    ifs->seekg(0, ios::beg);
    return temp;
}

void set_next_free_page(int64_t table_id, pagenum_t target_page, pagenum_t val)
{
    set_fstreams(table_id);
    ifs->seekg(page_location_on_file(target_page), ios::beg);

    page_t t_page;    
    ifs->read((char*)&t_page, sizeof(page_t));
    if(ifs->bad()) { cerr << "Error : std::ifstream::read() failed!\n"; exit(EXIT_FAILURE);}

    memmove(&t_page, &val, sizeof(pagenum_t));
    ifs->sync();
    ifs->seekg(0, ios::beg);

    ofs->seekp(page_location_on_file(target_page), ios::beg);
    ofs->write((char*)&t_page, sizeof(page_t));
    if(ofs->bad()) { cerr << "Error : std::ofstream::write() failed!\n"; exit(EXIT_FAILURE);}
    ofs->seekp(0, ios::beg);
    ofs->flush();
    sync(); //sync
}

void expand_trace_map(int64_t table_id)
{
    set_fstreams(table_id);
    vector<bool>* trace_map = table_id_map_allocmap[table_id];

    pagenum_t num_of_pages;
    ifs->seekg(8,ios::beg);
    ifs->read((char*)&num_of_pages, sizeof(pagenum_t));
    if(ifs->bad()) { cerr << "Error : std::ifstream::read() failed!\n"; exit(EXIT_FAILURE);}

    for(int i = 0; i < num_of_pages; i++)
    {
        trace_map->push_back(false);
    }
}