#ifndef I_HELPER_H
#define I_HELPER_H
#include "../query/QueryBuilders.h"
#include "../query/QueryParser.h"

#include <getopt.h>
#include <unistd.h>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
//#include <pair>

struct inf_qry {
	bool read;
	bool write;
	bool affectAll;		  // if the query affect all the tables
	std::string targetTable;  // "" for no target table
	std::string newTable;     // "" for no new table
	int line;
};


struct one_table_query {
	//std::string table_name;
	bool ifexist=false;
	bool havereader=false;
	bool havewriter=false; 
	int reader_count = 0;
	size_t head =0;
	std::vector<inf_qry> query_data;
};

struct Query_queue_arr {
	std::map<std::string, int> table_name;

	std::vector<one_table_query> arr;

	// TODO: handle quit;
	inf_qry quit_query;
	
};

inf_qry getInformation(std::string qry, int &count);

#endif  // !I_HELPER_H
