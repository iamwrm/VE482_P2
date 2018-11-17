#ifndef I_HELPER_H
#define I_HELPER_H
#include "../query/QueryBuilders.h"
#include "../query/QueryParser.h"

#include <getopt.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
struct inf_qry {
	bool read;
	bool write;
	bool affectAll;		  // if the query affect all the tables
	std::string targetTable;  // "" for no target table
	std::string newTable;     // "" for no new table
	int line;
};

inf_qry getInformation(std::string qry,int & count);

#endif  // !I_HELPER_H
