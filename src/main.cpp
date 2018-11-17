//
// Created by liu on 18-10-21.
//

#include "query/QueryParser.h"
#include "query/QueryBuilders.h"
#include "utils/i_helper.h"

#include <getopt.h>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <map>
#include <condition_variable>
#include <unistd.h>

using std::string;
using std::endl;
using std::cout;
using std::vector;

std::mutex mtx_query_queue;
std::mutex mtx_query_queue_property;
std::mutex mtx_query_queue_arr;

std::condition_variable  threadLimit;


struct Query_queue_arr{
    std::map< std::string, int > table_name;

    std::vector<one_table_query> arr;
};

struct one_table_query{
    std::string table_name;
    std::mutex mtx;
    std::vector<Query::Ptr> query_data;
};

struct {
    std::string listen;
    long threads = 0;
} parsedArgs;

//record line of query
int count_for_getInformation = 0;
std::mutex mtx_count_for_getInformation;

int counter_for_result_reader = 0;
std::mutex mtx_counter_for_result_reader;

int max_line_num = -1;
std::mutex mtx_max_line_num;


// from liu
size_t counter = 0;

//structure to store information of a query

void parseArgs(int argc, char *argv[]) {
    const option longOpts[] = {
            {"listen",  required_argument, nullptr, 'l'},
            {"threads", required_argument, nullptr, 't'},
            {nullptr,   no_argument,       nullptr, 0}
    };
    const char *shortOpts = "l:t:";
    int opt, longIndex;
    while ((opt = getopt_long(argc, argv, shortOpts, longOpts, &longIndex)) != -1) {
        if (opt == 'l') {
            parsedArgs.listen = optarg;
        } else if (opt == 't') {
            parsedArgs.threads = std::strtol(optarg, nullptr, 10);
        } else {
            std::cerr << "lemondb: warning: unknown argument " << longOpts[longIndex].name << std::endl;
        }
    }

}

std::string extractQueryString(std::istream &is) {
    std::string buf;
    do {
        int ch = is.get();
        if (ch == ';') return buf;
        if (ch == EOF) throw std::ios_base::failure("End of input");
        buf.push_back((char) ch);
    } while (true);
}


// TODO:
void qq_reader(std::istream &is, QueryParser &p,
	       std::vector<Query::Ptr> &query_queue,
	       std::vector<inf_qry> &query_queue_property,
	       Query_queue_arr query_queue_arr)
{
    // This function is in the main thread
    //
    //
	while (is) {
		try {
			std::string queryStr = extractQueryString(is);
			Query::Ptr query = p.parseQuery(queryStr);

            // enqueue query
			mtx_query_queue.lock();
			query_queue.emplace_back(std::move(query));
			mtx_query_queue.unlock();

			mtx_query_queue_property.lock();
			query_queue_property.emplace_back(
			    std::move(getInformation(
				query_queue[query_queue.size() - 1]->toString(),
				count_for_getInformation)));
			mtx_query_queue_property.unlock();



            // count for how many query information has been stored
			mtx_count_for_getInformation.lock();
			count_for_getInformation++;  // record the line of query
			mtx_count_for_getInformation.unlock();

			std::cout << "in qq:" << count_for_getInformation
				  << endl;


			threadLimit.notify_one();
			counter++;
		} catch (const std::ios_base::failure &e) {
			// End of input
			break;
		} catch (const std::exception &e) {
			std::cout.flush();
			std::cerr << e.what() << std::endl;
		}
	}
	mtx_max_line_num.lock();
	max_line_num = counter;
	mtx_max_line_num.unlock();
}


// TODO:
void result_reader()
{
	while (1) {
        mtx_max_line_num.lock();
        mtx_counter_for_result_reader.lock();
        if ((max_line_num>0)&&(counter_for_result_reader>max_line_num-1)){
        mtx_max_line_num.unlock();
        mtx_counter_for_result_reader.unlock();
            break;
        }
        mtx_max_line_num.unlock();
        mtx_counter_for_result_reader.unlock();
        
		std::unique_lock<std::mutex> lock(mtx_count_for_getInformation);
		if (counter_for_result_reader > count_for_getInformation)
			threadLimit.wait(lock);
		std::cout << "in rr ------"<< counter_for_result_reader << endl;
        mtx_counter_for_result_reader.lock();
		counter_for_result_reader++;
        mtx_counter_for_result_reader.unlock();
	}
}

int main(int argc, char *argv[])
{
	// Assume only C++ style I/O is used in lemondb
	// Do not use printf/fprintf in <cstdio> with this line
	std::ios_base::sync_with_stdio(false);

	parseArgs(argc, argv);

	std::fstream fin;
	if (!parsedArgs.listen.empty()) {
		fin.open(parsedArgs.listen);
		if (!fin.is_open()) {
			std::cerr << "lemondb: error: " << parsedArgs.listen
				  << ": no such file or directory" << std::endl;
			exit(-1);
		}
	}
	std::istream is(fin.rdbuf());

#ifdef NDEBUG
    // In production mode, listen argument must be defined
    if (parsedArgs.listen.empty()) {
        std::cerr << "lemondb: error: --listen argument not found, not allowed in production mode" << std::endl;
        exit(-1);
    }
#else
    // In debug mode, use stdin as input if no listen file is found
    if (parsedArgs.listen.empty()) {
        std::cerr << "lemondb: warning: --listen argument not found, use stdin instead in debug mode" << std::endl;
        is.rdbuf(std::cin.rdbuf());
    }
#endif

    if (parsedArgs.threads < 0) {
        std::cerr << "lemondb: error: threads num can not be negative value " << parsedArgs.threads << std::endl;
        exit(-1);
    } else if (parsedArgs.threads == 0) {
        // DONE: get thread num from system
        parsedArgs.threads = std::thread::hardware_concurrency();
        std::cerr << "lemondb: info: auto detect thread num "<< parsedArgs.threads << std::endl;
    } else {
        std::cerr << "lemondb: info: running in " << parsedArgs.threads << " threads" << std::endl;
    }


    QueryParser p;

    p.registerQueryBuilder(std::make_unique<QueryBuilder(Debug)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(ManageTable)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(Complex)>());


    std::vector<Query::Ptr> query_queue;
    std::vector<inf_qry> query_queue_property;
    std::vector<QueryResult::Ptr> query_result_queue;

    // QueryResult::Ptr result = query->execute();

    // TODO: a table for each query
    Query_queue_arr query_queue_arr;


    // TODO: output result
    std::thread result_reader_th{result_reader};


    // read the listened file
    qq_reader(is,p,query_queue,query_queue_property,query_queue_arr);

    /*
    for (auto it = query_queue_property.begin();
	 it != query_queue_property.end(); it++) {
	    std::cout << it->line << std::endl;
    }
     */


    // wait for the result printing thread to end
    result_reader_th.join();
    std::cout<<"finish"<<endl;
    //std::this_thread::sleep_for(std::chrono::seconds(10));

    return 0;
}
