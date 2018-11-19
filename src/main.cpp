//
// Created by liu on 18-10-21.
//

#include "query/QueryParser.h"
#include "query/QueryBuilders.h"
#include "utils/i_helper.h"

#include <cassert>


using std::string;
using std::endl;
using std::cout;
using std::vector;

bool has_quit=false;
std::mutex mtx_has_quit;

std::mutex mtx_query_queue;
std::mutex mtx_query_result_queue;
std::mutex mtx_query_queue_property;

std::vector<inf_qry> query_queue_property;


std::condition_variable  threadLimit;
std::condition_variable  cd_read_limit;

std::condition_variable  cd_real_thread_limit;
int present_thread_num = 0;
std::mutex mtx_present_thread_num;



std::vector<std::mutex> query_arr_mtx(10);



Query_queue_arr query_queue_arr;
std::mutex mtx_query_queue_arr;


struct {
    std::string listen;
    long threads = 0;
} parsedArgs;

//record line of query
int count_for_getInformation = 0;
std::mutex mtx_count_for_getInformation;

int count_for_executed = 0;
std::mutex mtx_count_for_executed;

int counter_for_result_reader = 0;
std::mutex mtx_counter_for_result_reader;

int max_line_num = -1;
std::mutex mtx_max_line_num;

std::vector<bool> if_query_done_arr;
std::mutex mtx_if_query_done_arr;

std::vector<Query::Ptr> query_queue;

std::vector<QueryResult::Ptr> query_result_queue;

std::mutex mtx_print_if;

std::condition_variable cd_scheduler_sleep;

std::mutex mtx_scheduler_sleep;

std::mutex mtx_nothing_to_do_2;

void print_if_query_done_arr()
{
	return;
	mtx_print_if.lock();
	//cout<<"_";
	for (int i = 0; i < 200; i++) {
		if (if_query_done_arr[i] == true) {
			cout << "1";
		} else {
			cout << "0";
		}
	}
	cout << endl;
	mtx_print_if.unlock();
}

// from liu
size_t counter = 0;
std::mutex mtx_counter;

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
	       Query_queue_arr &query_queue_arr)
{
	// This function is in the main thread
	while (is) {
		try {
			std::string queryStr = extractQueryString(is);
			Query::Ptr query = p.parseQuery(queryStr);

			// enqueue query
			mtx_query_queue.lock();
			query_queue.emplace_back(std::move(query));
			mtx_query_queue.unlock();

			// ==============================
			mtx_query_queue_property.lock();
			inf_qry local_inf_qry = getInformation(
			    query_queue[query_queue.size() - 1]->toString(),
			    count_for_getInformation);
			mtx_query_queue_property.unlock();
			// ==============================

			std::string query_string = query_queue[query_queue.size() - 1]->toString();

			int local_query_id = local_inf_qry.line;

			mtx_if_query_done_arr.lock(); if_query_done_arr.push_back(false); mtx_if_query_done_arr.unlock();

			mtx_query_result_queue.lock(); query_result_queue.push_back(nullptr); mtx_query_result_queue.unlock();

			mtx_query_queue_arr.lock();
			if (query_string[0 + 8] == 'Q' && query_string[3 + 8] == 't') {
				query_queue_arr.quit_query = local_inf_qry;

				mtx_query_queue_arr.unlock();

				has_quit=true;

				counter++;
				break;
			} else  if (query_queue_arr.table_name.find( local_inf_qry.targetTable) != query_queue_arr.table_name.end()) {
				auto it = query_queue_arr.table_name.find( local_inf_qry.targetTable);
				query_queue_arr.arr[it->second] .query_data.emplace_back( std::move(local_inf_qry));
			} else {
				// new table name
				//query_queue_arr.arr.emplace_back(
				//    std::move(one_table_query()));
				query_queue_arr.arr.emplace_back(one_table_query());

				unsigned long jii = query_queue_arr.arr.size() - 1;

				if (query_string[0 + 8] == 'L' && query_string[3 + 8] == 'd'){
					 query_queue_arr.arr[jii].ifexist=true;}

				query_queue_arr.table_name.insert( std::pair<std::string, int>( local_inf_qry.targetTable, jii));

				query_queue_arr.arr[jii] .query_data.emplace_back( std::move(local_inf_qry));
			}
			mtx_query_queue_arr.unlock();

			// count for how many query information has been stored
			mtx_count_for_getInformation.lock();
			count_for_getInformation++;  // record the line of query
			mtx_count_for_getInformation.unlock();


			threadLimit.notify_one();
			counter++;
		} catch (const std::ios_base::failure &e) {
			// End of input
			break;
		} catch (const std::exception &e) {
			//std::cout.flush();
			//std::cerr << e.what() << std::endl;
		}
	}
	mtx_max_line_num.lock();
	if (has_quit) {
		max_line_num = counter - 1;
	} else {
		max_line_num = counter - 0;
	}
	mtx_max_line_num.unlock();
	//std::cerr<<"max query:"<<max_line_num<<query_queue[max_line_num]->toString()<<endl;
}

// TODO:
void thread_starter(int queryID)
{
	//std::cerr<<"in thread starter queryID:"<<queryID<<"resentTH:"<<present_thread_num<<std::endl;

	mtx_present_thread_num.lock(); present_thread_num++; mtx_present_thread_num.unlock();

	query_result_queue[queryID]= query_queue[queryID]->execute();

	// TODO: set if the query is copy table
	std::string this_query_string = query_queue[queryID]->toString();

	int unuseful;
	inf_qry local_inf_qry=getInformation(this_query_string,unuseful);

	std::string new_table_name = local_inf_qry.newTable;
	std::string this_table_name = local_inf_qry.targetTable;

	mtx_query_queue_arr.lock();

	if (local_inf_qry.write){
		int idx =query_queue_arr.table_name.find(this_table_name)->second;
		//std::cerr<<"in thread starter write queryID:"<<queryID<<"resentTH:"<<present_thread_num<<std::endl;
		query_queue_arr.arr[idx].havewriter=false;
		query_queue_arr.arr[idx].havereader=false;
		query_queue_arr.arr[idx].reader_count=0;
	}

	if (local_inf_qry.read) {
		int idx = query_queue_arr.table_name.find(this_table_name)->second;
		query_queue_arr.arr[idx].reader_count--;
		if (query_queue_arr.arr[idx].reader_count == 0) {
			query_queue_arr.arr[idx].havereader=false;
		}
		query_queue_arr.arr[idx].havewriter=false;
	}

	if (this_query_string[0 + 8] == 'C' && this_query_string[3 + 8] == 'y') {
		int idx =query_queue_arr.table_name.find(new_table_name)->second;
		query_queue_arr.arr[idx].ifexist=true;
	}
	if (this_query_string[0 + 8] == 'D' && this_query_string[3 + 8] == 'P') {
		int idx =query_queue_arr.table_name.find(this_table_name)->second;

		query_queue_arr.arr[idx].ifexist=false;

		size_t that_head = query_queue_arr.arr[idx].head;

		if (that_head < query_queue_arr.arr[idx].query_data.size()){
			int that_id = query_queue_arr.arr[idx].query_data[that_head].line;
			std::string that_query_string =  query_queue[that_id]->toString();
			if (that_query_string[0 + 8] == 'L' && that_query_string[3 + 8] == 'd') {
				query_queue_arr.arr[idx].ifexist= true;
			}
		}

	}
	mtx_query_queue_arr.unlock();


	mtx_present_thread_num.lock(); present_thread_num--; mtx_present_thread_num.unlock();

	mtx_count_for_executed.lock(); count_for_executed++; mtx_count_for_executed.unlock();

	mtx_if_query_done_arr.lock(); if_query_done_arr[queryID] = true; mtx_if_query_done_arr.unlock();

	cd_read_limit.notify_one();
	cd_real_thread_limit.notify_one();
	cd_scheduler_sleep.notify_one();

	/*
	std::cerr << "FINISH qID:" << queryID << " tbl:" << this_table_name
		  << " Th:" << present_thread_num << "w:" << local_inf_qry.write
		  << "r:" << local_inf_qry.read << " \n";
		  */
}

// TODO:
void scheduler()
{
	while (1) {

		mtx_max_line_num.lock();
		mtx_count_for_executed.lock();
		if ((max_line_num>0)&&(count_for_executed>max_line_num-1)){
			mtx_count_for_executed.unlock();
			mtx_max_line_num.unlock();
			break;
		}
		mtx_count_for_executed.unlock();
		mtx_max_line_num.unlock();

		bool have_executed=false;


		mtx_query_queue_arr.lock();
		for (size_t i = 0; i < query_queue_arr.arr.size(); ++i) {
			/*
			mtx_query_queue_arr.lock();
			std::cerr << "i:" << i << query_queue_arr.arr[i].query_data[0].targetTable << "head:" << query_queue_arr.arr[i].head<<"|" << query_queue_arr.arr[i].query_data[query_queue_arr.arr[i].head].line<< "\n";
			for (size_t j = 0; j < query_queue_arr.arr.size(); ++j) {
			std::cerr << query_queue_arr.arr[j].havereader << query_queue_arr.arr[j].havewriter << " " << query_queue_arr.arr[j].reader_count << "\n";}
			mtx_query_queue_arr.unlock();
			*/
			if (query_queue_arr.arr[i].ifexist) {

				distribute:

					mtx_query_queue_arr.unlock();
				{
					std::unique_lock<std::mutex> lock(mtx_present_thread_num);
					
					if (present_thread_num > (int)std::thread::hardware_concurrency()||present_thread_num>8) {
					cd_real_thread_limit.wait(lock,[]{return present_thread_num <= 8 && present_thread_num <(int)std::thread::hardware_concurrency();});
					}
				}
					mtx_query_queue_arr.lock();


				size_t queryID = query_queue_arr.arr[i].query_data[query_queue_arr.arr[i].head].line;

				//std::cerr<<"out for,"<<have_executed<<endl;

				// tableID++    for loop
				if (query_queue_arr.arr[i].head >= query_queue_arr.arr[i].query_data.size()) {
					// unlock the mutex
					//mtx_query_queue_arr.unlock();
					continue;
				}

				if (query_queue_arr.arr[i].query_data[query_queue_arr.arr[i].head].read) {
					if (query_queue_arr.arr[i].havereader) {

						query_queue_arr.arr[i].reader_count++;

						have_executed = true;
						std::thread{thread_starter, queryID}.detach();

						// thread num--

						query_queue_arr.arr[i].head++;
						//mtx_query_queue_arr.unlock();
						goto distribute;
					} else 
					if (query_queue_arr.arr[i].havewriter) {
						// unlock the mutex
						//mtx_query_queue_arr.unlock();
						continue;
					} else 
					{
						query_queue_arr.arr[i].havereader = true;
						query_queue_arr.arr[i].reader_count=1;

						have_executed = true;
						std::thread{thread_starter, queryID}.detach();

						query_queue_arr.arr[i].head++;
						//mtx_query_queue_arr.unlock();
						goto distribute;
					}
				} else 
				if (query_queue_arr.arr[i].query_data[query_queue_arr.arr[i].head].write) {
					if (query_queue_arr.arr[i].havereader || query_queue_arr.arr[i].havewriter) {
						// unlock the mutex
						//mtx_query_queue_arr.unlock();
						continue;
					} else {
						query_queue_arr.arr[i].havewriter = true;
						have_executed = true;
						std::thread{thread_starter, queryID}.detach();

						query_queue_arr.arr[i].head++;
						//mtx_query_queue_arr.unlock();
						goto distribute;
					}
				}

				// unlock the mutex
				//mtx_query_queue_arr.unlock();
			}
				//std::cerr<<"out for,"<<have_executed<<endl;
		}
		mtx_query_queue_arr.unlock();

		// one loop finish
		//std::cerr<<"!!!out for,"<<have_executed<<"|"<<count_for_executed<<endl;

		if (!have_executed){
		std::unique_lock<std::mutex> lock(mtx_scheduler_sleep);
		cd_scheduler_sleep.wait(lock);
		}


	}
}

// TODO:
void result_reader()
{
	//std::cerr << "===============in rr" << counter_for_result_reader<< " "<< endl;
				//return;
	while (1) {
		//std::cerr << "just after while in cv" << counter_for_result_reader<< " "<< endl;

		mtx_max_line_num.lock();
		if (max_line_num>0&&counter_for_result_reader>max_line_num-1){
			mtx_max_line_num.unlock();
			break;
		}
		mtx_max_line_num.unlock();


		// check if counter is bigger than executed
		{
			std::unique_lock<std::mutex> lock(mtx_if_query_done_arr);
			cd_read_limit.wait(lock,[]{return if_query_done_arr[counter_for_result_reader];});
		}

		//std::cerr << "in rr ------" << counter_for_result_reader<< " "<< endl;
		//print_if_query_done_arr();

		QueryResult::Ptr result = std::move(query_result_queue[counter_for_result_reader]);

		std::cout<<counter_for_result_reader+1;
		std::cout<<"\n";

			//std::cout << "|"<<counter_for_result_reader+1<<std::endl;
		if (result->display()){
			std::cout << *result;
			//std::cout.flush();
		} else{
			//std::cerr << *result;
		}

		counter_for_result_reader++;
		//mtx_counter_for_result_reader.lock();
		//std::cerr << "in rr -finished-----" << counter_for_result_reader<< " "<< "\n";
		//mtx_counter_for_result_reader.unlock();
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
        parsedArgs.threads = std::thread::hardware_concurrency();
        std::cerr << "lemondb: info: auto detect thread num "<< parsedArgs.threads << std::endl;
    } else {
        std::cerr << "lemondb: info: running in " << parsedArgs.threads << " threads" << std::endl;
    }


    QueryParser p;

    p.registerQueryBuilder(std::make_unique<QueryBuilder(Debug)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(ManageTable)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(Complex)>());



    // read the listened file
    qq_reader(is,p,query_queue,query_queue_property,query_queue_arr);

    std::thread scheduler_th{scheduler};


    /*
    for (auto it = query_queue_property.begin();
	 it != query_queue_property.end(); it++) {
	    std::cout << it->line << std::endl;
    }
     */

    /*
    cout<<query_queue_arr.arr.size()<<endl;

    for (auto it = query_queue_arr.arr.begin(); 
        it != query_queue_arr.arr.end();
	    it++) {
	    std::vector<inf_qry> &the_arr = it->query_data;
	    std::cout << the_arr[0].targetTable << std::endl;
	    for (auto jt = the_arr.begin(); jt != the_arr.end(); jt++) {
            std::cout<<jt->line<<" ";

	    }
        std::cout<<std::endl;
    }
     */

    std::thread result_reader_th{result_reader};
    scheduler_th.join();
    // wait for the result printing thread to end
    result_reader_th.join();

    //std::cout<<"finish"<<endl;

	//std::cerr<<"--------------out of while";
	//query_queue[max_line_num]->execute();
	//std::cerr<<"--------------out of quit";
    //std::this_thread::sleep_for(std::chrono::seconds(10));

    return 0;
}
