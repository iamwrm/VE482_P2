//
// Created by liu on 18-10-21.
//

#include "query/QueryParser.h"
#include "query/QueryBuilders.h"
#include "utils/i_helper.h"


using std::string;
using std::endl;
using std::cout;
using std::vector;

std::mutex mtx_query_queue;
std::mutex mtx_query_result_queue;
std::mutex mtx_query_queue_property;

std::vector<inf_qry> query_queue_property;


std::condition_variable  threadLimit;
std::condition_variable  readLimit;

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

std::vector<bool> if_query_done_arr(100,false);
std::mutex mtx_if_query_done_arr;

std::vector<Query::Ptr> query_queue;

std::vector<QueryResult::Ptr> query_result_queue(10);

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
	       Query_queue_arr &query_queue_arr)
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

			// ==============================
			mtx_query_queue_property.lock();

			/*
				    query_queue_property.emplace_back(
					std::move(getInformation(
					    query_queue[query_queue.size() -
			   1]->toString(), count_for_getInformation)));
			*/

			inf_qry local_inf_qry = getInformation(
			    query_queue[query_queue.size() - 1]->toString(),
			    count_for_getInformation);

			mtx_query_queue_property.unlock();
			// ==============================
			std::string query_string =
			    query_queue[query_queue.size() - 1]->toString();

			mtx_query_queue_arr.lock();
			if (query_string[0 + 8] == 'Q' &&
			    query_string[3 + 8] == 't') {
				std::cout << "find Quit!:" << counter
					  << std::endl;
				query_queue_arr.quit_query = local_inf_qry;
			} else {
				if (query_queue_arr.table_name.find(
					local_inf_qry.targetTable) !=
				    query_queue_arr.table_name.end()) {
					auto it =
					    query_queue_arr.table_name.find(
						local_inf_qry.targetTable);

					query_queue_arr.arr[it->second]
					    .query_data.emplace_back(
						std::move(local_inf_qry));
					// find the table name
				} else {
					// new table name
					query_queue_arr.arr.emplace_back(
					    std::move(one_table_query()));
					// just inserted index
					int jii =
					    query_queue_arr.arr.size() - 1;

					query_queue_arr.table_name.insert(
					    std::pair<std::string, int>(
						local_inf_qry.targetTable,
						jii));

					query_queue_arr.arr[jii]
					    .query_data.emplace_back(
						std::move(local_inf_qry));
				}
			}
			mtx_query_queue_arr.unlock();

			// count for how many query information has been stored
			mtx_count_for_getInformation.lock();
			count_for_getInformation++;  // record the line of query
			mtx_count_for_getInformation.unlock();

			// std::cout << "in qq:" << count_for_getInformation
			//	  << endl;

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
void thread_starter(int queryID)
{

   mtx_query_result_queue.lock();
   //std::cout<<"in thread starter queryID:"<<queryID<<"  "<<query_queue.size()<<std::endl;
   if ((size_t)queryID>query_result_queue.size()-1){
       query_result_queue.resize(2 * queryID);
   } 
   mtx_query_result_queue.unlock();

   mtx_present_thread_num.lock();
   present_thread_num++;
   mtx_present_thread_num.unlock();



	query_result_queue[queryID]= query_queue[queryID]->execute();



   mtx_if_query_done_arr.lock();
   if (if_query_done_arr.size() - 1 < (size_t)queryID) {
	   if_query_done_arr.resize(2 * queryID);
   }
   if_query_done_arr[queryID] = true;
   mtx_if_query_done_arr.unlock();



   mtx_count_for_executed.lock();
   count_for_executed++;
   mtx_count_for_executed.unlock();

   readLimit.notify_one();
   cd_real_thread_limit.notify_one();
}

// TODO:
void scheduler()
{
	// inspect whether the thread number is bigger than the MAX number;

	//
	// int ttt = 0;

	// while (1) {
	while (1) {

    std::cout<<"loop\n";

		for (size_t i = 0; i < query_queue_arr.arr.size(); ++i) {
			if (query_queue_arr.arr[i].ifexist) {
				// lock the mutex
				mtx_query_queue_arr.lock();

				distribute:

				std::cout<<"distribute\n";
				// if there's a thread remaining, do
				size_t queryID = query_queue_arr.arr[i].query_data[query_queue_arr.arr[i].head].line;

				// tableID++    for loop
				if (query_queue_arr.arr[i].head >= query_queue_arr.arr[i].query_data.size()) {
					// unlock the mutex
					mtx_query_queue_arr.unlock();
					continue;
				}

				if (query_queue_arr.arr[i].query_data[query_queue_arr.arr[i].head].read) {
					if (query_queue_arr.arr[i].havereader) {

						std::thread{thread_starter, queryID}.detach();

						// thread num--
						mtx_present_thread_num.lock();
						present_thread_num--;
						mtx_present_thread_num.unlock();

						query_queue_arr.arr[i].head++;
						goto distribute;
					} else 
					if (query_queue_arr.arr[i] .havewriter) {
						// unlock the mutex
						mtx_query_queue_arr.unlock();
						continue;
					} else 
					{
						query_queue_arr.arr[i].havereader = true;
						std::thread{thread_starter, queryID}.detach();

						// thread num--
						mtx_present_thread_num.lock();
						present_thread_num--;
						mtx_present_thread_num.unlock();

						query_queue_arr.arr[i].head++;
						goto distribute;
					}
				} else 
				if (query_queue_arr.arr[i].query_data[query_queue_arr.arr[i].head].write) {
					if (query_queue_arr.arr[i].havereader || query_queue_arr.arr[i].havewriter) {
						// unlock the mutex
						mtx_query_queue_arr.unlock();
						continue;
					} else {
						query_queue_arr.arr[i].havewriter = true;
						std::thread{thread_starter, queryID}.detach();

						// thread num--
						mtx_present_thread_num.lock();
						present_thread_num--;
						mtx_present_thread_num.unlock();

						query_queue_arr.arr[i].head++;
						goto distribute;
					}
				}
				// if there's no thread remaining, sleep until awaken

				std::unique_lock<std::mutex> lock(mtx_present_thread_num);
				if (present_thread_num>8) cd_real_thread_limit.wait(lock);

				std::cout<<"pass wait\n";

				// unlock the mutex
				mtx_query_queue_arr.unlock();
			}
		}
		// one loop finish
		continue;
	}
}

// TODO:
void result_reader()
{
    //return;
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

        
        /*
		std::unique_lock<std::mutex> lock(mtx_count_for_getInformation);
		if (counter_for_result_reader > count_for_getInformation)
			threadLimit.wait(lock);

            */

	while (1) {
		std::unique_lock<std::mutex> lock(mtx_if_query_done_arr);
		if (if_query_done_arr[counter_for_result_reader] == false){
			readLimit.wait(lock);
			continue;
		}
		break;
	}

	QueryResult::Ptr & result = query_result_queue[counter_for_result_reader];

	std::cout << "in rr ------" << counter_for_result_reader<<
	" "<< endl;

	std::cout << *result;

	//std::cout<<query_result_queue[counter_for_result_reader];

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
        parsedArgs.threads = std::thread::hardware_concurrency();
        std::cerr << "lemondb: info: auto detect thread num "<< parsedArgs.threads << std::endl;
    } else {
        std::cerr << "lemondb: info: running in " << parsedArgs.threads << " threads" << std::endl;
    }


    QueryParser p;

    p.registerQueryBuilder(std::make_unique<QueryBuilder(Debug)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(ManageTable)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(Complex)>());



    // QueryResult::Ptr result = query->execute();

    // TODO: a table for each query


    // TODO: output result


    // read the listened file
    qq_reader(is,p,query_queue,query_queue_property,query_queue_arr);


    std::thread result_reader_th{result_reader};
    std::thread scheduler_th{scheduler};

    /*
    for (auto it = query_queue_property.begin();
	 it != query_queue_property.end(); it++) {
	    std::cout << it->line << std::endl;
    }
     */

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

    scheduler_th.join();
    // wait for the result printing thread to end
    result_reader_th.join();
    std::cout<<"finish"<<endl;
    //std::this_thread::sleep_for(std::chrono::seconds(10));

    return 0;
}
