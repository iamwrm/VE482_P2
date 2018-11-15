//
// Created by liu on 18-10-21.
//

#include "query/QueryParser.h"
#include "query/QueryBuilders.h"

#include <getopt.h>
#include <fstream>
#include <sstream>
#include <iostream>
#include <string>
#include <thread>

using std::string;
using std::endl;


struct {
    std::string listen;
    long threads = 0;
} parsedArgs;

//record line of query
int count = 0;

//structure to store information of a query
struct inf_qry {
    bool read;
    bool write;
    bool affectAll; //if the query affect all the tables
    std::string targetTable; // "" for no target table
    std::string newTable; // "" for no new table
    int line;
};

//return information of a query
inf_qry getInformation(string qry){
    int size = qry.size();
    int begin, end;
    inf_qry inf;
    for(int i = 0; i < size; ++i){
        if(qry[i] == '='){
            i+=2;
            begin = i;
            for(int j = i; j < size; ++j){
                if(qry[j] == ' '){
                    end = j;
                    break;
                }
            }
            break;
        }
    }
    //get command
    string command = qry.substr(begin, end-begin);
    //first case: data query
    if(command=="ADD"||command=="COUNT"||command=="DELETE"||command=="DUPLICATE"||
        command=="INSERT"||command=="MAX"||command=="MIN"||command=="SELECT"||
        command=="SUB"||command=="SUM"||command=="SWAP"||command=="UPDATE"){
        begin = end + 1;
        for(int i = begin; i < size; ++i){
            if(qry[i] == '\"'){
                end = i;
                break;
            }
        }
        string target = qry.substr(begin, end-begin);
        inf.targetTable = target;
        inf.affectAll = false;
        inf.newTable = "";
    }
    else{
        //if copy table
        if(command=="Copy"){
            for(int i = end; i < size; ++i){
                if(qry[i] == '\"'){
                    begin = i + 1;
                    for(int j = begin; j < size; ++j){
                        if(qry[j] == '\"'){
                            end = j;
                            break;
                        }
                    }
                    break;
                }
            }
            inf.targetTable = qry.substr(begin, end-begin);
            inf.affectAll = false;
            for(int i = end+1; i < size; ++i){
                if(qry[i] == '\"'){
                    begin = i + 1;
                    for(int j = begin; j < size; ++j){
                        if(qry[j] == '\"'){
                            end = j;
                            break;
                        }
                    }
                    break;
                }
            }
            inf.newTable = qry.substr(begin, end-begin);
        }
        else if(command=="LIST"||command=="Quit"){
            inf.targetTable = "";
            inf.newTable = "";
            inf.affectAll = true;
        }
        else{
            for(int i = end; i < size; ++i){
                if(qry[i] == '\"'){
                    begin = i + 1;
                    for(int j = begin; j < size; ++j){
                        if(qry[j] == '\"'){
                            end = j;
                            break;
                        }
                    }
                    break;
                }
            }
            inf.targetTable = qry.substr(begin, end-begin);
            inf.affectAll = false;
            inf.newTable = "";
        }
    }
    //record line
    inf.line = count;
    //reocrd write/read
    if(command=="COUNT"||command=="MAX"||command=="MIN"||command=="SELECT"||
        command=="SUM"||command=="Copy"||command=="Dump"||command=="LIST"||
        command=="Load"||command=="SHOWTABLE"){
        inf.read = true;
        inf.write = false;
    }
    else{
        inf.read = false;
        inf.write = true;
    }
    return inf;
}

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

int main(int argc, char *argv[]) {
    // Assume only C++ style I/O is used in lemondb
    // Do not use printf/fprintf in <cstdio> with this line
    std::ios_base::sync_with_stdio(false);

    parseArgs(argc, argv);

    std::fstream fin;
    if (!parsedArgs.listen.empty()) {
        fin.open(parsedArgs.listen);
        if (!fin.is_open()) {
            std::cerr << "lemondb: error: " << parsedArgs.listen << ": no such file or directory" << std::endl;
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

    size_t counter = 0;

    while (is) {
        try {
            // A very standard REPL
            // REPL: Read-Evaluate-Print-Loop
            std::string queryStr = extractQueryString(is);
            Query::Ptr query = p.parseQuery(queryStr);
            count++; //record the line of query
            //std::cout<<query->toString()<<endl;
            //std::cout<<getInformation(query->toString()).targetTable<<
            //"   "<<getInformation(query->toString()).newTable<<endl;
            QueryResult::Ptr result = query->execute();
            std::cout << ++counter << "\n";
            if (result->success()) {
                if (result->display()) {
                    std::cout << *result;
                } else {
#ifndef NDEBUG
                    std::cout.flush();
                    std::cerr << *result;
#endif
                }
            } else {
                std::cout.flush();
                std::cerr << "QUERY FAILED:\n\t" << *result;
            }
        }  catch (const std::ios_base::failure& e) {
            // End of input
            break;
        } catch (const std::exception& e) {
            std::cout.flush();
            std::cerr << e.what() << std::endl;
        }
    }

    return 0;
}
