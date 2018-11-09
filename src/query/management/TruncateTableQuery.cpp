//
// Created by wwt on 10/31/18.
//

#include "TruncateTableQuery.h"

constexpr const char* TruncateTableQuery::qname;

std::string TruncateTableQuery::toString() {
    return "QUERY = TRUNCATE, Table = \"" + targetTable + "\"";
}

QueryResult::Ptr TruncateTableQuery::execute() {
    using namespace std;
    Database &db = Database::getInstance();
    try {
    	db[targetTable].clear();   
    }
    catch (const std::exception &e) {
        return std::make_unique<ErrorMsgResult>(qname, e.what());
    }
   return std::make_unique<SuccessMsgResult>(qname);
}
