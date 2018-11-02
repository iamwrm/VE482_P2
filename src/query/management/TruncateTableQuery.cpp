//
// Created by wwt on 10/31/18.
//

#include "TruncateTableQuery.h"

constexpr const char* TruncateTableQuery::qname;

std::string TruncateTableQuery::toString() {
    return "QUERY = TRUNCATE, Table = \"" + targetTable + "\"";
}

QueryResult::Ptr TruncateTableQuery::execute() {
    Database &db = Database::getInstance();
    try {
    	db[targetTable].clear();
        return std::make_unique<SuccessMsgResult>(qname);
    }
    catch (const std::exception &e) {
        return std::make_unique<ErrorMsgResult>(qname, e.what());
    }
   
}
