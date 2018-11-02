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
    db[targetTable].clear();
    return make_unique<NullQueryResult>();
}
