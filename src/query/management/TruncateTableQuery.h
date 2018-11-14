//
// Created by wwt on 10/31/18.
//

#ifndef PROJECT_TRUNCATETABLEQUERY_H
#define PROJECT_TRUNCATETABLEQUERY_H

#include "../Query.h"
#include "../../db/Table.h"
#include "../../db/Database.h"
#include "../QueryResult.h"

class TruncateTableQuery : public Query {
    static constexpr const char* qname = "TRUNCATE";
    const std::string tableName;

public:
    using Query::Query;

    QueryResult::Ptr execute() override;

    std::string toString() override;
};

#endif //PROJECT_TRUNCATETABLEQUERY_H
