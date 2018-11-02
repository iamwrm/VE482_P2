//
// Created by wwt on 11/1/18.
//

#ifndef PROJECT_SELECTQUERY_H
#define PROJECT_SELECTQUERY_H

#include "../Query.h"
#include "../../db/Table.h"
#include "../../db/Database.h"
#include "../QueryResult.h"
#include <map>
#include <algorithm>

class SelectQuery : public ComplexQuery {
    static constexpr const char *qname = "SELECT";

public:
    using ComplexQuery::ComplexQuery;
    QueryResult::Ptr execute() override;
    std::string toString() override;
};
#endif //PROJECT_SELECTQUERY_H
