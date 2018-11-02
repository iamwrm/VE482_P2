//
// Created by wwt on 11/1/18.
//

#ifndef PROJECT_COUNTQUERY_H
#define PROJECT_COUNTQUERY_H

#include "../Query.h"
#include "../../db/Table.h"
#include "../../db/Database.h"
#include "../QueryResult.h"


class CountQuery : public ComplexQuery {
    static constexpr const char *qname = "COUNT";

public:
    using ComplexQuery::ComplexQuery;
    QueryResult::Ptr execute() override;
    std::string toString() override;
};

#endif //PROJECT_COUNTQUERY_H
