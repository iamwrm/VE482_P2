#ifndef PROJECT_SUBQUERY_H
#define PROJECT_SUBQUERY_H

#include "../Query.h"
#include <vector>

class SubQuery : public ComplexQuery {
    static constexpr const char *qname = "SUB";

    std::vector<Table::FieldIndex> fieldids;
public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;
};

#endif //PROJECT_SUBQUERY_H
