#ifndef PROJECT_ADDQUERY_H
#define PROJECT_ADDQUERY_H

#include "../Query.h"
#include <vector>

class AddQuery : public ComplexQuery {
    static constexpr const char *qname = "ADD";

    std::vector<Table::FieldIndex> fieldids;
public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;
};

#endif //PROJECT_ADDQUERY_H
