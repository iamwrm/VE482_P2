//
// Created by Zhang on 18-11-1.
//

#ifndef PROJECT_SUMQUERY_H
#define PROJECT_SUMQUERY_H

#include "../Query.h"

class SumQuery : public ComplexQuery {
    static constexpr const char *qname = "SUM";
    Table::ValueType fieldValue;// = (operands[0]=="KEY")? 0 :std::stoi(operands[1]);
    Table::FieldIndex fieldId;
    Table::KeyType keyValue;
public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;
};


#endif //PROJECT_SUMQUERY_H
