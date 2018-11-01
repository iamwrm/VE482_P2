//
// Created by wang on 18-11-1.
//

#ifndef PROJECT_MINQUERY_H
#define PROJECT_MINQUERY_H

#include "../Query.h"

class MinQuery : public ComplexQuery {
	static constexpr const char *qname = "MIN";
	Table::ValueType
	    fieldValue;  // = (operands[0]=="KEY")? 0 :std::stoi(operands[1]);
	Table::FieldIndex fieldId_0;
	Table::FieldIndex fieldId_1;

	Table::KeyType keyValue_0;
	Table::KeyType keyValue_1;

       public:
	using ComplexQuery::ComplexQuery;

	QueryResult::Ptr execute() override;

	std::string toString() override;
};

#endif  // 	PROJECT_MINQUERY_H

