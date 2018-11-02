//
// Created by wang on 18-11-1.
//

#ifndef PROJECT_MAXQUERY_H
#define PROJECT_MAXQUERY_H

#include "../Query.h"

class MaxQuery : public ComplexQuery {
	static constexpr const char *qname = "MAX";

	Table::KeyType keyValue_0;
	Table::KeyType keyValue_1;

       public:
	using ComplexQuery::ComplexQuery;

	QueryResult::Ptr execute() override;

	std::string toString() override;
};

#endif  // 	PROJECT_MAXQUERY_H

