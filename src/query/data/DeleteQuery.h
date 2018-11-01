//
// Created by wang on 18-11-1.
//

#ifndef PROJECT_DELETEQUERY_H
#define PROJECT_DELETEQUERY_H

#include "../Query.h"

class DeleteQuery : public ComplexQuery {
	static constexpr const char *qname = "DELETE";
	Table::KeyType keyValue;

       public:
	using ComplexQuery::ComplexQuery;

	QueryResult::Ptr execute() override;

	std::string toString() override;
};

#endif  // 	PROJECT_DELETEQUERY_H
