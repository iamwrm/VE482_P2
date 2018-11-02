//
// Created by wang on 18-11-1.
//

#ifndef PROJECT_SWAPQUERY_H
#define PROJECT_SWAPQUERY_H

#include "../Query.h"

class SwapQuery : public ComplexQuery {
	static constexpr const char *qname = "SWAP";
	Table::FieldIndex fieldId_0;
	Table::FieldIndex fieldId_1;

	Table::KeyType keyValue_0;
	Table::KeyType keyValue_1;

       public:
	using ComplexQuery::ComplexQuery;

	QueryResult::Ptr execute() override;

	std::string toString() override;
};

#endif  // 	PROJECT_SWAPQUERY_H

