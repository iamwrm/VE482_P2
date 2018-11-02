//
// Created by wang on 18-10-31.
//

#ifndef PROJECT_COPYTABLEQUERY_H
#define PROJECT_COPYTABLEQUERY_H

#include "../Query.h"

class CopyTableQuery : public Query {
	static constexpr const char *qname = "COPYTABLE";
	const std::string fileName;
public:
    CopyTableQuery(std::string table, std::string filename)
            : Query(std::move(table)), fileName(std::move(filename)) {}

    QueryResult::Ptr execute() override;

    std::string toString() override;

};

#endif  //PROJECT_COPYTABLEQUERY_H