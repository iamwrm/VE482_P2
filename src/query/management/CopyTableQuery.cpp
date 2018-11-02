//
// Created by wang on 18-10-25.
//

#include "CopyTableQuery.h"
#include "../../db/Database.h"

#include <fstream>

constexpr const char *CopyTableQuery::qname;

QueryResult::Ptr CopyTableQuery::execute()
{
	using namespace std;
	auto &db = Database::getInstance();
	try {
		// DONE:
		db.testDuplicate(fileName);
		// find the source table -> see dumptable
		auto &sourceTable = db[this->targetTable];
		// copy to new table -> copy constructor
		auto newTable = std::make_unique<Table>(sourceTable);
		// set table name
		newTable->setName(fileName);
		// save to db	-> database.registerTable
		db.registerTable(std::move(newTable));
		return make_unique<SuccessMsgResult>(qname, targetTable);

	} catch (const exception &e) {
		return make_unique<ErrorMsgResult>(qname, e.what());
	}
}

std::string CopyTableQuery::toString()
{
	return "QUERY = Copy TABLE, FILE = \"" + this->fileName + "\"";
}
