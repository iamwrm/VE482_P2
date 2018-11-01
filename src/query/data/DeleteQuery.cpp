//
// Created by wang on 18-10-25.
//

#include "DeleteQuery.h"
#include "../../db/Database.h"

#include <iostream> 

constexpr const char *DeleteQuery::qname;

QueryResult::Ptr DeleteQuery::execute()
{
	using namespace std;
	if (this->operands.size() != 0)
		return make_unique<ErrorMsgResult>(
		    qname, this->targetTable.c_str(),
		    "Invalid number of operands (? operands)."_f %
			operands.size());
	Database &db = Database::getInstance();
	Table::SizeType counter = 0;
	try {
		auto &table = db[this->targetTable];
		/*
		if (this->operands[0] == "KEY") {
			this->keyValue = this->operands[1];
		} else {
			this->fieldId = table.getFieldIndex(this->operands[0]);
			this->fieldValue = (Table::ValueType)strtol(
			    this->operands[1].c_str(), nullptr, 10);
		}
		*/
		auto result = initCondition(table);
		if (result.second) {
			auto it = table.begin();
			while (it!=table.end()){
				if (this->evalCondition(*it)) {
					it = table.pub_erase(it);
					//table.deleteByIndex((*it).key());
					++counter;
				}else {
					it++;
				}
			}

		}

		return make_unique<RecordCountResult>(counter);
	} catch (const TableNameNotFound &e) {
		return make_unique<ErrorMsgResult>(qname, this->targetTable,
						   "No such table."s);
	} catch (const IllFormedQueryCondition &e) {
		return make_unique<ErrorMsgResult>(qname, this->targetTable,
						   e.what());
	} catch (const invalid_argument &e) {
		// Cannot convert operand to string
		return make_unique<ErrorMsgResult>(
		    qname, this->targetTable, "Unknown error '?'"_f % e.what());
	} catch (const exception &e) {
		return make_unique<ErrorMsgResult>(
		    qname, this->targetTable,
		    "Unkonwn error '?'."_f % e.what());
	}
}

std::string DeleteQuery::toString()
{
	return "QUERY = DELETE " + this->targetTable + "\"";
}
