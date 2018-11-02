//
// Created by wang on 18-10-25.
//

#include "SwapQuery.h"
#include "../../db/Database.h"

constexpr const char *SwapQuery::qname;

QueryResult::Ptr SwapQuery::execute()
{
	using namespace std;
	if (this->operands.size() != 2)
		return make_unique<ErrorMsgResult>(
		    qname, this->targetTable.c_str(),
		    "Invalid number of operands (? operands)."_f %
			operands.size());
	Database &db = Database::getInstance();
	Table::SizeType counter = 0;
	try {
		auto &table = db[this->targetTable];
		this->fieldId_0 = table.getFieldIndex(this->operands[0]);
		this->fieldId_1 = table.getFieldIndex(this->operands[1]);
		auto result = initCondition(table);
		if (result.second) {
			for (auto it = table.begin(); it != table.end(); ++it) {
				if (this->evalCondition(*it)) {
					swap((*it)[this->fieldId_0],
					     (*it)[this->fieldId_1]);
					++counter;
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

std::string SwapQuery::toString()
{
	return "QUERY = SWAP " + this->targetTable + "\"";
}
