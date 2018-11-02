//
// Created by wang on 18-11-1.
//

#include "MinQuery.h"
#include "../../db/Database.h"

constexpr const char *MinQuery::qname;

QueryResult::Ptr MinQuery::execute()
{
	using namespace std;
	/* 
	Don't need to check operands number
	if it's 0, just output Answer = ( )
	if (this->operands.size() < 1)
		return make_unique<ErrorMsgResult>(
		    qname, this->targetTable.c_str(),
		    "Invalid number of operands (? operands)."_f %
			operands.size());
	*/
	Database &db = Database::getInstance();
	Table::SizeType counter = 0;
	try {
		auto &table = db[this->targetTable];

		// DONE: use a vector to store all the fields ( c0 c1 c2 .. cn) 's min
		// initialize it with 0, then assign it with the first
		// group that meet the condition
		vector<int> minValue(this->operands.size());
		for (auto i_v = minValue.begin(); i_v != minValue.end(); i_v++) {
			*i_v = 0;
		}

		auto result = initCondition(table);
		if (result.second) {
			for (auto it = table.begin(); it != table.end(); ++it) {
				if (this->evalCondition(*it)) {
					if (counter == 0) {
						// DONE: copy all fields value
						// to minValue vector
						for (decltype(this->operands.size()) i = 0; i < this->operands.size(); i++) {
							minValue[i] = (*it)[operands[i]];
						}
					} else {
						// DONE: compare each field with the
						// vector<value>[cn]
						for (decltype(this->operands.size()) i = 0; i < this->operands.size(); i++) {
							if (minValue[i] > (*it) [operands[i]]) { 
								minValue[i] = (*it) [operands [i]];
							}
						}
					}
					++counter;
				}
			}
		}
		// DONE: modify the return value to show ( a b c d)
		// don't forget to differ between empty and exist
		if (counter==0){
			minValue.clear();
		}
		return make_unique<SuccessMsgResult>(minValue);
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

std::string MinQuery::toString()
{
	return "QUERY = MIN " + this->targetTable + "\"";
}
