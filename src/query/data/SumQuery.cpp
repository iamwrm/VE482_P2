//
// Created by Zhang on 18-11-1.
//

#include "SumQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"
#include <algorithm>

constexpr const char *SumQuery::qname;

QueryResult::Ptr SumQuery::execute() 
{
	using namespace std;
	if (this->operands.empty())
		return make_unique<ErrorMsgResult>(
			qname, this->targetTable.c_str(),
			"No operand (? operands)."_f % operands.size()
		);
	Database &db = Database::getInstance();
	try
	{
		auto &table=db[this->targetTable];
		unsigned int operand_count=0;
		vector<size_t> sum_field_index;
		sum_field_index.reserve(this->operands.size());
		for(operand_count=0;operand_count<this->operands.size();operand_count++)
		{
			sum_field_index.push_back(table.getFieldIndex(this->operands[operand_count]));
		}
		operand_count=0;
		vector<int> sum_result;
		sum_result.reserve(this->operands.size());
		for(operand_count=0;operand_count<this->operands.size();operand_count++)
		{
			sum_result.push_back(0);
		}
		operand_count=0;
		auto result=initCondition(table);
		if(result.second)
		{
			for(auto it=table.begin();it!=table.end();++it)
			{
				if(this->evalCondition(*it))
				{
					for(operand_count=0;operand_count<this->operands.size();operand_count++)
					{
						sum_result[operand_count]+=(*it)[sum_field_index[operand_count]];
					}
				}
			}
		}
		return make_unique<SuccessMsgResult>(sum_result);
	}
	catch (const TableNameNotFound &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
    } catch (const IllFormedQueryCondition &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
    } catch (const invalid_argument &e) {
        // Cannot convert operand to string
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unknown error '?'"_f % e.what());
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unkonwn error '?'."_f % e.what());
    }	
}

std::string SumQuery::toString() {
    return "QUERY = SUM " + this->targetTable + "\"";
}

