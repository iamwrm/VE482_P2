//
// Created by wwt on 11/1/18.
//

#include "SelectQuery.h"

constexpr const char *SelectQuery::qname;

std::string SelectQuery::toString()
{
    return "QUERY = SELECT " + this->targetTable + "\"";
}

QueryResult::Ptr SelectQuery::execute()
{
    using namespace std;
    string outputString = "";
    if (this->operands.empty())
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Invalid number of operands (? operands)."_f % operands.size());
    Database &db = Database::getInstance();
    try {
        auto &table = db[this->targetTable];
        map<string, vector<int>> output;
        for (auto object : table) {
            if (myEvalCondition(condition, object)) {
                output[object.key()] = vector<int>();
                for (int i = 1; (unsigned long)i < operands.size();  i++)
                    output[object.key()].push_back(object[operands[i]]);
            }
        }
        auto it = output.begin();
        if (it!=output.end()) {
            ostringstream outputStream;
            int flag = 0;
            while (it != output.end()) {
                if (flag == 0)
                    flag = 1;
                else
                    outputStream << "\n";

                outputStream << "( ? "_f % (*it).first;
                for (int i = 0; (unsigned long)i < (*it).second.size(); i++)
                    outputStream << (*it).second[i] << " ";
                outputStream << ")";
                ++it;
            }
            outputString = outputStream.str();
        }
        	return make_unique<SuccessMsgResult>(outputString);
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
