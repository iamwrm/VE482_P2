//
// Created by wwt on 11/1/18.
//

#include "CountQuery.h"


constexpr const char *CountQuery::qname;

std::bool CountQuery::display() { return true;}

std::string CountQuery::toString() {
    return "QUERY = COUNT " + this->targetTable + "\"";
}

QueryResult::Ptr CountQuery::execute() {
    using namespace std;
    if (this->operands.size() != 0)
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Invalid number of operands (? operands)."_f % operands.size());
    Database &db = Database::getInstance();
    try {
        auto &table = db[this->targetTable];
        int counter = 0;
        for (auto object : table) {
            if (myEvalCondition(condition, object))
                counter++;
        }
        return make_unique<SuccessMsgResult>(counter);
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
