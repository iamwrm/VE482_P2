#include "SubQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"

#include <algorithm>

constexpr const char *SubQuery::qname;

QueryResult::Ptr SubQuery::execute(){
    using namespace std;
    if (this->operands.size() < 2){
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "Not enough operands (? operands)."_f % operands.size()
        );
    }
    Database &db = Database::getInstance();
    try{
        auto &table = db[this->targetTable];
        fieldids.reserve(this->operands.size());
        for(Table::FieldIndex i = 0; i < (Table::FieldIndex)this->operands.size(); ++i){
            fieldids.push_back(table.getFieldIndex(this->operands[i]));
        }
        Table::SizeType counter = 0;
        auto result = initCondition(table);
        if (result.second){
            for (auto it = table.begin(); it != table.end(); ++it){
                if (this->evalCondition(*it)) {
                    Table::ValueType ans = (*it)[fieldids[0]];
                    for(unsigned int i = 0; i < fieldids.size() - 2; ++i){
                        ans -= (*it)[fieldids[i+1]];
                    }
                    (*it)[this->fieldids[fieldids.size()-1]] = ans;
                    counter++;
                }
            }
        }
        return make_unique<RecordCountResult>(counter);
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

std::string SubQuery::toString() {
    return "QUERY = SUB " + this->targetTable + "\"";
}
