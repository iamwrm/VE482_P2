#include "DuplicateQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"

#include <algorithm>

constexpr const char *DuplicateQuery::qname;

QueryResult::Ptr DuplicateQuery::execute(){
    using namespace std;
    if (!this->operands.empty())
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "No operand should be found(? operands)."_f % operands.size()
        );
    Database &db = Database::getInstance();
    try{
        auto &table = db[this->targetTable];
        auto result = initCondition(table);
        vector<Table::ValueType> data;
        Table::SizeType counter = 0, sz = table.field().size();
        data.reserve(sz);
        if (result.second){
            auto t1 = table.begin();
            auto t2 = table.end();
            for (auto it = t1; it != t2; ++it){
                data.clear();
                if (this->evalCondition(*it)){
                    Table::KeyType tmp = it->key();
                    Table::KeyType copykey = tmp + "_copy";
                    //cout<<copykey<<endl;
                    //search copy for the key
                    bool iscopied = false;
                    for (auto it2 = it; it2 != table.end(); ++it2){
                        if (it2->key() == copykey){
                            iscopied = true;
                            break;
                        }   
                    }
                    if (iscopied) continue;
                    //if there are no copy, insert a copy
                    for (unsigned int i = 0; i < sz; i++){
                        data.push_back((*it)[i]);
                    }
                    table.insertByIndex(copykey, move(data));
                    counter++;
                    //printf("key: %s%d\n", (string)(it->key()), (*it)[1]);
                }
            }
        }
        //db.printAllTable();
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

std::string DuplicateQuery::toString() {
    return "QUERY = DUPLICATE " + this->targetTable + "\"";
}
    
