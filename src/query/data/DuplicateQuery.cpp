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
        vector<decltype(*(table.begin()))>  v_to_be_inserted;
    	Table::SizeType counter = 0, sz = table.field().size();

	if (result.second){
        auto t1 = table.begin();
        auto t2 = table.end();
        auto original_size = table.size();
	    for (decltype(table.size()) i = 0; i < original_size; i++) {
		    auto it = table.begin() + i;
		    if (this->evalCondition(*(it))) {
			    Table::KeyType tmp = it->key();
			    Table::KeyType copykey = tmp + "_copy";

			    // search copy for the key
			    // Added by Wang =====
		        if (table.checkKeyExistence(copykey)) {
			       continue;
		        }
		        // ===================
		        // if there are no copy, insert a copy
		        vector<Table::ValueType> new_data;
		        new_data.clear();
		        for (unsigned int i = 0; i < sz; i++) {
			       new_data.push_back((*it)[i]);
		        }

		        table.insertByIndex(copykey, std::move(new_data));
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

std::string DuplicateQuery::toString() {
    return "QUERY = DUPLICATE " + this->targetTable + "\"";
}
    
