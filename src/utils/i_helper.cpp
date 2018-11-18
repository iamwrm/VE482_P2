#include "i_helper.h"

using std::string;

// return information of a query
inf_qry getInformation(std::string qry, int& count)
{
	int size = qry.size();
	int begin=0, end=0;
	inf_qry inf;
	for (int i = 0; i < size; ++i) {
		if (qry[i] == '=') {
			i += 2;
			begin = i;
			for (int j = i; j < size; ++j) {
				if (qry[j] == ' ') {
					end = j;
					break;
				}
			}
			break;
		}
	}
	// get command
	string command = qry.substr(begin, end - begin);
	// first case: data query
	if (command == "ADD" || command == "COUNT" || command == "DELETE" ||
	    command == "DUPLICATE" || command == "INSERT" || command == "MAX" ||
	    command == "MIN" || command == "SELECT" || command == "SUB" ||
	    command == "SUM" || command == "SWAP" || command == "UPDATE") {
		begin = end + 1;
		for (int i = begin; i < size; ++i) {
			if (qry[i] == '\"') {
				end = i;
				break;
			}
		}
		string target = qry.substr(begin, end - begin);
		inf.targetTable = target;
		inf.affectAll = false;
		inf.newTable = "";
	} else {
		// if copy table
		if (command == "Copy") {
			for (int i = end; i < size; ++i) {
				if (qry[i] == '\"') {
					begin = i + 1;
					for (int j = begin; j < size; ++j) {
						if (qry[j] == '\"') {
							end = j;
							break;
						}
					}
					break;
				}
			}
			inf.targetTable = qry.substr(begin, end - begin);
			inf.affectAll = false;
			for (int i = end + 1; i < size; ++i) {
				if (qry[i] == '\"') {
					begin = i + 1;
					for (int j = begin; j < size; ++j) {
						if (qry[j] == '\"') {
							end = j;
							break;
						}
					}
					break;
				}
			}
			inf.newTable = qry.substr(begin, end - begin);
		} else if (command == "LIST" || command == "Quit") {
			inf.targetTable = "";
			inf.newTable = "";
			inf.affectAll = true;
		} else {
			for (int i = end; i < size; ++i) {
				if (qry[i] == '\"') {
					begin = i + 1;
					for (int j = begin; j < size; ++j) {
						if (qry[j] == '\"') {
							end = j;
							break;
						}
					}
					break;
				}
			}
			inf.targetTable = qry.substr(begin, end - begin);
			inf.affectAll = false;
			inf.newTable = "";
		}
	}
	// record line
	inf.line = count;
	// reocrd write/read
	if (command == "COUNT" || command == "MAX" || command == "MIN" ||
	    command == "SELECT" || command == "SUM" || command == "Copy" ||
	    command == "Dump" || command == "LIST"  ||
	    command == "SHOWTABLE") {
		inf.read = true;
		inf.write = false;
	} else {
		inf.read = false;
		inf.write = true;
	}
	return inf;
}