/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "schema.h"

#include <string>
#include <iostream>
#include <sstream>
using namespace std;

namespace badgerdb
{

TableSchema TableSchema::fromSQLStatement(const string &sql)
{
    string tableName;
    vector<Attribute> attrs;
    bool isTemp = false;
    string declare;
    string header = sql.substr(0, 12);
    if (header == "CREATE TABLE" || header == "create table")
    {
        for (size_t i = 13; i < sql.size(); i++)
        {
            if (sql[i] == ' ')
            {
                while (sql[i] == ' ' || sql[i] == '(')
                    i++;
                declare = sql.substr(i, sql.size());
                break;
            }
            else
                tableName += sql[i];
        }
        bool flagName = false, flagType = false;
        string attrName;
        DataType attrType;
        int maxSize = 0;
        bool isNotNull = false;
        bool isUnique = false;
        for (size_t i = 0; i < declare.size() - 1;)
        {
            if (declare[i] == ',' || declare[i] == ')')
            {
                Attribute tmpAttr(*new string(attrName), *new DataType(attrType), maxSize, isNotNull, isUnique);
                attrs.push_back(tmpAttr);
                isNotNull = isUnique = flagName = flagType = false;
                maxSize = 0;
                attrName.clear();
                i++;
                while (declare[i] == ' ')
                    i++;
                continue;
            }
            if (flagName == false)
            {
                if (declare[i] == ' ')
                {
                    flagName = true;
                    i++;
                    continue;
                }
                attrName += declare[i];
                i++;
            }
            else if (flagType == false)
            {
                bool flagLength = false;
                if (declare[i] == 'I')
                {
                    attrType = DataType(0);
                    i += 3;
                }
                else 
                {
                    int tric = declare[i] == 'C' ? 1 : 2;
                    attrType = DataType(tric);
                    i += (tric * 3 + 1);
                    flagLength = true;
                }
                if (flagLength)
                {
                    i++;
                    int length = 0;
                    while (declare[i] != ')')
                    {
                        length *= 10;
                        length += declare[i] - '0';
                        i++;
                    }
                    i++;
                    maxSize = length;
                }
                flagType = true;
            }
            else
            {
                if (declare[i] == ' ')
                {
                    i++;
                    continue;
                }
                if (declare[i] == 'N')
                {
                    i += 8;
                    isNotNull = true;
                }
                else if (declare[i] == 'U')
                {
                    i += 6;
                    isUnique = true;
                }
            }
        }
    }
    else
    {
        cout << "create table need \"create table\" or \"CREATE TABLE\" start.";
        throw new exception;
    }
    return TableSchema(tableName, attrs, isTemp);
}

void TableSchema::print() const
{
    cout << "table name : " << tableName;
    if(isTemp)
    {
        cout << "(Temp)";
    }
    cout << endl;
    vector<Attribute>::const_iterator it;
    for(it = attrs.begin(); it != attrs.end(); it++)
    {
        
        string type;
        switch(it->attrType)
        {
            case INT:type="INT";break;
            case CHAR:type="CHAR";break;
            case VARCHAR:type="VARCHAR";break;
        }
        cout << it->attrName << " : " << type;
        if(it->maxSize!=0)
        {
            cout << "(" << it->maxSize << ")";
        }
        cout  << " | ";
        if(it->isNotNull)
        {
            cout << " " << "NOT NULL";
        }
        if(it->isUnique)
        {
            cout << " " << "UNIQUE";
        }
        cout << endl;
    }
}

} // namespace badgerdb