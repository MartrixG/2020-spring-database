/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "storage.h"

using namespace std;

namespace badgerdb
{

RecordId HeapFileManager::insertTuple(const string &tuple, File &file, BufMgr *bufMgr)
{
    PageId pageId;
    Page *page;
    RecordId rid;
    bufMgr->allocPage(&file, pageId, page);
    rid = page->insertRecord(tuple);
    bufMgr->unPinPage(&file, pageId, true);
}

void HeapFileManager::deleteTuple(const RecordId &rid, File &file, BufMgr *bufMgr)
{
    Page *page;
    bufMgr->readPage(&file, rid.page_number, page);
    page->deleteRecord(rid);
    bufMgr->unPinPage(&file, rid.page_number, true);
}

string HeapFileManager::createTupleFromSQLStatement(const string &sql, const Catalog *catalog)
{
    string tableName = sql.substr(12, 1);
    bool recordFlag = false;
    string str1, str2;
    for (int i = 12;;i++)
    {
        if (sql[i] == '(')
        {
            i++;
            while (i < sql.size())
            {
                string &tmpStr = str1.empty() ? str1 : str2;
                if (sql[i] == '\'')
                {
                    i++;
                    while (sql[i] != '\'')
                        tmpStr += sql[i++];
                    i++;
                }
                while (sql[i] >= '0' && sql[i] <= '9')
                    tmpStr += sql[i++];
                i++;
            }
            break;
        }
    }
    string tuple = str1 + ' ' + str2;
    return tuple;
}
} // namespace badgerdb