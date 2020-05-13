/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "storage.h"
#include "file_iterator.h"
#include "page_iterator.h"
#include <iostream>
#include <string>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sstream>
#include "exceptions/insufficient_space_exception.h"
#include "exceptions/page_not_pinned_exception.h"

using namespace std;

namespace badgerdb
{

RecordId HeapFileManager::insertTuple(const string &tuple, File &file, BufMgr *bufMgr)
{
    Page *nowBufPage;
    PageId nowPageId;
    Page nowPage;
    RecordId recordId;
    FileIterator iter = file.begin();
    while ( iter != file.end())
    {
        nowPage = *iter;
        nowPageId = nowPage.page_number();
        if(nowPageId == Page::INVALID_NUMBER) break;
        bufMgr->readPage(&file, nowPageId, nowBufPage);
        try
        {
            recordId = nowBufPage->insertRecord(tuple);
            bufMgr->unPinPage(&file, nowPageId, true);
            return recordId;
        }
        catch (PageNotPinnedException&)
        {
        }
        catch(InsufficientSpaceException&)
        {
            bufMgr->unPinPage(&file, nowPageId, false);
        }
        ++iter;
    }
    bufMgr->allocPage(&file, nowPageId, nowBufPage);
    recordId = nowBufPage->insertRecord(tuple);
    bufMgr->unPinPage(&file, nowPageId, true);
  return recordId;
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
    string str1, str2;
    for (size_t i = 12;;i++)
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