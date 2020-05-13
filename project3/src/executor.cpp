/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "executor.h"

#include <functional>
#include <string>
#include <iostream>
#include <ctime>

#include "storage.h"
#include "file_iterator.h"
#include "page_iterator.h"
#include <iomanip>
#include <sstream>
#include <cstring>
#include <cmath>

using namespace std;

namespace badgerdb
{

static string index_join;
void TableScanner::print() const
{
    File file = TableScanner::tableFile;
    bufMgr->flushFile(&tableFile);
    stringstream names;
    stringstream header;
    header << "+";
    names << "|\t";
    for (int i = 0; i < tableSchema.getAttrCount(); i++)
    {
        string atrname = tableSchema.getAttrName(i);
        names << atrname << "\t|\t";
        header << "---------------+";
    }
    cout << "------------------------------" << endl;
    cout << "table name: " << tableSchema.getTableName() << endl;
    cout << header.str() << endl;
    cout << names.str() << endl;
    cout << header.str() << endl;

    Page *nowPage;
    for (FileIterator it = file.begin(); it != file.end(); it++)
    {
        PageId nowPageNumber = (*it).page_number();
        bufMgr->readPage(&file, nowPageNumber, nowPage);
        SlotId nowSlotNumber = nowPage->begin().getNextUsedSlot(0);
        while (nowSlotNumber)
        {
            RecordId nowRecord = {nowPageNumber, nowSlotNumber};
            string record = nowPage->getRecord(nowRecord);
            string printStr = "|\t";
            string tmp;
            for (size_t i = 0; i < record.size(); i++)
            {
                if (record[i] == ' ')
                {
                    printStr += (tmp + "\t|\t");
                    tmp.clear();
                }
                else
                {
                    tmp += record[i];
                }
            }
            printStr += (tmp + "\t|\t");
            cout << printStr << endl;
            nowSlotNumber = nowPage->begin().getNextUsedSlot(nowSlotNumber);
        }
        bufMgr->unPinPage(&file, nowPageNumber, false);
        bufMgr->flushFile(&file);
    }
}

bool check(const File &leftTableFile, const File &rightTableFile)
{
    File lf = leftTableFile;
    File rf = rightTableFile;
    int l = 0;
    int r = 0;
    for (FileIterator it = lf.begin(); it != lf.end(); it++)
        l++;
    for (FileIterator it = rf.begin(); it != rf.end(); it++)
        r++;
    return l < r;
}
JoinOperator::JoinOperator(const File &leftTableFile,
                           const File &rightTableFile,
                           const TableSchema &leftTableSchema,
                           const TableSchema &rightTableSchema,
                           Catalog *catalog,
                           BufMgr *bufMgr)
    : leftTableFile(leftTableFile),
      rightTableFile(rightTableFile),
      leftTableSchema(leftTableSchema),
      rightTableSchema(rightTableSchema),
      resultTableSchema(
          createResultTableSchema(leftTableSchema, rightTableSchema)),
      catalog(catalog),
      bufMgr(bufMgr),
      isComplete(false)
{
    if (!check(this->leftTableFile, this->rightTableFile))
        resultTableSchema = createResultTableSchema(rightTableSchema, leftTableSchema);
}

TableSchema JoinOperator::createResultTableSchema(
    const TableSchema &leftTableSchema,
    const TableSchema &rightTableSchema)
{
    vector<Attribute> attrs;
    int size_l = leftTableSchema.getAttrCount();
    int size_r = rightTableSchema.getAttrCount();
    cout << "---------------left---------------" << endl;
    leftTableSchema.print();
    cout << "---------------right--------------" << endl;
    rightTableSchema.print();
    for (int i = 0; i < size_l; i++)
    {
        Attribute a(leftTableSchema.getAttrName(i), leftTableSchema.getAttrType(i), leftTableSchema.getAttrMaxSize(i), leftTableSchema.isAttrNotNull(i), leftTableSchema.isAttrUnique(i));
        attrs.push_back(a);
    }
    for (int i = 0; i < size_r; i++)
    {
        int have_same_attr = 1;
        for (size_t j = 0; j < attrs.size(); j++)
        {
            if ((string &)attrs[j].attrName == (string &)rightTableSchema.getAttrName(i))
            {
                index_join = rightTableSchema.getAttrName(i);
                have_same_attr = 0;
            }
        }
        if (have_same_attr)
        {
            Attribute a(rightTableSchema.getAttrName(i), rightTableSchema.getAttrType(i), rightTableSchema.getAttrMaxSize(i), rightTableSchema.isAttrNotNull(i), rightTableSchema.isAttrUnique(i));
            attrs.push_back(a);
        }
    }
    return TableSchema("T", attrs, true);
}
std::multimap<string, string> finding;
void build(Page *page, BufMgr *buf, const TableSchema &table)
{
    int size = table.getAttrCount();
    string atr[size];
    int tuple_index = 0;
    for (int i = 0; i < size; i++)
        if (index_join == table.getAttrName(i))
            tuple_index = i;
    PageId nowPageNumber = page->page_number();
    SlotId nowSlotNumber = page->begin().getNextUsedSlot(0);
    while (nowSlotNumber)
    {
        RecordId nowRecordID = {nowPageNumber, nowSlotNumber};
        stringstream atr_value;
        nowSlotNumber = page->begin().getNextUsedSlot(nowSlotNumber);

        string record = page->getRecord(nowRecordID) + ' ';
        string tmp;
        int index = 0;
        for (size_t i = 0; i < record.size(); i++)
        {
            if (record[i] == ' ')
            {
                atr[index++] = tmp;
                if (tmp != index_join)
                {
                    atr_value << tmp;
                    if (index < size)
                        atr_value << ' ';
                }
                else
                    tuple_index = index;
                tmp.clear();
            }
            else
                tmp += record[i];
        }
        finding.insert(make_pair(atr[tuple_index], atr_value.str()));
    }
}

int join(File file, Page *page, const TableSchema &resultable,
         const TableSchema &table, Catalog *catalog, BufMgr *bufMgr)
{
    int numResultTuples = 0;
    SlotId nowSlotNumber = page->begin().getNextUsedSlot(0);
    PageId nowPageNumber = page->page_number();
    int size = table.getAttrCount();
    string atr[size];
    size_t tuple_index = 0;
    for (int i = 0; i < size; i++)
        if (index_join == table.getAttrName(i))
            tuple_index = i;
    while (nowSlotNumber)
    {
        RecordId nowRecordID = {nowPageNumber, nowSlotNumber};
        nowSlotNumber = page->begin().getNextUsedSlot(nowSlotNumber);
        string record = page->getRecord(nowRecordID) + ' ';
        string tmp;
        stringstream atr_value;
        int index = 0;
        for (size_t i = 0; i < record.size(); i++)
        {
            if (record[i] == ' ')
            {
                atr[index++] = tmp;
                if (i != tuple_index)
                    atr_value << tmp << ' ';
                tmp.clear();
            }
            else
                tmp += record[i];
        }
        tmp = atr[tuple_index];
        if (finding.count(tmp) != 0)
        {
            auto it = finding.find((tmp));
            for (size_t i = 0; i < finding.count(tmp); i++, it++)
            {
                string s1 = it->second;
                string s2 = atr[1 - tuple_index];
                string tuple = s1 + ' ' + s2;
                HeapFileManager::insertTuple(tuple, file, bufMgr);
                numResultTuples++;
            }
        }
    }
    bufMgr->unPinPage(&file, nowPageNumber, true);
    bufMgr->flushFile(&file);
    return numResultTuples;
}

void JoinOperator::printRunningStats() const
{
    cout << "# Result Tuples: " << numResultTuples << endl;
    cout << "# Used Buffer Pages: " << numUsedBufPages << endl;
    cout << "# I/Os: " << numIOs << endl;
}

bool OnePassJoinOperator::execute(int numAvailableBufPages, File &resultFile)
{
    catalog->addTableSchema(resultTableSchema, resultTableSchema.getTableName());
    TableSchema &restable = resultTableSchema;
    File lfile = leftTableFile;
    File rfile = rightTableFile;
    TableSchema rtable = rightTableSchema;
    TableSchema ltable = leftTableSchema;
    if(!check(lfile, rfile))
    {
        rfile = leftTableFile;
        lfile = rightTableFile;
        rtable = leftTableSchema;
        ltable = rightTableSchema;
    }
    numResultTuples = 0;
    numUsedBufPages = 0;
    numIOs = 0;
    finding.clear();
    for (FileIterator it = lfile.begin(); it != lfile.end(); it++)
    {
        Page *nowPage;
        bufMgr->readPage(&lfile, (*it).page_number(), nowPage);
        numIOs++;
        build(nowPage, bufMgr, ltable);
        numUsedBufPages++;
        bufMgr->unPinPage(&lfile, (*it).page_number(), false);
        bufMgr->flushFile(&lfile);
    }
    for (FileIterator it = rfile.begin(); it != rfile.end(); it++)
    {
        Page *nowPage;
        bufMgr->readPage(&rfile, (*it).page_number(), nowPage);
        numIOs++;
        numResultTuples += join(resultFile, nowPage, restable, rtable, catalog, bufMgr);
        bufMgr->unPinPage(&rfile, (*it).page_number(), false);
        bufMgr->flushFile(&rfile);
    }
    numUsedBufPages++;
    isComplete = true;
    return true;
}

bool NestedLoopJoinOperator::execute(int numAvailableBufPages, File &resultFile)
{
    catalog->addTableSchema(resultTableSchema, resultTableSchema.getTableName());
    TableSchema restable = resultTableSchema;
    File rfile = rightTableFile;
    File lfile = leftTableFile;
    TableSchema rtable = rightTableSchema;
    TableSchema ltable = leftTableSchema;
    if(!check(lfile, rfile))
    {
        rfile = leftTableFile;
        lfile = rightTableFile;
        rtable = leftTableSchema;
        ltable = rightTableSchema;
    }
    numResultTuples = 0;
    numUsedBufPages = 0;
    numIOs = 0;
    int size = numAvailableBufPages - 1;
    finding.clear();
    FileIterator it = lfile.begin();
    while (it != lfile.end())
    {
        for (int i = 0; i < size && it != lfile.end(); it++)
        {
            Page *nowPage;
            bufMgr->readPage(&lfile, (*it).page_number(), nowPage);
            numUsedBufPages++;
            build(nowPage, bufMgr, ltable);
            bufMgr->unPinPage(&lfile, (*it).page_number(), false);
            bufMgr->flushFile(&lfile);
        }

        for (FileIterator rit = rfile.begin(); rit != rfile.end(); rit++)
        {
            Page *nowPage;
            bufMgr->readPage(&rfile, (*rit).page_number(), nowPage);
            numResultTuples += join(resultFile, nowPage, restable, rtable, catalog, bufMgr);
            bufMgr->unPinPage(&rfile, (*rit).page_number(), false);
            bufMgr->flushFile(&rfile);
        }
        finding.clear();
    }
    numUsedBufPages++;
    int left_count = 0, right_count = 0;
    for (it = lfile.begin(); it != lfile.end(); it++)
        left_count++;
    for (it = rfile.begin(); it != rfile.end(); it++)
        right_count++;
    numIOs = left_count + (left_count * right_count) / (numAvailableBufPages - 1);
    isComplete = true;
    return true;
}

BucketId GraceHashJoinOperator::hash(const string &key) const
{
    std::hash<string> strHash;
    return strHash(key) % numBuckets;
}

bool GraceHashJoinOperator ::execute(int numAvailableBufPages, File &resultFile)
{
    if (isComplete)
        return true;

    numResultTuples = 0;
    numUsedBufPages = 0;
    numIOs = 0;

    // TODO: Execute the join algorithm

    isComplete = true;
    return true;
}

} // namespace badgerdb