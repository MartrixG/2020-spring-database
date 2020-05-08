/**
 * 
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb
{

BufMgr::BufMgr(std::uint32_t bufs) : numBufs(bufs)
{
    bufDescTable = new BufDesc[bufs]; //建立起bufs大小的缓冲池

    for (FrameId i = 0; i < bufs; i++)
    {
        bufDescTable[i].frameNo = i;
        bufDescTable[i].valid = false;
    }

    bufPool = new Page[bufs];

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

    clockHand = bufs - 1;
}

BufMgr::~BufMgr()
{
    for (FrameId i = 0; i < numBufs; i++)
    {
        if (bufDescTable[i].dirty == true)
        {
            flushFile(bufDescTable[i].file);
        }
    }
    delete[] bufDescTable;
    delete[] bufPool;
    delete hashTable;
}

void BufMgr::advanceClock()
{
    clockHand++;
    clockHand %= numBufs;
}

void BufMgr::allocBuf(FrameId &frame)
{
    uint32_t pinCount = 0;
    while (1)
    {
        advanceClock();
        BufDesc *nowBufDesc = &bufDescTable[clockHand];
        if (pinCount >= numBufs)
        {
            throw BufferExceededException();
        }
        if (nowBufDesc->valid == false)
        {
            nowBufDesc->Clear();
            frame = nowBufDesc->frameNo;
            break;
        }
        else
        {
            if (nowBufDesc->refbit)
                nowBufDesc->refbit = false;
            else
            {
                if (nowBufDesc->pinCnt == 0)
                {
                    hashTable->remove(nowBufDesc->file, nowBufDesc->pageNo);
                    if (nowBufDesc->dirty)
                        nowBufDesc->file->writePage(bufPool[clockHand]);
                    nowBufDesc->Clear();
                    frame = nowBufDesc->frameNo;
                    break;
                }
                else
                    pinCount++;
            }
        }
    }
}

void BufMgr::readPage(File *file, const PageId pageNo, Page *&page)
{
    FrameId frame;
    try
    {
        hashTable->lookup(file, pageNo, frame);
        bufDescTable[frame].pinCnt++;
        bufDescTable[frame].refbit = true;
        page = &bufPool[frame];
    }
    catch (HashNotFoundException e)
    {
        allocBuf(frame);
        bufPool[frame] = file->readPage(pageNo);
        hashTable->insert(file, pageNo, frame);
        bufDescTable[frame].Set(file, pageNo);
        page = &bufPool[frame];
    }
}

void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty)
{
    FrameId frame;
    hashTable->lookup(file, pageNo, frame);
    BufDesc *unBufDesc = &bufDescTable[frame];
    if (unBufDesc->pinCnt == 0)
    {
        throw PageNotPinnedException("Pincount is 0, can not undo.", pageNo, frame);
    }
    unBufDesc->dirty = dirty;
    unBufDesc->pinCnt--;
}

void BufMgr::flushFile(const File *file)
{
    for (uint32_t i = 0; i < numBufs; i++)
    {
        BufDesc *nowBufDesc = &bufDescTable[i];
        if (nowBufDesc->valid && nowBufDesc->file == file)
        {
            if (nowBufDesc->pinCnt > 0)
            {
                throw PagePinnedException("Page is pinned", nowBufDesc->pageNo, nowBufDesc->frameNo);
            }
            if (nowBufDesc->dirty)
            {
                nowBufDesc->file->writePage(bufPool[nowBufDesc->frameNo]);
                nowBufDesc->dirty = false;
            }
            hashTable->remove(file, nowBufDesc->pageNo);
            nowBufDesc->Clear();
        }
        else
        {
            throw BadBufferException(nowBufDesc->frameNo, nowBufDesc->dirty, nowBufDesc->valid, nowBufDesc->refbit);
        }
    }
}

void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page)
{
    FrameId frame;
    allocBuf(frame);
    bufPool[frame] = file->allocatePage();
    hashTable->insert(file, bufPool[frame].page_number(), frame);
    bufDescTable[frame].Set(file, bufPool[frame].page_number());
    pageNo = bufPool[frame].page_number();
    page = &bufPool[frame];
}

void BufMgr::disposePage(File *file, const PageId PageNo)
{
    FrameId frame;
    hashTable->lookup(file, PageNo, frame);
    bufDescTable[frame].Clear();
    hashTable->remove(file, PageNo);
    file->deletePage(PageNo);
}

void BufMgr::printSelf(void)
{
    BufDesc *tmpbuf;
    int validFrames = 0;

    for (std::uint32_t i = 0; i < numBufs; i++)
    {
        tmpbuf = &(bufDescTable[i]);
        std::cout << "FrameNo:" << i << " ";
        tmpbuf->Print();

        if (tmpbuf->valid == true)
            validFrames++;
    }

    std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

} // namespace badgerdb
