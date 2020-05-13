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
	unsigned count = 0;
	for (unsigned j = 0; j < numBufs; j++)
		if (bufDescTable[j].pinCnt != 0)
			count++;
	if (count == numBufs)
		throw BufferExceededException();
	while (1)
	{
		advanceClock();
		BufDesc* nowDesc =  &bufDescTable[clockHand];
		if (nowDesc->valid == false)
		{
			frame = clockHand;
			break;
		}
		if (nowDesc->refbit == false)
		{
			if (nowDesc->pinCnt == 0)
			{
				if (nowDesc->dirty == true)
				{
					nowDesc->file->writePage(bufPool[clockHand]);
					nowDesc->dirty = false;
				}
				frame = clockHand;
				try
				{
					hashTable->remove(nowDesc->file, nowDesc->pageNo);
				}
				catch (HashNotFoundException &)
				{
				}
				break;
			}
		}
		else nowDesc->refbit = false;
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
	}
	catch (HashNotFoundException &)
	{
		allocBuf(frame);
		bufPool[frame] = file->readPage(pageNo);
		hashTable->insert(file, pageNo, frame);
		bufDescTable[frame].Set(file, pageNo);
	}
	page = &bufPool[frame];
}

void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty)
{
	FrameId frame;
	try
	{
		hashTable->lookup(file, pageNo, frame);
	}
	catch (HashNotFoundException &)
	{
		printf("the page is not in bufpool\n");
		return;
	}
	BufDesc *nowDesc = &bufDescTable[frame];
	if (nowDesc->pinCnt)
	{
		nowDesc->pinCnt--;
		nowDesc->dirty |= dirty;
	}
}

void BufMgr::flushFile(const File *file)
{
	for (FrameId i = 0; i < numBufs; i++)
	{
		BufDesc *nowDesc = &bufDescTable[i];
		if (nowDesc->file == file)
		{
			if (!nowDesc->valid)
			{
				throw BadBufferException(i, nowDesc->dirty, nowDesc->valid, nowDesc->refbit);
			}
			if (nowDesc->pinCnt)
			{
				throw PagePinnedException(file->filename(), nowDesc->pageNo, i);
			}
			if (nowDesc->dirty)
			{
				nowDesc->file->writePage(bufPool[i]);
				nowDesc->dirty = false;
			}
			hashTable->remove(file, nowDesc->pageNo);
			nowDesc->Clear();
		}
	}
}

void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page)
{
	Page newpage = file->allocatePage();
	FrameId frame;
	allocBuf(frame);
	bufPool[frame] = newpage;
	pageNo = newpage.page_number();
	hashTable->insert(file, pageNo, frame);
	bufDescTable[frame].Set(file, pageNo);
	page = bufPool + frame;
}

void BufMgr::disposePage(File *file, const PageId PageNo)
{
	FrameId frame;
	try
	{
		hashTable->lookup(file, PageNo, frame);
		hashTable->remove(file, PageNo);
		bufDescTable[frame].Clear();
	}
	catch (const std::exception&)
	{
		printf("the page is not in bufpool\n");
	}
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
