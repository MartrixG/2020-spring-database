/**
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
using namespace std;
/**
	* @brief 类的构造函数
	* @param bufs  缓冲池大小
	*/
BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs)
{
	bufDescTable = new BufDesc[bufs];

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
	delete hashTable;
	delete[] bufPool;
	delete[] bufDescTable;
}
void BufMgr::advanceClock()
{
	clockHand++;
	clockHand %= numBufs;
}

void BufMgr::allocBuf(FrameId &frame)
{
	unsigned pinned = 0;
	while (1)
	{
		advanceClock();
		BufDesc *tmpDesc = &bufDescTable[clockHand];
		if (!tmpDesc->valid)
		{
			frame = clockHand;
			return;
		}
		if (tmpDesc->refbit)
		{
			tmpDesc->refbit = false;
			continue;
		}
		if (tmpDesc->pinCnt)
		{
			pinned++;
			if (pinned == numBufs)
			{
				throw BufferExceededException();
			}
			else
				continue;
		}
		if (tmpDesc->dirty)
		{
			tmpDesc->file->writePage(bufPool[clockHand]);
			tmpDesc->dirty = false;
		}
		frame = clockHand;
		if (tmpDesc->valid)
		{
			try
			{
				hashTable->remove(tmpDesc->file, tmpDesc->pageNo);
			}
			catch (HashNotFoundException &)
			{
			}
		}
		break;
	}
}

void BufMgr::readPage(File *file, const PageId pageNo, Page *&page)
{
	FrameId frame;
	try
	{
		hashTable->lookup(file, pageNo, frame);
		bufDescTable[frame].refbit = true;
		bufDescTable[frame].pinCnt++;
		page = &bufPool[frame];
	}
	catch (HashNotFoundException &)
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
	try
	{
		hashTable->lookup(file, pageNo, frame);
	}
	catch (HashNotFoundException &)
	{
		cerr << "Warning: unpinning a nonexistent page" << endl;
		return;
	}
	if (bufDescTable[frame].pinCnt > 0)
	{
		bufDescTable[frame].pinCnt--;
		if (dirty)
			bufDescTable[frame].dirty = true;
	}
	else
	{
		throw PageNotPinnedException(bufDescTable[frame].file->filename(), bufDescTable[frame].pageNo, frame);
	}
}

void BufMgr::flushFile(const File *file)
{
	for (FrameId i = 0; i < numBufs; i++)
	{
		BufDesc *tmpDesc = &bufDescTable[i];
		if (tmpDesc->file == file)
		{
			if (!tmpDesc->valid)
			{
				throw BadBufferException(i, tmpDesc->dirty, tmpDesc->valid, tmpDesc->refbit);
			}
			if (tmpDesc->pinCnt > 0)
			{
				throw PagePinnedException(file->filename(), tmpDesc->pageNo, i);
			}
			if (tmpDesc->dirty)
			{
				tmpDesc->file->writePage(bufPool[i]);
				tmpDesc->dirty = false;
			}
			hashTable->remove(file, tmpDesc->pageNo);
			tmpDesc->Clear();
		}
	}
}

void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page)
{
	FrameId frame;
	Page p = file->allocatePage();
	allocBuf(frame);
	bufPool[frame] = p;
	pageNo = p.page_number();
	hashTable->insert(file, pageNo, frame);
	bufDescTable[frame].Set(file, pageNo);
	page = &bufPool[frame];
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
	catch (HashNotFoundException &)
	{
	}
	file->deletePage(PageNo);
}

void BufMgr::printSelf(void)
{
	BufDesc *tmpbuf;
	int validFrames = 0;

	for (unsigned i = 0; i < numBufs; i++)
	{
		tmpbuf = &(bufDescTable[i]);
		cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

		if (tmpbuf->valid == true)
			validFrames++;
	}

	cout << "Total Number of Valid Frames:" << validFrames << endl;
}

} // namespace badgerdb
