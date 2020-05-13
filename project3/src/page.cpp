/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <cassert>

#include "exceptions/insufficient_space_exception.h"
#include "exceptions/invalid_record_exception.h"
#include "exceptions/invalid_slot_exception.h"
#include "exceptions/slot_in_use_exception.h"
#include "page_iterator.h"
#include "page.h"

namespace badgerdb
{

Page::Page() { initialize(); }

void Page::initialize()
{
	header_.free_space_lower_bound = 0;			  //可用空间的下限。 这是slot阵列之后的第一个未使用字节的偏移量。初始化为0
	header_.free_space_upper_bound = DATA_SIZE;	  //可用空间的上限。 这是第一个数据记录之前最后一个未使用字节的偏移量。8192字节
	header_.num_slots = 0;						  //当前分配的slot数。 该数字可能包括未使用但位于slot阵列中间的slot（由于记录删除）。初始化为0
	header_.num_free_slots = 0;					  //剩余slots个数，初始化为0
	header_.current_page_number = INVALID_NUMBER; //文件内页面的编号。初始化为无效的页面数0
	header_.next_page_number = INVALID_NUMBER;	  //文件中下一个使用的页面的编号。初始化为无效的页面数0
	data_.assign(DATA_SIZE, char());
}

RecordId Page::insertRecord(const std::string &record_data)
{
	if (!hasSpaceForRecord(record_data))
	{
		throw InsufficientSpaceException(
			page_number(), record_data.length(), getFreeSpace());
	}
	const SlotId slot_number = getAvailableSlot(); //获取可以实际使用的slot编号
	insertRecordInSlot(slot_number, record_data);
	return {page_number(), slot_number};
}

std::string Page::getRecord(const RecordId &record_id) const
{
	validateRecordId(record_id); //确保记录ID可用
	const PageSlot &slot = getSlot(record_id.slot_number);
	return data_.substr(slot.item_offset, slot.item_length); //获取内容
}

void Page::updateRecord(const RecordId &record_id, const std::string &record_data)
{
	validateRecordId(record_id);
	const PageSlot *slot = getSlot(record_id.slot_number);							//获取slot位置
	const std::size_t free_space_after_delete = getFreeSpace() + slot->item_length; //删除后的剩余空间
	if (record_data.length() > free_space_after_delete)								//插入内容大于删除后的空间，抛出异常
	{
		throw InsufficientSpaceException(
			page_number(), record_data.length(), free_space_after_delete);
	}
	// We have to disallow slot compaction here because we're going to place the
	// record data in the same slot, and compaction might delete the slot if we
	// permit it.
	deleteRecord(record_id, false /* allow_slot_compaction */);
	insertRecordInSlot(record_id.slot_number, record_data);
}

void Page::deleteRecord(const RecordId &record_id)
{
	deleteRecord(record_id, true /* allow_slot_compaction */);
}

void Page::deleteRecord(const RecordId &record_id, const bool allow_slot_compaction)
{
	validateRecordId(record_id);
	PageSlot *slot = getSlot(record_id.slot_number);
	data_.replace(slot->item_offset, slot->item_length, slot->item_length, '\0'); //使用'\0'替换所有的数据

	// Compact the data by removing the hole left by this record (if necessary).
	std::uint16_t move_offset = slot->item_offset; //move_offset是需要移动的字节的最小值（自删除位置开始，向下最小的偏置的位置）
	std::size_t move_bytes = 0;
	for (SlotId i = 1; i <= header_.num_slots; ++i)
	{
		PageSlot *other_slot = getSlot(i);
		if (other_slot->used && other_slot->item_offset < slot->item_offset) //如果当前slot是使用的并且在删除数据以左
		{
			if (other_slot->item_offset < move_offset) //更新move_slot最小值
			{
				move_offset = other_slot->item_offset;
			}
			move_bytes += other_slot->item_length; //更新move_bytes总数
			// Update the slot for the other data to reflect the soon-to-be-new
			// location.
			other_slot->item_offset += slot->item_length; //当前slot的新的偏置起始位置
		}
	}
	// If we have data to move, shift it to the right.
	// 如果需要移动，向右移动
	if (move_bytes > 0)
	{
		const std::string &data_to_move = data_.substr(move_offset, move_bytes);
		data_.replace(move_offset + slot->item_length, move_bytes, data_to_move);
	}
	header_.free_space_upper_bound += slot->item_length; //更新空闲空间的上限

	// Mark slot as unused.
	// 标记删除的slot是未使用
	slot->used = false;
	slot->item_offset = 0;
	slot->item_length = 0;
	++header_.num_free_slots; //增加空闲slot数目

	if (allow_slot_compaction && record_id.slot_number == header_.num_slots)
	{
		// Last slot in the list, so we need to free any unused slots that are at
		// the end of the slot list.
		// 删除了最后一个slot，所以需要释放所有未使用的slot的空间
		int num_slots_to_delete = 1;
		for (SlotId i = 1; i < header_.num_slots; ++i)
		{
			// Traverse list backwards, looking for unused slots.
			// 从尾部倒序查看，遇到不是未使用就跳出
			const PageSlot *other_slot = getSlot(header_.num_slots - i);
			if (!other_slot->used)
			{
				++num_slots_to_delete;
			}
			else
			{
				// Stop at the first used slot we find, since we can't move used slots
				// without affecting record IDs.
				// 遇到第一个使用的slot就应该停止，因为无法移动slot并且保证不改变slot编号
				break;
			}
		}
		// 删除所有连续的未使用的slot
		header_.num_slots -= num_slots_to_delete;
		header_.num_free_slots -= num_slots_to_delete;
		header_.free_space_lower_bound -= sizeof(PageSlot) * num_slots_to_delete;
	}
}

bool Page::hasSpaceForRecord(const std::string &record_data) const
{
	std::size_t record_size = record_data.length(); //记录data的长度
	if (header_.num_free_slots == 0)				//如果已分配但是没有使用的slot数目是0，应该申请新的slot，此时的数据的大小应该包括添加的slot头的大小
	{
		record_size += sizeof(PageSlot);
	}
	return record_size <= getFreeSpace();
}

PageSlot *Page::getSlot(const SlotId slot_number) //返回了指针，由于是值的返回所以不需要const
{
	return reinterpret_cast<PageSlot *>(
		&data_[(slot_number - 1) * sizeof(PageSlot)]);
}

const PageSlot &Page::getSlot(const SlotId slot_number) const //return 后的*取了指针的内容，并且返回了引用，所以用const修饰
{

	return *reinterpret_cast<const PageSlot *>(&data_[(slot_number - 1) * sizeof(PageSlot)]);
}

SlotId Page::getAvailableSlot()
{
	SlotId slot_number = INVALID_SLOT;
	if (header_.num_free_slots > 0) //存在已经分配但是由于删除未使用的slot
	{
		// Have an allocated but unused slot that we can reuse.
		// 寻找已经申请，但是没有使用的slot
		for (SlotId i = 1; i <= header_.num_slots; ++i)
		{
			const PageSlot *slot = getSlot(i);
			if (!slot->used)
			{
				// We don't decrement the number of free slots until someone actually
				// puts data in the slot.
				// 在将实际数据放入slot之前，不会减少可用slot的数量。
				slot_number = i;
				break;
			}
		}
	}
	else
	{
		// Have to allocate a new slot.
		// 必须申请新的slot
		slot_number = header_.num_slots + 1;
		++header_.num_slots;
		++header_.num_free_slots;											   //将实际数据放入之前，不减少可使用slot数量
		header_.free_space_lower_bound = sizeof(PageSlot) * header_.num_slots; //修正空闲空间的大小
	}
	assert(slot_number != INVALID_SLOT);
	return static_cast<SlotId>(slot_number);
}

void Page::insertRecordInSlot(const SlotId slot_number, const std::string &record_data)
{
	if (slot_number > header_.num_slots || slot_number == INVALID_SLOT)
	{
		throw InvalidSlotException(page_number(), slot_number);
	}
	PageSlot *slot = getSlot(slot_number);
	if (slot->used)
	{
		throw SlotInUseException(page_number(), slot_number);
	}
	const int record_length = record_data.length();
	slot->used = true;
	slot->item_length = record_length;
	slot->item_offset = header_.free_space_upper_bound - record_length; //使用upper_bound确定偏置的位置
	header_.free_space_upper_bound = slot->item_offset;					//更新upper_bound的数值
	--header_.num_free_slots;											//实际把数据储存到page上以后再减少可用slot数目
	data_.replace(slot->item_offset, slot->item_length, record_data);	//更新数据
}

void Page::validateRecordId(const RecordId &record_id) const
{
	if (record_id.page_number != page_number())
	{
		throw InvalidRecordException(record_id, page_number());
	}
	const PageSlot &slot = getSlot(record_id.slot_number);
	if (!slot.used)
	{
		throw InvalidRecordException(record_id, page_number());
	}
}

PageIterator Page::begin()
{
	return PageIterator(this);
}

PageIterator Page::end()
{
	const RecordId &end_record_id = {page_number(), Page::INVALID_SLOT};
	return PageIterator(this, end_record_id);
}

} // namespace badgerdb
