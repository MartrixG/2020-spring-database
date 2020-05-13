/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#pragma once

#include "types.h"

#include <cstddef>
#include <stdint.h>
#include <memory>
#include <string>

namespace badgerdb
{

/**
 * @brief Header metadata in a page.
 * 页面的信息数据
 *
 * Header metadata in each page which tracks where space has been used and
 * contains a pointer to the next page in the file.
 */
struct PageHeader
{
    /**
   * Lower bound of the free space.  This is the offset of the first unused byte
   * after the slot array.
   * 可用空间的下限。 这是slot阵列之后的第一个未使用字节的偏移量。
   */
    std::uint16_t free_space_lower_bound;

    /**
   * Upper bound of the free space.  This is the offset of the last unused byte
   * before the first data record.
   * 可用空间的上限。 这是第一个数据记录之前最后一个未使用字节的偏移量。
   */
    std::uint16_t free_space_upper_bound;

    /**
   * Number of slots currently allocated.  This number may include slots which
   * are unused but are in the middle of the slot array (due to record
   * deletions).
   * 当前分配的slot数。 该数字可能包括未使用但位于slot阵列中间的slot（由于记录删除）。
   */
    SlotId num_slots;

    /**
   * Number of slots allocated but not in use.
   * 已分配但未使用的slot数。
   */
    SlotId num_free_slots;

    /**
   * Number of the page within the file.
   * 文件内页面的编号。
   */
    PageId current_page_number;

    /**
   * Number of the next used page in the file.
   * 文件中下一个使用的页面的编号。
   */
    PageId next_page_number;

    /**
   * Returns true if this page header is equal to the other.
   *
   * @param rhs   Other page header to compare against.
   * @return  True if the other header is equal to this one.
   */
    bool operator==(const PageHeader &rhs) const
    {
        return num_slots == rhs.num_slots &&
               num_free_slots == rhs.num_free_slots &&
               current_page_number == rhs.current_page_number &&
               next_page_number == rhs.next_page_number;
    }
};

/**
 * @brief Slot metadata that tracks where a record is in the data space.
 * slot元数据，用于跟踪记录在数据空间中的位置
 */
struct PageSlot
{
    /**
   * Whether the slot currently holds data.  May be false if this slot's
   * record has been deleted after insertion.
   * slot当前是否保存数据。 如果插入后已删除此slot的记录，则可能为false。
   */
    bool used;

    /**
   * Offset of the data item in the page.
   * 数据的偏移量
   */
    std::uint16_t item_offset;

    /**
   * Length of the data item in this slot.
   * 数据的长短
   */
    std::uint16_t item_length;
};

class PageIterator;

/**
 * @brief Class which represents a fixed-size database page containing records.
 *
 * A page is a fixed-size unit of data storage.  Each page holds zero or more
 * records, which consist of arbitrary binary data.  Records are placed into
 * slots and identified by a RecordId.  Although a record's actual contents may
 * be moved on the page, accessing a record by its slot is consistent.
 *
 * @warning This class is not threadsafe.
 */
class Page
{
public:
    /**
   * Page size in bytes.  If this is changed, database files created with a
   * different page size value will be unreadable by the resulting binaries.
   * 页面的大小默认8KiB
   */
    static const std::size_t SIZE = 8192;

    /**
   * Size of page free space area in bytes.
   * 页面剩余大小
   */
    static const std::size_t DATA_SIZE = SIZE - sizeof(PageHeader);

    /**
   * Number of page indicating that it's invalid.
   * 指示无效的页面数。
   */
    static const PageId INVALID_NUMBER = 0;

    /**
   * Number of slot indicating that it's invalid.
   * 无效的slot编号
   */
    static const SlotId INVALID_SLOT = 0;

    /**
   * Constructs a new, uninitialized page.
   */
    Page();

    /**
   * Inserts a new record into the page.
   * 向page中插入新的记录
   *
   * @param record_data  Bytes that compose the record.
   * @return  ID of the newly inserted record.
   */
    RecordId insertRecord(const std::string &record_data);

    /**
   * Returns the record with the given ID.  Returned data is a copy of what is
   * stored on the page; use updateRecord to change it.
   * 获取提供的记录ID的内容，返回的是一个复制。如果需要改变使用updateRecord方法
   *
   * @see updateRecord
   * @param record_id  ID of the record to return.
   * @return  The record.
   */
    std::string getRecord(const RecordId &record_id) const;

    /**
   * Updates the record with the given ID, replacing its data with a new
   * version.  This is equivalent to deleting the old record and inserting a
   * new one, with the exception that the record ID will not change.
   * 更新记录的内容，使用新的版本替代实际数据。这等价于删除一个记录然后再插入一个新的记录。使用了相同的异常。
   *
   * @param record_id   ID of record to update.
   * @param record_data Updated bytes that compose the record.
   */
    void updateRecord(const RecordId &record_id, const std::string &record_data);

    /**
   * Deletes the record with the given ID.  Page is compacted upon delete to
   * ensure that data of all records is contiguous.  Slot array is compacted if
   * the slot deleted is at the end of the slot array.
   * 删除给定ID的数据，页面会自动压缩空间，确保所有的数据都是连续的。如果删除的slot在末尾，则会压缩slot序列
   *
   * @param record_id   ID of the record to delete.
   */
    void deleteRecord(const RecordId &record_id);

    /**
   * Returns true if the page has enough free space to hold the given data.
   * 如果页面有足够的可用空间来保存给定的数据，则返回true。
   *
   * @param record_data Bytes that compose the record.
   * @return  Whether the page can hold the data.
   */
    bool hasSpaceForRecord(const std::string &record_data) const;

    /**
   * Returns this page's free space in bytes.
   * 返回此页面的可用空间（以字节为单位）。
   *
   * @return  Free space in bytes.
   */
    std::uint16_t getFreeSpace() const { return header_.free_space_upper_bound -
                                                header_.free_space_lower_bound; }

    /**
   * Returns this page's number in its file.
   * 返回当前页面的编号
   *
   * @return  Page number.
   */
    PageId page_number() const { return header_.current_page_number; }

    /**
   * Returns the number of the next used page this page in its file.
   * 双链表保存页面，获取下一个页面的指针
   *
   * @return  Page number of next used page in file.
   */
    PageId next_page_number() const { return header_.next_page_number; }

    /**
   * Returns an iterator at the first record in the page.
   * 
   * @return  Iterator at first record of page.
   */
    PageIterator begin();

    /**
   * Returns an iterator representing the record after the last record in the
   * page.  This iterator should not be dereferenced.
   *
   * @return  Iterator representing record after the last record in the page.
   */
    PageIterator end();

private:
    /**
   * Initializes this page as a new page with no header information or data.
   */
    void initialize();

    /**
   * Sets this page's number in its file.
   *
   * @param page_number   Number of page in file.
   */
    void set_page_number(const PageId new_page_number)
    {
        header_.current_page_number = new_page_number;
    }

    /**
   * Sets the number of the next used page after this page in its file.
   *
   * @param next_page_number  Page number of next used page in file.
   */
    void set_next_page_number(const PageId new_next_page_number)
    {
        header_.next_page_number = new_next_page_number;
    }

    /**
   * Deletes the record with the given ID.  Page is compacted upon delete to
   * ensure that data of all records is contiguous.  Slot array is compacted if
   * the slot deleted is at the end of the slot array and
   * <allow_slot_compaction> is set.
   *
   * @param record_id             ID of the record to delete.
   * @param allow_slot_compaction If true, the slot array will be compacted if
   *                              possible.
   */
    void deleteRecord(const RecordId &record_id,
                      const bool allow_slot_compaction);

    /**
   * Returns the slot with the given number.  This method will return
   * unallocated slots if requested; it is up to the caller to ensure they
   * have a valid slot number.
   * 返回给定编号的slot。 此方法有可能返回未分配的slot，调用者要确保他们具有有效的slot编号。
   *
   * @param slot_number   Number of slot to retrieve.
   * @return  Pointer to the slot.//返回slot的指针
   */
    PageSlot *getSlot(const SlotId slot_number);

    /**
   * Returns the slot with the given number.  This method will return
   * unallocated slots if requested; it is up to the caller to ensure they
   * have a valid slot number.
   * 
   * @param slot_number   Number of slot to retrieve.
   * @return  The slot.//返回slot本身
   */
    const PageSlot &getSlot(const SlotId slot_number) const;

    /**
   * Returns the slot number of an available slot.  If no slots are available
   * to be reused, allocates a new slot.  Updates available slot count in the
   * header metadata, but does not mark returned slot as used.  If a new slot is
   * allocated, updates the free space lower bound.
   *
   * Callers are responsible for making sure there is enough space to allocate a
   * new slot before calling this method.
   *
   * Since the returned slot is not marked as used, callers must take care to
   * fill the slot or mark it used before someone else calls this method.
   *
   * @return  Slot number of an unused slot.
   */
    SlotId getAvailableSlot();

    /**
   * Inserts record data into the given slot.  The slot should not be currently
   * in use.  <slot_number> must be less than <header_.num_slots>.
   * 将记录数据插入给定的slot中，slot编号不应是已经使用的slot编号，并且slot编号必须小于总slot的数目
   *
   * Callers are responsible for making sure there is enough space to hold the
   * record before calling this method.
   * 调用者在调用此方法之前必须确保有足够的空间
   *
   * @param slot_number   Number of slot to insert record into.
   * @param record_data   Bytes that compose the record.
   * @throws  InvalidSlotException  Thrown when given slot number refers to an
   *                                unallocated slot.
   * @throws  SlotInUseException  Thrown when given slot is in use.
   */
    void insertRecordInSlot(const SlotId slot_number,
                            const std::string &record_data);

    /**
   * Throws an exception if the given record ID is not valid for this page
   * 如果给定的记录ID在此页面不可用抛出异常
   * (i.e., it has the right page number and the slot it references is in use).
   *
   * @param record_id   Record ID to validate.
   * @throws  InvalidRecordException  Thrown if the ID has a bad page or slot
   *                                  number.
   */
    void validateRecordId(const RecordId &record_id) const;

    /**
   * Returns whether the page is in use or is a free page.
   *
   * @return  True if page is in use; false if page is free.
   */
    bool isUsed() const { return page_number() != INVALID_NUMBER; }

    /**
   * Header metadata.
   */
    PageHeader header_;

    /**
   * Data stored on the page.  Includes bookkeeping information about slots as
   * well as actual content.
   * 储存在page上的数据，包括关于slot的记录信息，以及实际内容
   */

    std::string data_;

    friend class File;
    friend class PageIterator;
    friend class PageTest;
    friend class BufferTest;
};

static_assert(Page::SIZE > sizeof(PageHeader),
              "Page size must be large enough to hold header and data.");
static_assert(Page::DATA_SIZE > 0,
              "Page must have some space to hold data.");

} // namespace badgerdb
