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

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

	for (FrameId i = 0; i < bufs; i++)
	{
		bufDescTable[i].frameNo = i;
		bufDescTable[i].valid = false;
	}

	bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
	hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

	clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
	/*
	 * deallocates bufDescTable, hashTable and the bufPool, which is
	 * everything that is created in the constructor (keyword new)
	 *
	 * */
	for(FrameId i = 0; i < numBufs; i++){
		while(bufDescTable[i].valid){
			if(bufDescTable[i].dirty){
				 //flushes out dirty bit
				bufDescTable[i].file -> writePage(bufPool[i]);
				bufDescTable[i].dirty = false;
			}
			else { //enters else only if bufPool[i] is true. then deallocates bufpool

			}
		}
	}
	delete bufDescTable;
	delete bufPool;
	delete hashTable;
}

void BufMgr::advanceClock()
{
	if(clockHand == (numBufs - 1)){
		clockHand = 0;
	}
	else{
		clockHand = clockHand + 1;
	}
}

void BufMgr::allocBuf(FrameId & frame) 
{ 
	for(FrameId i = 0; i < numBufs; i++){
		int pinC = 0;
		if (bufDescTable[i].refbit){ //
			bufDescTable[i].refbit = false;
			advanceClock();
		}
		else{
			if(bufDescTable[i].dirty){
				// write to page to disk, then select for replacement
				hashTable.remove(bufDescTable.file, bufDescTable.pageNo) // removes page from frame
			  hashTable.insert(bufDescTable.file, // ????
			}
			else if(bufDescTable[i].pinCnt > 0){ // if page is pinned (should value be 0?) *****
				pinC = pinC + 1; // increment pinned count.
			}
			else{
				// this frame is selected for replacement
			}
		}
	}
	if(pinC = numBufs){
		throw BufferExceededException():
	}



}


void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
}

void BufMgr::flushFile(const File* file) 
{

	void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
	{
	}

	void BufMgr::disposePage(File* file, const PageId PageNo)
	{

	}

	void BufMgr::printSelf(void)
	{
		BufDesc* tmpbuf;
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

}
