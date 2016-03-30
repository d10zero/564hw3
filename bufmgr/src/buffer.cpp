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
	/*deallocates BufDesc table and the buffer pool */
	for(FrameId i = 0; i < numBufs; i++){
		bufDesc* tmpBufDesc = bufDescTable[i];
		bufPool* tmpBufPool = bufpool[i];
		while(bufDescTable[i].valid || bufPool[i]){
			if(bufDescTable[i].valid){
				bufDescTable[i].dirty = false; //flushes out dirty bit
				tmpBufDesc = bufDescTable[i];
				bufDescrtable[i] = bufDescTable[i]->next;
				delete tmpBufDesc;
			}
			else { //enters else only if bufPool[i] is true. then deallocates bufpool
				tmpBufPool = bufpool[i];
				bufPool[i] = bufpool[i]->next;
				delete tmpbufPool;
			}
		}
	}
	delete bufDescTable;
	delete bufpool;
}

void BufMgr::advanceClock()
{
	if(clockHand = numBufs - 1){
		clockHand = 0;
	}
	else{
		clockHand = clockHand + 1;
	}
}

void BufMgr::allocBuf(FrameId & frame) 
{ 
	int pinC = 0; // counts number of pinned frames
	bool cont = true;
	while(cont){
		advanceClock();
		if(bufDescTable[clockHand].valid){ // else: call set() on the frame
			if(!bufDescTable[clockHand].refbit){
				if(bufDescTable[clockHand].pinCnt == 0){
					if(bufDescTable[clockHand].dirty){
						// flush page to disk
					}
					else{
						bufDescTable[i].set(file, pageNo); // correct parameters ?
					}
				}
				else{ // advance clock pointer
					pinC = pinC + 1;
					cont = true;
				}
			}
			else{// advance clock pointer
				cont = true;
			}
		}
		else{
			bufDescTable[i].set(file, pageNo); // correct parameters ?
		}
	}
	// throws exception if all buffer pages are pinned
	if(pinC = numBufs){
		throw BufferExceededException():
	}
	// Use frame
	if(bufDescTable[clockHand].valid){
		// remove entry from hash table
		if (hashTable.lookup(file,pageNo, frame) == frame){
			hashTable.remove(file, pageNo); // correct parameters ?
		}
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
