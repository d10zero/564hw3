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
	int pinC = 0; // counts number of pinned frames
	bool cont = true;
	while(cont){
		advanceClock();
		if(bufDescTable[clockHand].valid){ // else: call set() on the frame
			hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
			if(!bufDescTable[clockHand].refbit){
				if(bufDescTable[clockHand].pinCnt == 0){
					if(bufDescTable[clockHand].dirty){
						Page tempPage = bufDescTable[clockHand].file->readPage(bufDescTable[clockHand].pageNo);
						flushFile(bufDescTable[clockHand].file);
						bufDescTable[clockHand].Set(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
						cont = false;
					}
				}
				else{ // advance clock pointer
					pinC = pinC + 1;
					cont = true;
				}
			}
			else{// advance clock pointer
				bufDescTable[clockHand].refbit = false;
				cont = true;
			}
		}
		else{
			bufDescTable[clockHand].Set(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
			cont = false;
		}
	}
	// throws exception if all buffer pages are pinned
	if(sizeof(pinC) == sizeof(numBufs)){
		throw BufferExceededException();
	}
	// Use frame
	frame = bufDescTable[clockHand].frameNo;

}

void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo;
	try{
		hashTable->lookup(file, pageNo, frameNo);  // Case 2: Page is in the buffer pool
		bufDescTable[frameNo].refbit = true;
		bufDescTable[frameNo].pinCnt++;
		// "Return a pointer to the frame containing the page via the page parameter" *******************8
		*page = bufPool[frameNo];
	}
	catch(HashNotFoundException e){ // Case 1: Page is not in the buffer pool
		allocBuf(frameNo);
		Page newPage = file->readPage(pageNo);
		hashTable->insert(file, pageNo, frameNo);
		bufDescTable[frameNo].Set(file, pageNo);
		// "Return a pointer to the frame containing the page via the page parameter" *********************
		*page = bufPool[frameNo];
	}
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{
	FrameId frameNo;
	try{
		hashTable->lookup(file, pageNo, frameNo);
		try{
			if(bufDescTable[frameNo].pinCnt == 0){
				throw PageNotPinnedException(file->filename(), pageNo, frameNo);
			}
			else{
				bufDescTable[frameNo].pinCnt--;
			}
		}
		catch(PageNotPinnedException e ){
			return;
			// PIAZZA QUESTION *****************************************
		}
	}
	catch(HashNotFoundException e){
		return;
	}
	if(!bufDescTable[frameNo].dirty){ // ************************* piazza question
		bufDescTable[frameNo].dirty = true;
	}


}

void BufMgr::flushFile(const File* file) 
{
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
{
	Page newPage = file->allocatePage();
	FrameId frameNo;
	allocBuf(frameNo);
	hashTable->insert(file, pageNo, frameNo);
	bufDescTable[frameNo].Set(file, pageNo);
	//returns newly allocated page to the caller via the pageNo parameter
	pageNo = newPage.page_number();
	*page = bufPool[frameNo];
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
