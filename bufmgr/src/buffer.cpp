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
#include "exceptions/hash_already_present_exception.h"

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
	if(clockHand == numBufs-1){
		clockHand = 0;
	}
	else{
		clockHand = clockHand + 1;
	}
	//std::cout << "numBufs: " << numBufs << "\n";
	//std::cout << "clockHand: " << clockHand << "\n";
}

void BufMgr::allocBuf(FrameId & frame) 
{ 

	std::uint32_t pinC = 0; // counts number of pinned frames

	while(true){
		advanceClock();
		std::cout <<"\n" <<"advanceclock\n";
		if (bufDescTable[clockHand].valid){
			if(!bufDescTable[clockHand].refbit){
				//advanceClock();
				if (bufDescTable[clockHand].pinCnt == 0){
					break;
				} else {
					pinC = pinC + 1;
				}
			} else {
				//clear refbit
				bufDescTable[clockHand].refbit = false;
			}
		} else {
			//exit
			break;
		}
		if(pinC == (numBufs -1)){
				throw BufferExceededException();
			}
	}
	//dirt bit set?
	if (bufDescTable[clockHand].valid){
		if (bufDescTable[clockHand].dirty){
			flushFile(bufDescTable[clockHand].file);
		}
	}

	//std::cout << "Setting at clockHand: " << clockHand << "\n";
	bufDescTable[clockHand].Set(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);

	//Use Frame
	frame = bufDescTable[clockHand].frameNo;

	if(pinC == (numBufs -1)){
		throw BufferExceededException();
	}
	return;

}

void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo;
	try{
		std::cout <<"\n" <<"CASE 2: --------------------";
		hashTable->lookup(file, pageNo, frameNo);  // Case 2: Page is in the buffer pool
		bufDescTable[frameNo].refbit = true;
		bufDescTable[frameNo].pinCnt++;
		// "Return a pointer to the frame containing the page via the page parameter"
		page = &bufPool[frameNo];
	}
	catch(HashNotFoundException e){ // Case 1: Page is not in the buffer pool
		std::cout <<"\n" <<"CASE 1: --------------------";
		allocBuf(frameNo);
		//Page newPage = file->readPage(pageNo);
		//bufPool[frameNo] = newPage;
		bufPool[frameNo] = file->readPage(pageNo);
		hashTable->insert(file, pageNo, frameNo);
		bufDescTable[frameNo].Set(file, pageNo);
		// "Return a pointer to the frame containing the page via the page parameter"
		page = &bufPool[frameNo];
		std::cout<<"\n" << "finished catch statement" << "\n";
	}
	std::cout <<"\n" <<"outside catch" << "\n";
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{
	//can throw a hashnotfoundexception
	FrameId frameNo;
	hashTable->lookup(file, pageNo, frameNo);
	//try{
	if(bufDescTable[frameNo].pinCnt == 0){
		throw PageNotPinnedException(file->filename(), pageNo, frameNo);
	}
	else{
		bufDescTable[frameNo].pinCnt--;
	}
	if(dirty){
		bufDescTable[frameNo].dirty = true;
	}


}

void BufMgr::flushFile(const File* file) 
{
	for(FrameId i = 0; i < numBufs; i++){
		if (bufDescTable[i].file == file){
			if(bufDescTable[i].pinCnt != 0){
				throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
			}
			if(!bufDescTable[i].valid){
				throw BadBufferException(bufDescTable[i].frameNo,bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
			}
			// (a)
			if (bufDescTable[i].dirty){
				bufDescTable[i].file -> writePage(bufPool[i]); // flushes the page to disk
				bufDescTable[i].dirty = false; // sets dirty bit to false
			}
			//(b)
			hashTable->remove(file, bufDescTable[i].pageNo);
			//(c)
			bufDescTable[i].Clear(); // clears frame
			//(d)
		}
	}
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
{

	//Page newPage = file->allocatePage();
	FrameId frameNo;
	//std::cout <<"\n" << "******************\n";
	allocBuf(frameNo);
	//std::cout <<"\n" <<"*++++++++++++*\n";
	bufPool[frameNo] = file->allocatePage();
	//returns newly allocated page to the caller via the pageNo parameter
	pageNo = bufPool[frameNo].page_number();
	bufDescTable[frameNo].Set(file, pageNo);
	hashTable->insert(file, pageNo, frameNo);

	//bufPool[frameNo] = newPage;
	//returns pointer to the frame via the page parameter
	page = &bufPool[frameNo];
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
	FrameId frameNo;
	hashTable->lookup(file, PageNo, frameNo);
	if(frameNo == (FrameId)-1){
		file->deletePage(PageNo);
	}
	else{
		hashTable->remove(file, PageNo); // removes entry from hash table
		bufDescTable[frameNo].Clear(); // frees frame
	}
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
