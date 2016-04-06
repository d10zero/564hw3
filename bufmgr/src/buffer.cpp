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

	for (FrameId i = 0; i < bufs; i++){
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
			if(bufDescTable[i].dirty == true){
				//flushes out dirty bit
				bufDescTable[i].file -> writePage(bufPool[i]);
				bufDescTable[i].dirty = false;
			}
			else { //enters else only if bufPool[i] is accurate with memory. then deallocates bufpool
			}
		}
	}

	delete bufDescTable;
	delete bufPool;
	delete hashTable;
}

void BufMgr::advanceClock()
{
	if(clockHand >= (numBufs - 1)){
		clockHand = 0;
	}
	else{
		clockHand = (clockHand + 1);
	}
}

void BufMgr::allocBuf(FrameId & frame) 
{ 
 std::uint32_t pinC = 0; // counts number of pinned frames
	bool cont = true;
	while(cont){
		advanceClock();
		if (bufDescTable[clockHand].valid == true){
			if(bufDescTable[clockHand].refbit == false){
				if (bufDescTable[clockHand].pinCnt == 0){
					cont = false;
					break;
				} else {
					pinC = 0;
					for(FrameId i = 0; i < numBufs; i++){
						if(bufDescTable[i].valid){
							if(bufDescTable[i].refbit == false){
								if(bufDescTable[i].pinCnt >= 1){
									pinC = pinC + 1;
								}
							}
						}
					}
					if(pinC > (numBufs - 1)){
						throw BufferExceededException();
					}
					else{
						cont = true;
					}
				}
			} else {
				//clear refbit
				bufDescTable[clockHand].refbit = false;
				cont = true;
			}
		} else {
			bufDescTable[clockHand].pinCnt = 0;
			//exit
			cont = false;
			break;
		}
	}
	//dirt bit set?
	if (bufDescTable[clockHand].valid){
		if (bufDescTable[clockHand].dirty){
			bufDescTable[clockHand].pinCnt = 0;
			bufDescTable[clockHand].valid = false;
			bufDescTable[clockHand].file -> writePage(bufPool[clockHand]);
			//flushFile(bufDescTable[clockHand].file);
		}
		hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
	}

	bufDescTable[clockHand].Clear();
	bufDescTable[clockHand].pinCnt = 0;
	bufDescTable[clockHand].Set(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);

	//Use Frame
	frame = bufDescTable[clockHand].frameNo;
	return;

}

void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo;
	try{
		hashTable->lookup(file, pageNo, frameNo);  // Case 2: Page is in the buffer pool
		bufDescTable[frameNo].refbit = true;
		bufDescTable[frameNo].pinCnt++;
		// "Return a pointer to the frame containing the page via the page parameter"
		page = &bufPool[frameNo];
	}
	catch(HashNotFoundException e){ // Case 1: Page is not in the buffer pool
		allocBuf(frameNo);
		bufPool[frameNo] = file->readPage(pageNo);
		hashTable->insert(file, pageNo, frameNo);
		bufDescTable[frameNo].Set(file, pageNo);
		bufDescTable[frameNo].refbit = true;
		// "Return a pointer to the frame containing the page via the page parameter"
		page = &bufPool[frameNo];
	}
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{
	//can throw a hashnotfoundexception
	FrameId frameNo;
	try{
		hashTable->lookup(file, pageNo, frameNo);
	} catch(HashNotFoundException e){
		return;
	}
	if(bufDescTable[frameNo].pinCnt == 0){
		throw PageNotPinnedException(file->filename(), pageNo, frameNo);
		return;
	}

	bufDescTable[frameNo].pinCnt = (bufDescTable[frameNo].pinCnt - 1);
	
	if(dirty == true){
		bufDescTable[frameNo].dirty = true;
	}
}

void BufMgr::flushFile(const File* file) 
{
	for(FrameId i = 0; i < numBufs; i++){
		if (bufDescTable[i].file == file){
			if(bufDescTable[i].pinCnt > 0){
				throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
			}
			if(bufDescTable[i].valid == false){
				bufDescTable[i].pinCnt = 0;
				throw BadBufferException(bufDescTable[i].frameNo,bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
			}
			// (a)
			if (bufDescTable[i].dirty){
				bufDescTable[i].file -> writePage(bufPool[i]); // flushes the page to disk
				bufDescTable[i].dirty = false; // sets dirty bit to false
			}
			//(b)
			try{
				hashTable->remove(file, bufDescTable[i].pageNo);
			} catch(HashNotFoundException e){
				return;
			}
			//(c)
			bufDescTable[i].Clear(); // clears frame
			bufDescTable[i].valid = false;
			bufDescTable[i].pinCnt = 0;
		}
	}
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
{
	int pinCC = 0;
	for(FrameId i = 0; i < numBufs; i++){
		if(bufDescTable[i].valid == true){
			if(bufDescTable[i].refbit == false){
				if(bufDescTable[i].pinCnt > 0){
					pinCC++;
				}
			}
		}
	}
	FrameId frameNo;
	allocBuf(frameNo);
	bufPool[frameNo] = file->allocatePage();
	//returns newly allocated page to the caller via the pageNo parameter
	PageId pageNo1 = bufPool[frameNo].page_number();
	bufDescTable[frameNo].Set(file, pageNo1);
	hashTable->insert(file, pageNo1, frameNo);
	//returns pointer to the frame via the page parameter
	page = &bufPool[frameNo];
	pageNo = pageNo1;
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
	FrameId frameNo = numBufs + 20;
	try{
		hashTable->lookup(file, PageNo, frameNo);
		if(frameNo != (numBufs + 20)){
			file->deletePage(PageNo);
		}
		else{
			hashTable->remove(file, PageNo); // removes entry from hash table
			bufDescTable[frameNo].valid = false;
			bufDescTable[frameNo].Clear(); // frees frame
		}
	}catch(HashNotFoundException e){
		return;
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
