#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
	// TODO: Implement this method by looking at the description in the writeup.
    
}


const Status BufMgr::allocBuf(int & frame) {
	// TODO: Implement this method by looking at the description in the writeup.
    
    //Advance clockHand by 1
    advanceClock();
    
    int pinPageNum = 0;
    while(true){
        //If not valid frame, return this frame
        if(!bufTable[clockHand].valid){
            bufTable[clockHand].valid = true;
            bufTable[clockHand].refbit = true;
            frame = clockHand;
            return OK;
        }
        //If referred, set to false and advance clock hand
        if(bufTable[clockHand].refbit){
            bufTable[clockHand].refbit = false;
            advanceClock();
            continue;
        }
        //If pined, advance clock hand
        if(bufTable[clockHand].pinCnt > 0){
            advanceClock();
            pinPageNum ++;
            if(pinPageNum == numBufs)
                return BUFFEREXCEEDED;
            continue;
        }
        //If dirty, flush this page and return this frame.
        frame = clockHand;
        hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
        if(bufTable[clockHand].dirty){
            bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &bufPool[clockHand]);
            return OK;
        }
        return OK;
    }
	return OK;
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page) {
	// TODO: Implement this method by looking at the description in the writeup.
    int frameNo = 0;
    Status hashStatus = hashTable->lookup(file, PageNo, frameNo);
    //If page in the buffer pool, reture that page address in page pool
    if(hashStatus == OK){
        bufTable[frameNo].refbit = true;
        bufTable[frameNo].pinCnt++;
        page = &bufPool[frameNo];
        return OK;
    }else{
        //Allocate a new page and read content to that frame
        Status status = allocBuf(frameNo);
        if(status == OK){
            page = &bufPool[frameNo];
            Status readStatus = file->readPage(PageNo, page);
            if(readStatus == OK){
                hashTable->insert(file, PageNo, frameNo);
                bufTable[frameNo].Set(file, PageNo);
                return OK;
            }
            return readStatus;
            
        }
    }
	return hashStatus;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) {
	// TODO: Implement this method by looking at the description in the writeup.
    int frameNo = 0;
    Status hashStatus = hashTable->lookup(file, PageNo, frameNo);
    //If page in the buffer pool, process it
    if(hashStatus == OK){
        if(bufTable[frameNo].pinCnt == 0){
            return PAGENOTPINNED;
        }
        //if pinCnt is not zero, minus one
        bufTable[frameNo].pinCnt--;
        if(dirty)   bufTable[frameNo].dirty = true;
        return OK;
    }
	return hashStatus;
}


const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page)  {
	// TODO: Implement this method by looking at the description in the writeup.
    Status alloPageStatus = file->allocatePage(pageNo);
    if(alloPageStatus == OK){
        int frameNo = -1;
        //allocate a new frame for the page
        Status allocBufStatus = allocBuf(frameNo);
        if(allocBufStatus == OK){
            //insert (page, pageno, frame no) to hashtable
            Status hashInsertStatus = hashTable->insert(file, pageNo, frameNo);
            if(hashInsertStatus == OK){
                bufTable[frameNo].Set(file, pageNo);
                page = &bufPool[frameNo];
                return OK;
            }
            return hashInsertStatus;
        }
        return allocBufStatus;
    }
	return alloPageStatus;
}


const Status BufMgr::disposePage(File* file, const int pageNo) {
	// TODO: Implement this method by looking at the description in the writeup.
    int frameNo = 0;
    //If page in the buffer pool, dispose it
    Status hashStatus = hashTable->lookup(file, pageNo, frameNo);
    if(hashStatus == OK){
        bufTable[frameNo].Clear();
        Status hashRemoveStatus = hashTable->remove(file, pageNo);
        if(hashRemoveStatus == OK){
            return file->disposePage(pageNo);
        }
        return hashRemoveStatus;
    }
    return hashStatus;
}


const Status BufMgr::flushFile(const File* file) {
	// TODO: Implement this method by looking at the description in the writeup.
    for(int i = 0; i < numBufs; i ++){
        //flush every page for this file to disk.
        if(bufTable[i].file == file){
            if(bufTable[i].pinCnt > 0){
                return PAGEPINNED;
            }
            //If the file is dirty, write to the disk
            if(bufTable[i].dirty){
                const Page* p =  &bufPool[i];
                Status writeStatus = bufTable[i].file->writePage(bufTable[i].pageNo, p);
                if(writeStatus != OK){
                    return writeStatus;
                }
                bufTable[i].dirty = false;
            }
            //remove from the hashtable
            Status removeStatus = hashTable->remove(file, bufTable[i].pageNo);
            if(removeStatus == OK){
                bufTable[i].Clear();
                continue;
            }
            return removeStatus;
        }
    }
	return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


