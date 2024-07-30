/*
 * @(#) BTIndexPage.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu)
 *
 */
package bitmap;
import java.io.*;
import global.*;
import heap.*;
import btree.*;
import diskmgr.*;
import bufmgr.*;

/**
 * CBMFileScan implements a search/iterate interface to B+ tree 
 * index files (class CBMFileScan).  It derives from abstract base
 * class IndexFileScan.  
 */
public class CBMFileScan  extends IndexFileScan
             implements  GlobalConst
{
	CBitMapFile cbmfile;
	String cbmFilename;		// Bitmap we're scanning
	KeyDataEntry curEntry;	//current entry in the BTree index
	CBMPage curPage;			//current CBMPage being scanned
	PageId curPageId;		//current PageId being scanned
	int runningBitCnt;		//counter to help translate bits to positions
	int curBitCheck;		//current bit in a page being checked
	int curGetNextPosition;	//value to store the position of the most recent return
	BTFileScan mappedIndexScan;
	KeyDataEntry checkedEntry;
	
	KeyClass scanLowKey;
	KeyClass scanHiKey;
	
	int keyType;
	int maxKeysize;
	
	RID curRid;       		// position in current leaf; note: this is 
							// the RID of the key/RID pair within the
							// leaf page.                                    
	boolean didfirst;       // false only before getNext is called
	boolean deletedcurrent; // true after deleteCurrent is called (read
							// by get_next, written by deleteCurrent).
	
	//These are complements to the above variables of
	//similar name, except they are scoped for the
	//get_position() based scan implementation
	CBMPage posPage;
	PageId posPageId;
	RID posRid;
	BTFileScan posIndexScan;
	KeyDataEntry posEntry;
	
	
	/*
	 * The get_next method leverages the BTFileScan in in it's implementation
	 * The BitMap has a BTree that is tracking the values with valid bitmaps. This
	 * method iterates over each entry in the BTree that satisfies the scan criteria.
	 * Once it finds an index within the search space it will take the CBitMapHeaderPage
	 * and begin to scan the bits in the page. If it finds a set bit it will stop looping
	 * and form a KeyDataEntry using the current mapping being checked (ie, 1 or 2 or 3)
	 * and the RID of the page matching mapped position with the set bit.
	 * On the next call it will pick up from the n+1 bit after it's last get_next call. It
	 * will keep iterating over this CBMPage list of bits until reaching the end of the last
	 * Page. It will then go to the next index in the BTree of mapped values that is within
	 * the search criteria and redo the process from the first CBitMapHeaderPage.
	 * If it reaches the end of the search space it will return a null KeyDataEntry.
	 */
	public KeyDataEntry get_next()
		throws ScanIteratorException
	{
		KeyDataEntry keyToReturn = null;
		
		try
		{
			boolean foundMatch = false;
			int matchedBitCnt = 0;
			int matchedPageCnt = 0;
			boolean keepIterating = true;
			boolean reachedPageEnd = false;
			int prevPageBitCnt = 0;
			while( keepIterating ) 
			{
				//we do not have a page of bits to iterate
				if( null == curPage )
				{
					//get the next value in the BTree that has a linked list of CBMPages
					if( (checkedEntry = mappedIndexScan.get_next()) != null )
					{
						//System.out.println("Checking Contents " + checkedEntry.key );
						//make these non-null to avoid repeatedly calling BTree.get_next without checking bits
						curPage = new CBMPage();
						curPageId = new PageId();
						// Get the PageNo matching the start of this linked list of CBMPages
						curRid = ((LeafData)checkedEntry.data).getData();
						runningBitCnt = 0; //reset bit count for this page
						curBitCheck = 0; //reset bit coutner
					}
					else //if mappedIndexScan.get_next() returns null we have iterate all values in scan range
					{
						keepIterating = false;
						//keyToReturn is already null, no need to set it null for return
					}
				}
				else //else scan bits for a value
				{
					curPageId.pid = curRid.pageNo.pid;
					int bitsInPage = 0;
					
					boolean checkBits = true;
					//iteratively check the bits in the page while looking for
					//set bits
					while( checkBits )
					{
						//pin page with this PageId for use, if it is in the buffer
						//it will be pinned otherwise it will be pulled into the buffer
						pinPage(curPageId, curPage, false/*read disk*/);
						
						bitsInPage = curPage.getBitsInPage();
						reachedPageEnd = false;
						
						boolean bitAtPosSet = false;
						//check each bit in this page for a set bit
						//also be sure to iterate each page [0, bitsInPage) and it
						//is intentional not to check the nth bit because it would
						//exceed the data array bounds (which go to bitsInPage-1)
						while( (!bitAtPosSet) && (curBitCheck < bitsInPage) )
						{
							bitAtPosSet = curPage.checkBit( curBitCheck );
							if( bitAtPosSet )
							{
								//track state for the page & bit that matched
								matchedBitCnt = curBitCheck;
								//indicate we can exit this loop
								checkBits = false;
								//setting this to false indicates a match to return
								foundMatch = true;
								keepIterating = false;
							}
							//increment to the next bit in the list
							curBitCheck++;
						}
						
						//if we exceed the max bits in this page, go to the next page
						//in the linked list of CBMPages for this value (we do this
						//even if we got a match above on bit set)
						if( curBitCheck >= bitsInPage )
						{
							prevPageBitCnt = bitsInPage;
							reachedPageEnd = true;
							//not the right page, go to next link
							PageId nextPageId = new PageId();
							nextPageId = curPage.getNextPage();
							//advancing to the next page means we may need to add the total bits in
							//this page to our running total
							runningBitCnt += bitsInPage;
							//if we reach the end of the list, exit
							if( -1 == nextPageId.pid )
							{
								//unpin this page, we are done with it
								unpinPage(curPageId, false /*not DIRTY*/);
								//set curPage to null to indicate the BTreeIndex loop
								//should grab the next mapped value
								curPage = null;
								//indicate we can exit this loop & try the next page
								checkBits = false;
							}
							else
							{
								//else continue to next page
								unpinPage(curPageId, false /*not DIRTY*/);
								curPageId.pid = nextPageId.pid;
								//new page means reset to check bit 0
								curBitCheck = 0;
								//doing this helps preserve page position across multiple method calls
								curRid.pageNo.pid = curPageId.pid;
							}
						}
						else
						{
							//if we matched without exceeding the page bit count, we shoulds still unpin the
							//page for now while we handle the return
							unpinPage(curPageId, false /*not DIRTY*/);
						}
					}
				}
			}
			
			if( foundMatch )
			{
				//compute the position of a match given the page & bit that was set
				curGetNextPosition = 0;
				//when reaching the end of a page we may get a match, but the scan above
				//will also add the total bit count to the running total. In such a case,
				//if not acocunted for, we could offset by a large bit count from the
				//intended match
				if( reachedPageEnd )
				{
					curGetNextPosition = runningBitCnt + matchedBitCnt - prevPageBitCnt;
				}
				else
				{
					curGetNextPosition = runningBitCnt + matchedBitCnt;
				}
				RID matchRid = cbmfile.getSrcColumnarFile().getRidFromPosition( curGetNextPosition, cbmfile.getMappedColumn() );
				//create a return key using the value of the index KeyDataEntry & the RID
				//corresponding to the position
				keyToReturn = new KeyDataEntry( checkedEntry.key, matchRid );
			}
		}
		catch( Exception ex )
		{
			ex.printStackTrace();
		}
		
		return keyToReturn;
	}
	
	/*
	 * A support method to return the position of the last value that was
	 * returned by get_next
	 */
	public int getLatestPositionMatch()
	{
		return curGetNextPosition;
	}
	
	/*
	 * The get_next method serves a complement to the get_next() for queries
	 * across multiple bitmaps
	 * Similar in design, except this method checks a specific position instead
	 * of every bit iteratively.
	 * This is intended to be used to support queries across multiple bitmaps,
	 * such as (bitmap1.criteria && bitmap2.criteria). By allowing bitmap1 to
	 * iterate matching values via calls to get_next, we can leverage get_position
	 * in bitmap2 to validate the corresponding position for the 2nd criteria.
	 */
	public KeyDataEntry get_position( int position )
		throws ScanIteratorException
	{
		KeyDataEntry keyToReturn = null;
		
		try
		{
			boolean foundMatch = false;
			//store position in a modifiable variable so we can get overall
			//position RID if we find a match in any CBMPage
			int matchedPosition = position;
			boolean matchedOrDone = false;
			while( !matchedOrDone ) 
			{
				//we do not have a page to check
				if( null == posPage )
				{
					//get the next value in the BTree that has a linked list of CBMPages
					if( (posEntry = posIndexScan.get_next()) != null )
					{
						//System.out.println("Checking Contents " + posEntry.key );
						//make these non-null to avoid repeatedly calling BTree.get_next without checking bits
						posPage = new CBMPage();
						posPageId = new PageId();
						// Get the PageNo matching the start of this linked list of CBMPages
						posRid = ((LeafData)posEntry.data).getData();
					}
					else //if posIndexScan.get_next() returns null we have iterate all values in scan range
					{
						matchedOrDone = true;
						//keyToReturn is already null, no need to set it null for return
					}
				}
				else //else scan bits for a value
				{
					posPageId.pid = posRid.pageNo.pid;
					
					//variable for reaching the desired position in a page
					int bitsInPage = 0;
					
					//attempt to set a bit on an already declared page
					boolean keepChecking = true;
					while( keepChecking )
					{
						//pin page with this PageId for use, if it is in the buffer
						//it will be pinned otherwise it will be pulled into the buffer
						pinPage(posPageId, posPage, false/*read disk*/);
						
						//measure the bits in this page
						bitsInPage = curPage.getBitsInPage();
						
						//check if this is the page to insert into
						if( matchedPosition <= bitsInPage )
						{
							//if the bit is set, we can exit the outer while loop
							//else we need to go to the next value in the BTree range
							//being checked
							matchedOrDone = posPage.checkBit( matchedPosition );
							//we found the right bit and page, so we can exit the
							//inner while loop
							keepChecking = false;
							//page wasn't updated so it's not dirty
							unpinPage(posPageId, false /*not DIRTY*/);
							//Each BTree index will only ever have 1 positive repsonse for
							//a passed position, so after confirming a match we should
							//move on to the next index in the BTree
							posPage = null;
						}
						else
						{
							//not the right page, go to next link
							PageId nextPageId = new PageId();
							nextPageId = posPage.getNextPage();
							//subtract the bit count in this page from position to keep
							//accurate measure of position between pages (else we'd keep
							//checking values out of bounds of a single page)
							matchedPosition -= bitsInPage;
							//if we reach the end of the list, exit
							if( -1 == nextPageId.pid )
							{
								keepChecking = false;
								//set to null to get BTree index to move on to next index
								posPage = null;
								//situations where we go to a "null" page in a linked list indicates
								//we have the value, but only up to the nth position & we are checking
								//some value beyond that which wasn't explicitly set to 1, so it is
								//implicitly 0
								unpinPage(posPageId, false /*not DIRTY*/);
							}
							else
							{
								//else continue to next page
								unpinPage(posPageId, false /*not DIRTY*/);
								posPageId.pid = nextPageId.pid;
							}
						}
					}
				}
			}
			
			if( foundMatch )
			{
				//get the RID for the specified position & column
				RID matchRid = cbmfile.getSrcColumnarFile().getRidFromPosition( position, cbmfile.getMappedColumn() );
				//create a return key using the value of the index KeyDataEntry & the RID
				//corresponding to the position
				keyToReturn = new KeyDataEntry( posEntry.key, matchRid );
			}
		}
		catch( Exception ex )
		{
			ex.printStackTrace();
		}
		
		return keyToReturn;
	}
	
	/*
	 * There may be instances where a single scan may want to check the positions multiple
	 * times, but given get_position() is iteratively designed there is a need to be able
	 * to reset that scan back to it's head.
	 */
	public void resetPositionBasedScan()
	{
		try
		{
			posIndexScan = cbmfile.getIndexBTree().new_scan(scanLowKey, scanHiKey);
			posPage = null;
			posPageId = null;
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	/*
	 * Because the Bitmap Scan uses BTree scans to iterate the mapped indices it
	 * is necesary to close those supporting scans
	 */
	public void closeCbitmapScans()
	{
		try
		{
			mappedIndexScan.DestroyBTreeFileScan();
			posIndexScan.DestroyBTreeFileScan();
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	/** 
	 * Delete the current record.
	 * @exception ScanDeleteException delete current record failed
	 * (based on BTreeFileScan logic of same method)
	 */
	public void delete_current()
		throws ScanDeleteException
	{
		//bitmaps don't support the functionality of "delete_current" but need the method
		//to satisfy absraction
	}
	
	/**
	 * Returns the size of the key
	 * @return the keysize
	 * (based on BTreeFileScan logic of same method)
	 */
	public int keysize()
	{
		return maxKeysize;
	}
	
	//-----------------
	//Buffer support methods
	//-----------------
	private void pinPage(PageId pageno, Page page, boolean emptyPage)
		throws HFBufMgrException
	{
		try
		{
			SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
		}
		catch (Exception e)
		{
			throw new HFBufMgrException(e,"BitmapHeaderFile.java: pinPage() failed");
		}
	}
	
	private void unpinPage(PageId pageno, boolean dirty)
		throws HFBufMgrException
	{
		try
		{
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		}
		catch (Exception e)
		{
			throw new HFBufMgrException(e,"Heapfile.java: unpinPage() failed");
		}
	}
}
