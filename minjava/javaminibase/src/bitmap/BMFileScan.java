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
 * BMFileScan implements a search/iterate interface to B+ tree 
 * index files (class BMFileScan).  It derives from abstract base
 * class IndexFileScan.  
 */
public class BMFileScan  extends IndexFileScan
             implements  GlobalConst
{
	BitMapFile bmfile;
	String bmFilename;		// Bitmap we're scanning
	KeyDataEntry curEntry;	//current entry in the BTree index
	BMPage curPage;			//current BMPage being scanned
	PageId curPageId;		//current PageId being scanned
	int curPageCnt;			//counter to help translate bits to positions
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
	BMPage posPage;
	PageId posPageId;
	RID posRid;
	BTFileScan posIndexScan;
	KeyDataEntry posEntry;
	
	
	/*
	 * The get_next method leverages the BTFileScan in in it's implementation
	 * The BitMap has a BTree that is tracking the values with valid bitmaps. This
	 * method iterates over each entry in the BTree that satisfies the scan criteria.
	 * Once it finds an index within the search space it will take the BitMapHeaderPage
	 * and begin to scan the bits in the page. If it finds a set bit it will stop looping
	 * and form a KeyDataEntry using the current mapping being checked (ie, 1 or 2 or 3)
	 * and the RID of the page matching mapped position with the set bit.
	 * On the next call it will pick up from the n+1 bit after it's last get_next call. It
	 * will keep iterating over this BMPage list of bits until reaching the end of the last
	 * Page. It will then go to the next index in the BTree of mapped values that is within
	 * the search criteria and redo the process from the first BitMapHeaderPage.
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
			while( keepIterating ) 
			{
				//we do not have a page of bits to iterate
				if( null == curPage )
				{
					//get the next value in the BTree that has a linked list of BMPages
					if( (checkedEntry = mappedIndexScan.get_next()) != null )
					{
						//System.out.println("Checking Contents " + checkedEntry.key );
						//make these non-null to avoid repeatedly calling BTree.get_next without checking bits
						curPage = new BMPage();
						curPageId = new PageId();
						// Get the PageNo matching the start of this linked list of BMPages
						curRid = ((LeafData)checkedEntry.data).getData();
						curPageCnt = 0; //reset page counter
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
					int bitsPerPage = BMPage.BIT_COUNT;
					
					boolean checkBits = true;
					//iteratively check the bits in the page while looking for
					//set bits
					while( checkBits )
					{
						//pin page with this PageId for use, if it is in the buffer
						//it will be pinned otherwise it will be pulled into the buffer
						pinPage(curPageId, curPage, false/*read disk*/);
						
						boolean bitAtPosSet = false;
						//check each bit in this page for a set bit
						//also be sure to iterate each page [0, bitsPerPage) and it
						//is intentional not to check the nth bit because it would
						//exceed the data rray bounds (which go to bitsPerPage-1)
						while( (!bitAtPosSet) && (curBitCheck < bitsPerPage) )
						{
							bitAtPosSet = curPage.checkBit( curBitCheck );
							if( bitAtPosSet )
							{
								//track state for the page & bit that matched
								matchedBitCnt = curBitCheck;
								matchedPageCnt = curPageCnt;
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
						//in the linked list of BMPages for this value (we do this
						//even if we got a match above on bit set)
						if( curBitCheck >= bitsPerPage )
						{
							//not the right page, go to next link
							PageId nextPageId = new PageId();
							nextPageId = curPage.getNextPage();
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
								//increment counter to indicate which page we're on
								curPageCnt++;
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
				curGetNextPosition = (matchedPageCnt * BMPage.BIT_COUNT) + matchedBitCnt;
				RID matchRid = bmfile.getSrcColumnarFile().getRidFromPosition( curGetNextPosition, bmfile.getMappedColumn() );
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
			boolean matchedOrDone = false;
			while( !matchedOrDone ) 
			{
				//we do not have a page to check
				if( null == posPage )
				{
					//get the next value in the BTree that has a linked list of BMPages
					if( (posEntry = posIndexScan.get_next()) != null )
					{
						//System.out.println("Checking Contents " + posEntry.key );
						//make these non-null to avoid repeatedly calling BTree.get_next without checking bits
						posPage = new BMPage();
						posPageId = new PageId();
						// Get the PageNo matching the start of this linked list of BMPages
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
					
					/*
					 * The computation below is to translate a positional argument into a
					 * specific bit in a page. For instance, if each BMPage could store 100bits
					 * and the user passed argument 202, then we would check bit 2 in page 3.
					 * (notice, page3 stores bits [200, 299])
					 */
					//compute which page this position should go in
					int bitsPerPage = BMPage.BIT_COUNT;
					//which page has the bit being set (from 0 to n)
					int pageToCheck = position / bitsPerPage;
					//compute which bit in this page to set
					int bitInPage = position % bitsPerPage;
					int curPageCount = 0;
					
					//attempt to set a bit on an already declared page
					boolean keepChecking = true;
					while( keepChecking )
					{
						//pin page with this PageId for use, if it is in the buffer
						//it will be pinned otherwise it will be pulled into the buffer
						pinPage(posPageId, posPage, false/*read disk*/);
						
						//check if this is the page to insert into
						if( pageToCheck == curPageCount )
						{
							//if the bit is set, we can exit the outer while loop
							//else we need to go to the next value in the BTree range
							//being checked
							matchedOrDone = posPage.checkBit( bitInPage );
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
								//increment counter to indicate which page we're on
								curPageCount++;
							}
						}
					}
				}
			}
			
			if( foundMatch )
			{
				//get the RID for the specified position & column
				RID matchRid = bmfile.getSrcColumnarFile().getRidFromPosition( position, bmfile.getMappedColumn() );
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
			posIndexScan = bmfile.getIndexBTree().new_scan(scanLowKey, scanHiKey);
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
	public void closeBitmapScans()
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
