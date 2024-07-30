/*
 * File - CBitMapHeaderPage.java
 *
 * Original Author - Jackson Nichols
 *
 * Description - 
 *		Header page for the Bitmap file that
 *		performs management functions over all
 *		pages in the file
 */
package bitmap;

import java.io.*;
import java.lang.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import btree.*;

/*  
 * CBitMapHeaderPage Class
 *
 * Has a pointer the first page in the list of CBMPages
 * for a particular bitmap file.
 * It is expected for each CBitMapFile class object to have
 * one CBitMapHeaderPage, and each Header Page has a single
 * Page ID for the start of the page list. The Pages can be
 * traversed in a doubly linked list manner so long as this
 * head value is maintained.
 */
public class CBitMapHeaderPage extends HFPage
{
	//----------------
	//Class Variables
	//----------------
	
	//Note -- inherits a prevPage, curPage, & nextPage from HFPage
	//		  no need to redeclare locally
	
	PageId headCBMPageId;
	
	
	//----------------
	//Constructors
	//----------------
	
	//Default Constructor
	public CBitMapHeaderPage()
		throws HFException, HFBufMgrException, IOException
	{
		//Does nothing, inherits an empty bytes[] from Page class
		super();
		//create an empty CBMPage to point to
		CBMPage headPage = new CBMPage();
		headCBMPageId = new PageId();
		
		//create the Page in DB
		headCBMPageId = newPage(headPage, 1);
		// check error
		if(headCBMPageId == null)
			throw new HFException(null, "can't new page");
		
		//initialize the page buffer
		headPage.init( headCBMPageId, headPage );
		
		unpinPage(headCBMPageId, true /*dirty*/ );
	}
	
	public CBitMapHeaderPage( PageId pageNo )
		throws ConstructPageException
	{
		super();
		headCBMPageId = pageNo;
		/*try
		{
			//page already exists, so a pin Call will get it from the disk/buffer
			SystemDefs.JavabaseBM.pinPage(pageNo, this, false); 
		}
		catch (Exception e)
		{
			throw new ConstructPageException(e, "pinpage failed");
		}*/
	}
	
	//initialize this Bitmap to align with the type of the columnar file column
	public void init( ValueClass value )
		throws HFBufMgrException, IOException
	{
		CBMPage headPage = new CBMPage();
		//nothing to init at this time
	}
	
	//----------------
	//Page Manipulation Methods
	//(mirrored from Heapfile.java)
	//----------------
	
	/**
	 * short cut to access the pinPage function in bufmgr package.
	 * @see bufmgr.pinPage
	 */
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

	/**
	 * short cut to access the unpinPage function in bufmgr package.
	 * @see bufmgr.unpinPage
	 */
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
	
	//method to create a page that will exist in the DB
	private PageId newPage(Page page, int num)
		throws HFBufMgrException
	{
		PageId tmpId = new PageId();
		try
		{
			tmpId = SystemDefs.JavabaseBM.newPage(page,num);
		}
		catch (Exception e)
		{
			throw new HFBufMgrException(e,"Heapfile.java: newPage() failed");
		}

		return tmpId;
	}
	
	//----------------
	//Accessors
	//----------------
	
	PageId getPageId()
		throws IOException
    {
		return getCurPage();
    }
	
	//----------------
	//Functional Methods
	//----------------
	
	/*
	 * Take an overall positional argument and attempt insert it into
	 * a compressed bitmap page. This method will first identify if
	 * the position is within or could be added to an existing page.
	 * If it can it will attempt to insert it there. If it can't, or
	 * the page ends up being too small to store this position then
	 * it will create another CBMPage and try again. It will continue
	 * to create pages until the position is set.
	 * Once the bit is set, this method will call to checkCrossPageBehavior()
	 * to address any compression issues the arose from setting this
	 * bit
	 */
	public boolean insertMap( int position )
		throws HFException, HFBufMgrException, IOException
	{
		boolean successfulInsert = false;
		//helper boolean to avoid inf. loop risk
		boolean unexpectedFailure = false;
		
		//start at head and read forward in list
		PageId currentPageId = new PageId();
		currentPageId.pid = headCBMPageId.pid;
		CBMPage currentCBMPage = new CBMPage();
		
		//we'll need to iterate the list of CBMPages to
		//determine where the position is compressed
		int bitsOnPage = 0;
		int runingTotal = 0;
		
		//attempt to set a bit on an already declared page
		boolean keepChecking = true;
		boolean noRepin = false;
		while( keepChecking )
		{
			//pin page with this PageId for use, if it is in the buffer
			//it will be pinned otherwise it will be pulled into the buffer
			pinPage(currentPageId, currentCBMPage, false/*read disk*/);
			
			//get the compressed bit count on this page
			bitsOnPage = currentCBMPage.getBitsInPage();
			
			//is the bit within range? or is there space to append the bit?
			if( (position < bitsOnPage) || (currentCBMPage.tailIsEmpty()) )
			{
				successfulInsert = currentCBMPage.setBit( position );
				//we can exit after this loop
				keepChecking = false;
				//there is the chance this page couldn't store the bit we're
				//trying to set (ie, not enough space) so we'd get a "false"
				//return and need to make more pages
				if( successfulInsert )
				{
					//successful set, page was updated so it's dirty
					unpinPage(currentPageId, true /*DIRTY*/);
				}
				else
				{
					//unsuccessful set
					noRepin = true; //don't unpin this page as it is still needed
					//compute how many bits are still need to reach position
					bitsOnPage = currentCBMPage.getBitsInPage();
					position -= bitsOnPage;
				}
			}
			//else go to the next page in the list
			else
			{
				//decrement position by the count of bits stored in the preceding
				//page. Failure to do this could result in an inf. loop (ie, page
				//holds 150 bits and we're trying to set bit 160, we'd loop inf.
				//trying to get a large enough page)
				position -= bitsOnPage;
				//not the right page, go to next link
				PageId nextPageId = new PageId();
				nextPageId = currentCBMPage.getNextPage();
				//if we reach the end of the list, exit
				if( -1 == nextPageId.pid )
				{
					keepChecking = false;
					noRepin = true; //don't unpin this page as it is still needed
				}
				else
				{
					//else continue to next page
					unpinPage(currentPageId, false /*not DIRTY*/);
					currentPageId.pid = nextPageId.pid;
				}
			}
		}
		
		//If we exit the above without setting "successfulInsert" to true
		//that means we haven't successfully set the bit yet & need more
		//pages to do so
		while( false == successfulInsert )
		{
			if( noRepin )
			{
				//page was already pinned, so we shouldn't repin it here
				noRepin = false;
			}
			else
			{
				//pin curPageId, which should be pointing to the last page in the list
				pinPage(currentPageId, currentCBMPage, false/*read disk*/);
			}
			
			//create the new page
			CBMPage freshPage = new CBMPage();
			PageId freshPageId = new PageId();
		
			//create the Page in DB
			freshPageId = newPage(freshPage, 1);
			// check error
			if(freshPageId == null)
				throw new HFException(null, "can't new page");
			
			freshPage.init( freshPageId, freshPage );
			
			//link it into the list of pages
			currentCBMPage.setNextPage( freshPage.getCurPage() );
			freshPage.setPrevPage( currentCBMPage.getCurPage() );
			
			//attempt to set the bit
			successfulInsert = freshPage.setBit( position );
			//compute how many bits are still need to reach position
			bitsOnPage = currentCBMPage.getBitsInPage();
			position -= bitsOnPage;
			if( (position < 0) && (false == successfulInsert) )
			{
				//something has failed and we'd loop inf. trying
				//to set negative bits (an unexpected case, but
				//should be checked)
				unexpectedFailure = true;
				successfulInsert = true;
			}
			
			//pages had its pointers updated at least, so both are dirty
			unpinPage(currentPageId, true /*DIRTY*/);
			unpinPage(freshPageId, true /*dirty*/ );
			
			//set new page as the curent
			currentPageId.pid = freshPageId.pid;
		}
		//check if we exited on an unexpected failure
		if( unexpectedFailure )
		{
			successfulInsert = false;
		}
		
		//due to shifting in page content from run splits & joins
		//we need to see if any cross CBMPage operations need to be done
		checkCrossPageBehavior( currentPageId );
		
		return successfulInsert;
		
	}
	
	/*
	 * Take an overall positional argument and attempt clear the bit
	 * a compressed bitmap page. This method will first identify if
	 * the position is within an existing page. If it isn't on an
	 * existing page then the method exits and assumes the bit is
	 * already clear.
	 * Once the bit is cleared, this method will call to checkCrossPageBehavior()
	 * to address any compression issues the arose from clearing this
	 * bit
	 */
	public boolean deleteMap( int position )
		throws HFException, HFBufMgrException, IOException
	{
		boolean successfulDelete = false;
		
		//start at head and read forward in list
		PageId currentPageId = new PageId();
		currentPageId.pid = headCBMPageId.pid;
		CBMPage currentCBMPage = new CBMPage();
		
		//we'll need to iterate the list of CBMPages to
		//determine where the position is compressed
		int bitsOnPage = 0;
		int runingTotal = 0;
		
		//attempt to set a bit on an already declared page
		boolean keepChecking = true;
		boolean noRepin = false;
		while( keepChecking )
		{
			//pin page with this PageId for use, if it is in the buffer
			//it will be pinned otherwise it will be pulled into the buffer
			pinPage(currentPageId, currentCBMPage, false/*read disk*/);
			
			//get the compressed bit count on this page
			bitsOnPage = currentCBMPage.getBitsInPage();
			
			//is the bit within range?
			if( position < bitsOnPage )
			{
				successfulDelete = currentCBMPage.clearBit( position );
				//we can exit after this loop
				keepChecking = false;
				//there is the chance this page couldn't store the bit we're
				//trying to set (ie, not enough space) so we'd get a "false"
				//return and need to make more pages
				if( successfulDelete )
				{
					//successful set, page was updated so it's dirty
					unpinPage(currentPageId, true /*DIRTY*/);
				}
				else
				{
					//unsuccessful set
					noRepin = true; //don't unpin this page as it is still needed
					//compute how many bits are still need to reach position
					bitsOnPage = currentCBMPage.getBitsInPage();
					position -= bitsOnPage;
				}
			}
			//else go to the next page in the list
			else
			{
				//decrement position by the count of bits stored in the preceding
				//page. Failure to do this could result in an inf. loop (ie, page
				//holds 150 bits and we're trying to set bit 160, we'd loop inf.
				//trying to get a large enough page)
				position -= bitsOnPage;
				//not the right page, go to next link
				PageId nextPageId = new PageId();
				nextPageId = currentCBMPage.getNextPage();
				//if we reach the end of the list, exit
				if( -1 == nextPageId.pid )
				{
					keepChecking = false;
					unpinPage(currentPageId, false /*not DIRTY*/);
				}
				else
				{
					//else continue to next page
					unpinPage(currentPageId, false /*not DIRTY*/);
					currentPageId.pid = nextPageId.pid;
				}
			}
		}
		
		//due to shifting in page content from run splits & joins
		//we need to see if any cross CBMPage operations need to be done
		checkCrossPageBehavior( currentPageId );
		
		return successfulDelete;
	}
	
	/*
	 * When we perform inserts and deletes we may potentially break up compressions
	 * of 0s or 1s, for example changing [ 0|80 ] >> [ 0|39 ][ 1|1 ][ 0|40 ] (1 byte to 3)
	 * Alternatively we may create situations where the runs can be compressed further,
	 * such as [ 0|39 ][ 1|1 ][ 0|40 ] >> [ 0|39 ][ 0|1 ][ 0|40 ] >> [ 0|80 ] (3 bytes to 1)
	 * When we do operations like this we may create more space in the current page to
	 * store more runs, or need to push some runs onto succeeding pages.
	 * This method serves as a callable means to perform such compression/extension given
	 * a page id to start with
	 */
	public void checkCrossPageBehavior( PageId currentPageId )
		throws HFException, HFBufMgrException, IOException
	{
		CBMPage currentCBMPage = new CBMPage();
		pinPage(currentPageId, currentCBMPage, false/*read disk*/);
		PageId nextPageId = new PageId();
		nextPageId = currentCBMPage.getNextPage();
		//check if either spillover or underflow occurred on this page
		if( ((currentCBMPage.tailIsEmpty()) && (-1 != nextPageId.pid)) ||
			(0 != currentCBMPage.getSpillOver1()) )
		{
			
			//check case of current page having space at the end and a successor
			//page exists. In such a case we'd iteratively pull the tip of the
			//next page into the current page
			while( (currentCBMPage.tailIsEmpty()) && (-1 != nextPageId.pid) )
			{
				CBMPage nextCBMPage = new CBMPage();
				pinPage(nextPageId, nextCBMPage, false/*read disk*/);
				//iterate until currentPage has a non-empty tail or everything from
				//the next page has been pulled in
				byte tipOfNextPage = 1;
				while( (currentCBMPage.tailIsEmpty()) && ((byte)0 != tipOfNextPage) )
				{
					tipOfNextPage = nextCBMPage.dequeueByte();
					currentCBMPage.postfixByte( tipOfNextPage );
					//recompress the page after adding this byte
					currentCBMPage.compressAndCountBits();
				}
				//all byte have been pulled out of nextPage, recompress
				//to get the new count of compressed bits in the page
				nextCBMPage.compressAndCountBits();
				//get the next apge in list to check cascading effects
				PageId tmpNextId = new PageId();
				tmpNextId = nextCBMPage.getNextPage();
				if( -1 == tmpNextId.pid )
				{
					//won't loop again, unpin both pages
					unpinPage(currentPageId, true /*DIRTY*/);
					unpinPage(nextPageId, true /*DIRTY*/);
					currentPageId.pid = nextPageId.pid;
					nextPageId.pid = tmpNextId.pid;
				}
				else
				{
					//will need to check cascades, update objects
					unpinPage(currentPageId, true /*DIRTY*/);
					unpinPage(nextPageId, true /*DIRTY*/);
					
					currentPageId.pid = nextPageId.pid;
					nextPageId.pid = tmpNextId.pid;
					pinPage(currentPageId, currentCBMPage, false/*read disk*/);
				}
			} //end of next page pull in
			
			//check case where splits in the current CBMPage created spillover
			//that needs to be prefixed to the head of the next page
			while( 0 != currentCBMPage.getSpillOver1() )
			{
				//check if a next page exists to take the spillover
				if( -1 != nextPageId.pid )
				{
					CBMPage nextCBMPage = new CBMPage();
					pinPage(nextPageId, nextCBMPage, false/*read disk*/);
					//prefix the bytes on the next page
					if( 0 != currentCBMPage.getSpillOver2() )
					{
						nextCBMPage.prefixByte( currentCBMPage.getSpillOver1(), currentCBMPage.getSpillOver2() );
					}
					else
					{
						nextCBMPage.prefixByte( currentCBMPage.getSpillOver1(), (byte)0 );
					}
					//clear spillover on current page
					currentCBMPage.setSpillOvers( (byte)0, (byte)0 );
					//all byte have been [ushed to nextPage, recompress
					//to get the new count of compressed bits in the page
					nextCBMPage.compressAndCountBits();
					//check cascading effects
					if( 0 == nextCBMPage.getSpillOver1() )
					{
						//won't loop again, unpin both pages
						unpinPage(currentPageId, true /*DIRTY*/);
						unpinPage(nextPageId, true /*DIRTY*/);
					}
					else
					{
						//will need to check cascades, update objects
						unpinPage(currentPageId, true /*DIRTY*/);
						unpinPage(nextPageId, true /*DIRTY*/);
						
						currentPageId.pid = nextPageId.pid;
						pinPage(currentPageId, currentCBMPage, false/*read disk*/);
						nextPageId.pid = (currentCBMPage.getNextPage()).pid;
					}
					
				}
				//make a new page for the spillover
				else
				{
					//create the new page
					CBMPage freshPage = new CBMPage();
					PageId freshPageId = new PageId();
					//create the Page in DB
					freshPageId = newPage(freshPage, 1);
					// check error
					if(freshPageId == null)
						throw new HFException(null, "can't new page");
					freshPage.init( freshPageId, freshPage );
					//link it into the list of pages
					currentCBMPage.setNextPage( freshPage.getCurPage() );
					freshPage.setPrevPage( currentCBMPage.getCurPage() );
					//prefix the bytes on the new page
					if( 0 != currentCBMPage.getSpillOver2() )
					{
						freshPage.prefixByte( currentCBMPage.getSpillOver1(), currentCBMPage.getSpillOver2() );
					}
					else
					{
						freshPage.prefixByte( currentCBMPage.getSpillOver1(), (byte)0 );
					}
					//clear spillover on current page
					currentCBMPage.setSpillOvers( (byte)0, (byte)0 );
					//all byte have been [ushed to nextPage, recompress
					//to get the new count of compressed bits in the page
					freshPage.compressAndCountBits();
					//pages were updated
					unpinPage(currentPageId, true /*DIRTY*/);
					unpinPage(freshPageId, true /*dirty*/ );
				}
				
			} //end of page spillover
		}
		else
		{
			//no special cases occurred from byte shifts
			//release the page without changes
			unpinPage(currentPageId, false /*not DIRTY*/);
		}
	}
	
}