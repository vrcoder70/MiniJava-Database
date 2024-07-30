/*
 * File - BitMapHeaderPage.java
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
 * BitMapHeaderPage Class
 *
 * Has a pointer the first page in the list of BMPages
 * for a particular bitmap file.
 * It is expected for each BitMapFile class object to have
 * one BitMapHeaderPage, and each Header Page has a single
 * Page ID for the start of the page list. The Pages can be
 * traversed in a doubly linked list manner so long as this
 * head value is maintained.
 */
public class BitMapHeaderPage extends HFPage
{
	//----------------
	//Class Variables
	//----------------
	
	//Note -- inherits a prevPage, curPage, & nextPage from HFPage
	//		  no need to redeclare locally
	
	PageId headBMPageId;
	
	
	//----------------
	//Constructors
	//----------------
	
	//Default Constructor
	public BitMapHeaderPage()
		throws HFException, HFBufMgrException, IOException
	{
		//Does nothing, inherits an empty bytes[] from Page class
		super();
		//create an empty BMPage to point to
		BMPage headPage = new BMPage();
		headBMPageId = new PageId();
		
		//create the Page in DB
		headBMPageId = newPage(headPage, 1);
		// check error
		if(headBMPageId == null)
			throw new HFException(null, "can't new page");
		
		//initialize the page buffer
		headPage.init( headBMPageId, headPage );
		
		unpinPage(headBMPageId, true /*dirty*/ );
	}
	
	public BitMapHeaderPage( PageId pageNo )
		throws ConstructPageException
	{
		super();
		headBMPageId = pageNo;
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
		BMPage headPage = new BMPage();
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
	
	//Take an overall positional argument and compute which
	//page holds the bit representing that bit, then iterate
	//to that page and set the bit
	//If the number of allocated pages does not extend to that page
	//value (ie, n allocated and looking for n+1), then allocate
	//the difference in order to set the correct bit
	public boolean insertMap( int position )
		throws HFException, HFBufMgrException, IOException
	{
		boolean successfulInsert = false;
		
		//start at head and read forward in list
		PageId currentPageId = new PageId();
		currentPageId.pid = headBMPageId.pid;
		BMPage currentBMPage = new BMPage();
		
		/*
		 * The computation below is to translate a positional argument into a
		 * specific bit in a page. For instance, if each BMPage could store 100bits
		 * and the user passed argument 202, then we would set bit 2 in page 3.
		 * (notice, page3 stores bits [200, 299])
		 */
		//compute which page this position should go in
		int bitsPerPage = BMPage.BIT_COUNT;
		//which page has the bit being set (from 0 to n)
		int pageToSet = position / bitsPerPage;
		//compute which bit in this page to set
		int bitInPage = position % bitsPerPage;
		int curPageCount = 0;
		
		//attempt to set a bit on an already declared page
		boolean keepChecking = true;
		boolean noRepin = false;
		while( keepChecking )
		{
			//pin page with this PageId for use, if it is in the buffer
			//it will be pinned otherwise it will be pulled into the buffer
			pinPage(currentPageId, currentBMPage, false/*read disk*/);
			
			//check if this is the page to insert into
			if( pageToSet == curPageCount )
			{
				successfulInsert = currentBMPage.setBit( bitInPage );
				//we can exit after this loop
				keepChecking = false;
				//page was updated so it's dirty
				unpinPage(currentPageId, true /*DIRTY*/);
			}
			else
			{
				//not the right page, go to next link
				PageId nextPageId = new PageId();
				nextPageId = currentBMPage.getNextPage();
				//if we reach the end of the list, exit
				if( -1 == nextPageId.pid )
				{
					keepChecking = false;
					noRepin = true; //don't unpin this page as it is still needed
					//do not increment page counter as the nth
					//page doesn't exist
				}
				else
				{
					//else continue to next page
					unpinPage(currentPageId, false /*not DIRTY*/);
					currentPageId.pid = nextPageId.pid;
					//increment counter to indicate which page we're on
					curPageCount++;
				}
			}
		}
		
		//If we exit the above without setting the correct bit in
		//the page then that means we don't have the necessary n-th
		//page. We need to create pages until we reach that necessary page
		while( curPageCount < pageToSet )
		{
			if( noRepin )
			{
				//page was already pinned, so we shouldn't repin it here
				noRepin = false;
			}
			else
			{
				//pin curPageId, which should still be pointing to the last page in the list
				pinPage(currentPageId, currentBMPage, false/*read disk*/);
			}
			
			//create the new page
			BMPage freshPage = new BMPage();
			PageId freshPageId = new PageId();
		
			//create the Page in DB
			freshPageId = newPage(freshPage, 1);
			// check error
			if(freshPageId == null)
				throw new HFException(null, "can't new page");
			
			freshPage.init( freshPageId, freshPage );
			//we have a new page to use, increment our page counter
			curPageCount++;
			
			//link it into the list of pages
			currentBMPage.setNextPage( freshPage.getCurPage() );
			freshPage.setPrevPage( currentBMPage.getCurPage() );
			
			//check if the fresh page is the page we need to set the bit in
			if( pageToSet == curPageCount )
			{
				//it is the correct page, set the bit
				successfulInsert = freshPage.setBit( bitInPage );
			}
			
			//pages had its pointers updated at least, so both are dirty
			unpinPage(currentPageId, true /*DIRTY*/);
			unpinPage(freshPageId, true /*dirty*/ );
			
			//go to the next page
			currentPageId.pid = freshPageId.pid;
		}
		
		return successfulInsert;
		
	}
	
	//Take an overall positional argument and compute which
	//page holds the bit representing that bit, then iterate
	//to that page and clear the bit
	public boolean deleteMap( int position )
		throws HFBufMgrException, IOException,
				InvalidSlotNumberException
	{
		boolean successfulDelete = false;
		
		//start at head and read forward in list
		PageId currentPageId = new PageId();
		currentPageId.pid = headBMPageId.pid;
		BMPage currentBMPage = new BMPage();
		
		//compute which page this position should go in
		int bitsPerPage = BMPage.BIT_COUNT;
		//which page has the bit being set (from 0 to n)
		int pageToClear = position / bitsPerPage;
		int curPageCount = 0;
		//We also need to calculate the bit in the page to set
		//Ie, if each page holds 10 bits and we are setting bit 11
		//we'll want to set bit 1 in page 2
		int bitInPage = position - ( pageToClear * bitsPerPage );
		
		//attempt to set a bit on an already declared page
		boolean keepChecking = true;
		while( keepChecking )
		{
			//pin page with this PageId for use, if it is in the buffer
			//it will be pinned otherwise it will be pulled into the buffer
			pinPage(currentPageId, currentBMPage, false/*read disk*/);
			
			//check if this is the page to insert into
			if( pageToClear == curPageCount )
			{
				successfulDelete = currentBMPage.clearBit( bitInPage );
				//we can exit after this loop
				keepChecking = false;
				//page was updated so it's dirty
				unpinPage(currentPageId, true /*DIRTY*/);
			}
			else
			{
				//not the right page, go to next link
				PageId nextPageId = new PageId();
				nextPageId = currentBMPage.getNextPage();
				//if we reach the end of the list, exit
				if( -1 == nextPageId.pid )
				{
					keepChecking = false;
					//situation where we attempt to clear an out of bounds bit
					//exit having done nothing
					unpinPage(currentPageId, false /*not DIRTY*/);
				}
				else
				{
					//else continue to next page
					unpinPage(currentPageId, false /*not DIRTY*/);
					currentPageId.pid = nextPageId.pid;
					//increment counter to indicate which page we're on
					curPageCount++;
				}
			}
		}
		
		return successfulDelete;
	}
	
}