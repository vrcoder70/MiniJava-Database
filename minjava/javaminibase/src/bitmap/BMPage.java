/*
 * File - BMPage.java
 *
 * Original Author - Jackson Nichols
 *
 * Description - 
 *		Single Page in the Bitmap File that contains a set of
 *		mapped attributes from a columnar file. The mapped attributes
 *		also store metadata to locate the page in the columnarfile
 *		it maps to.
 */
package bitmap;

import java.util.Arrays;
import java.io.*;
import java.lang.*;
import global.*;

import diskmgr.*;
import btree.*; //for exceptions
import heap.*; //for exceptions

 /*
  * Define constant values for INVALID_SLOT and EMPTY_SLOT
  */
interface ConstPtr
{
	short INVALID_PTR =  -1;
	short EMPTY_PTR = -1;
}

/*=======================
 * BMPage Class
 *=======================
 * 
 * This class relies on the columnarfile & pages it relates to to be laid out in a manner similar to the following
 * 
 * The Bitmap will map to that Columnar Page as follows
 *	 _______________________________________________
 *	|				[page metadata here]			|
 *	| Byte 0: 8 bits set or not for 8 positions		| //Note: position-0 will go in the MSB and position-7 will go in LSB
 *	| Byte 1: 8 bits set or not for 8 positions		|
 *	|					...							|
 *	| Byte N: 8 bits set or not for 8 positions		|
 *	|_______________________________________________|
 */
public class BMPage extends Page
					implements ConstPtr
{
	//----------------
	//Class Variables
	//----------------
	
	//Helper constants (in bytes)
	private static final int SIZEOF_BYTE = 1;
	private static final int SIZEOF_SHORT = 2;
	private static final int SIZEOF_INT = 4;
	private static final int BITS_PER_BYTE = 8;
	
	//Bit constants in a byte (we do it this way so
	//the 1st entry maps to the MSB and 7th to LSB of a byte)
	private static final int BIT7 = 1;
	private static final int BIT6 = 2;
	private static final int BIT5 = 4;
	private static final int BIT4 = 8;
	private static final int BIT3 = 16;
	private static final int BIT2 = 32;
	private static final int BIT1 = 64;
	private static final int BIT0 = 128;
	
	/*
	 * Notice, the following are computed as the offsets of the metadata in the page
	 *	 _______________________________________
	 *	|	PREV_PAGE	| NEXT_PAGE	| CUR_PAGE	|
	 *	|_______________________________________|
	 *	^Buffer Start
	 */
	public static final int START_OF_BUFFER = 0;
	public static final int PREV_PAGE = START_OF_BUFFER; 			//PREV_PAGE is an int
	public static final int NEXT_PAGE = PREV_PAGE + SIZEOF_INT; 	//NEXT_PAGE is an int
	public static final int CUR_PAGE  = NEXT_PAGE + SIZEOF_INT; 	//CUR_PAGE  is an int
	//size of page metadata fields
	// sizeof(PREV_PAGE) + sizeof(NEXT_PAGE) + sizeof(CUR_PAGE)
	public static final int METADATA_SIZE = 3*SIZEOF_INT;
	
	//now that we know the size of the metadata, the BIT_COUNT is how many bytes
	//are left in the page x8 for the max bits that can be set in this page
	//MAX_SPACE is size of data[] array defined in GlobalConst
	//This should be the same for all pages since all pages have same metadata size
	public static final int BIT_COUNT = (MAX_SPACE - METADATA_SIZE) * BITS_PER_BYTE;
	
	//backward pointer to data page
	private		PageId	prevPage = new PageId();
	//forward pointer to data page
	private		PageId	nextPage = new PageId();
	//page number of this page
	protected	PageId	curPage  = new PageId();
	
	//----------------
	//Constructors
	//----------------
	
	//Default Constructor
	public BMPage()
	{
		//Does nothing unique here
		//creates an empty data[] from Page constructor
		super();
	}
	
	//Constructor of class BMPage open a BMPage and
	//make this BMPage point to the given page
	public BMPage( Page page )
	{
		//take data from other page and store in this page
		data = page.getpage();
	}
	
	//Constructor of class BMPage initialize a new page
	//it is expected the page be init() after construction for complete set-up
	//this method will also override a page with data if passed in with content
	public void init(PageId pageNo, Page apage)
		throws IOException
	{
		//take data from other page and store in this page
		data = apage.getpage();
		//ensure new page is fully initialized to 0
		Arrays.fill( data, (byte)0 );
		
		//initialize the Page IDs of the previous & next page to -1
		nextPage.pid = prevPage.pid = INVALID_PAGE;
		Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
		Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);
		
		//Assign the Page ID of the passed PageID object to this BMPage
		curPage.pid = pageNo.pid;
		Convert.setIntValue(curPage.pid, CUR_PAGE, data);
	}

	//Constructor of class BMPage open an existing BMPage
	public void openBMpage(Page apage)
	{
		//take data from other page and store in this page
		data = apage.getpage();
	}
	
	//----------------
	//Page Navigation Methods
	//----------------
	
	//get value of BITSPACE
	public int getBitSpace()
    {
		return BIT_COUNT;
    }
	
	//get value of curPage
	public PageId getCurPage()
		throws IOException
    {
		curPage.pid =  Convert.getIntValue (CUR_PAGE, data);
		return curPage;
    }
	
	//sets value of curPage to pageNo
	public void setCurPage(PageId pageNo)   
		throws IOException
    {
		curPage.pid = pageNo.pid;
		Convert.setIntValue (curPage.pid, CUR_PAGE, data);
    }

	//get value of nextPage
	public PageId getNextPage()
		throws IOException
    {
		nextPage.pid =  Convert.getIntValue(NEXT_PAGE, data);    
		return nextPage;
    }

	//sets value of nextPage to pageNo
	public void setNextPage(PageId pageNo)
		throws IOException
    {
		nextPage.pid = pageNo.pid;
		Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);
    }

	//get value of prevPage
	public PageId getPrevPage()
		throws IOException 
    {
		prevPage.pid = Convert.getIntValue(PREV_PAGE, data);
		return prevPage;
    }

	//sets value of prevPage to pageNo
	public void setPrevPage(PageId pageNo)
		throws IOException
    {
		prevPage.pid = pageNo.pid;
		Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
    }
	
	//----------------
	//Functional Methods
	//----------------
	
	//setBit takes a positional argument (the nth bit) of this page
	//and sets it to 1. It is assumed the caller will take an overall
	//position and identify which page the bit belongs to before calling
	//this method. Ie, if each page has 10 bits  & insert is called for
	//position 11, it will be the 1st bit on page 2, and identifying page
	//2 should occur before calling this method
	public boolean setBit ( int position )
		throws IOException
    {
		boolean successfulSet = false;
		
		//confirm setting in range
		if( position < BIT_COUNT )
		{
			successfulSet = true;
			
			int byteInData = position / BITS_PER_BYTE;
			int bitInByte = position % BITS_PER_BYTE;
			int byteOffset = METADATA_SIZE + (byteInData * SIZEOF_BYTE);
			//bitwise-or the specific bit to set it
			switch( bitInByte )
			{
				case 0: data[byteOffset] |= BIT0;
					break;
				case 1: data[byteOffset] |= BIT1;
					break;
				case 2: data[byteOffset] |= BIT2;
					break;
				case 3: data[byteOffset] |= BIT3;
					break;
				case 4: data[byteOffset] |= BIT4;
					break;
				case 5: data[byteOffset] |= BIT5;
					break;
				case 6: data[byteOffset] |= BIT6;
					break;
				case 7: data[byteOffset] |= BIT7;
					break;
				//unknown bit accessed
				default: successfulSet = false;
					break;
			}
		}
		
		return successfulSet;
    } 
  
	//clearBit takes a positional argument (the nth bit) of this page
	//and sets it to 1. It is assumed the caller will take an overall
	//position and identify which page the bit belongs to before calling
	//this method. Ie, if each page has 10 bits  & insert is called for
	//position 11, it will be the 1st bit on page 2, and identifying page
	//2 should occur before calling this method
	public boolean clearBit( int position )
		throws IOException
    {
		boolean successfulClear = false;
		
		//confirm clearing in range
		if( position < BIT_COUNT )
		{
			successfulClear = true;
			
			int byteInData = position / BITS_PER_BYTE;
			int bitInByte = position % BITS_PER_BYTE;
			int byteOffset = METADATA_SIZE + (byteInData * SIZEOF_BYTE);
			//bitwise-and the negation of the specific bit to clear it
			switch( bitInByte )
			{
				case 0: data[byteOffset] &= ~(BIT0);
					break;
				case 1: data[byteOffset] &= ~(BIT1);
					break;
				case 2: data[byteOffset] &= ~(BIT2);
					break;
				case 3: data[byteOffset] &= ~(BIT3);
					break;
				case 4: data[byteOffset] &= ~(BIT4);
					break;
				case 5: data[byteOffset] &= ~(BIT5);
					break;
				case 6: data[byteOffset] &= ~(BIT6);
					break;
				case 7: data[byteOffset] &= ~(BIT7);
					break;
				//unknown bit accessed
				default: successfulClear = false;
					break;
			}
		}
		
		return successfulClear;
    }
	
	//A helper method to check the set/clear state of a particular
	//position in this page
	public boolean checkBit( int position )
		throws IOException
    {
		boolean bitSet = false;
		
		//confirm in range
		if( position < BIT_COUNT )
		{
			int byteInData = position / BITS_PER_BYTE;
			int bitInByte = position % BITS_PER_BYTE;
			int byteOffset = METADATA_SIZE + (byteInData * SIZEOF_BYTE);
			//bitwise-and the byte and the specific bit being checked
			//and if the result is non-zero then it is set
			switch( bitInByte )
			{
				case 0: bitSet = (0 != (data[byteOffset] & BIT0));
					break;
				case 1: bitSet = (0 != (data[byteOffset] & BIT1));
					break;
				case 2: bitSet = (0 != (data[byteOffset] & BIT2));
					break;
				case 3: bitSet = (0 != (data[byteOffset] & BIT3));
					break;
				case 4: bitSet = (0 != (data[byteOffset] & BIT4));
					break;
				case 5: bitSet = (0 != (data[byteOffset] & BIT5));
					break;
				case 6: bitSet = (0 != (data[byteOffset] & BIT6));
					break;
				case 7: bitSet = (0 != (data[byteOffset] & BIT7));
					break;
				//unknown bit accessed
				default: bitSet = false;
					break;
			}
		}
		
		return bitSet;
    }
	
	//Dump contents of a page in a formatted style
	public void dumpPage()
		throws IOException
	{
		//dump the metadata
		System.out.println("Bitmap Page dump");
		System.out.println("curPage = " + Convert.getIntValue(CUR_PAGE, data) );
		System.out.println("nextPage = " + Convert.getIntValue(NEXT_PAGE, data));
		System.out.println("prevPage = " + Convert.getIntValue(PREV_PAGE, data));
		
		System.out.print("BITMAP: " );
		int totalBytes = (BIT_COUNT/BITS_PER_BYTE);
		for(int curByte = 0; curByte < totalBytes; curByte++ )
		{
			//line break every 8-bytes (64bits) dumped
			if( 0 == (curByte % 10) )
			{
				System.out.println("");
			}
			int byteOffset = METADATA_SIZE + curByte;
			String bitRep = String.format("%8s", Integer.toBinaryString(data[byteOffset] & 0xFF)).replace(' ', '0');
			System.out.print( bitRep );
			System.out.print(" ");
		}
		System.out.println("");
		System.out.println("");
	}

	public byte[] getBMpageArray()
	{
		return data;
	}

	//copy an input byte array into the page bytes
	public void writeBMPageArray(byte[] inputBytes)
	{
		//we do not allow copies of buffers of mismatched size
		//as the resulting behavior could be undefined
		if( inputBytes.length == MAX_SPACE )
		{
			System.arraycopy(inputBytes, 0, data, 0, MAX_SPACE);
		}
	}
	
	
}