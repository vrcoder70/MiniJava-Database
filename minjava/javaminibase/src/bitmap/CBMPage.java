/*
 * File - CBMPage.java
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


/*=======================
 * CBMPage Class
 *=======================
 * 
 * This class relies on the columnarfile & pages it relates to to be laid out in a manner similar to the following
 * 
 * The Bitmap will map to that Columnar Page as follows
 *	 _______________________________________
 *	|		[page metadata here]			|
 *	| Byte 0: run length compression		|
 *	| Byte 1: run length compression		|
 *	|					...					|
 *	| Byte N: run length compression		|
 *	|_______________________________________|
 *
 * Each run length compression takes the form of
 *	 _______________________________________
 *	| 0/1	| Count of Contiguous readings	| //since each run is a byte we can have [0,127] count
 *	|_______|_______________________________|
 * 
 * The MSB in each byte will indicate if this run length compression is
 * for a series of 1s or 0s. The remaining 7 bits will be used to count
 * how many contiguous 0/1s were compressed. This allows for up to 127
 * contiguous positions to be represented per byte.
 *
 */
public class CBMPage extends Page
{
	//----------------
	//Class Variables
	//----------------
	
	//Helper constants (in bytes)
	private static final int SIZEOF_BYTE = 1;
	private static final int SIZEOF_SHORT = 2;
	private static final int SIZEOF_INT = 4;
	
	//Bit constants in a byte
	//The DATA_BITS are al lthe bits representing the count of contiguous 0/1s
	//The VALUE_BIT is the MSB indicating if a 0 or 1 is compressed in this byte
	private static final int DATA_BITS = 127;
	private static final int VALUE_BIT = 128;
	
	/*
	 * Notice, the following are computed as the offsets of the metadata in the page
	 *	 _______________________________________________________
	 *	|	PREV_PAGE	| NEXT_PAGE	| CUR_PAGE	| BITS_IN_PAGE	|
	 *	|_______________________________________________________|
	 *	^Buffer Start
	 */
	public static final int START_OF_BUFFER = 0;
	public static final int PREV_PAGE = START_OF_BUFFER; 			//PREV_PAGE is an int
	public static final int NEXT_PAGE = PREV_PAGE + SIZEOF_INT; 	//NEXT_PAGE is an int
	public static final int CUR_PAGE  = NEXT_PAGE + SIZEOF_INT; 	//CUR_PAGE  is an int
	public static final int BITS_IN_PAGE=CUR_PAGE + SIZEOF_INT; 	//BITS_IN_PAGE  is an int
	//size of page metadata fields
	// sizeof(PREV_PAGE) + sizeof(NEXT_PAGE) + sizeof(CUR_PAGE) + sizeof(BITS_IN_PAGE)
	public static final int METADATA_SIZE = 4*SIZEOF_INT;
	
	//now that we know the size of the metadata, the MAX_BYTES is how many bytes
	//are left in the page
	//This should be the same for all pages since all pages have same metadata size
	public static final int MAX_BYTES = MAX_SPACE - METADATA_SIZE;
	
	//To make set/clear operations faster we will track how many
	//positions are compressed in this page
	private 	int		bitsInPage = 0;
	
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
	public CBMPage()
	{
		//Does nothing unique here
		//creates an empty data[] from Page constructor
		super();
	}
	
	//Constructor of class CBMPage open a CBMPage and
	//make this CBMPage point to the given page
	public CBMPage( Page page )
	{
		//take data from other page and store in this page
		data = page.getpage();
	}
	
	//Constructor of class CBMPage initialize a new page
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
		nextPage.pid = prevPage.pid = -1;
		Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
		Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);
		
		//Assign the Page ID of the passed PageID object to this CBMPage
		curPage.pid = pageNo.pid;
		Convert.setIntValue(curPage.pid, CUR_PAGE, data);
		
		//initialize the bits in page to 0
		bitsInPage = 0;
		Convert.setIntValue(bitsInPage, BITS_IN_PAGE, data);
	}

	//Constructor of class CBMPage open an existing CBMPage
	public void openBMpage(Page apage)
	{
		//take data from other page and store in this page
		data = apage.getpage();
	}
	
	//----------------
	//Metadata Navigation Methods
	//----------------
	
	//get value of bits in page
	public int getBitsInPage()
		throws IOException
    {
		bitsInPage = Convert.getIntValue (BITS_IN_PAGE, data);
		return bitsInPage;
    }
	
	//sets value of bits in page
	public void setBitsInPage(int bitCnt)   
		throws IOException
    {
		bitsInPage = bitCnt;
		Convert.setIntValue (bitsInPage, BITS_IN_PAGE, data);
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
	//Bit Manipulation Support Methods
	//----------------
	
	//get the length of the compressed bit count here
	private byte getByte( int byteOffset )
	{
		byte value = data[METADATA_SIZE + byteOffset];
		return value;
	}
	
	private void setByte( int byteOffset, byte newByte )
	{
		data[METADATA_SIZE + byteOffset] = newByte;
	}
	
	//return if this run is for 1s or 0s
	private boolean runOfOnes( int byteOffset )
	{
		boolean isOne = false;
		if( 0 != (VALUE_BIT & data[METADATA_SIZE + byteOffset]) )
		{
			isOne = true;
		}
		return isOne;
	}
	
	//get the length of the compressed bit count here
	public int readRun( int byteOffset )
	{
		int value = (int)(DATA_BITS & data[METADATA_SIZE + byteOffset]);
		return value;
	}
	
	//constants to improve readability when calling this method
	private static final boolean RUN_OF_ONES = true;
	private static final boolean RUN_OF_ZEROES = false;
	//Method to set as many bits in a run compression as possible
	private int setRun( int byteOffset, int runLength, boolean isOne )
	{
		//do not attempt to set a value longer than a run length
		//encoding
		int overflow = runLength - DATA_BITS;
		if( overflow < 0 )
		{
			overflow = 0;
			//set the length of this run
			data[METADATA_SIZE + byteOffset] = (byte)runLength;
		}
		else
		{
			//run length exceeds max a single byte will represent
			data[METADATA_SIZE + byteOffset] = (byte)DATA_BITS;
		}
		//map is for run length of 1s, so set MSB
		if( isOne )
		{
			data[METADATA_SIZE + byteOffset] |= VALUE_BIT;
		}
		//return 0 or the number of 0/1s not successfully stored at this offset
		return overflow;
	}
	
	/*
	 * These following group of methods support designs to shift data right in the data array
	 * 
	 * This can occur if we break up an existing run (say of 0s) and need to define more runs
	 * to represent the new set of runs
	 * ex) byte n stores bits [5,12] as 0s & we set bit 7 to 1, we'd need to add 2 bytes to create
	 * 	[0 | 5,6] [1 | 7] [0 | 8,12]
	 * 
	 * When doing a data shift to the right we are simply moving we need to consider how full
	 * the current data array is. If it is already maxed out then a shifting operation would push
	 * some amount of data out of the array. In the worst case it would be 2 bytes, but those
	 * 2 bytes would need to be moved to the next BMPage in the doubly linked list
	 */
	private void shiftDataRight( int startRun, int shiftCnt )
	{
		startRun += METADATA_SIZE;
		//confirm if shifting n-many bytes right that the last shift-many
		//bytes are not in use, otherwise they'll be shifted out of memory
		boolean safeShift = true;
		for( int i = (MAX_BYTES-1); i > ((MAX_BYTES-1) - shiftCnt); i-- )
		{
			if( 0 != readRun(i) )
			{
				safeShift = false;
			}
		}
		
		//no data will be pushed out of memory doing this shift
		if( safeShift )
		{
			for( int i = (MAX_BYTES-1); i > (startRun + shiftCnt); i-- )
			{
				data[ i ] = data[ i - shiftCnt ];
			}
		}
		//some data (at most 2 bytes) will be lost when we shift
		else
		{
			//capture the spillover
			if( 1 == shiftCnt )
			{
				setSpillOvers( getByte(MAX_BYTES-1), (byte)0 );
			}
			else
			{
				setSpillOvers( getByte(MAX_BYTES-1), getByte(MAX_BYTES-2) );
			}
			//now that the spillover is captured, complete the shift
			for( int i = (MAX_BYTES-1); i > (startRun + shiftCnt); i-- )
			{
				data[ i ] = data[ i - shiftCnt ];
			}
		}
	}
	
	private byte spillOver1 = 0;
	private byte spillOver2 = 0;
	public void setSpillOvers( byte spill1, byte spill2 )
	{
		spillOver1 = spill1;
		spillOver2 = spill2;
	}
	public byte getSpillOver1()
	{
		return spillOver1;
	}
	public byte getSpillOver2()
	{
		return spillOver2;
	}
	
	/*
	 * These following group of methods support designs to shift data left in the data array
	 * 
	 * This can occur if we merge an existing run (say of 0s) with another pair of runs
	 * ex) We have sequence [0 | 5,6] [1 | 7] [0 | 8,12] [1 | 13] and we clear bit 7, we'd  compress
	 *		things down to just [0 | 5, 12] [] [] [1 | 13] & have 2 empty bytes of space before bit 13
	 *
	 * This will override data currently stored in bytes, so any operations that need to be performed
	 * using those bytes should be done before doing the shift
	 */
	private void shiftDataLeft( int startRun, int shiftCnt )
	{
		for( int i = startRun; i < (MAX_BYTES - shiftCnt); i++ )
		{
			if( 0 != readRun( i ) )
			{
				//don't forget to offset runs by the page metadata length
				data[ i + METADATA_SIZE ] = data[ i + shiftCnt + METADATA_SIZE ];
			}
			else
			{
				//save some computation time by not shifting a bunch of 0s
				//by exiting early
				i = MAX_BYTES - shiftCnt;
			}
		}
		//0 fill the tail of the data array
		for( int i = 1; i <= shiftCnt; i++ )
		{
			data[ MAX_BYTES - i ] = (byte)0;
		}
	}
	
	/*
	 * This method appends a byte of data to the front of the data array
	 * It's purpose is to support migration of spillover data from a preceding
	 * CMBPage to this page (at most 2 at a time)
	 */
	public void prefixByte( byte prefixA, byte prefixB )
	{
		//determine if both prefixes are being appended
		if( 0 == prefixB )
		{
			//only appending A
			shiftDataRight( 0, 1 );
			setByte( 0, prefixA );
		}
		else
		{
			//appending both
			shiftDataRight( 0, 2 );
			setByte( 0, prefixA );
			setByte( 1, prefixB );
		}
	}
	
	/*
	 * This method appends a byte of data to the end of the data array
	 * It's purpose is to support migration of underflow on a CBMPage
	 * that should be filled with data from a succeeding page.
	 * It is expected to be used in tandem to dequeueByte() to get the head byte
	 */
	public void postfixByte( byte postifx )
	{
		//find last 0 length run in page
		int curByte = MAX_BYTES - 1;
		boolean check = true;
		while( check )
		{
			if( 0 != readRun( curByte ) )
			{
				curByte--;
			}
			else
			{
				//exit the loop
				check = false;
				//increment curByte since preceding byte was last 0 length run
				curByte++;
			}
		}
		//ensure not accessing out of bounds memory
		if( curByte < MAX_BYTES )
		{
			setByte( curByte, postifx );
		}
	}
	
	public byte dequeueByte()
	{
		//get the first byte in the data array
		byte value = getByte( 0 );
		//left shift the remaining bytes in the array
		shiftDataLeft(0, 1);
		return value;
	}
	
	public boolean tailIsEmpty()
	{
		boolean isEmpty = true;
		if( 0 != readRun( MAX_BYTES-1 ) )
		{
			isEmpty = false;
		}
		return isEmpty;
	}
	
	/*
	 * Method to call after a set/clear operation to quantify the number of bits in this page
	 * and compress changes in byte ordering
	 */
	public void compressAndCountBits()
		throws IOException
	{
		int runningTotal = 0;
		
		int curByte = 0;
		int nextByte = 1;
		//loop over the list of compressed bits
		while( nextByte < MAX_BYTES )
		{
			boolean pulledInSpillOverAtEnd = false;
			//check if the run type of the 2 bytes match
			if( runOfOnes( curByte ) == runOfOnes( nextByte ) )
			{
				//System.out.println("  Compress match!!!");
				//there is a match, attempt compression
				int runLengthCombined = readRun( curByte ) + readRun( nextByte );
				//System.out.println("  " + runLengthCombined + " = " + readRun( curByte ) + " + " + readRun( nextByte ) );
				boolean runType = runOfOnes( curByte ); //returns true for RUN_OF_ONES
				int overflow = setRun( curByte, runLengthCombined, runType );
				//System.out.println("  Compress overflow = " + overflow );
				//check if a complete compression was achieved
				if( 0 == overflow )
				{
					//pull everything after the nextByte over 1 byte (this
					//guarantees at least the tail being 0)
					shiftDataLeft( nextByte, 1 );
					//check if we can post-fix any spillover data from earlier right shifts
					byte spillOver = getSpillOver1();
					if( 0 != (DATA_BITS & spillOver) )
					{
						//System.out.println("  Spillover found. Pulling it into array");
						//spillover was found, append to end
						postfixByte( spillOver );
						//shift remaining spillover
						setSpillOvers( getSpillOver2(), (byte)0 );
						//if at list end, decrement curByte & nextByte to recheck
						//the compress-ability of this pulled in spillover
						if( nextByte == (MAX_BYTES-1) )
						{
							pulledInSpillOverAtEnd = true;
						}
					}
				}
				else
				{
					//else we'll put the remaining length of the run in the nextByte
					setRun( nextByte, overflow, runType );
				}
			}
			if( !pulledInSpillOverAtEnd )
			{
				//any potential compression was completed, count the bytes
				runningTotal += readRun( curByte );
				//increment to the next bytes
				curByte++;
				nextByte++;
				//quick exit if nothing in next byte to compress with
				if( (nextByte < MAX_BYTES) && (0 == readRun( nextByte )) )
				{
					nextByte = MAX_BYTES;
				}
			}
			//else we pulled in a spillover on the last iteration. That spill over
			//run length might be compressible. Don't count the curByte & don't iterate
			//yet until we recheck the compressibility of cur & next byte
		}
		//count the last byte in the list (now that it is equal to MAX_BYTES-1)
		runningTotal += readRun( curByte );
		
		//update the page metadata of bit count
		setBitsInPage( runningTotal );
	}
	
	//----------------
	//Functional Methods
	//----------------
	
	/*
	 * setBit takes a positional argument (the nth bit) and then iterates over
	 * the compressed set of bits (referred to as runs). It will iterate until
	 * it finds the range containing the bit it is attempting to set. If it finds
	 * the range & it within a run of ones it will exit without changes. If it
	 * finds the range withing a run of zeroes it will split the run and add a
	 * run for the set bit.
	 * If it cannot find the range of bits within the page (ie, set bit 450 and
	 * highest run ends at bit 120) it will insert runs of zeroes until reaching
	 * the nth and set it
	 * In either case the method will conclude by running a recompression loop that
	 * will ensure no same typed runs (ie, 2+ zeroes) are less than full (ie, no
	 * case of [ 0|30 ][ 0|25 ] which should be [ 0|55 ]).
	 */
	public boolean setBit ( int position )
		throws IOException
    {
		boolean successfulSet = false;
		//ensure no carry-over spillover values
		setSpillOvers( (byte)0, (byte)0 );
		
		//Identify where in this page the bit being set exists
		int rngStart = -1;
		int rngEnd = 0;
		int byteCount = -1;
		/*
		 * This loop will exit with a range of [i,j). This range
		 * will meet one of two cases
		 *    Case 1: position exists between i & j
		 *    Case 2: position is greater than j
		 * Each case dictates the steps necessary to set the bit
		 * 
		 * Ex) Set bit 35
		 * init -- start = -1 || end = 0
		 * 0: [ 10 ] //start =  0 || end = 10 (bits  0-9 )
		 * 1: [ 8  ] //start = 10 || end = 18 (bits 10-17)
		 * 2: [ 10 ] //start = 18 || end = 28 (bits 18-27)
		 * 3: [ 0 ]  //start = 28 || end = 28 (bit  28)
		 * Exit -- start = 28 || end = 28
		 * 
		 */
		//System.out.println("Setting -- " + position);
		while( (position > (rngEnd-1)) && (rngStart != rngEnd) && (byteCount < MAX_BYTES) )
		{
			//increment to the next byte
			byteCount++;
			//update the starting value
			rngStart = rngEnd;
			//increment the end of the checked range
			rngEnd += readRun( byteCount );
		}
		//System.out.println("Range Match { start = " + rngStart + " || end = " + rngEnd + " || byte " + byteCount + "}");
		
		
		/*
		 * CASE 1 -- position exists within range [i, j)
		 */
		if( position < rngEnd )
		{
			//System.out.println("Within [i,j) range");
			//nothing to change for a set operation, already a run of 1s
			if( runOfOnes( byteCount ) )
			{
				successfulSet = true;
			}
			else
			{
				/*
				 * CASE 1.1 -- position is equal to i (the first value in this compressed run)
				 * 
				 * [ 0|n ] ==> [ 1|1 ][ 0|n-1 ]
				 */
				if( position == rngStart )
				{
					//shift succeeding bytes and insert new run of 1s
					shiftDataRight( byteCount, 1 );
					setRun( byteCount, 1, RUN_OF_ONES );
					
					successfulSet = true; //end of Case 1.1
				}
				
				/*
				 * CASE 1.2 -- position is equal to j-1 (the last value in this run)
				 * 
				 * [ 0|n ] ==> [ 0|n-1 ][ 1|1 ]
				 */
				else if( position == (rngEnd - 1) )
				{
					if( byteCount == (MAX_BYTES-1) )
					{
						//no space to append & no data to shift
						//place this new run directly in spillover
						setSpillOvers( (byte)(VALUE_BIT | 1), (byte)0 );
					}
					else
					{
						//shift succeeding bytes and insert new run of 1s
						shiftDataRight( byteCount+1, 1 );
						setRun( byteCount+1, 1, RUN_OF_ONES );
					}
					
					successfulSet = true; //end of Case 1.2
				}
				
				/*
				 * CASE 1.3 -- position is neslted deeper between i & j
				 * 
				 * [ 0|n ] ==> [ 0|x-1 ][ 1|1 ][ 0|n-x ]
				 */
				else
				{
					//compute where to perform split
					int bitsInRun = readRun( byteCount );
					int bitInRun = position - rngStart;
					int leadingBits = bitInRun - 1;
					int followingBits = bitsInRun - bitInRun;
					//check if we have space to allocate 2 more bytes without spillover
					if( (byteCount+2) < MAX_BYTES )
					{
						
						//this will need 2 new runs for bytes, shift data accordingly
						shiftDataRight( byteCount, 2 );
						//split the data to capture the new run of 1
						setRun( byteCount, leadingBits, RUN_OF_ZEROES );
						setRun( byteCount+1, 1, RUN_OF_ONES );
						setRun( byteCount+2, followingBits, RUN_OF_ZEROES );
					}
					else
					{
						//identify how much will spillover
						if( byteCount == (MAX_BYTES-1) )
						{
							//byte count was already at end of list, so both bytes spillover
							setRun( byteCount, leadingBits, RUN_OF_ZEROES );
							//set the value bit to indicate run of 1s in spillover
							setSpillOvers( (byte)(VALUE_BIT | 1), (byte)followingBits );
						}
						else
						{							
							//there's only 1 more byte to shift, and it will spillover
							shiftDataRight( byteCount, 1 );
							setRun( byteCount, leadingBits, RUN_OF_ZEROES );
							setRun( byteCount+1, 1, RUN_OF_ONES );
							setSpillOvers( (byte)followingBits, getSpillOver1() );
						}
					}
					
					successfulSet = true; //end of Case 1.3
				}
			}
		}
		/*
		 * CASE 2 -- position is greater than j
		 * 
		 * [ 0|x ] ==> [ 0|x ][ 0|y ][ 0|z ][ 1|1 ]
		 * such that x+y+z = position
		 */
		else
		{
			//System.out.println("Exceeds upper j bound");
			//In this case we'd need to extend/add runs of zeroes in order to reach
			//the position that is being set
			int zeroesBeforePosition = position - rngEnd;
			//extend current run of 0s before adding new ones
			if( false == runOfOnes( byteCount ) )
			{
				//add the current amount of zeroes in the run to the amount needed
				//to reach desired position
				zeroesBeforePosition += readRun( byteCount );
			}
			//add runs of 0s until no more 0s need to be set
			while( (0 != zeroesBeforePosition) && (byteCount < MAX_BYTES) )
			{
				//setRun will return any overflow of bytes in excess of a single run-space
				zeroesBeforePosition = setRun( byteCount, zeroesBeforePosition, RUN_OF_ZEROES );
				//increment to next byte
				byteCount++;
			}
			//if we haven't exceeded the length of this CBMPage.data[], we can set the next
			//byte to 1
			if( byteCount < MAX_BYTES )
			{
				//we have successfully set this bit
				successfulSet = true;
				//we know this run is a single value, so we can ignore the possibility of overflow
				setRun( byteCount, 1, RUN_OF_ONES );
			}
			//else we have failed to set the bit on this page & will return a failure
		}
		
		//bits have been changed, so it's time to recalc
		//and ensure everything is compressed
		compressAndCountBits();
		
		return successfulSet;
    } 
  
	/*
	 * clearBit takes a positional argument (the nth bit) and then iterates over
	 * the compressed set of bits (referred to as runs). It will iterate until
	 * it finds the range containing the bit it is attempting to clear. If it finds
	 * the range & it within a run of ones it will exit without changes. If it
	 * finds the range withing a run of zeroes it will split the run and add a
	 * run for the set bit.
	 * If it cannot find the range of bits within the page (ie, set bit 450 and
	 * highest run ends at bit 120) it will exit without updating anything as
	 * any bit above the highest bit in a run is inferred to be 0.
	 * In either case the method will conclude by running a recompression loop that
	 * will ensure no same typed runs (ie, 2+ zeroes) are less than full (ie, no
	 * case of [ 0|30 ][ 0|25 ] which should be [ 0|55 ]).
	 */
	public boolean clearBit( int position )
		throws IOException
    {
		boolean successfulClear = false;
		
		//ensure no carry-over spillover values
		setSpillOvers( (byte)0, (byte)0 );
		
		//Identify where in this page the bit being set exists
		int rngStart = -1;
		int rngEnd = 0;
		int byteCount = -1;
		/*
		 * This loop will exit with a range of [i,j). This range
		 * will meet one of two cases
		 *    Case 1: position exists between i & j
		 *    Case 2: position is greater than j
		 * Each case dictates the steps necessary to set the bit
		 * 
		 * Ex) Clear bit 35
		 * init -- start = -1 || end = 0
		 * 0: [ 10 ] //start =  0 || end = 10 (bits  0-9 )
		 * 1: [ 8  ] //start = 10 || end = 18 (bits 10-17)
		 * 2: [ 10 ] //start = 18 || end = 28 (bits 18-27)
		 * 3: [ 0 ]  //start = 28 || end = 28 (bit  28)
		 * Exit -- start = 28 || end = 28
		 * 
		 */
		while( (position > (rngEnd-1)) && (rngStart != rngEnd) && (byteCount < MAX_BYTES) )
		{
			//increment to the next byte
			byteCount++;
			//update the starting value
			rngStart = rngEnd;
			//increment the end of the checked range
			rngEnd += readRun( byteCount );
		}
		
		
		/*
		 * CASE 1 -- position exists within range [i, j)
		 */
		if( position < rngEnd )
		{
			//nothing to change for a set operation, already a run of 0s
			if( false == runOfOnes( byteCount ) )
			{
				successfulClear = true;
			}
			else
			{
				/*
				 * CASE 1.1 -- position is equal to i (the first value in this compressed run)
				 * 
				 * [ 1|n ] ==> [ 0|1 ][ 1|n-1 ]
				 */
				if( position == rngStart )
				{
					//shift succeeding bytes and insert new run of 1s
					shiftDataRight( byteCount, 1 );
					setRun( byteCount, 1, RUN_OF_ZEROES );
					
					successfulClear = true; //end of Case 1.1
				}
				
				/*
				 * CASE 1.2 -- position is equal to j-1 (the last value in this run)
				 * 
				 * [ 1|n ] ==> [ 1|n-1 ][ 0|1 ]
				 */
				else if( position == (rngEnd - 1) )
				{
					if( byteCount == (MAX_BYTES-1) )
					{
						//no space to append & no data to shift
						//place this new run directly in spillover
						setSpillOvers( (byte)1, (byte)0 );
					}
					else
					{
						//shift succeeding bytes and insert new run of 0s
						shiftDataRight( byteCount+1, 1 );
						setRun( byteCount+1, 1, RUN_OF_ZEROES );
					}
					
					successfulClear = true; //end of Case 1.2
				}
				
				/*
				 * CASE 1.3 -- position is neslted deeper between i & j
				 * 
				 * [ 1|n ] ==> [ 1|x-1 ][ 0|1 ][ 1|n-x ]
				 */
				else
				{
					//compute where to perform split
					int bitsInRun = readRun( byteCount );
					int bitInRun = position - rngStart;
					int leadingBits = bitInRun - 1;
					int followingBits = bitsInRun - bitInRun;
					//check if we have space to allocate 2 more bytes without spillover
					if( (byteCount+2) < MAX_BYTES )
					{
						
						//this will need 2 new runs for bytes, shift data accordingly
						shiftDataRight( byteCount, 2 );
						//split the data to capture the new run of 0
						setRun( byteCount, leadingBits, RUN_OF_ONES );
						setRun( byteCount+1, 1, RUN_OF_ZEROES );
						setRun( byteCount+2, followingBits, RUN_OF_ONES );
					}
					else
					{
						//identify how much will spillover
						if( byteCount == (MAX_BYTES-1) )
						{
							//byte count was already at end of list, so both bytes spillover
							setRun( byteCount, leadingBits, RUN_OF_ONES );
							setSpillOvers( (byte)1, (byte)followingBits );
						}
						else
						{							
							//there's only 1 more byte to shift, and it will spillover
							shiftDataRight( byteCount, 1 );
							setRun( byteCount, leadingBits, RUN_OF_ONES );
							setRun( byteCount+1, 1, RUN_OF_ZEROES );
							setSpillOvers( (byte)followingBits, getSpillOver1() );
						}
					}
					
					successfulClear = true; //end of Case 1.3
				}
			}
		}
		/*
		 * CASE 2 -- position is greater than j
		 */
		else
		{
			//If we are attempting to clear a bit higher than the highest compression we can
			//infer it to be 0 already. This will save computation time & possibly pages of
			//all 0s
			successfulClear = true;
		}
		
		//bits have been changed, so it's time to recalc
		//and ensure everything is compressed
		compressAndCountBits();
		
		return successfulClear;
    }
	
	/*
	 * A helper method to check the set/clear state of a particular
	 * position in this page by finding the run that bit exists in
	 * and checking if it's a run of 0s or 1s
	 * If the highest run does not reach the checked bit then the
	 * method assumes the bit is 0
	 */
	public boolean checkBit( int position )
		throws IOException
    {
		boolean bitSet = false;
		
		//Identify where in this page the bit being set exists
		int rngStart = -1;
		int rngEnd = 0;
		int byteCount = -1;
		/*
		 * This loop will exit with a range of [i,j). This range
		 * will meet one of two cases
		 *    Case 1: position exists between i & j
		 *    Case 2: position is greater than j
		 * Either case will provide resolution for bit state
		 
		 Ex) Check bit 3
		 * 		init --	start =-1 || end = 0
		 * 0: [ 0|1 ] //start = 0 || end = 1 (bit  0 )
		 * 1: [ 1|2 ] //start = 1 || end = 3 (bits 1-2 )
		 * 2: [ 1|1 ] //start = 3 || end = 4 (bits 2-3 )
		 * 3: [ 0|0 ] //start = 4 || end = 4 (bit  3 )
		 
		 */
		//System.out.println("Checking -- " + position);
		while( (position > (rngEnd-1)) && (rngStart != rngEnd) && (byteCount < MAX_BYTES) )
		{
			//increment to the next byte
			byteCount++;
			//update the starting value
			rngStart = rngEnd;
			//increment the end of the checked range
			rngEnd += readRun( byteCount );
		}
		//System.out.println("Results { start = " + rngStart + " || end = " + rngEnd + " || byte " + byteCount + "}");
		
		//if we didn't reach the position before the length of our runs became
		//0 then we can infer the specific bit was not set since the runs don't reach it
		//Then we just check if the matched run is of 0s or not
		if( (position < rngEnd) && (runOfOnes( byteCount )) )
		{
			bitSet = true;
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
		
		int contigEmpty = 0;
		System.out.print("BITMAP: " );
		for(int curByte = 0; curByte < MAX_BYTES; curByte++ )
		{
			//line break every 10 runs of data
			if( 0 == (curByte % 10) )
			{
				System.out.println("");
			}
			int runLength = readRun( curByte );
			if( 0 != runLength )
			{
				if( runOfOnes( curByte ) )
				{
					System.out.print("[ 1|" + runLength + " ] ");
				}
				else
				{
					System.out.print("[ 0|" + runLength + " ] ");
				}
			}
			else
			{
				System.out.print("[ 0|0 ] ");
				contigEmpty++;
				if( contigEmpty > 2 )
				{
					System.out.println("...");
					curByte = MAX_BYTES; // quick exit
				}
			}
		}
		System.out.println("");
		System.out.println("");
	}

	public byte[] getBMpageArray()
	{
		return data;
	}

	//copy an input byte array into the page bytes
	public void writeCBMPageArray(byte[] inputBytes)
	{
		//we do not allow copies of buffers of mismatched size
		//as the resulting behavior could be undefined
		if( inputBytes.length == MAX_SPACE )
		{
			System.arraycopy(inputBytes, 0, data, 0, MAX_SPACE);
		}
	}
	
	
}