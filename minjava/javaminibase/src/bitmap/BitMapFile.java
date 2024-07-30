/*
 * File - BitMapFile.java
 *
 * Original Author - Jackson Nichols
 *
 * Description - 
 *		...
 */
package bitmap;

import java.util.*;
import java.io.*;
import java.lang.*;
import global.*;
import diskmgr.*;
import bufmgr.*;
import heap.*;
import btree.*;
import columnar.*;

 
/*  
 * BitMapFile Class
 */
public class BitMapFile extends IndexFile
	implements GlobalConst
{
	//----------------
	//Class Variables
	//----------------
	private Columnarfile srcColumnar;
	private PageId bmFileId;
	private BTreeFile indexOfMappedElements;
	
	private String  dbname;
	private int columnMap = -1;
	private int mapType = AttrType.attrNull;
	
	//----------------
	//Helpful constatns
	//----------------
	private static final int SIZEOF_BYTE = 1;
	private static final int SIZEOF_SHORT = 2;
	private static final int SIZEOF_INT = 4;
	
	//----------------
	//Constructors
	//----------------
	
	//BitMapFile class; an index file with given filename
	//should already exist, then this opens it.
	//mirror from BTreeFile
	public BitMapFile( String filename )
		throws GetFileEntryException,  
		   PinPageException, 
		   ConstructPageException    
	{
		//System.out.println("\tDEBUG -- fetch existing " + filename);
		
		//get the id of the page for the passed filename
		bmFileId = get_file_entry(filename);
		
		//open the existing element map
		indexOfMappedElements = new BTreeFile( filename );
		
		dbname = new String(filename);
	}
	
	//BitMapFile class
	//Takes a columnar file, a column to map, and the ValueClass type to map
	//Checks if the passed filename exists to open, else makes a new one
	//if a new one is made it will iterate each record in the corresponding
	//column's heapfile and insert it into the bitmap
	public BitMapFile( String filename, Columnarfile columnfile,
						int ColumnNo, ValueClass value )
		throws GetFileEntryException, ConstructPageException,
				IOException, AddFileEntryException, HFBufMgrException,
				HFException, HFDiskMgrException, PinPageException
	{
		//get the id of the page for the passed filename
		bmFileId = get_file_entry(filename);
		//file not exist, create one
		if( bmFileId == null )
		{
			//System.out.println("\tDEBUG -- open new " + filename);
			
			//associate the columnarfile input
			srcColumnar = columnfile;
			//store the column count
			columnMap = ColumnNo;
			//get the type of map this is (string/int) which we'll
			//use to validate correctness of insertions
			mapType = value.getType();
			
			//create a BTree containing the elements that exist in thie bitmap file
			//Some added care will be needed to prevent duplicate entries
			int deleteType = 1; // DeleteFashion.FULL_DELETE argument
			int maxKeySizeArg = 0;
			if (AttrType.attrInteger == mapType)
			{
				maxKeySizeArg = 5;
			}
			else if (AttrType.attrString == mapType)
			{
				maxKeySizeArg = GlobalConst.MAX_NAME;
			}
			// Create the BTree
			indexOfMappedElements = new BTreeFile(filename, mapType, maxKeySizeArg, deleteType);
			
			//Map column values from columnar file into the bitmap
			/*
			---Pseudo-code of bitmap creation---
			for page in HeapFile@columnNo
				for record in page
					Map value
			*/
			RID rid = new RID();
			int mapProgress = 0;
			try
			{
				//open a scan of the corresponding columnar column Heapfile
				Scan scan = columnfile.openColumnScan(columnMap);
				Tuple tuple = scan.getNext(rid);
				//iterate and insert each record
				while (tuple != null)
				{
					//nice helper to indicate progress is being made
					mapProgress++;
					if( 0 == (mapProgress % 1000) )
					{
						System.out.println("Mapped " + mapProgress + " entries");
					}
					//identify if we should insert strings or integers
					if( mapType == AttrType.attrInteger )
					{
						//get the value to insert
						int intValue = Convert.getIntValue(0, tuple.getTupleByteArray());
                        IntegerValueClass tmpValueClass = new IntegerValueClass( intValue );
						//get the position to use
						int position = columnfile.getPositionFromRid( rid, ColumnNo );
						//set the bit at that position
						Insert( tmpValueClass, position );
					}
					if( mapType == AttrType.attrString  )
					{
						byte[] byteArr = tuple.getTupleByteArray();
                        String stringValue = Convert.getStrValue(0, byteArr, byteArr.length);
                        StringValueClass tmpValueClass = new StringValueClass( stringValue );
						//get the position to use
						int position = columnfile.getPositionFromRid( rid, ColumnNo );
						//set the bit at that position
						Insert( tmpValueClass, position );
					}
					//get next entry in scan
					tuple = scan.getNext(rid);
				}
				//all entries scanned, close scanner
				scan.closescan();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			
		}
		else //else opening existing file
		{
			//System.out.println("\tDEBUG -- open existing " + filename);
			
			//associate the columnarfile input
			srcColumnar = columnfile;
			//store the column count
			columnMap = ColumnNo;
			//get the type of map this is (string/int) which we'll
			//use to validate correctness of insertions
			mapType = value.getType();
			
			//open the existing element map
			indexOfMappedElements = new BTreeFile( filename );
		}
		
		dbname = new String(filename);
	}
	
	//----------------
	//Page Manipulation Methods
	//(mirrored from BTreeFile.java)
	//----------------
	
	private PageId get_file_entry(String filename)
		throws GetFileEntryException
	{
		try
		{
			return SystemDefs.JavabaseDB.get_file_entry(filename);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new GetFileEntryException(e,"");
		}
	}
	
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
	
	private Page pinPage(PageId pageno)
		throws PinPageException
	{
		try
		{
			Page page = new Page();
			SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
			return page;
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new PinPageException(e,"");
		}
	}
  
	private void add_file_entry(String fileName, PageId pageno)
		throws AddFileEntryException
	{
		try
		{
			SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new AddFileEntryException(e,"");
		}
	}
  
	private void unpinPage(PageId pageno)
		throws UnpinPageException
	{ 
		try
		{
			SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new UnpinPageException(e,"");
		}
	}
	  
	private void freePage(PageId pageno)
		throws FreePageException
	{
		try
		{
			SystemDefs.JavabaseBM.freePage(pageno);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new FreePageException(e,"");
		}
	}
	
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
		
	private void delete_file_entry(String filename)
		throws DeleteFileEntryException
	{
		try
		{
			SystemDefs.JavabaseDB.delete_file_entry( filename );
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new DeleteFileEntryException(e,"");
		}
	}
	
	//----------------
	//Accessor Methods
	//----------------
	
	public BTreeFile getIndexBTree()
	{
		return indexOfMappedElements;//btree of headerPages
	}
	
	public void setSrcColumnarFile( Columnarfile colFile )
	{
		srcColumnar = colFile;
	}
	
	public Columnarfile getSrcColumnarFile()
	{
		return srcColumnar;
	}
	
	public void setMappedColumn( int columnNo )
	{
		columnMap = columnNo;
	}
	
	public int getMappedColumn()
	{
		return columnMap;
	}
	
	public void setMapType( int newType )
	{
		mapType = newType;
	}
	
	public int getMapType()
	{
		return mapType;
	}
	
	//----------------
	//Clean-up Methods
	//----------------
	
	//Close the BitMap File
	//mirror from BTreeFile::close()
	public void close()
		throws PageUnpinnedException, InvalidFrameNumberException,
				HashEntryNotFoundException, ReplacerException
	{
		//check a valid file is being closed
		indexOfMappedElements.close();
	}
	
	//Destroy entire BitMap file
	//mirror from BTreeFile::destroyBTreeFile()
	public void destroyBitMapFile()
		throws IOException, IteratorException, UnpinPageException,
				FreePageException, DeleteFileEntryException,
				ConstructPageException, PinPageException
	{
		//confirm non-null page to destroy
		indexOfMappedElements.destroyFile();
	}
	
	//----------------
	//Functional Methods
	//----------------
	
	/*
	 * This method will take a ValueClass argument and then check the BTree tracking
	 * unique values mapped by this BitMapFile. If it finds the value is already in
	 * the BTree it will return a RID with the PageNo set to the first page in the
	 * doubly linked BMPage list for that value
	 */
	public RID valueIsMapped( ValueClass compare )
		throws IOException
	{
		PageId tmpPid = new PageId( -1 );
		RID ridMatch = new RID( tmpPid, -1 );
		ridMatch.pageNo.pid = -1;
		try
		{
			BTFileScan btfScan = null;
			//set up search criteria according to if this map is integer or string
			if( AttrType.attrInteger == mapType )
			{
				// Creates a BTreeFile object using the index column name
				int tmpInt = ((IntegerValueClass)compare).classValue;
				IntegerKey intKey = new IntegerKey( tmpInt );
				btfScan = indexOfMappedElements.new_scan(intKey, intKey);
			}
			else
			{
				// Creates a BTreeFile object using the index column name
				String tmpString = ((StringValueClass)compare).classValue;
				StringKey strKey = new StringKey( tmpString );
				btfScan = indexOfMappedElements.new_scan(strKey, strKey);
			}
			
			//Iterate through B-Tree index entires
			//it is expected to only ever find 1 match if any
			KeyDataEntry entry;
			while( (entry = btfScan.get_next()) != null )
			{
				// Get the PageNo matching the start of this linked list of BMPages
				ridMatch = ((LeafData)entry.data).getData();
			}
			
			btfScan.DestroyBTreeFileScan();
		}
		catch( Exception ex )
		{
			System.out.println("Error in BitMapFile::valueIsMapped()");
			ex.printStackTrace();
		}
		
		return ridMatch;
	}
	
	/*
	 * This method takes in a Value Class and RID. It will check if the RID is pointing
	 * to an existing PageId, and if it is then the method will return the BitMapHeaderPage
	 * pointed to by that PageId
	 * If the RID.PageId is invalid, then we will create a new BitMapHeaderPage for the value
	 * in the Value Class.
	 */
	public BitMapHeaderPage findOrCreateHeader( ValueClass checkVal, RID listStartPage )
		throws ConstructPageException, IOException, HFException, HFBufMgrException,
				KeyTooLongException, KeyNotMatchException, LeafInsertRecException,   
				IndexInsertRecException, UnpinPageException, PinPageException, 
				NodeNotMatchException, ConvertException, DeleteRecException,
				IndexSearchException, IteratorException, LeafDeleteException, 
				InsertException
	{
		BitMapHeaderPage pageToReturn = null;
		
		if( -1 == listStartPage.pageNo.pid )
		{
			//need a new page for this element value
			BitMapHeaderPage newBMHeader = new BitMapHeaderPage();
			//associate the new BMHeader Page to this RID
			listStartPage.pageNo.pid = newBMHeader.headBMPageId.pid;
			
			if( AttrType.attrInteger == mapType )
			{
				// Creates a BTreeFile object using the index column name
				int tmpInt = ((IntegerValueClass)checkVal).classValue;
				IntegerKey intKey = new IntegerKey( tmpInt );
				//insert the new mapped element into the BTree index
				indexOfMappedElements.insert(intKey, listStartPage);
			}
			else
			{
				// Creates a BTreeFile object using the index column name
				String tmpString = ((StringValueClass)checkVal).classValue;
				StringKey strKey = new StringKey( tmpString );
				//insert the new mapped element into the BTree index
				indexOfMappedElements.insert(strKey, listStartPage);
			}
			
			pageToReturn = newBMHeader;
		}
		else
		{
			//found matching page
			pageToReturn = new BitMapHeaderPage( listStartPage.pageNo );
		}
		
		return pageToReturn;
	}
	
	/*
	 * Takes a Value & position. It will get/create a BitMapHeaderPage
	 * for the doubly linked list set of BMPages for this value and then
	 * call the insertMap() method, and that method will set the appropriate
	 * bit in the BMPage to 1
	 */
	public boolean Insert( ValueClass value, int position )
	{
		boolean successfulInsert = false;
		
		try
		{
			//check if this element is mapped already or should be added
			//to the BTree
			RID listStartPage = valueIsMapped( value );
			if( AttrType.attrInteger == mapType )
			{
				// Creates a BTreeFile object using the index column name
				int tmpInt = ((IntegerValueClass)value).classValue;
				//System.out.println("Set bit " + position + " for value " + tmpInt + " on Page " + listStartPage.pageNo.pid ); //DEBUG PRINT
			}
			else
			{
				// Creates a BTreeFile object using the index column name
				String tmpString = ((StringValueClass)value).classValue;
				//System.out.println("Set bit " + position + " for value " + tmpString + " on Page " + listStartPage.pageNo.pid ); //DEBUG PRINT
			}
			BitMapHeaderPage pageListToUse = findOrCreateHeader( value, listStartPage );
			successfulInsert = pageListToUse.insertMap( position );
		}
		catch( Exception ex )
		{
			System.out.println("Error in BitMapFile::Insert()");
			ex.printStackTrace();
		}
		
		return successfulInsert;
	}
	
	/*
	 * This method will check if there is a linked list of BMPages for
	 * the specified value. If there is it will clear the bit at the
	 * specified position in the appropriate BMPage
	 */
	public boolean Delete( ValueClass value, int position )
	{
		boolean successfulDelete = false;
		
		try
		{
			RID index = valueIsMapped( value );
			if( -1 != index.pageNo.pid )
			{
				BitMapHeaderPage pageListToUse = findOrCreateHeader( value, index );
				successfulDelete = pageListToUse.deleteMap( position );
			}
		}
		catch( Exception ex )
		{
			System.out.println("Error in BitMapFile::Delete()");
			ex.printStackTrace();
		}
		
		return successfulDelete;
	}
	
	/*
	 * This method effectively calls Delete() on the old value & then
	 * Insert on the new value, or clear the old bit then set the new bit
	 */
	public boolean Update( ValueClass oldValue, ValueClass newValue, int position )
	{
		boolean successfulUpdate = false;
		
		try
		{
			//clear the old value & then set the new one
			successfulUpdate = Delete( oldValue, position );
			successfulUpdate = Insert( newValue, position );
		}
		catch( Exception ex )
		{
			System.out.println("Error in BitMapFile::Update()");
			ex.printStackTrace();
		}
		
		return successfulUpdate;
	}
	
	/*
	 * 
	 */
	public boolean Insert( int position )
		throws CFException, HFException, HFBufMgrException,
				HFDiskMgrException, IOException,
				InvalidTupleSizeException
	{
		boolean successfulInsert = false;
		
		//TODO -- create dynamically order list of Header Pages for different maps
		//headerPage.insertMap( position );
		
		return successfulInsert;
	}
	
	/*
	 * 
	 */
	public boolean Delete( int position )
		throws CFException, HFBufMgrException, HFException, HFDiskMgrException,
				IOException, InvalidSlotNumberException,
				InvalidTupleSizeException
	{
		boolean successfulDelete = false;
		
		//TODO -- create dynamically order list of Header Pages for different maps
		//headerPage.deleteMap( position );
		
		return successfulDelete;
	}
	
	/** create a scan with given keys
	 * Cases:
	 *      (1) lo_key = null, hi_key = null
	 *              scan the whole index
	 *      (2) lo_key = null, hi_key!= null
	 *              range scan from min to the hi_key
	 *      (3) lo_key!= null, hi_key = null
	 *              range scan from the lo_key to max
	 *      (4) lo_key!= null, hi_key!= null, lo_key = hi_key
	 *              exact match ( might not unique)
	 *      (5) lo_key!= null, hi_key!= null, lo_key < hi_key
	 *              range scan from lo_key to hi_key
	 *@param lo_key the key where we begin scanning. Input parameter.
	 *@param hi_key the key where we stop scanning. Input parameter.
	 */
	public BMFileScan new_scan(KeyClass lo_key, KeyClass hi_key)
		throws IOException, KeyNotMatchException, IteratorException, 
			ConstructPageException, PinPageException, UnpinPageException
	{
		BMFileScan scan = new BMFileScan();
		
		scan.bmFilename = dbname;
		scan.bmfile = this;
		scan.mappedIndexScan = indexOfMappedElements.new_scan(lo_key, hi_key);
		scan.curPage = null;
		scan.curPageId = null;
		scan.posIndexScan = indexOfMappedElements.new_scan(lo_key, hi_key);
		scan.posPage = null;
		scan.posPageId = null;
		
		scan.scanLowKey = lo_key;
		scan.scanHiKey = hi_key;
		
		return scan;
	}
	
	//----------------------
	//Simple implementation of IndexFile abstract methods
	//insert is used to set bits & delete will clear
	//both will get a positional argument from the RID
	//----------------------
	
	public void insert(final KeyClass data, final RID rid)
		throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,   
			IndexInsertRecException,ConstructPageException, UnpinPageException,
			PinPageException, NodeNotMatchException, ConvertException,
			DeleteRecException, IndexSearchException, IteratorException,
			LeafDeleteException, InsertException, IOException
	{
		//int position = srcColumnar.getPositionFromRid( rid, columnMap );
		
		//Due to some "throws" complications, java does not allow us to use the logic inside
		//thise try catch without throwing appropriate exceptions. However, we cannot "throw"
		//them as part of the method signature since this method defines the abstract method
		//IndexFile.Delete(), so the solution is to put all exception throwin logic in a try/catch
		try
		{
			//TODO -- create dynamically order list of Header Pages for different maps
		
		}catch ( Exception e ) {}
	}
	
	public boolean Delete(final KeyClass data, final RID rid)  
		throws  DeleteFashionException, LeafRedistributeException,RedistributeException,
			InsertRecException, KeyNotMatchException, UnpinPageException,
			IndexInsertRecException, FreePageException, RecordNotFoundException, 
			PinPageException, IndexFullDeleteException, LeafDeleteException,
			IteratorException, ConstructPageException, DeleteRecException,
			IndexSearchException, IOException
	{
		//int position = srcColumnar.getPositionFromRid( rid, columnMap );
		
		boolean successfulDelete = false;
		
		//Due to some "throws" complications, java does not allow us to use the logic inside
		//thise try catch without throwing appropriate exceptions. However, we cannot "throw"
		//them as part of the method signature since this method defines the abstract method
		//IndexFile.Delete(), so the solution is to put all exception throwin logic in a try/catch
		try
		{
			successfulDelete = true;
		
			//TODO -- create dynamically order list of Header Pages for different maps
		
		}catch ( Exception e ) {}
		
		return successfulDelete;
	}
}
