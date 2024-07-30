/*
 * File - Columnarfile.java
 *
 * Original Author - Vivian Roshan Adithan
 *
 * Description -
 *		...
 */
package columnar;

import bitmap.*;
import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.*;
import global.*;
import heap.*;
import index.IndexUtils;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;

import java.io.IOException;
import java.util.Arrays;

public class Columnarfile implements GlobalConst {
  public static int numColumns;
  public AttrType[] type;
  public short[] strSizes;
  public String[] columnNames;

  private String _fileName;
  private boolean _file_deleted;

  public String get_fileName() {
    return _fileName;
  }

  public String[] getColumnName() {
    return columnNames;
  }

  public void setColumnTypes( AttrType[] attributeTypes )
  {
	type = attributeTypes;
  }
  public AttrType[] getColumnTypes() {
    return type;
  }

  public Columnarfile(String name)
      throws FileIOException,
      InvalidPageNumberException,
      DiskMgrException,
      CFException,
      IOException,
      InvalidTupleSizeException,
      HFException,
      HFBufMgrException,
      HFDiskMgrException {
    if (SystemDefs.JavabaseDB.get_file_entry(name + ".hdr") == null) {
      throw new CFException(null, "file does not exist");
    }
    this._fileName = name;
    Heapfile hdr = new Heapfile(name + ".hdr");
    RID rid = new RID();
    Scan scan = hdr.openScan();
    try {
      Tuple tuple = scan.getNext(rid);
      if (tuple == null) {
        throw new CFException(null, "file corrupted");
      }
      byte[] data = tuple.getTupleByteArray();
      Columnarfile.numColumns = Convert.getShortValue(0, data);
      this.type = new AttrType[Columnarfile.numColumns];
      for (int i = 0; i < Columnarfile.numColumns; i++) {
        this.type[i] = new AttrType(Convert.getIntValue(2 + 4 * i, data));
      }
      tuple = scan.getNext(rid);
      if (tuple == null) {
        throw new CFException(null, "file corrupted");
      }
      data = tuple.getTupleByteArray();
      short strSizesLength = Convert.getShortValue(0, data);
      if (strSizesLength == 0) {
        this.strSizes = null;
      } else {
        this.strSizes = new short[strSizesLength];
      }
      for (int i = 0; i < strSizesLength; i++) {
        this.strSizes[i] = Convert.getShortValue(2 + 2 * i, data);
      }
      tuple = scan.getNext(rid);
      if (tuple == null) {
        throw new CFException(null, "file corrupted");
      }
      data = tuple.getTupleByteArray();
      short columnNamesLength = Convert.getShortValue(0, data);
      if (columnNamesLength != Columnarfile.numColumns) {
        throw new CFException(null, "file corrupted");
      }
      this.columnNames = new String[columnNamesLength];
      for (int i = 0; i < Columnarfile.numColumns; i++) {
        this.columnNames[i] = Convert.getStrValue(2 + 50 * i, data, 50);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      scan.closescan();
    }
  }

  /*
   * Constructor summary
   * Columnarfile(java.lang.String name, int numColumns, AttrType[] type)
   * Initialize: if columnar file does not exits, create one
   * heapfile (‘‘name.columnid’’) per column; also create a
   * ‘‘name.hdr’’ file that contains relevant metadata.
   */
  public Columnarfile(String name, int numColumns, AttrType[] type,
      short[] sSizes, String[] colNames)
      throws HFException,
      HFBufMgrException,
      HFDiskMgrException,
      IOException,
      InvalidSlotNumberException,
      InvalidTupleSizeException,
      SpaceNotAvailableException,
      CFException,
      FileIOException,
      InvalidPageNumberException,
      DiskMgrException {
    if (SystemDefs.JavabaseDB.get_file_entry(name + ".hdr") != null) {
      throw new CFException(null, "file alread exists");
    }
    _file_deleted = true;
    Columnarfile.numColumns = numColumns;
    this.type = type;
    this._fileName = name;
    this.strSizes = sSizes;
    this.columnNames = colNames;
    Heapfile hdr = new Heapfile(name + ".hdr");
    byte[] data = new byte[2 + 4 * numColumns];
    Convert.setShortValue((short) Columnarfile.numColumns, 0, data);
    for (int i = 0; i < Columnarfile.numColumns; i++) {
      Convert.setIntValue(this.type[i].attrType, 2 + 4 * i, data);
    }
    hdr.insertRecord(data);
    data = new byte[2 + 2 * numColumns];
    if (sSizes == null) {
      data = new byte[2];
      Convert.setShortValue((short) 0, 0, data);
    } else {
      Convert.setShortValue((short) sSizes.length, 0, data);
      for (int i = 0; i < sSizes.length; i++) {
        Convert.setShortValue(sSizes[i], 2 + 2 * i, data);
      }
    }
    hdr.insertRecord(data);
    data = new byte[2 + 50 * Columnarfile.numColumns];
    Convert.setShortValue((short) Columnarfile.numColumns, 0, data);
    for (int i = 0; i < numColumns; i++) {
      Convert.setStrValue(this.columnNames[i], 2 + 50 * i, data);
    }
    hdr.insertRecord(data);
    for (int i = 0; i < numColumns; i++) {
      new Heapfile(name + "." + Integer.toString(i + 1));
    }
    new Heapfile(name + ".deleted");
    try {
      new BTreeFile(
          name + ".deletedBTree",
          AttrType.attrInteger,
          4, // keysize
          DeleteFashion.FULL_DELETE)
          .close();
    } catch (Exception e) {
      e.printStackTrace();
      throw new CFException(e, "Unable to create btree deleted file");
    }
    _file_deleted = false;
  }

  // Delete all relevant files from the database.
  public void deleteColumnarFile()
      throws FileAlreadyDeletedException,
      CFException {
    if (_file_deleted)
      throw new FileAlreadyDeletedException(null, "file alread deleted");
    boolean isCFException = false;
    try {
      Heapfile hdr = new Heapfile(_fileName + ".hdr");
      hdr.deleteFile();
    } catch (Exception e) {
      isCFException = true;
      System.err.println("FileName :" + _fileName + ".hdr"
          + " deleteColumnarFile: " + e);
    }
    for (int i = 0; i < numColumns; i++) {
      try {
        Heapfile hf = new Heapfile(_fileName + "." + Integer.toString(i));
        hf.deleteFile();
      } catch (Exception e) {
        isCFException = true;
        System.err.println("FileName :" + _fileName + "." + Integer.toString(i)
            + " deleteColumnarFile: " + e);
      }
    }
    try {
      Heapfile deleted = new Heapfile(_fileName + ".deleted");
      deleted.deleteFile();
    } catch (Exception e) {
      isCFException = true;
      System.err.println("FileName :" + _fileName + ".deleted"
          + " deleteColumnarFile: " + e);
    }
    try {
      BTreeFile deleted = new BTreeFile(_fileName + ".deletedBTree");
      deleted.destroyFile();
    } catch (Exception e) {
      isCFException = true;
      System.err.println("FileName :" + _fileName + ".deletedBTree"
          + " deleteColumnarFile: " + e);
    }
    if (isCFException) {
      throw new CFException(null, "deleteColumnarFile failed");
    }
  }

  // Insert tuple into file, return its tid
  public TID insertTuple(byte[] tuplePtr)
      throws CFException,
      InvalidSlotNumberException,
      InvalidTupleSizeException,
      SpaceNotAvailableException,
      HFException,
      HFBufMgrException,
      HFDiskMgrException,
      IOException {
    int position = -1;
    RID[] recordIDs = new RID[numColumns];
    short fldCnt = Convert.getShortValue(0, tuplePtr);
    if (numColumns != fldCnt) {
      throw new CFException(null, "Number of columns in tuple != Columnarfile");
    }
    short[] fldOffset = new short[fldCnt + 1];
    for (int i = 0; i < fldCnt + 1; i++) {
      fldOffset[i] = Convert.getShortValue(2 * (i + 1), tuplePtr);
    }
    for (int i = 0; i < fldCnt; i++) {
      int length = fldOffset[i + 1] - fldOffset[i];
      byte[] data = new byte[length];
      System.arraycopy(tuplePtr, fldOffset[i], data, 0, length);
      Heapfile hf = new Heapfile(_fileName + "." + Integer.toString(i + 1));
      Heapfile.RIDPosition ridPosition = hf.insertRecordRaw(data);
      recordIDs[i] = ridPosition.rid;
      if (position != -1 && position != ridPosition.position) {
        throw new CFException(null, "Insertion failed: position mismatch");
      } else {
        position = ridPosition.position;
      }
    }
    return new TID(numColumns, position, recordIDs);
  }

  // Read the tuple with the given tid from the columnar file
  public Tuple getTuple(TID tid)
      throws IOException,
      Exception {
    Tuple[] tupleByte2DArray = new Tuple[numColumns];
    for (int i = 0; i < numColumns; i++) {
      Heapfile hf = new Heapfile(_fileName + "." + Integer.toString(i + 1));
      tupleByte2DArray[i] = hf.getRecordNoCheck(tid.recordIDs[i]);
    }
    return getTupleFromColTuples(tupleByte2DArray);
  }

  /*
   * Read the tuple with the given tid from the columnar file
   * and the list of columns specified in the projlist are alone are obtained
   * from the columnar file other columns are filled with default values
   */
  public Tuple getTupleProj(TID tid, FldSpec[] projlist)
      throws IOException,
      Exception {
    if (projlist == null) {
      return getTuple(tid);
    }
    Integer[] projOffset = new Integer[projlist.length];
    for (int i = 0; i < projlist.length; i++) {
      projOffset[i] = projlist[i].offset;
    }
    Tuple[] tupleByte2DArray = new Tuple[numColumns];
    for (int i = 0; i < numColumns; i++) {
      if (Arrays.asList(projOffset).contains(i + 1)) {
        Heapfile hf = new Heapfile(_fileName + "." + Integer.toString(i + 1));
        tupleByte2DArray[i] = hf.getRecordNoCheck(tid.recordIDs[i]);
      } else {
        byte[] data = null;
        switch (type[i].attrType) {
          case AttrType.attrInteger:
            data = new byte[4];
            Convert.setIntValue(0, 0, data);
            tupleByte2DArray[i] = new Tuple(data, 0, 4);
            break;
          case AttrType.attrString:
            int s_size_index = -1;
            for (int j = 0; j < this.type.length; j++) {
              if (this.type[j].attrType == AttrType.attrString && j != i) {
                s_size_index++;
              }
            }
            String s = new String();
            data = new byte[strSizes[s_size_index] + 2];
            Convert.setStrValue(s, 0, data);
            tupleByte2DArray[i] = new Tuple(data, 0, data.length);
            break;
          case AttrType.attrReal:
            data = new byte[4];
            Convert.setFloValue(0, 0, data);
            tupleByte2DArray[i] = new Tuple(data, 0, 4);
            break;
          default:
            throw new CFException(null, "Invalid attribute type");

        }
      }
    }
    return getTupleFromColTuples(tupleByte2DArray);
  }

  public Tuple getTupleFromColTuples(Tuple[] colTupleArray) throws IOException {
    byte[] data = new byte[MINIBASE_PAGESIZE];
    Convert.setShortValue((short) numColumns, 0, data);
    short[] fldOffset = new short[numColumns + 1];
    fldOffset[0] = (short) (2 * (numColumns + 2));
    Convert.setShortValue(fldOffset[0], 2 * (0 + 1), data);
    for (int i = 0; i < numColumns; i++) {
      if (colTupleArray[i] == null) {
        continue;
      }
      byte[] tupleByteArray = colTupleArray[i].getTupleByteArray();
      int leng = tupleByteArray.length;
      fldOffset[i + 1] = (short) (fldOffset[i] + leng);
      Convert.setShortValue(fldOffset[i + 1], 2 * ((i + 1) + 1), data);
      System.arraycopy(tupleByteArray, 0, data, fldOffset[i], leng);
    }
    Convert.setShortValue(fldOffset[numColumns], 2 * (numColumns + 1), data);
    byte[] tupleData = new byte[fldOffset[numColumns]];
    System.arraycopy(data, 0, tupleData, 0, fldOffset[numColumns]);
    Tuple t = new Tuple(tupleData, 0, fldOffset[numColumns]);
    t.setFldOffset(fldOffset);
    return t;
  }

	// Read the value with the given column and tid from the columnar file
	public ValueClass getClassType( int column )
		throws InvalidSlotNumberException,
		InvalidTupleSizeException,
		Exception
	{
		switch (type[column - 1].attrType)
		{
			case AttrType.attrInteger:
				return new IntegerValueClass(0);
			case AttrType.attrString:
				return new StringValueClass("null");
			case AttrType.attrReal:
				return new FloatValueClass(0);
			default:
				throw new CFException(null, "Invalid attribute type");
		}
	}

	// Read the value with the given column and tid from the columnar file
	public ValueClass getValue(TID tid, int column)
		throws InvalidSlotNumberException,
		InvalidTupleSizeException,
		Exception
	{
		Heapfile hf = new Heapfile(_fileName + "." + Integer.toString(column));
		Tuple t = hf.getRecord(tid.recordIDs[column - 1]);
		if (t == null)
		{
			throw new CFException(null, "Invalid TID");
		}
		byte[] data = t.getTupleByteArray();
		switch (type[column - 1].attrType)
		{
			case AttrType.attrInteger:
				return new IntegerValueClass(Convert.getIntValue(0, data));
			case AttrType.attrString:
				return new StringValueClass(Convert.getStrValue(0, data, data.length));
			case AttrType.attrReal:
				return new FloatValueClass(Convert.getFloValue(0, data));
			default:
				throw new CFException(null, "Invalid attribute type");
		}
	}

  // Return the number of tuples in the columnar file.
  public int getTupleCnt()
      throws HFException,
      InvalidSlotNumberException,
      InvalidTupleSizeException,
      HFDiskMgrException,
      HFBufMgrException,
      IOException {
    return new Heapfile(_fileName + ".1").getRecCnt();
  }

  // Initiate a sequential scan of tuples.
  public TupleScan openTupleScan()
      throws HFException,
      HFBufMgrException,
      HFDiskMgrException,
      IOException,
      InvalidTupleSizeException {
    return new TupleScan(this);
  }

  // Initiate a sequential scan along a given column.
  public Scan openColumnScan(int columnNo)
      throws HFException,
      InvalidTupleSizeException,
      IOException,
      HFBufMgrException,
      HFDiskMgrException {
    Heapfile hf = new Heapfile(_fileName + "." + Integer.toString(columnNo));
    return hf.openScan();
  }

  // Updates the specified record in the columnar file.
  public boolean updateTuple(TID tid, Tuple newtuple) {
    boolean status = true;
    for (int i = 0; i < numColumns; i++) {
      try {
        updateColumnofTuple(tid, newtuple, i + 1);
      } catch (Exception e) {
        System.err.println("updateTuple: " + e);
        status = false;
      }
    }
    return status;
  }

  // Updates the specified column of the specified record in the columnar file
  public boolean updateColumnofTuple(TID tid, Tuple newtuple, int column)
      throws CFException,
      HFException,
      HFBufMgrException,
      HFDiskMgrException,
      IOException,
      InvalidSlotNumberException,
      InvalidUpdateException,
      InvalidTupleSizeException,
      Exception {
    byte[] newTuplePtr = newtuple.getTupleByteArray();
    short fldCnt = Convert.getShortValue(0, newTuplePtr);
    if (numColumns != fldCnt) {
      throw new CFException(null, "Number of columns in tuple != Columnarfile");
    }
    if (column > numColumns) {
      throw new CFException(null, "Column number out of range");
    }
    short[] fldOffset = new short[fldCnt + 1];
    for (int i = 0; i < fldCnt + 1; i++) {
      fldOffset[i] = Convert.getShortValue(2 * (i + 1), newTuplePtr);
    }
    int length = fldOffset[column + 1] - fldOffset[column];
    byte[] data = new byte[length];
    System.arraycopy(newTuplePtr, fldOffset[column], data, 0, length);
    Tuple newColTuple = new Tuple(data, 0, length);
    Heapfile hf = new Heapfile(_fileName + "." + Integer.toString(column));
    return hf.updateRecord(tid.recordIDs[column], newColTuple);
  }

	/*
	 * When inserting a value to column we also need to check if there
	 * are indexes based on that column that will need updates. This column
	 * insert supports that checking methodology during the insertion.
	 */
	public void addToColumnarColumn(int column, byte[] data)
	{
		try
		{
			//either create or open an existing Heapfile of "<columnar-file-name>.<column-#>"
			Heapfile heapfile = new Heapfile(_fileName + "." + Integer.toString(column + 1));
			RID rid = heapfile.insertRecord(data);
			//currently no indexes support Real typed variables
			if( AttrType.attrReal != type[column].attrType )
			{
				//these methods check if a BTree, bitmap, or cbitmap exist for this column
				//and insert the new entry if one does. they do not return success or failure
				addToBTree(column, data, rid);
				addToBitmap(column, data, rid);
			}
		}
		catch (Exception e)
		{
			//something went wrong and we failed to create the BTree
			e.printStackTrace();
		}
	}

	/*
	 * If it doesn’t exist, create a BTree index for the given column
	 * As part of creation, it will insert all values currently in the
	 * Column Heapfile being indexed.
	 * This method will return false if a btree file already exists
	 * for the specified column or it encounters an error attempting
	 * to create a new btree.
	 */
	public boolean createBTreeIndex(int column)
		throws GetFileEntryException, PinPageException, ConstructPageException,
		HFException, InvalidTupleSizeException, HFBufMgrException,
		HFDiskMgrException, IOException, KeyTooLongException,
		KeyNotMatchException, LeafInsertRecException, IndexInsertRecException,
		UnpinPageException, NodeNotMatchException, ConvertException,
		DeleteRecException, IndexSearchException, IteratorException,
		LeafDeleteException, InsertException, CFException, AddFileEntryException, PageUnpinnedException,
		InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException
	{
		boolean createdNewBtree = false;
		//create the name of the file that is being created
		String btFileName = new String(_fileName + ".btree" + Integer.toString(column));
		try
		{
			PageId existenceCheck = SystemDefs.JavabaseDB.get_file_entry(btFileName);
			//if the file already exists we do not want to open it and then re-insert
			//ever tuple from the specified column
			if( null == existenceCheck )
			{
				//indicates a new BTree was created
				createdNewBtree = true;
				BTreeFile btf;
				AttrType keyTypeArg = type[column - 1];
				int deleteType = 1; // DeleteFashion.FULL_DELETE argument
				int maxKeySizeArg;
				if
				(AttrType.attrInteger == keyTypeArg.attrType)
				{
					maxKeySizeArg = 5;
				}
				else if (AttrType.attrString == keyTypeArg.attrType)
				{
					maxKeySizeArg = GlobalConst.MAX_NAME;
				}
				else
				{
					System.out.println("Invalid attribute type only handled for Integer and String types.");
					throw new CFException(null, "Invalid attribute type only handled for Integer and String types.");
				}
				// Create the BTree
				btf = new BTreeFile(btFileName, keyTypeArg.attrType, maxKeySizeArg, deleteType);
				RID rid = new RID();
				//open a scan to the specific column this BTree is being created for
				//and begin inserting tuples sequentially
				Scan scan = this.openColumnScan(column);
				int counter = 0;
				try
				{
					Tuple tuple = scan.getNext(rid);
					//iterate until reaching the end of the scan
					while (tuple != null)
					{
						counter++;
						if( 0 == (counter%1000) )
							System.out.println("Inserted " + counter);
						if (type[column - 1].attrType == AttrType.attrInteger)
						{
							int intValue = Convert.getIntValue(0, tuple.getTupleByteArray());
							btf.insert(new IntegerKey(intValue), rid);
						}
						else if (type[column - 1].attrType == AttrType.attrString)
						{
							byte[] byteArr = tuple.getTupleByteArray();
							String stringValue = Convert.getStrValue(0, byteArr, byteArr.length);
							btf.insert(new StringKey(stringValue), rid);
						}
						//get the next tuple from the current RID
						tuple = scan.getNext(rid);
					}
				}
				catch (Exception e)
				{
					//something went wrong and we failed to create the BTree
					createdNewBtree = false;
					e.printStackTrace();
				}
				finally
				{
					//we are done inserting elements, close the scanner & Btree file
					//(this will unpin some pages pinned in support of these operations)
					scan.closescan();
					btf.close();
				}
			}
		}
		catch (Exception e)
		{
			//something went wrong and we failed to create the BTree
			e.printStackTrace();
		}
		
		return createdNewBtree;
	}
	
	/*
	 * When inserting tuples into the columnar, we should check to see if there
	 * is an existing BTree associated with the column that needs updating as well
	 */
	public void addToBTree(int column, byte[] data, RID rid)
	{
		//get the corresponding BTree name to existence check
		//when creating indexes, column 0 is treated as "1"
		String btFileName = new String(_fileName + ".btree" + Integer.toString(column+1));
		try
		{
			PageId existenceCheck = SystemDefs.JavabaseDB.get_file_entry(btFileName);
			//if the file exists we should add the data to it
			if( null != existenceCheck )
			{
				BTreeFile btf = new BTreeFile( btFileName );
				if (type[column].attrType == AttrType.attrInteger)
				{
					int intValue = Convert.getIntValue(0, data);
					btf.insert(new IntegerKey(intValue), rid);
				}
				else if (type[column].attrType == AttrType.attrString)
				{
					String stringValue = Convert.getStrValue(0, data, data.length);
					btf.insert(new StringKey(stringValue), rid);
				}
				btf.close();
			}
		}
		catch (Exception e)
		{
			//something went wrong and we failed to open the BTree
			e.printStackTrace();
		}
	}

	/*
	 * If it doesn’t exist, create a Bitmap index for the given column
	 * As part of creation, it will map all values currently in the
	 * Column Heapfile being bitmapped (occurs in the called constructor).
	 * This method will return false if a bitmap file already exists
	 * for the specified column or it encounters an error attempting
	 * to create a new bitmap.
	 */
	public boolean createBitMapIndex(int columnNo, ValueClass value, boolean compressed)
		throws GetFileEntryException, ConstructPageException,
		IOException, AddFileEntryException, HFBufMgrException,
		HFException, HFDiskMgrException
	{
		boolean createdNewBitmap = false;
		//create the name of the file that is being created
		String bmFileName;
		if( compressed )
		{
			bmFileName = new String(_fileName + ".cbitmap" + Integer.toString(columnNo));
		}
		else
		{
			bmFileName = new String(_fileName + ".bitmap" + Integer.toString(columnNo));
		}
		try
		{
			PageId existenceCheck = SystemDefs.JavabaseDB.get_file_entry(bmFileName);
			//if the file already exists we do not want to open it and then re-insert
			//ever tuple from the specified column
			if( null == existenceCheck )
			{
				createdNewBitmap = true;
				if( compressed )
				{
					CBitMapFile tmpBMF;
					// note -- the value in the ValueClass is arbitrary for this constructor
					// the main focus is getting a column type argument passed in correctly
					int keyTypeArg = type[columnNo - 1].attrType;
					if( AttrType.attrInteger == keyTypeArg )
					{
						IntegerValueClass valueType = new IntegerValueClass();
						tmpBMF = new CBitMapFile(bmFileName, this, columnNo, valueType);
					}
					else
					{
						StringValueClass valueType = new StringValueClass();
						tmpBMF = new CBitMapFile(bmFileName, this, columnNo, valueType);
					}
					tmpBMF.close();
				}
				else
				{
					BitMapFile tmpBMF;
					// note -- the value in the ValueClass is arbitrary for this constructor
					// the main focus is getting a column type argument passed in correctly
					int keyTypeArg = type[columnNo - 1].attrType;
					if( AttrType.attrInteger == keyTypeArg )
					{
						IntegerValueClass valueType = new IntegerValueClass();
						tmpBMF = new BitMapFile(bmFileName, this, columnNo, valueType);
					}
					else
					{
						StringValueClass valueType = new StringValueClass();
						tmpBMF = new BitMapFile(bmFileName, this, columnNo, valueType);
					}
					tmpBMF.close();
				}
			}
			else
			{
				System.out.println( "ALERT: " + bmFileName + " already exists, will not create another.");
			}
		}
		catch(Exception e)
		{
			//something went wrong and we failed to create the bitmap
			e.printStackTrace();
		}

		return createdNewBitmap;
	}
	
	/*
	 * When inserting tuples into the columnar, we should check to see if there
	 * is an existing BTree associated with the column that needs updating as well
	 */
	public void addToBitmap(int column, byte[] data, RID rid)
	{
		//when creating indexes, column 0 is treated as "1"
		int columnNo = column+1;
		//get the corresponding bitmap name to existence check
		String bmFileName = new String(_fileName + ".bitmap" + Integer.toString(columnNo));
		String cbmFileName = new String(_fileName + ".cbitmap" + Integer.toString(columnNo));
		try
		{
			PageId existenceCheck = SystemDefs.JavabaseDB.get_file_entry(cbmFileName);
			//if the file exists we should add the data to it
			if( null != existenceCheck )
			{
				CBitMapFile tmpBMF = new CBitMapFile( cbmFileName );
				tmpBMF.setSrcColumnarFile( this );
				tmpBMF.setMappedColumn( columnNo );
				int keyTypeArg = type[columnNo - 1].attrType;
				tmpBMF.setMapType( keyTypeArg );
				if( AttrType.attrInteger == keyTypeArg )
				{
					//get the value to insert
					int intValue = Convert.getIntValue(0, data);
                    IntegerValueClass tmpValueClass = new IntegerValueClass( intValue );
					//get the position to use
					int position = getPositionFromRid( rid, columnNo );
					//set the bit at that position
					tmpBMF.Insert( tmpValueClass, position );
				}
				else
				{
                    String stringValue = Convert.getStrValue(0, data, data.length);
                    StringValueClass tmpValueClass = new StringValueClass( stringValue );
					//get the position to use
					int position = getPositionFromRid( rid, columnNo );
					//set the bit at that position
					tmpBMF.Insert( tmpValueClass, position );
				}
				tmpBMF.close();
			}
			
			existenceCheck = SystemDefs.JavabaseDB.get_file_entry(bmFileName);
			if( null != existenceCheck )
			{
				BitMapFile tmpBMF = new BitMapFile( bmFileName );
				tmpBMF.setSrcColumnarFile( this );
				tmpBMF.setMappedColumn( columnNo );
				int keyTypeArg = type[columnNo - 1].attrType;
				tmpBMF.setMapType( keyTypeArg );
				if( AttrType.attrInteger == keyTypeArg )
				{
					//get the value to insert
					int intValue = Convert.getIntValue(0, data);
                    IntegerValueClass tmpValueClass = new IntegerValueClass( intValue );
					//get the position to use
					int position = getPositionFromRid( rid, columnNo );
					//set the bit at that position
					tmpBMF.Insert( tmpValueClass, position );
				}
				else
				{
                    String stringValue = Convert.getStrValue(0, data, data.length);
                    StringValueClass tmpValueClass = new StringValueClass( stringValue );
					//get the position to use
					int position = getPositionFromRid( rid, columnNo );
					//set the bit at that position
					tmpBMF.Insert( tmpValueClass, position );
				}
				tmpBMF.close();
			}
		}
		catch (Exception e)
		{
			//something went wrong and we failed to open the bitmaps
			e.printStackTrace();
		}
	}

  // add the tuple to a heapfile tracking the deleted tuples from the columnar
  // file
  public boolean markTupleDeleted(TID tid)
      throws IOException,
      HFException,
      HFBufMgrException,
      HFDiskMgrException,
      InvalidSlotNumberException,
      InvalidTupleSizeException,
      SpaceNotAvailableException,
      HFException,
      HFBufMgrException,
      HFDiskMgrException,
      CFException {

    if (isTupleMarkedDeleted(tid.position)) {
      return true;
    }

    Tuple tuple = new Tuple();
    short numTidFlds = (short) (2 + numColumns * 2);
    int offest = 2 * (1 + numTidFlds + 1);
    AttrType[] attrType = new AttrType[numTidFlds];
    for (int i = 0; i < numTidFlds; i++) {
      attrType[i] = new AttrType(AttrType.attrInteger);
    }
    try {
      tuple.setHdr(numTidFlds, attrType, null);
    } catch (InvalidTypeException e) {
      e.printStackTrace();
    }
    byte[] tupleData = tuple.getTupleByteArray();
    tid.writeToByteArray(tupleData, offest);
    RID rid = new Heapfile(_fileName + ".deleted").insertRecord(tupleData);
    try {
      BTreeFile btf = new BTreeFile(_fileName + ".deletedBTree");
      btf.insert(new IntegerKey(tid.position), rid);
      btf.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return true;

  }

  // merge all deleted tuples from the file and all from all index files.
  public boolean purgeAllDeletedTuples()
      throws IOException,
      HFDiskMgrException,
      HFBufMgrException,
      HFException,
      InvalidTupleSizeException,
      InvalidSlotNumberException,
      CFException,
      Exception {
    Heapfile deletedHf = new Heapfile(_fileName + ".deleted");

    // setting up the Iterator for deleted sorted scan
    short numTidFlds = (short) (2 + numColumns * 2);
    int offset = 2 * (1 + numTidFlds + 1);
    AttrType[] attrType = new AttrType[numTidFlds];
    for (int i = 0; i < numTidFlds; i++) {
      attrType[i] = new AttrType(AttrType.attrInteger);
    }
    FldSpec[] projlist = new FldSpec[numTidFlds];
    for (int i = 0; i < numTidFlds; i++) {
      projlist[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
    }
    FileScan deletedFileScan = new FileScan(
        _fileName + ".deleted",
        attrType,
        null, // strSizes[]
        (short) numTidFlds, // in_flds
        numTidFlds, // out_flds
        projlist,
        null // condExpr
    );

    Tuple tuple = deletedFileScan.get_next();
    while (tuple != null) {
      TID tid = new TID(tuple.getTupleByteArray(), offset);
      // System.out.println("Purging TID: " + tid.position);
      boolean isDeleted = this._deleteTuple(tid, true);
      if (isDeleted == false) {
        throw new CFException(null, "purgeAllDeletedTuples failed: deletedfile");
      }
      tuple = deletedFileScan.get_next();
    }
    deletedFileScan.close();

    // purging
    boolean isDeleted = false;
    TupleScan tupleScan = this.openTupleScan();
    TID tid = new TID(Columnarfile.numColumns, -1, new RID[numColumns]);
    Tuple toBeReinserted = new Tuple();
    TID toBeReinsertedTID = new TID(Columnarfile.numColumns);
    tuple = tupleScan.getNext(tid);
    while (tuple != null) {
      toBeReinserted = new Tuple(tuple);
      toBeReinsertedTID.copyTid(tid);
      tuple = tupleScan.getNext(tid);
      isDeleted = this._deleteTuple(toBeReinsertedTID, true);
      if (isDeleted == false) {
        throw new CFException(null, "purgeAllDeletedTuples failed: delete");
      }
      toBeReinsertedTID = this.insertTuple(toBeReinserted.getTupleByteArray());
      if (toBeReinsertedTID == null) {
        throw new CFException(null, "purgeAllDeletedTuples failed: reinsert");
      }
    }
    tupleScan.closetuplescan();
    deletedHf.deleteFile();
    deletedHf = new Heapfile(_fileName + ".deleted");

    new BTreeFile(_fileName + ".deletedBTree").destroyFile();
    new BTreeFile(
        _fileName + ".deletedBTree",
        AttrType.attrInteger,
        4, // keysize
        DeleteFashion.FULL_DELETE)
        .close();

    // purging Data pages and Directory pages
    for (int i = 0; i < numColumns; i++) {
      Heapfile hf = new Heapfile(_fileName + "." + Integer.toString(i + 1));
      hf.purgeDataPages();
      hf.purgeDirPages();
    }

    return true;
  }

  // return the position of the rid in the columnar files otherwise -1
  public int getPositionFromRid(RID rid, int column)
      throws InvalidTupleSizeException,
      IOException,
      HFException,
      HFBufMgrException,
      HFDiskMgrException {
    Heapfile hf = new Heapfile(_fileName + "." + Integer.toString(column));
    try {
      return hf.getPositionFromRid(rid);
    } catch (Exception e) {
      throw new HFException(e, "getPositionFromRid failed");
    }
  }

  // return the rid from the position in the columnar files otherwise throws
  // exception
  public RID getRidFromPosition(int position, int column)
      throws InvalidTupleSizeException,
      IOException,
      HFException,
      HFBufMgrException,
      HFDiskMgrException,
      CFException {
    Heapfile hf = new Heapfile(_fileName + "." + Integer.toString(column));
    try {
      return hf.getRidFromPosition(position);
    } catch (Exception e) {
      throw new CFException(e, "getRidFromPosition failed");
    }
  }

  // return the tid from the position in the columnar files otherwise throws
  // exception
  public TID getTidFromPosition(int position)
      throws InvalidTupleSizeException,
      HFException,
      HFBufMgrException,
      HFDiskMgrException,
      CFException,
      IOException {
    RID[] recordIDs = new RID[numColumns];
    for (int i = 0; i < numColumns; i++) {
      recordIDs[i] = getRidFromPosition(position, i + 1);
    }
    return new TID(numColumns, position, recordIDs);
  }

  public TID getTidFromPosition(
      int position,
      RID[] recordIDs,
      FldSpec[] projlist)
      throws InvalidTupleSizeException,
      HFException,
      HFBufMgrException,
      HFDiskMgrException,
      CFException,
      IOException {
    Integer[] projOffset;
    if (projlist == null) {
      projOffset = new Integer[numColumns];
      for (int i = 0; i < numColumns; i++) {
        projOffset[i] = i + 1;
      }
    } else {
      projOffset = new Integer[projlist.length];
      for (int i = 0; i < projlist.length; i++) {
        projOffset[i] = projlist[i].offset;
      }
    }
    if (recordIDs == null) {
      recordIDs = new RID[numColumns];
    }
    for (int i = 0; i < numColumns; i++) {
      if (recordIDs[i] == null) {
        if (Arrays.asList(projOffset).contains(i + 1)) {
          recordIDs[i] = getRidFromPosition(position, i + 1);
        } else {
          recordIDs[i] = new RID();
        }
      }
    }
    return new TID(numColumns, position, recordIDs);
  }

  private boolean _deleteTuple(TID tid, boolean isPreserveDirPages)
      throws InvalidSlotNumberException,
      InvalidTupleSizeException,
      HFException,
      HFBufMgrException,
      HFDiskMgrException,
      Exception {
    boolean isDeleted = true;
    for (int i = 0; i < numColumns; i++) {
      Heapfile colHf = new Heapfile(_fileName + "." + Integer.toString(i + 1));
      boolean isColDeleted = false;
      if (isPreserveDirPages) {
        isColDeleted = colHf.deleteRecordPreserveDirPages(tid.recordIDs[i]);
      } else {
        isColDeleted = colHf.deleteRecord(tid.recordIDs[i]);
      }
      if (isColDeleted == false)
        isDeleted = false;
    }
    return isDeleted;
  }

  public boolean isTupleMarkedDeleted(int position) throws CFException {
    CondExpr[] condExpr = new CondExpr[2];
    condExpr[0] = new CondExpr();
    condExpr[0].op = new AttrOperator(AttrOperator.aopEQ);
    condExpr[0].type1 = new AttrType(AttrType.attrSymbol);
    condExpr[0].type2 = new AttrType(AttrType.attrInteger);
    condExpr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
    condExpr[0].operand2.integer = position;
    condExpr[0].next = null;
    condExpr[1] = null;
    KeyDataEntry entry = null;
    try {
      BTreeFile btf = new BTreeFile(_fileName + ".deletedBTree");
      BTFileScan btscan = (BTFileScan) IndexUtils.BTree_scan(condExpr, btf);
      entry = btscan.get_next();
      ((BTFileScan) btscan).DestroyBTreeFileScan();
      btf.close();
    } catch (Exception e) {
      e.printStackTrace();
      throw new CFException(e, "isTupleMarkedDeleted failed");
    }
    if (entry != null) {
      return true;
    }
    return false;
  }

}
