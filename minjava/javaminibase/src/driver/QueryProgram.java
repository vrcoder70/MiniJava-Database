package driver; 

import java.io.*;
import java.lang.*;
import java.util.*;

import btree.*;
import bufmgr.*;
import global.*;
import heap.*;
import bitmap.*;
import index.*;
import columnar.*;
import heap.Tuple;
import iterator.*;
import diskmgr.*;

public class QueryProgram {
    public static void queryDB(String colDB, String colFile, String tgtCols, String colToQuery,
								String inequalityComp, String valToCheck, int bufCnt, String access, boolean isDelete ) {
        try {

            // Parse Command Line
            String columnDBName = colDB + ".minibase-db";
            String columnarFileName = colFile;
			
			//The target columns are the set of columns in the tuple to print to the screen
			//on match of the query. The argument should be passed in as "A,B,D" (and the
			//DBDriver will enforce the upper case condition).
			//Here we translate those characters into numeric column numbers to enable
			//limited prints (ie, we get tuple <valA, valB, valC> but are only printing A
			//it will print only column 1 as "valA")
            String[] targetColumnNames = tgtCols.split(",");
			int[] targetColumnIndices = new int[targetColumnNames.length];
			int asciiBase = 64; //because A is 65, so column 1 = 65 - 64
			for(int i = 0; i < targetColumnIndices.length; i++ )
			{
				//takes the first character in an input and converts it to an
				//ascii equivalent & then subtracts a base
				targetColumnIndices[i] = (int)(targetColumnNames[i].charAt(0)) - asciiBase;
			}
            
            String columnName = colToQuery;
            String operator = inequalityComp;
            String value = valToCheck;

            int numBuf = bufCnt;
            String accessType = access;

            // Parse value constraints (in form of {columnName operator value})
            String[] valueConstraintsParts = new String[3];
            valueConstraintsParts[0] = columnName;
            valueConstraintsParts[1] = operator;
            valueConstraintsParts[2] = value;
            

            //Print all arguments (DEBUG Support)
            /*System.out.println(columnDBName);
            System.out.println(columnarFileName);
            System.out.println(Arrays.toString(targetColumnNames));
            System.out.println(columnName+" "+operator+" "+value);
            System.out.println(numBuf);
            System.out.println(accessType);*/

			//attempt to open the DB to query
            try {
                System.out.println("Open the DB");
                SystemDefs.MINIBASE_RESTART_FLAG = true;
                SystemDefs sysdef = new SystemDefs( columnDBName, 10000, GlobalConst.NUMBUF,"Clock"); 
            } catch (Exception e) {
				//if there's no DB, don't make one since it will be empty
                System.out.println("DB does not exists.");
                e.printStackTrace();
                return;
            }

			//verify the columnar file being queried exists
            Columnarfile columnarfile = null;
            try{
                columnarfile = new Columnarfile(columnarFileName);
                System.out.println("File exists");
            }catch(Exception e){
                System.out.println("File not present: " + columnarFileName );
            }

            // Perfrom Query Operation
            try {
                switch (accessType) {
                    case "FILESCAN":
                        System.out.println("Executing Filescan query...");
                        executeFileScanQuery(columnarfile, columnDBName, columnarFileName, targetColumnNames, valueConstraintsParts);
                        break;
                    case "COLUMNSCAN":
                        System.out.println("Executing Columnscan query...");
                        executeColumnScanQuery(columnDBName, columnarFileName, targetColumnNames, valueConstraintsParts);
                        break;
                    case "BITMAP":
                        System.out.println("Executing Bitmap query...");
                        executeBitMapQuery(columnDBName, columnarFileName, targetColumnIndices, valueConstraintsParts, isDelete);
                        break;
					case "CBITMAP":
                        System.out.println("Executing Compressed Bitmap query...");
                        executeCBitMapQuery(columnDBName, columnarFileName, targetColumnIndices, valueConstraintsParts, isDelete);
                        break;
                    case "BTREE":
                        System.out.println("Executing Btreescan query...");
                        executeBTreeQuery(columnDBName, columnarFileName, targetColumnIndices, valueConstraintsParts, isDelete);
                        break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            // Monitor Disk I/O
            // Close Resources

        }catch (Exception e){
            e.printStackTrace();
        }
    }


    private static void executeFileScanQuery(Columnarfile columnarFile, String columnDBName, String columnarFileName, String[] targetColumns, String[] valueConstraints)
    {
        PCounter.initialize();
        try {
            String columnName = valueConstraints[0];
            String operator = valueConstraints[1];
            String value = valueConstraints[2]; 

            int columnNumber = Arrays.asList(columnarFile.columnNames).indexOf(columnName)+1;
            

            // Columnar Attribute Types
            AttrType[] types = columnarFile.type;
                       
            // Prepare the output fields specification for the projection
            FldSpec[] Sprojection = new FldSpec[targetColumns.length];
            for (int i = 0; i < targetColumns.length; i++) {
                Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            }  

            // Prepare string sizes
            short[] strSizes = columnarFile.strSizes;        
            
            // Query Condition Expression
            CondExpr[] expr = new CondExpr[2];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(operator);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = types[columnNumber-1];
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), columnNumber);
            expr[0].next = null;
            expr[1] = null;
            

            System.out.println(value);
            // Fidn the Type of the real value and set the operand2
            if (expr[0].type2.attrType == AttrType.attrString) {
                expr[0].operand2.string = value;
            } else {
                expr[0].operand2.integer = Integer.parseInt(value);
            }
            
            
            // Create the file scan
            ColumnarFileScan fileScan = new ColumnarFileScan(columnarFileName, types, strSizes, (short)columnarFile.numColumns, targetColumns.length, Sprojection, expr);            
            
            
            // Retrieve tuples that match the value constraint
            Tuple tuple = new Tuple();
            while ((tuple = fileScan.get_next()) != null) {
                // Print the tuple
                tuple.print(types);                
                // Increment disk pages read count
            }            
            // Close the file scan
            fileScan.close();            
            // Output the number of disk pages read and written
            System.out.println("Number of disk pages read: " + PCounter.getReadCount());
            System.out.println("Number of disk pages written: " + PCounter.getWriteCount());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }  

    private static void executeColumnScanQuery(String columnDBName, String columnarFileName, String[] targetColumns, String[] valueConstraints)
    {
        PCounter.initialize();
       try {
            String columnName = valueConstraints[0];

            // Create a ColumnarFile object
            Columnarfile columnarFile = new Columnarfile(columnarFileName);
            
            // Get the column number for the column page name
            int columnNumber = Arrays.asList(columnarFile.columnNames).indexOf(columnName)+1;
            // System.out.println("Column Num: "+ columnNumber);
            //String columnFileName = columnarFileName+"."+Integer.toString(columnNumber);
            
            // Create a ColumnarFileScan object
            Scan scan = columnarFile.openColumnScan(columnNumber); 

            // Open File to get PageID of the column page
            //PageId pageId = SystemDefs.JavabaseDB.get_file_entry(columnFileName);

            // Get RID for first record in the page
            RID rid = new RID();
            

            // Retrieve tuples that match the value constraint
            Tuple tuple = new Tuple();
            while ((tuple = scan.getNext(rid)) != null) {
                // Print the tuple
                //tuple.print(type);
                byte[] data = tuple.getTupleByteArray();
                switch (columnarFile.type[columnNumber-1].attrType) {
                case AttrType.attrInteger:
                    System.out.println(Convert.getIntValue(0, data));
                    break;
                case AttrType.attrString:
                    System.out.println(Convert.getStrValue(0, data, data.length));
                    break;
                case AttrType.attrReal:
                    System.out.println(Convert.getFloValue(0, data));
                    break;
                }

            }

            // Print the number of disk pages read and written
            System.out.println("Number of disk pages read: " + PCounter.getReadCount());
            System.out.println("Number of disk pages written: " + PCounter.getWriteCount());

            // Close the file scan
            scan.closescan();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

	/*
	
	*/
	private static void printTupleColumns( String columnarFileName, Tuple tuple, int[] targetColumns, TID tid, boolean isDelete )
	{
		try
		{
			//get a reference for column types
			Columnarfile columnarFile = new Columnarfile(columnarFileName);
			
			if( columnarFile.isTupleMarkedDeleted(tid.position) )
			{
				return;
			}
			if (isDelete)
			{
				columnarFile.markTupleDeleted(tid);
			}
			
			System.out.print("{");
			for( int i = 0; i < targetColumns.length; i++ )
			{
				//formatting print
				if( 0 != i )
				{
					System.out.print(", ");
				}
				
				//parse the type of this column
				int printColumn = targetColumns[i];
				if( columnarFile.type[printColumn-1].attrType == AttrType.attrInteger )
				{
					System.out.print( tuple.getIntFld(printColumn) );
				}
				else if( columnarFile.type[printColumn-1].attrType == AttrType.attrReal )
				{
					System.out.print( tuple.getFloFld(printColumn) );
				}
				else if( columnarFile.type[printColumn-1].attrType == AttrType.attrString )
				{
					System.out.print( tuple.getStrFld(printColumn) );
				}
				else
				{
					System.out.print("<ERROR-PRINTING>");
				}
			}
			System.out.println("}");
		}
		catch( Exception ex )
		{
			System.out.println("<ERROR-PRINTING>");
			ex.printStackTrace();
		}
	}

	/*
	 * Method to complete a query utilizing the BTree index
	 * Takes a DB to query, a columnar file the BTree is created from, the
	 * columns of information to return on match, & the conditions of the query
	 * (in format "<column> <expression> <value>")
	 */
    private static void executeBTreeQuery(String columnDBName, String columnarFileName,
											int[] targetColumns, String[] valueConstraints,
											boolean isDelete )
    {   
        PCounter.initialize();
        // Create a BTreeFile and get the header heapfile for the columnar file
        try{
            // Parse value constraints
            String columnName = valueConstraints[0];
            String operator = valueConstraints[1];
            String value = valueConstraints[2]; 
            boolean isStringKey = false;

            
            Columnarfile columnarFile = new Columnarfile(columnarFileName);
            int columnNumber = Arrays.asList(columnarFile.columnNames).indexOf(columnName)+1;

            if(!columnarFile.createBTreeIndex(columnNumber)){
                System.out.println("BTree Index for column " + columnName + " already exists");
            }

            System.out.println(columnarFileName + ".btree" + Integer.toString(columnNumber));
            BTreeFile btf = new BTreeFile(columnarFileName + ".btree" + Integer.toString(columnNumber));

            // Create a ColumnarIndexScan
            //ColumnarIndexScan columnarIndexScan = new ColumnarIndexScan(columnName, columnNumber, 1, columnName, columnarFile.type, columnarFile.strSizes, columnarFile.numColumns, targetColumns.length, null, null, true);

            // Creates a BTreeFile object using the index column name
            IntegerKey intKey = null;
            StringKey strKey = null;
            
            //If value is a string, create a BTree with a string key, if its an integer, create a BTree with an integer key
            if (columnarFile.type[columnNumber-1].attrType == AttrType.attrInteger) {
                intKey = new IntegerKey(Integer.parseInt(value));
            } else if (columnarFile.type[columnNumber-1].attrType == AttrType.attrString) {
                // Create a key for the search value
                isStringKey = true;
                strKey = new StringKey(value);
            } 

            // Create a BTreeFileScan object
            BTFileScan btfScan = null;
            // System.out.println(operator);
            //if operator = range, use the range scan method
            if (operator.equals("RANGE")) {
                btfScan = btf.new_scan(null, null);
            }
            // If operator is less than or equal to, use the range scan method
            else if (operator.equals("<=")) {
                if (isStringKey) {
                    btfScan = btf.new_scan(null, strKey);
                } else {
                    btfScan = btf.new_scan(null, intKey);
                }
            } // If operator is greater than or equal to, use the range scan method
            else if (operator.equals(">=")) {
                if (isStringKey) {
                    btfScan = btf.new_scan(strKey, null);
                } else {
                    btfScan = btf.new_scan(intKey, null);
                }
            } 
			else if (operator.equals(">")){
                if (isStringKey) {
                    //Figure out how to increment string key
                    btfScan = btf.new_scan(strKey, null);
                } else {
                    intKey = new IntegerKey(Integer.parseInt(value)+1);
                    btfScan = btf.new_scan(intKey, null);
                }
            } else if (operator.equals("<")){
                if (isStringKey) {
                    btfScan = btf.new_scan(null, strKey);
                } else {
                    intKey = new IntegerKey(Integer.parseInt(value)-1);
                    btfScan = btf.new_scan(null, intKey);
                }
            }
			// If operator is Not equal to, use the range scan method
            else if (operator.equals("!=") || operator.equals("NOT")) {
                if (isStringKey) {
                    btfScan = btf.new_scan(null, strKey);
                } else {
                    intKey = new IntegerKey(Integer.parseInt(value)-1);
                    btfScan = btf.new_scan(null, intKey);
                }
            } 
            // If operator is equal to, use the point scan method 
            else {
                // Create a point scan
                if (isStringKey) {
                    btfScan = btf.new_scan(strKey, strKey);
                } else {
                    btfScan = btf.new_scan(intKey, intKey);
                }
            }

            // Iterate through B-Tree index entires
            KeyDataEntry entry;
			int counter = 0;
            while ((entry = btfScan.get_next()) != null) {
                // Print the tuple
                int pos = columnarFile.getPositionFromRid(((LeafData)entry.data).getData(), columnNumber);
                TID tid1 = columnarFile.getTidFromPosition(pos);
                Tuple tuple = columnarFile.getTuple(tid1);
                if( (isStringKey) && (operator.equals(">") || operator.equals("<") || operator.equals("!=")) )
				{
					//<, >, != check over a range, but we want to ignore cases where
					//the returned value matches the query condition
					if( !(strKey.getKey().equals(tuple.getStrFld(columnNumber))) )
					{
						//print only the requested column values
						printTupleColumns( columnarFileName, tuple, targetColumns, tid1, isDelete );
					}
				}
				else
				{
					//print only the requested column values
					printTupleColumns( columnarFileName, tuple, targetColumns, tid1, isDelete );
				}
            }
			
			//SPECIAL CASE -- In a not equal to instance, we will now do a follow-up
			//iteration over the values after the passed argument
			if(operator.equals("!=") || operator.equals("NOT"))
			{
				if (isStringKey)
				{
					btfScan.DestroyBTreeFileScan();
					btfScan = btf.new_scan(strKey, null);
				} else {
					btfScan.DestroyBTreeFileScan();
					intKey = new IntegerKey(Integer.parseInt(value)+1);
					btfScan = btf.new_scan(intKey, null);
				}
				
				while ((entry = btfScan.get_next()) != null)
				{
					// Print the tuple
					int pos = columnarFile.getPositionFromRid(((LeafData)entry.data).getData(), columnNumber);
					TID tid1 = columnarFile.getTidFromPosition(pos);
					Tuple tuple = columnarFile.getTuple(tid1);
					if( isStringKey )
					{
						//!= checks over a range, but we want to ignore cases where
						//the returned value matches the query condition
						if( !(strKey.getKey().equals(tuple.getStrFld(columnNumber))) )
						{
							//print only the requested column values
							printTupleColumns( columnarFileName, tuple, targetColumns, tid1, isDelete );
						}
					}
					else
					{
						printTupleColumns( columnarFileName, tuple, targetColumns, tid1, isDelete );
					}
				}
			}

            // Print the number of disk pages read and written
            System.out.println("Number of disk pages read: " + PCounter.getReadCount());
            System.out.println("Number of disk pages written: " + PCounter.getWriteCount());

            // Close the BTreeFileScan
            btfScan.DestroyBTreeFileScan();
            btf.close();

        } catch (Exception e){
            e.printStackTrace();
        } 

    }
    

	/*
	 * Method to complete a query utilizing the Bitmap index
	 * Takes a DB to query, a columnar file the Bitmap is created from, the
	 * columns of information to return on match, & the conditions of the query
	 * (in format "<column> <expression> <value>")
	 */
	private static void executeBitMapQuery(String columnDBName, String columnarFileName,
											int[] targetColumns, String[] valueConstraints,
											boolean isDelete )
	{
		PCounter.initialize();
		try {
			// Parse value constraints
			String columnName = valueConstraints[0];
			String operator = valueConstraints[1];
			String value = valueConstraints[2]; 
			boolean isStringKey = false;

			// Create a ColumnarFile object
			Columnarfile columnarFile = new Columnarfile(columnarFileName);
			int columnNumber = Arrays.asList(columnarFile.columnNames).indexOf(columnName)+1;

			String bmFileName = columnarFileName + ".bitmap" + Integer.toString(columnNumber);
			BitMapFile bitMap;
			try
			{
				bitMap = new BitMapFile( bmFileName );
			} catch (Exception e) {
				System.out.println("Could not open " + bmFileName);
				e.printStackTrace();
				return;
			} 
			bitMap.setSrcColumnarFile( columnarFile );
			bitMap.setMappedColumn( columnNumber );
			
			IntegerKey intKey = null;
			StringKey strKey = null;
			
			System.out.println("colNo " + columnNumber + " & attr " + columnarFile.type[columnNumber-1].attrType );
			
			//If value is a string, create a BTree with a string key, if its an integer, create a BTree with an integer key
			if (columnarFile.type[columnNumber-1].attrType == AttrType.attrInteger) {
				intKey = new IntegerKey(Integer.parseInt(value));
			} else if (columnarFile.type[columnNumber-1].attrType == AttrType.attrString) {
				// Create a key for the search value
				isStringKey = true;
				strKey = new StringKey(value);
			} 

			// Create a BitMapFileScan object
			BMFileScan bitMapScan = null; 

			//CASE -- Searching over a RANGE
			if (operator.equals("RANGE"))
			{
				bitMapScan = bitMap.new_scan(null, null);
			}
			//CASE -- Searching for all values less-than-or-equal-to
			else if (operator.equals("<="))
			{
				if (isStringKey) {
					bitMapScan = bitMap.new_scan(null, strKey);
				} else {
					bitMapScan = bitMap.new_scan(null, intKey);
				}
			}
			//CASE -- Searching for all values less-than
			else if (operator.equals("<")){
                if (isStringKey) {
					//need means to check up to but not including
                    bitMapScan = bitMap.new_scan(null, strKey);
                } else {
                    intKey = new IntegerKey(Integer.parseInt(value)-1);
                    bitMapScan = bitMap.new_scan(null, intKey);
                }
            }
			//CASE -- Searching for all values greater-than-or-equal-to
			else if (operator.equals(">="))
			{
				if (isStringKey) {
					bitMapScan = bitMap.new_scan(strKey, null);
				} else {
					bitMapScan = bitMap.new_scan(intKey, null);
				}
			}
			//CASE -- Searching for all values greater-than
			else if (operator.equals(">"))
			{
                if (isStringKey) {
					//need to figure out way to go to n+1 string index
                    bitMapScan = bitMap.new_scan(strKey, null);
                } else {
                    intKey = new IntegerKey(Integer.parseInt(value)+1);
                    bitMapScan = bitMap.new_scan(intKey, null);
                }
            } 
			//CASE -- Searching for all values not-equal to
			else if (operator.equals("!=") || operator.equals("NOT"))
			{
				if (isStringKey)
				{
					bitMapScan = bitMap.new_scan(null, strKey);
				} else {
					intKey = new IntegerKey(Integer.parseInt(value)-1);
					bitMapScan = bitMap.new_scan(null, intKey);
				}
            }
			//DEFAULT CASE -- Searching for all values equal to
			else
			{
				if (isStringKey) {
					bitMapScan = bitMap.new_scan(strKey, strKey);
				} else {
					bitMapScan = bitMap.new_scan(intKey, intKey);
				}
			}
			
			// Iterate through B-Tree index entires
			KeyDataEntry entry;
			while ((entry = bitMapScan.get_next()) != null)
			{
				// Print the tuple
				int pos = columnarFile.getPositionFromRid(((LeafData)entry.data).getData(), columnNumber);
				TID tid1 = columnarFile.getTidFromPosition(pos);
				Tuple tuple = columnarFile.getTuple(tid1);
				if( (isStringKey) && (operator.equals(">") || operator.equals("<") || operator.equals("!=")) )
				{
					//<, >, != check over a range, but we want to ignore cases where
					//the returned value matches the query condition
					if( !(strKey.getKey().equals(tuple.getStrFld(columnNumber))) )
					{
						//print only the requested column values
						printTupleColumns( columnarFileName, tuple, targetColumns, tid1, isDelete );
					}
				}
				else
				{
					//print only the requested column values
					printTupleColumns( columnarFileName, tuple, targetColumns, tid1, isDelete );
				}
			}
			
			//SPECIAL CASE -- In a not equal to instance, we will now do a follow-up
			//iteration over the values after the passed argument
			if(operator.equals("!=") || operator.equals("NOT"))
			{
				if (isStringKey)
				{
					bitMapScan.closeBitmapScans();
					bitMapScan = bitMap.new_scan(strKey, null);
				} else {
					bitMapScan.closeBitmapScans();
					intKey = new IntegerKey(Integer.parseInt(value)+1);
					bitMapScan = bitMap.new_scan(intKey, null);
				}
				
				while ((entry = bitMapScan.get_next()) != null)
				{
					// Print the tuple
					int pos = columnarFile.getPositionFromRid(((LeafData)entry.data).getData(), columnNumber);
					TID tid1 = columnarFile.getTidFromPosition(pos);
					Tuple tuple = columnarFile.getTuple(tid1);
					if( isStringKey )
					{
						//!= checks over a range, but we want to ignore cases where
						//the returned value matches the query condition
						if( !(strKey.getKey().equals(tuple.getStrFld(columnNumber))) )
						{
							//print only the requested column values
							printTupleColumns( columnarFileName, tuple, targetColumns, tid1, isDelete );
						}
					}
					else
					{
						printTupleColumns( columnarFileName, tuple, targetColumns, tid1, isDelete );
					}
				}
			}

			// Print the number of disk pages read and written
			System.out.println("Number of disk pages read: " + PCounter.getReadCount());
			System.out.println("Number of disk pages written: " + PCounter.getWriteCount());
			
			//clean up allocations accordingly
			bitMapScan.closeBitmapScans();
            bitMap.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * Method to complete a query utilizing the Compressed Bitmap index
	 * Takes a DB to query, a columnar file the Compressed Bitmap is created from, the
	 * columns of information to return on match, & the conditions of the query
	 * (in format "<column> <expression> <value>")
	 */
	private static void executeCBitMapQuery(String columnDBName, String columnarFileName,
											int[] targetColumns, String[] valueConstraints,
											boolean isDelete )
	{
		PCounter.initialize();
		try {
			// Parse value constraints
			String columnName = valueConstraints[0];
			String operator = valueConstraints[1];
			String value = valueConstraints[2]; 
			boolean isStringKey = false;

			// Create a ColumnarFile object
			Columnarfile columnarFile = new Columnarfile(columnarFileName);
			int columnNumber = Arrays.asList(columnarFile.columnNames).indexOf(columnName)+1;

			String bmFileName = columnarFileName + ".cbitmap" + Integer.toString(columnNumber);
			CBitMapFile cbitmap;
			try
			{
				cbitmap = new CBitMapFile( bmFileName );
			} catch (Exception e) {
				System.out.println("Could not open " + bmFileName);
				e.printStackTrace();
				return;
			} 
			cbitmap.setSrcColumnarFile( columnarFile );
			cbitmap.setMappedColumn( columnNumber );
			
			IntegerKey intKey = null;
			StringKey strKey = null;
			
			System.out.println("colNo " + columnNumber + " & attr " + columnarFile.type[columnNumber-1].attrType );
			
			//If value is a string, create a BTree with a string key, if its an integer, create a BTree with an integer key
			if (columnarFile.type[columnNumber-1].attrType == AttrType.attrInteger)
			{
				intKey = new IntegerKey(Integer.parseInt(value));
			}
			else if (columnarFile.type[columnNumber-1].attrType == AttrType.attrString)
			{
				// Create a key for the search value
				isStringKey = true;
				strKey = new StringKey(value);
			} 

			// Create a BitMapFileScan object
			CBMFileScan cbitmapScan = null; 

			//if operator = range, use the range scan method
			if (operator.equals("RANGE"))
			{
				cbitmapScan = cbitmap.new_scan(null, null);
			}
			// If operator is less than or equal to, use the range scan method
			else if (operator.equals("<="))
			{
				if (isStringKey) {
					cbitmapScan = cbitmap.new_scan(null, strKey);
				} else {
					cbitmapScan = cbitmap.new_scan(null, intKey);
				}
			}
			// If operator is greater than or equal to, use the range scan method
			else if (operator.equals(">="))
			{
				if (isStringKey) {
					cbitmapScan = cbitmap.new_scan(strKey, null);
				} else {
					cbitmapScan = cbitmap.new_scan(intKey, null);
				}
			}
			//greater than case
			else if (operator.equals(">"))
			{
                if (isStringKey) {
                    cbitmapScan = cbitmap.new_scan(strKey, null);
                } else {
                    intKey = new IntegerKey(Integer.parseInt(value)+1);
                    cbitmapScan = cbitmap.new_scan(intKey, null);
                }
            }
			//less than case
			else if (operator.equals("<"))
			{
                if (isStringKey) {
                    cbitmapScan = cbitmap.new_scan(null, strKey);
                } else {
                    intKey = new IntegerKey(Integer.parseInt(value)-1);
                    cbitmapScan = cbitmap.new_scan(null, intKey);
                }
            }
			// If operator is Not equal to, use the range scan method
			else if (operator.equals("!=") || operator.equals("NOT"))
			{
				if (isStringKey){
					cbitmapScan = cbitmap.new_scan(null, strKey);
				} else {
					intKey = new IntegerKey(Integer.parseInt(value)-1);
					cbitmapScan = cbitmap.new_scan(null, intKey);
				}
			}
			// DEFAULT -- operator is equals
			else
			{
				// Create a point scan
				if (isStringKey) {
					cbitmapScan = cbitmap.new_scan(strKey, strKey);
				} else {
					cbitmapScan = cbitmap.new_scan(intKey, intKey);
				}
			}
			
			// Iterate through B-Tree index entires
			KeyDataEntry entry;
			while ((entry = cbitmapScan.get_next()) != null)
			{
				// Print the tuple
				int pos = columnarFile.getPositionFromRid(((LeafData)entry.data).getData(), columnNumber);
				TID tid1 = columnarFile.getTidFromPosition(pos);
				Tuple tuple = columnarFile.getTuple(tid1);
				if( (isStringKey) && (operator.equals(">") || operator.equals("<") || operator.equals("!=")) )
				{
					//<, >, != check over a range, but we want to ignore cases where
					//the returned value matches the query condition
					if( !(strKey.getKey().equals(tuple.getStrFld(columnNumber))) )
					{
						//print only the requested column values
						printTupleColumns( columnarFileName, tuple, targetColumns, tid1, isDelete );
					}
				}
				else
				{
					//print only the requested column values
					printTupleColumns( columnarFileName, tuple, targetColumns, tid1, isDelete );
				}
			}
			
			//SPECIAL CASE -- In a not equal to instance, we will now do a follow-up
			//iteration over the values after the passed argument
			if(operator.equals("!=") || operator.equals("NOT"))
			{
				if (isStringKey)
				{
					cbitmapScan.closeCbitmapScans();
					cbitmapScan = cbitmap.new_scan(strKey, null);
				} else {
					cbitmapScan.closeCbitmapScans();
					intKey = new IntegerKey(Integer.parseInt(value)+1);
					cbitmapScan = cbitmap.new_scan(intKey, null);
				}
				
				while ((entry = cbitmapScan.get_next()) != null)
				{
					// Print the tuple
					int pos = columnarFile.getPositionFromRid(((LeafData)entry.data).getData(), columnNumber);
					TID tid1 = columnarFile.getTidFromPosition(pos);
					Tuple tuple = columnarFile.getTuple(tid1);
					if( isStringKey )
					{
						//!= checks over a range, but we want to ignore cases where
						//the returned value matches the query condition
						if( !(strKey.getKey().equals(tuple.getStrFld(columnNumber))) )
						{
							//print only the requested column values
							printTupleColumns( columnarFileName, tuple, targetColumns, tid1, isDelete );
						}
					}
					else
					{
						printTupleColumns( columnarFileName, tuple, targetColumns, tid1, isDelete );
					}
				}
			}

			// Print the number of disk pages read and written
			System.out.println("Number of disk pages read: " + PCounter.getReadCount());
			System.out.println("Number of disk pages written: " + PCounter.getWriteCount());
			
			//clean up allocations accordingly
			cbitmapScan.closeCbitmapScans();
            cbitmap.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}


