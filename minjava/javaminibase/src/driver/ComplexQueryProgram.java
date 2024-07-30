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


public class ComplexQueryProgram
{
	private static final int AND_OP = 1;
	private static final int OR_OP = 2;
	
    public static void queryDB(String colDB, String colFile, String tgtCols,
								String colToQuery1, String inequalityComp1, String valToCheck1,
								String colToQuery2, String inequalityComp2, String valToCheck2,
								int constraintOp, int bufCnt, String access, boolean isDelete )
	{
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
            
            String columnName1 = colToQuery1;
            String operator1 = inequalityComp1;
            String value1 = valToCheck1;
			
			String columnName2 = colToQuery2;
            String operator2 = inequalityComp2;
            String value2 = valToCheck2;

            int numBuf = bufCnt;
            String accessType = access;

            // Parse value constraints (in form of {columnName operator value})
            String[] valueConstraintsParts1 = new String[3];
            valueConstraintsParts1[0] = columnName1;
            valueConstraintsParts1[1] = operator1;
            valueConstraintsParts1[2] = value1;
			String[] valueConstraintsParts2 = new String[3];
            valueConstraintsParts2[0] = columnName2;
            valueConstraintsParts2[1] = operator2;
            valueConstraintsParts2[2] = value2;
            

            //Print all arguments (DEBUG Support)
            /*System.out.println(columnDBName);
            System.out.println(columnarFileName);
            System.out.println(Arrays.toString(targetColumnNames));
            System.out.println(columnName1+" "+operator1+" "+value1);
            System.out.println(columnName2+" "+operator2+" "+value2);
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
                        executeFileScanQuery(columnDBName, columnarFileName, targetColumnIndices,
											valueConstraintsParts1, valueConstraintsParts2, constraintOp, isDelete);
                        break;
                    case "COLUMNSCAN":
                        System.out.println("Executing Columnscan query...");
                        executeColumnScanQuery(columnDBName, columnarFileName, targetColumnIndices,
											valueConstraintsParts1, valueConstraintsParts2, constraintOp, isDelete);
                        break;
                    case "BITMAP":
                        System.out.println("Executing Bitmap query...");
                        executeBitMapQuery(columnDBName, columnarFileName, targetColumnIndices,
											valueConstraintsParts1, valueConstraintsParts2, constraintOp, isDelete);
                        break;
					case "CBITMAP":
                        System.out.println("Executing Compressed Bitmap query...");
                        executeCBitMapQuery(columnDBName, columnarFileName, targetColumnIndices,
											valueConstraintsParts1, valueConstraintsParts2, constraintOp, isDelete);
                        break;
                    case "BTREE":
                        System.out.println("Executing Btreescan query...");
                        executeBTreeQuery(columnDBName, columnarFileName, targetColumnIndices,
											valueConstraintsParts1, valueConstraintsParts2, constraintOp, isDelete);
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


	private static CondExpr getExpr(
		  String operator, // = | < | > | <= | >= | != |
		  // aopEQ | aopLT | aopGT | aopLE | aopGE | aopNE
		  String value,
		  int colNum,
		  AttrType[] colTypes)
		  throws IllegalArgumentException
	{
		CondExpr expr = new CondExpr();
		expr.op = new AttrOperator(operator);
		if (expr.op.toString().equals("aopNOP")
			|| expr.op.toString().equals("aopRANGE")
			|| expr.op.toString().contains("Unexpected AttrOperator ")
			|| (expr.op.attrOperator == 0
				&& !operator.equals("=")
				&& !operator.equals("==")
				&& !operator.equals("aopEQ"))) {
		  System.err.println("[x] Invalid operator: " + operator);
		  throw new IllegalArgumentException("Invalid operator");
		}
		expr.type1 = new AttrType(AttrType.attrSymbol);
		expr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), colNum);
		expr.type2 = colTypes[colNum - 1];
		switch (expr.type2.attrType) {
		  case AttrType.attrInteger:
			try {
			  expr.operand2.integer = Integer.parseInt(value);
			} catch (Exception e) {
			  System.err.println("[x] Invalid valueConstraint : parseInt fail");
			  throw new IllegalArgumentException("Invalid valueConstraint");
			}
			break;
		  case AttrType.attrReal:
			try {
			  expr.operand2.real = Float.parseFloat(value);
			} catch (Exception e) {
			  System.err.println("[x] Invalid valueConstraint : parseFloat fail");
			  throw new IllegalArgumentException("Invalid valueConstraint");
			}
			break;
		  case AttrType.attrString:
			expr.operand2.string = value;
			break;
		  default:
			System.err.println("[x] Invalid column type: Columnar file Corrupted");
			throw new IllegalArgumentException("Invalid column type");
		}
		expr.next = null;
		return expr;
	}

    private static void executeFileScanQuery(String columnDBName, String columnarFileName, int[] targetColumns,
											String[] valueConstraintsC1, String[] valueConstraintsC2, int constraint,
											boolean isDelete)
    {
        PCounter.initialize();
        try {
			// Parse value constraints
			String columnAName = valueConstraintsC1[0];
			String operatorA = valueConstraintsC1[1];
			String valueA = valueConstraintsC1[2];
			String columnBName = valueConstraintsC2[0];
			String operatorB = valueConstraintsC2[1];
			String valueB = valueConstraintsC2[2];

			//open Columnar File
			Columnarfile columnarFile = new Columnarfile(columnarFileName);

            int columnANumber = Arrays.asList(columnarFile.columnNames).indexOf(columnAName)+1;
            int columnBNumber = Arrays.asList(columnarFile.columnNames).indexOf(columnBName)+1;

            // Columnar Attribute Types
            AttrType[] types = columnarFile.type;
                       
            // Prepare the output fields specification for the projection
            FldSpec[] Sprojection = new FldSpec[targetColumns.length];
            for (int i = 0; i < targetColumns.length; i++) {
                Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            }
			
			AttrType[] projtypes = new AttrType[Sprojection.length];
			for (int i = 0; i < Sprojection.length; i++)
			{
				projtypes[i] = new AttrType(columnarFile.type[Sprojection[i].offset - 1].attrType);
			}

            // Prepare string sizes
            short[] strSizes = columnarFile.strSizes;        
            
            // Query Condition Expression
            CondExpr[] expr = new CondExpr[3];
			expr[0] = getExpr(operatorA, valueA, columnANumber, types);
			expr[1] = null;
			expr[2] = null;
			if( AND_OP == constraint )
			{
				expr[1] = getExpr(operatorB, valueB, columnBNumber, types);
			}
			else
			{
				expr[0].next = getExpr(operatorB, valueB, columnBNumber, types);
			}

			// open columnar file scan
			ColumnarFileScan cfscan;
			BTreeFile btf = null;
			try {
				cfscan = new ColumnarFileScan(
								columnarFile.get_fileName(),
								columnarFile.type,
								columnarFile.strSizes,
								(short) Columnarfile.numColumns,
								(short) Sprojection.length,
								Sprojection,
								expr);
				btf = new BTreeFile(columnarFile.get_fileName() + ".deletedBTree");
			} catch (Exception e) {
				System.err.println("[x] Error opening file scan");
				throw new CFException(e, "Error opening ColumnarFileScan");
			}

			// get next tuple
			try {
				TID tid = new TID(Columnarfile.numColumns);
				Tuple tuple = cfscan.get_next(tid);
				while (tuple != null) {
					tuple.print(projtypes);
					if (isDelete) {
						columnarFile.markTupleDeleted(tid);
					}
					tuple = cfscan.get_next(tid);
				}
			} catch (Exception e) {
				System.err.println("[x] Error getting next tuple");
				throw new CFException(e, "Error getting next tuple");
			} finally {
				// close columnar file scan
				try {
					cfscan.close();
					btf.close();
				} catch (Exception e) {
					System.err.println("[x] Error closing ColumnarFileScan");
					throw new CFException(e, "Error closing ColumnarFileScan");
				}
			}
			
            // Output the number of disk pages read and written
            System.out.println("Number of disk pages read: " + PCounter.getReadCount());
            System.out.println("Number of disk pages written: " + PCounter.getWriteCount());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


	static Columnarfile colscan_f = null;
	static Scan[] colscan_scans = null;
	static AttrType[] colscan_evalTypes = null;
	static short[] colscan_evalStrSizes = null;
	static CondExpr[] colscan_expr = null;
	static Integer colscan_pos = null;
	
	private static Integer get_next_colscan(RID[] rids)
		throws CFException,
		InvalidTupleSizeException,
		IOException,
		FieldNumberOutOfBoundException,
		InvalidTypeException,
		UnknowAttrType,
		PredEvalException
	{
		Tuple evalTuple = new Tuple();
		evalTuple.setHdr(
		(short) colscan_evalTypes.length,
		colscan_evalTypes,
		colscan_evalStrSizes);
		while (true) {
			for (int i = 0; i < colscan_scans.length; i++) {
				Tuple tempTuple = colscan_scans[i].getNext(rids[i]);
				if (tempTuple == null) {
					return null;
				}
				byte[] bytes = null;
				switch (colscan_evalTypes[i].attrType) {
					case AttrType.attrInteger:
					bytes = tempTuple.getTupleByteArray();
					int intValue = Convert.getIntValue(0, bytes);
					evalTuple.setIntFld(i + 1, intValue);
					break;
					case AttrType.attrReal:
					bytes = tempTuple.getTupleByteArray();
					float floValue = Convert.getFloValue(0, bytes);
					evalTuple.setFloFld(i + 1, floValue);
					break;
					case AttrType.attrString:
					bytes = tempTuple.getTupleByteArray();
					String strValue = Convert.getStrValue(0, bytes, bytes.length);
					evalTuple.setStrFld(i + 1, strValue);
					break;
				}
			}
			colscan_pos++;
			if (PredEval.Eval(colscan_expr, evalTuple, null, colscan_evalTypes, null)
				&& !colscan_f.isTupleMarkedDeleted(colscan_pos)) {
				return colscan_pos;
			}
		}
	}
	
    private static void executeColumnScanQuery(String columnDBName, String columnarFileName, int[] targetColumns,
											String[] valueConstraintsC1, String[] valueConstraintsC2, int constraint,
											boolean isDelete)
    {
		PCounter.initialize();
        try {
			// Parse value constraints
			String columnAName = valueConstraintsC1[0];
			String operatorA = valueConstraintsC1[1];
			String valueA = valueConstraintsC1[2];
			String columnBName = valueConstraintsC2[0];
			String operatorB = valueConstraintsC2[1];
			String valueB = valueConstraintsC2[2];

			//open Columnar File
			Columnarfile columnarFile = new Columnarfile(columnarFileName);

            int columnANumber = Arrays.asList(columnarFile.columnNames).indexOf(columnAName)+1;
            int columnBNumber = Arrays.asList(columnarFile.columnNames).indexOf(columnBName)+1;

            // Columnar Attribute Types
            AttrType[] types = columnarFile.type;
                       
            // Prepare the output fields specification for the projection
            FldSpec[] projlist = new FldSpec[targetColumns.length];
            for (int i = 0; i < targetColumns.length; i++) {
                projlist[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            }
			
			AttrType[] projtypes = new AttrType[projlist.length];
			for (int i = 0; i < projlist.length; i++)
			{
				projtypes[i] = new AttrType(columnarFile.type[projlist[i].offset - 1].attrType);
			}

            // Prepare string sizes
            short[] strSizes = columnarFile.strSizes;        
            
            // Query Condition Expression
            CondExpr[] expr = new CondExpr[3];
			expr[0] = getExpr(operatorA, valueA, columnANumber, types);
			expr[1] = null;
			expr[2] = null;
			if( AND_OP == constraint )
			{
				expr[1] = getExpr(operatorB, valueB, columnBNumber, types);
			}
			else
			{
				expr[0].next = getExpr(operatorB, valueB, columnBNumber, types);
			}
			
			// reconstructing the expr to get the column numbers to scan
			int[] colNumToScan = null;
			if (expr[0].next == null && expr[1] == null) {
				colNumToScan = new int[1];
			} else {
				colNumToScan = new int[2];
			}
			colNumToScan[0] = expr[0].operand1.symbol.offset;
			expr[0].operand1.symbol.offset = 1;
			if (colNumToScan.length == 2 && expr[1] == null) {
				colNumToScan[1] = expr[0].next.operand1.symbol.offset;
				expr[0].next.operand1.symbol.offset = 2;
			} else if (colNumToScan.length == 2 && expr[1] != null) {
				colNumToScan[1] = expr[1].operand1.symbol.offset;
				expr[1].operand1.symbol.offset = 2;
			}
		
			// open column scans
			Scan[] scans = new Scan[colNumToScan.length];
			BTreeFile btf = null;
			try {
			for (int i = 0; i < colNumToScan.length; i++) {
				scans[i] = columnarFile.openColumnScan(colNumToScan[i]);
			}
			btf = new BTreeFile(columnarFile.get_fileName() + ".deletedBTree");
			} catch (Exception e) {
				System.err.println("[x] Error opening column scan");
				throw new CFException(e, "Error opening column scan");
			}
		
			// get next tuple
			try {
		
				// construct eval tuple hdr
				AttrType[] evalType = new AttrType[colNumToScan.length];
				short[] evalStrSizes = null;
			
				// construct eval type
				for (int i = 0; i < colNumToScan.length; i++) {
					evalType[i] = new AttrType(columnarFile.type[colNumToScan[i] - 1].attrType);
				}
			
				// contruct evalStrSizes
				int evalStrSizesLength = 0;
				for (int i = 0; i < evalType.length; i++) {
					if (evalType[i].attrType == AttrType.attrString) {
						evalStrSizesLength++;
					}
				}
				if (evalStrSizesLength != 0) {
					evalStrSizes = new short[evalStrSizesLength];
					int evalStrIndex = 0;
					for (int i = 0; i < evalStrSizes.length; i++) {
						evalStrSizes[i] = columnarFile.strSizes[evalStrIndex];
						evalStrIndex++;
					}
				}
			
				RID[] rids = new RID[colNumToScan.length];
				for (int i = 0; i < colNumToScan.length; i++) {
					rids[i] = new RID();
				}
			
				// column scan setup
				colscan_f = columnarFile;
				colscan_scans = scans;
				colscan_evalTypes = evalType;
				colscan_evalStrSizes = evalStrSizes;
				colscan_expr = expr;
				colscan_pos = -1;
			
				// projecton tuple setup
				Tuple Jtuple = new Tuple();
				TupleUtils.setup_op_tuple(
					Jtuple,
					projtypes,
					columnarFile.type,
					Columnarfile.numColumns,
					columnarFile.strSizes,
					projlist,
					projlist.length);
			
				// get next tuple pos
				TID tid = null;
				Integer pos = get_next_colscan(rids);
				while (pos != null) {
					RID[] colRids = new RID[Columnarfile.numColumns];
					for (int i = 0; i < colNumToScan.length; i++) {
						colRids[colNumToScan[i] - 1] = rids[i];
					}
					if (isDelete) {
						tid = columnarFile.getTidFromPosition(pos, colRids, null);
						columnarFile.markTupleDeleted(tid);
					} else {
						tid = columnarFile.getTidFromPosition(pos, colRids, projlist);
					}
					Tuple t1 = columnarFile.getTupleProj(tid, projlist);
					Projection.Project(t1, columnarFile.type, Jtuple, projlist, projlist.length);
					Jtuple.print(projtypes);
					pos = get_next_colscan(rids);
				}
			} catch (Exception e) {
				System.err.println("[x] Error getting next tuple pos");
				throw new CFException(e, "Error getting next tuple pos");
			} finally {
				// close the scans
				try {
					for (int i = 0; i < scans.length; i++) {
						scans[i].closescan();
					}
				} catch (Exception e) {
					System.err.println("[x] Error closing iterator");
					throw new CFException(e, "Error closing iterator");
				}
			}
			
            // Output the number of disk pages read and written
            System.out.println("Number of disk pages read: " + PCounter.getReadCount());
            System.out.println("Number of disk pages written: " + PCounter.getWriteCount());
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
	 * To make code more readable, this repetetive & regularly called
	 * functionality was moved into it's own method
	 * It serves to check a position's 2nd column in the joint condition
	 * when paired with an "and" operation
	 * Returns true if the 2nd column criteria was met
	 */
	public static boolean andCheckColumnBool( Columnarfile columnarFile, int ColNo, Tuple tuple, String compOp, String checkValue )
    {
		boolean validMatch = false;
		try
		{
			boolean colBStringComp = false;
			if (columnarFile.type[ColNo-1].attrType == AttrType.attrString)
			{
				colBStringComp = true;
			}
			/*
			 * Check Column B at this position for using it's operator criteria
			 * and if the value also passes in Column B we will print the Tuple
			 */
			if (compOp.equals("<="))
			{
				if( colBStringComp  )
				{
					if( 0 <= checkValue.compareTo(tuple.getStrFld(ColNo)) )
						validMatch = true;
				}
				else
				{
					if( tuple.getIntFld(ColNo) <= Integer.parseInt(checkValue) )
						validMatch = true;
				}
			}
			else if (compOp.equals("<"))
			{
				if( colBStringComp  )
				{
					if( 0 < checkValue.compareTo(tuple.getStrFld(ColNo)) )
						validMatch = true;
				}
				else
				{
					if( tuple.getIntFld(ColNo) < Integer.parseInt(checkValue) )
						validMatch = true;
				}
			}
			else if (compOp.equals(">="))
			{
				if( colBStringComp  )
				{
					if( 0 >= checkValue.compareTo(tuple.getStrFld(ColNo)) )
						validMatch = true;
				}
				else
				{
					if( tuple.getIntFld(ColNo) >= Integer.parseInt(checkValue) )
						validMatch = true;
				}
			}
			else if (compOp.equals(">"))
			{
				if( colBStringComp  )
				{
					if( 0 > checkValue.compareTo(tuple.getStrFld(ColNo)) )
						validMatch = true;
				}
				else
				{
					if( tuple.getIntFld(ColNo) > Integer.parseInt(checkValue) )
						validMatch = true;
				}
			}
			else if (compOp.equals("!=") || compOp.equals("NOT"))
			{
				if( colBStringComp  )
				{
					if( 0 != checkValue.compareTo(tuple.getStrFld(ColNo)) )
						validMatch = true;
				}
				else
				{
					if( Integer.parseInt(checkValue) != tuple.getIntFld(ColNo) )
						validMatch = true;
				}
			}
			else
			{
				if( colBStringComp  )
				{
					if( 0 == checkValue.compareTo(tuple.getStrFld(ColNo)) )
						validMatch = true;
				}
				else
				{
					if( Integer.parseInt(checkValue) == tuple.getIntFld(ColNo) )
						validMatch = true;
				}
			}
		} catch (Exception e){
            e.printStackTrace();
        }
		return validMatch;
	}

	/*
	 * Method to complete a query utilizing the BTree index
	 * Takes a DB to query, a columnar file the BTree is created from, the
	 * columns of information to return on match, & the conditions of the query
	 * (in format "<column> <expression> <value>")
	 */
    private static void executeBTreeQuery(String columnDBName, String columnarFileName, int[] targetColumns,
											String[] valueConstraintsC1, String[] valueConstraintsC2, int constraint,
											boolean isDelete )
    {   
        PCounter.initialize();
        // Create a BTreeFile and get the header heapfile for the columnar file
        try{
            // Parse value constraints
			String columnAName = valueConstraintsC1[0];
			String operatorA = valueConstraintsC1[1];
			String valueA = valueConstraintsC1[2];
			boolean isStringKey = false;
			String columnBName = valueConstraintsC2[0];
			String operatorB = valueConstraintsC2[1];
			String valueB = valueConstraintsC2[2];

            //create counters for the columns of the complex query
			Columnarfile columnarFile = new Columnarfile(columnarFileName);
			int columnANumber = Arrays.asList(columnarFile.columnNames).indexOf(columnAName)+1;
			int columnBNumber = Arrays.asList(columnarFile.columnNames).indexOf(columnBName)+1;
			
			//we will need a linked list if the constraint is OR_OP
			Vector<Integer> dupeFreeList = new Vector<Integer>();

            if(!columnarFile.createBTreeIndex(columnANumber)){
                System.out.println("BTree Index for column " + columnAName + " already exists");
            }

            System.out.println(columnarFileName + ".btree" + Integer.toString(columnANumber));
            BTreeFile btf = new BTreeFile(columnarFileName + ".btree" + Integer.toString(columnANumber));

			KeyClass searchKey = null;
			
			//If value is a string, create a BTree with a string key, if its an integer, create a BTree with an integer key
			if (columnarFile.type[columnANumber-1].attrType == AttrType.attrInteger)
			{
				searchKey = new IntegerKey(Integer.parseInt(valueA));
			}
			else if (columnarFile.type[columnANumber-1].attrType == AttrType.attrString)
			{
				// Create a key for the search value
				isStringKey = true;
				searchKey = new StringKey(valueA);
			}

            // Create a BTreeFileScan object
            BTFileScan btfScan = null;
			/*
			 * Convert the operator against Column A into a formatted scan
			 * range using the following key iterators
			 */
			if (operatorA.equals("RANGE"))
			{
				btfScan = btf.new_scan(null, null);
			}
			else if (operatorA.equals("<="))
			{
				btfScan = btf.new_scan(null, searchKey);
			}
			else if (operatorA.equals("<")){
                if (isStringKey) {
                    btfScan = btf.new_scan(null, searchKey);
                } else {
                    searchKey = new IntegerKey(Integer.parseInt(valueA)-1);
                    btfScan = btf.new_scan(null, searchKey);
                }
            }
			else if (operatorA.equals(">="))
			{
				btfScan = btf.new_scan(searchKey, null);
			}
			else if (operatorA.equals(">"))
			{
                if (isStringKey) {
                    btfScan = btf.new_scan(searchKey, null);
                } else {
                    searchKey = new IntegerKey(Integer.parseInt(valueA)+1);
                    btfScan = btf.new_scan(searchKey, null);
                }
            } 
			else if (operatorA.equals("!=") || operatorA.equals("NOT"))
			{
				if (isStringKey)
				{
					btfScan = btf.new_scan(null, searchKey);
				} else {
					searchKey = new IntegerKey(Integer.parseInt(valueA)-1);
					btfScan = btf.new_scan(null, searchKey);
				}
            }
			else
			{
				btfScan = btf.new_scan(searchKey, searchKey);
			}
            
            // Iterate through B-Tree index entires
            KeyDataEntry entry;
            while ((entry = btfScan.get_next()) != null) {
                // Print the tuple
                int pos = columnarFile.getPositionFromRid(((LeafData)entry.data).getData(), columnANumber);
				//when iterating the first column we will never get
				//duplicates and can immediately append the result
				dupeFreeList.add( 0, pos );
            }
			//In a not equal to instance, we will now do a follow-up
			//iteration over the values after the passed argument
			//for Column A
			if(operatorA.equals("!=") || operatorA.equals("NOT"))
			{
				if (isStringKey)
				{
					btfScan.DestroyBTreeFileScan();
					btfScan = btf.new_scan(searchKey, null);
				} else {
					btfScan.DestroyBTreeFileScan();
					searchKey = new IntegerKey(Integer.parseInt(valueA)+1);
					btfScan = btf.new_scan(searchKey, null);
				}
				
				while ((entry = btfScan.get_next()) != null)
				{
					//match on Column A criteria, check column B and constraint for match
					int pos = columnarFile.getPositionFromRid(((LeafData)entry.data).getData(), columnANumber);
					//need to avoid duplicates in "!=" cases on the first column
					if( false == dupeFreeList.contains( pos ) )
					{
						dupeFreeList.add( 0, pos );
					}
				}
			}
			//close the scan on Column A as we are done using it
			btfScan.DestroyBTreeFileScan();
            btf.close();
			
			//now we need the values in column B matching the criteria
			//but in an OR case we will aim to avoid duplicate results to
			//what was matched in Column A
			if( OR_OP == constraint )
			{
				String btreeFileName = columnarFileName + ".btree" + Integer.toString(columnBNumber);
				try
				{
					btf = new BTreeFile( btreeFileName );
				} catch (Exception e) {
					System.out.println("Could not open " + btreeFileName);
					e.printStackTrace();
					return;
				}
				
				//If value is a string, create a BTree with a string key, if its an integer, create a BTree with an integer key
				isStringKey = false;
				if (columnarFile.type[columnBNumber-1].attrType == AttrType.attrInteger)
				{
					searchKey = new IntegerKey(Integer.parseInt(valueB));
				}
				else if (columnarFile.type[columnBNumber-1].attrType == AttrType.attrString)
				{
					// Create a key for the search value
					isStringKey = true;
					searchKey = new StringKey(valueB);
				}
				
				/*
				 * Convert the operator against Column B into a formatted scan
				 * range using the following key iterators
				 */
				if (operatorB.equals("<="))
				{
					btfScan = btf.new_scan(null, searchKey);
				}
				else if (operatorB.equals("<")){
					if (isStringKey) {
						btfScan = btf.new_scan(null, searchKey);
					} else {
						searchKey = new IntegerKey(Integer.parseInt(valueB)-1);
						btfScan = btf.new_scan(null, searchKey);
					}
				}
				else if (operatorB.equals(">="))
				{
					btfScan = btf.new_scan(searchKey, null);
				}
				else if (operatorB.equals(">"))
				{
					if (isStringKey) {
						btfScan = btf.new_scan(searchKey, null);
					} else {
						searchKey = new IntegerKey(Integer.parseInt(valueB)+1);
						btfScan = btf.new_scan(searchKey, null);
					}
				} 
				else if (operatorB.equals("!=") || operatorB.equals("NOT"))
				{
					if (isStringKey)
					{
						btfScan = btf.new_scan(null, searchKey);
					} else {
						searchKey = new IntegerKey(Integer.parseInt(valueB)-1);
						btfScan = btf.new_scan(null, searchKey);
					}
				}
				else
				{
					btfScan = btf.new_scan(searchKey, searchKey);
				}
				
				// Iterate through B-Tree index entires
				while ((entry = btfScan.get_next()) != null)
				{
					//match on Column A criteria, check column B and constraint for match
					int pos = columnarFile.getPositionFromRid(((LeafData)entry.data).getData(), columnBNumber);
					//need to avoid duplicates in second column
					if( false == dupeFreeList.contains( pos ) )
					{
						dupeFreeList.add( 0, pos );
					}
				}
				
				//In a not equal to instance, we will now do a follow-up
				//iteration over the values after the passed argument
				if(operatorB.equals("!=") || operatorB.equals("NOT"))
				{
					if (isStringKey)
					{
						btfScan.DestroyBTreeFileScan();
						btfScan = btf.new_scan(searchKey, null);
					} else {
						btfScan.DestroyBTreeFileScan();
						searchKey = new IntegerKey(Integer.parseInt(valueB)+1);
						btfScan = btf.new_scan(searchKey, null);
					}
					
					while ((entry = btfScan.get_next()) != null)
					{
						//match on Column A criteria, check column B and constraint for match
						int pos = columnarFile.getPositionFromRid(((LeafData)entry.data).getData(), columnBNumber);
						//need to avoid duplicates in second column
						if( false == dupeFreeList.contains( pos ) )
						{
							dupeFreeList.add( 0, pos );
						}
					}
				}
				//close the scan on Column A as we are done using it
				btfScan.DestroyBTreeFileScan();
				btf.close();
			}
			
			//The string checks for >/</!= have not filtered out exact matches yet
			//so at this point it is necessary to do a little extra filtering
			KeyClass searchKeyA = null;
			boolean filterColumnAFurther = false;
			if( (columnarFile.type[columnANumber-1].attrType == AttrType.attrString) && 
				(operatorA.equals(">") || operatorA.equals("<") || operatorA.equals("!=")) )
			{
				filterColumnAFurther = true;
				searchKeyA = new StringKey(valueA);
			}
			boolean filterColumnBFurther = false;
			if( (OR_OP == constraint) &&
				(columnarFile.type[columnBNumber-1].attrType == AttrType.attrString) && 
				(operatorB.equals(">") || operatorB.equals("<") || operatorB.equals("!=")) )
			{
				filterColumnBFurther = true;
			}	
			//iterate and print the duplicate free results
			for( int i = 0; i < dupeFreeList.size(); i++ )
			{				
				int pos = dupeFreeList.elementAt(i);
				TID tid1 = columnarFile.getTidFromPosition(pos);
				Tuple tuple = columnarFile.getTuple(tid1);
				boolean validMatch = true;
				if( filterColumnAFurther || filterColumnBFurther )
				{
					//On an AND_OP we only need to review the first column, as the second
					//isn't in the dupeFreeList
					if( (AND_OP == constraint) && filterColumnAFurther )
					{
						if( (((StringKey)searchKeyA).getKey().equals(tuple.getStrFld(columnANumber))) )
						{
							validMatch = false;
						}
					}
					//On an OR_OP we exclude it if BOTH constraint rejects the string
					else
					{
						//both columns are strings to potentially filter further
						if( filterColumnAFurther && filterColumnBFurther )
						{
							if( (((StringKey)searchKeyA).getKey().equals(tuple.getStrFld(columnANumber))) &&
								(((StringKey)searchKey).getKey().equals(tuple.getStrFld(columnBNumber))) )
							{
								validMatch = false;
							}
						}
						//these next 2 will be reached if 1 column is not a string, and we'll need and since
						//those filter >/</!= completely by here we only need to reconfirm the string inclusion
						else if( filterColumnAFurther )
						{
							boolean altColInvalidToo = andCheckColumnBool( columnarFile, columnBNumber, tuple, operatorB, valueB);
							if( (((StringKey)searchKeyA).getKey().equals(tuple.getStrFld(columnANumber))) && !altColInvalidToo )
							{
								validMatch = false;
							}
						}
						else if( filterColumnBFurther )
						{
							boolean altColInvalidToo = andCheckColumnBool( columnarFile, columnANumber, tuple, operatorA, valueA);
							if( (((StringKey)searchKey).getKey().equals(tuple.getStrFld(columnBNumber))) && !altColInvalidToo )
							{
								validMatch = false;
							}
						}
					}
				}
				if( validMatch )
				{
					//for an AND_OP we still need to confirm the
					//values in the 2nd column match criteria
					if( AND_OP == constraint )
					{
						boolean andCondMet = andCheckColumnBool( columnarFile, columnBNumber, tuple, operatorB, valueB);
						if( andCondMet )
						{
							printTupleColumns( columnarFileName, tuple, targetColumns, tid1, isDelete );
						}
					}
					//else it's either or and we know it's adupe free list by now
					else
					{
						printTupleColumns( columnarFileName, tuple, targetColumns, tid1, isDelete );
					}
				}
			}


			SystemDefs.JavabaseBM.flushAllPages();

            // Print the number of disk pages read and written
            System.out.println("Number of disk pages read: " + PCounter.getReadCount());
            System.out.println("Number of disk pages written: " + PCounter.getWriteCount());

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
	private static void executeBitMapQuery(String columnDBName, String columnarFileName, int[] targetColumns,
											String[] valueConstraintsC1, String[] valueConstraintsC2, int constraint,
											boolean isDelete )
	{
		PCounter.initialize();
		try {
			// Parse value constraints
			String columnAName = valueConstraintsC1[0];
			String operatorA = valueConstraintsC1[1];
			String valueA = valueConstraintsC1[2];
			boolean isStringKey = false;
			String columnBName = valueConstraintsC2[0];
			String operatorB = valueConstraintsC2[1];
			String valueB = valueConstraintsC2[2];

			// Create a ColumnarFile object
			Columnarfile columnarFile = new Columnarfile(columnarFileName);
			int columnANumber = Arrays.asList(columnarFile.columnNames).indexOf(columnAName)+1;
			int columnBNumber = Arrays.asList(columnarFile.columnNames).indexOf(columnBName)+1;
			
			//we will need a linked list if the constraint is OR_OP
			Vector<Integer> dupeFreeList = new Vector<Integer>();

			//init bitmap for column 1 (in the case of "and" cosntraint we won't need to
			//open the 2nd bitmap file, so for now leave it be)
			String bmFileName = columnarFileName + ".bitmap" + Integer.toString(columnANumber);
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
			bitMap.setMappedColumn( columnANumber );
			
			KeyClass searchKey = null;
			
			System.out.println("A: colNo " + columnANumber + " & attr " + columnarFile.type[columnANumber-1].attrType );
			System.out.println("B: colNo " + columnBNumber + " & attr " + columnarFile.type[columnBNumber-1].attrType );
			
			//If value is a string, create a BTree with a string key, if its an integer, create a BTree with an integer key
			if (columnarFile.type[columnANumber-1].attrType == AttrType.attrInteger)
			{
				searchKey = new IntegerKey(Integer.parseInt(valueA));
			}
			else if (columnarFile.type[columnANumber-1].attrType == AttrType.attrString)
			{
				// Create a key for the search value
				isStringKey = true;
				searchKey = new StringKey(valueA);
			}

			// Create a BitMapFileScan object
			BMFileScan bitMapScan = null; 

			/*
			 * Convert the operator against Column A into a formatted scan
			 * range using the following key iterators
			 */
			if (operatorA.equals("RANGE"))
			{
				bitMapScan = bitMap.new_scan(null, null);
			}
			else if (operatorA.equals("<="))
			{
				bitMapScan = bitMap.new_scan(null, searchKey);
			}
			else if (operatorA.equals("<")){
                if (isStringKey) {
                    bitMapScan = bitMap.new_scan(null, searchKey);
                } else {
                    searchKey = new IntegerKey(Integer.parseInt(valueA)-1);
                    bitMapScan = bitMap.new_scan(null, searchKey);
                }
            }
			else if (operatorA.equals(">="))
			{
				bitMapScan = bitMap.new_scan(searchKey, null);
			}
			else if (operatorA.equals(">"))
			{
                if (isStringKey) {
                    bitMapScan = bitMap.new_scan(searchKey, null);
                } else {
                    searchKey = new IntegerKey(Integer.parseInt(valueA)+1);
                    bitMapScan = bitMap.new_scan(searchKey, null);
                }
            } 
			else if (operatorA.equals("!=") || operatorA.equals("NOT"))
			{
				if (isStringKey)
				{
					bitMapScan = bitMap.new_scan(null, searchKey);
				} else {
					searchKey = new IntegerKey(Integer.parseInt(valueA)-1);
					bitMapScan = bitMap.new_scan(null, searchKey);
				}
            }
			else
			{
				bitMapScan = bitMap.new_scan(searchKey, searchKey);
			}
			
			// Iterate through the indices scoped within the scan for Column A
			KeyDataEntry entry;
			while ((entry = bitMapScan.get_next()) != null)
			{
				//match on Column A criteria, check column B and constraint for match
				int pos = bitMapScan.getLatestPositionMatch();
				//when iterating the first column we will never get
				//duplicates and can immediately append the result
				dupeFreeList.add( 0, pos );
			}
			
			//In a not equal to instance, we will now do a follow-up
			//iteration over the values after the passed argument
			//for Column A
			if(operatorA.equals("!=") || operatorA.equals("NOT"))
			{
				if (isStringKey)
				{
					bitMapScan.closeBitmapScans();
					bitMapScan = bitMap.new_scan(searchKey, null);
				} else {
					bitMapScan.closeBitmapScans();
					searchKey = new IntegerKey(Integer.parseInt(valueA)+1);
					bitMapScan = bitMap.new_scan(searchKey, null);
				}
				
				while ((entry = bitMapScan.get_next()) != null)
				{
					//match on Column A criteria, check column B and constraint for match
					int pos = bitMapScan.getLatestPositionMatch();
					//need to avoid duplicates in "!=" cases on the first column
					if( false == dupeFreeList.contains( pos ) )
					{
						dupeFreeList.add( 0, pos );
					}
				}
			}
			//close the scan on Column A as we are done using it
			bitMapScan.closeBitmapScans();
            bitMap.close();
			
			//now we need the values in column B matching the criteria
			//but in an OR case we will aim to avoid duplicate results to
			//what was matched in Column A
			if( OR_OP == constraint )
			{
				bmFileName = columnarFileName + ".bitmap" + Integer.toString(columnBNumber);
				try
				{
					bitMap = new BitMapFile( bmFileName );
				} catch (Exception e) {
					System.out.println("Could not open " + bmFileName);
					e.printStackTrace();
					return;
				} 
				bitMap.setSrcColumnarFile( columnarFile );
				bitMap.setMappedColumn( columnBNumber );
				
				//If value is a string, create a BTree with a string key, if its an integer, create a BTree with an integer key
				isStringKey = false;
				if (columnarFile.type[columnBNumber-1].attrType == AttrType.attrInteger)
				{
					searchKey = new IntegerKey(Integer.parseInt(valueB));
				}
				else if (columnarFile.type[columnBNumber-1].attrType == AttrType.attrString)
				{
					// Create a key for the search value
					isStringKey = true;
					searchKey = new StringKey(valueB);
				}
				
				/*
				 * Convert the operator against Column B into a formatted scan
				 * range using the following key iterators
				 */
				if (operatorB.equals("<="))
				{
					bitMapScan = bitMap.new_scan(null, searchKey);
				}
				else if (operatorB.equals("<")){
					if (isStringKey) {
						bitMapScan = bitMap.new_scan(null, searchKey);
					} else {
						searchKey = new IntegerKey(Integer.parseInt(valueB)-1);
						bitMapScan = bitMap.new_scan(null, searchKey);
					}
				}
				else if (operatorB.equals(">="))
				{
					bitMapScan = bitMap.new_scan(searchKey, null);
				}
				else if (operatorB.equals(">"))
				{
					if (isStringKey) {
						bitMapScan = bitMap.new_scan(searchKey, null);
					} else {
						searchKey = new IntegerKey(Integer.parseInt(valueB)+1);
						bitMapScan = bitMap.new_scan(searchKey, null);
					}
				} 
				else if (operatorB.equals("!=") || operatorB.equals("NOT"))
				{
					if (isStringKey)
					{
						bitMapScan = bitMap.new_scan(null, searchKey);
					} else {
						searchKey = new IntegerKey(Integer.parseInt(valueB)-1);
						bitMapScan = bitMap.new_scan(null, searchKey);
					}
				}
				else
				{
					bitMapScan = bitMap.new_scan(searchKey, searchKey);
				}
				
				// Iterate through B-Tree index entires
				while ((entry = bitMapScan.get_next()) != null)
				{
					//match on Column A criteria, check column B and constraint for match
					int pos = bitMapScan.getLatestPositionMatch();
					
					//need to avoid duplicates in second column
					if( false == dupeFreeList.contains( pos ) )
					{
						dupeFreeList.add( 0, pos );
					}
				}
				
				//In a not equal to instance, we will now do a follow-up
				//iteration over the values after the passed argument
				if(operatorB.equals("!=") || operatorB.equals("NOT"))
				{
					if (isStringKey)
					{
						bitMapScan.closeBitmapScans();
						bitMapScan = bitMap.new_scan(searchKey, null);
					} else {
						bitMapScan.closeBitmapScans();
						searchKey = new IntegerKey(Integer.parseInt(valueB)+1);
						bitMapScan = bitMap.new_scan(searchKey, null);
					}
					
					while ((entry = bitMapScan.get_next()) != null)
					{
						//match on Column A criteria, check column B and constraint for match
						int pos = bitMapScan.getLatestPositionMatch();
						
						//need to avoid duplicates in second column
						if( false == dupeFreeList.contains( pos ) )
						{
							dupeFreeList.add( 0, pos );
						}
					}
				}
				//close the scan on Column A as we are done using it
				bitMapScan.closeBitmapScans();
				bitMap.close();
			}
			
			//The string checks for >/</!= have not filtered out exact matches yet
			//so at this point it is necessary to do a little extra filtering
			KeyClass searchKeyA = null;
			boolean filterColumnAFurther = false;
			if( (columnarFile.type[columnANumber-1].attrType == AttrType.attrString) && 
				(operatorA.equals(">") || operatorA.equals("<") || operatorA.equals("!=")) )
			{
				filterColumnAFurther = true;
				searchKeyA = new StringKey(valueA);
			}
			boolean filterColumnBFurther = false;
			if( (OR_OP == constraint) &&
				(columnarFile.type[columnBNumber-1].attrType == AttrType.attrString) && 
				(operatorB.equals(">") || operatorB.equals("<") || operatorB.equals("!=")) )
			{
				filterColumnBFurther = true;
			}	
			//iterate and print the duplicate free results
			for( int i = 0; i < dupeFreeList.size(); i++ )
			{				
				int pos = dupeFreeList.elementAt(i);
				TID tid1 = columnarFile.getTidFromPosition(pos);
				Tuple tuple = columnarFile.getTuple(tid1);
				boolean validMatch = true;
				if( filterColumnAFurther || filterColumnBFurther )
				{
					//On an AND_OP we only need to review the first column, as the second
					//isn't in the dupeFreeList
					if( (AND_OP == constraint) && filterColumnAFurther )
					{
						if( (((StringKey)searchKeyA).getKey().equals(tuple.getStrFld(columnANumber))) )
						{
							validMatch = false;
						}
					}
					//On an OR_OP we exclude it if BOTH constraint rejects the string
					else
					{
						//both columns are strings to potentially filter further
						if( filterColumnAFurther && filterColumnBFurther )
						{
							if( (((StringKey)searchKeyA).getKey().equals(tuple.getStrFld(columnANumber))) &&
								(((StringKey)searchKey).getKey().equals(tuple.getStrFld(columnBNumber))) )
							{
								validMatch = false;
							}
						}
						//these next 2 will be reached if 1 column is not a string, and we'll need and since
						//those filter >/</!= completely by here we only need to reconfirm the string inclusion
						else if( filterColumnAFurther )
						{
							boolean altColInvalidToo = andCheckColumnBool( columnarFile, columnBNumber, tuple, operatorB, valueB);
							if( (((StringKey)searchKeyA).getKey().equals(tuple.getStrFld(columnANumber))) && !altColInvalidToo )
							{
								validMatch = false;
							}
						}
						else if( filterColumnBFurther )
						{
							boolean altColInvalidToo = andCheckColumnBool( columnarFile, columnANumber, tuple, operatorA, valueA);
							if( (((StringKey)searchKey).getKey().equals(tuple.getStrFld(columnBNumber))) && !altColInvalidToo )
							{
								validMatch = false;
							}
						}
					}
				}
				if( validMatch )
				{
					//for an AND_OP we still need to confirm the
					//values in the 2nd column match criteria
					if( AND_OP == constraint )
					{
						boolean andCondMet = andCheckColumnBool( columnarFile, columnBNumber, tuple, operatorB, valueB);
						if( andCondMet )
						{
							printTupleColumns( columnarFileName, tuple, targetColumns, tid1, isDelete );
						}
					}
					//else it's either or and we know it's adupe free list by now
					else
					{
						printTupleColumns( columnarFileName, tuple, targetColumns, tid1, isDelete );
					}
				}
			}

			SystemDefs.JavabaseBM.flushAllPages();

			// Print the number of disk pages read and written
			System.out.println("Number of disk pages read: " + PCounter.getReadCount());
			System.out.println("Number of disk pages written: " + PCounter.getWriteCount());
			
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
	private static void executeCBitMapQuery(String columnDBName, String columnarFileName, int[] targetColumns,
											String[] valueConstraintsC1, String[] valueConstraintsC2, int constraint,
											boolean isDelete )
	{
		PCounter.initialize();
		try {
			// Parse value constraints
			String columnAName = valueConstraintsC1[0];
			String operatorA = valueConstraintsC1[1];
			String valueA = valueConstraintsC1[2];
			boolean isStringKey = false;
			String columnBName = valueConstraintsC2[0];
			String operatorB = valueConstraintsC2[1];
			String valueB = valueConstraintsC2[2];

			// Create a ColumnarFile object
			Columnarfile columnarFile = new Columnarfile(columnarFileName);
			int columnANumber = Arrays.asList(columnarFile.columnNames).indexOf(columnAName)+1;
			int columnBNumber = Arrays.asList(columnarFile.columnNames).indexOf(columnBName)+1;
			
			//we will need a linked list if the constraint is OR_OP
			Vector<Integer> dupeFreeList = new Vector<Integer>();

			//init bitmap for column 1 (in the case of "and" cosntraint we won't need to
			//open the 2nd bitmap file, so for now leave it be)
			String cbmFileName = columnarFileName + ".cbitmap" + Integer.toString(columnANumber);
			CBitMapFile cbitMap;
			try
			{
				cbitMap = new CBitMapFile( cbmFileName );
			} catch (Exception e) {
				System.out.println("Could not open " + cbmFileName);
				e.printStackTrace();
				return;
			} 
			cbitMap.setSrcColumnarFile( columnarFile );
			cbitMap.setMappedColumn( columnANumber );
			
			KeyClass searchKey = null;
			
			System.out.println("A: colNo " + columnANumber + " & attr " + columnarFile.type[columnANumber-1].attrType );
			System.out.println("B: colNo " + columnBNumber + " & attr " + columnarFile.type[columnBNumber-1].attrType );
			
			//If value is a string, create a BTree with a string key, if its an integer, create a BTree with an integer key
			if (columnarFile.type[columnANumber-1].attrType == AttrType.attrInteger)
			{
				searchKey = new IntegerKey(Integer.parseInt(valueA));
			}
			else if (columnarFile.type[columnANumber-1].attrType == AttrType.attrString)
			{
				// Create a key for the search value
				isStringKey = true;
				searchKey = new StringKey(valueA);
			}

			// Create a CBitMapFileScan object
			CBMFileScan cbitMapScan = null; 

			/*
			 * Convert the operator against Column A into a formatted scan
			 * range using the following key iterators
			 */
			if (operatorA.equals("RANGE"))
			{
				cbitMapScan = cbitMap.new_scan(null, null);
			}
			else if (operatorA.equals("<="))
			{
				cbitMapScan = cbitMap.new_scan(null, searchKey);
			}
			else if (operatorA.equals("<")){
                if (isStringKey) {
                    cbitMapScan = cbitMap.new_scan(null, searchKey);
                } else {
                    searchKey = new IntegerKey(Integer.parseInt(valueA)-1);
                    cbitMapScan = cbitMap.new_scan(null, searchKey);
                }
            }
			else if (operatorA.equals(">="))
			{
				cbitMapScan = cbitMap.new_scan(searchKey, null);
			}
			else if (operatorA.equals(">"))
			{
                if (isStringKey) {
                    cbitMapScan = cbitMap.new_scan(searchKey, null);
                } else {
                    searchKey = new IntegerKey(Integer.parseInt(valueA)+1);
                    cbitMapScan = cbitMap.new_scan(searchKey, null);
                }
            } 
			else if (operatorA.equals("!=") || operatorA.equals("NOT"))
			{
				if (isStringKey)
				{
					cbitMapScan = cbitMap.new_scan(null, searchKey);
				} else {
					searchKey = new IntegerKey(Integer.parseInt(valueA)-1);
					cbitMapScan = cbitMap.new_scan(null, searchKey);
				}
            }
			else
			{
				cbitMapScan = cbitMap.new_scan(searchKey, searchKey);
			}
			
			// Iterate through the indices scoped within the scan for Column A
			KeyDataEntry entry;
			while ((entry = cbitMapScan.get_next()) != null)
			{
				//match on Column A criteria, check column B and constraint for match
				int pos = cbitMapScan.getLatestPositionMatch();
				//when iterating the first column we will never get
				//duplicates and can immediately append the result
				dupeFreeList.add( 0, pos );
			}
			
			
			//In a not equal to instance, we will now do a follow-up
			//iteration over the values after the passed argument
			//for Column A
			if(operatorA.equals("!=") || operatorA.equals("NOT"))
			{
				if (isStringKey)
				{
					cbitMapScan.closeCbitmapScans();
					cbitMapScan = cbitMap.new_scan(searchKey, null);
				} else {
					cbitMapScan.closeCbitmapScans();
					searchKey = new IntegerKey(Integer.parseInt(valueA)+1);
					cbitMapScan = cbitMap.new_scan(searchKey, null);
				}
				
				while ((entry = cbitMapScan.get_next()) != null)
				{
					//match on Column A criteria, check column B and constraint for match
					int pos = cbitMapScan.getLatestPositionMatch();
					//need to avoid duplicates in "!=" cases on the first column
					if( false == dupeFreeList.contains( pos ) )
					{
						dupeFreeList.add( 0, pos );
					}
				}
			}
			//close the scan on Column A as we are done using it
			cbitMapScan.closeCbitmapScans();
            cbitMap.close();
			
			//now we need the values in column B matching the criteria
			//but in an OR case we will aim to avoid duplicate results to
			//what was matched in Column A
			if( OR_OP == constraint )
			{
				cbmFileName = columnarFileName + ".cbitmap" + Integer.toString(columnBNumber);
				try
				{
					cbitMap = new CBitMapFile( cbmFileName );
				} catch (Exception e) {
					System.out.println("Could not open " + cbmFileName);
					e.printStackTrace();
					return;
				} 
				cbitMap.setSrcColumnarFile( columnarFile );
				cbitMap.setMappedColumn( columnBNumber );
				
				//If value is a string, create a BTree with a string key, if its an integer, create a BTree with an integer key
				isStringKey = false;
				if (columnarFile.type[columnBNumber-1].attrType == AttrType.attrInteger)
				{
					searchKey = new IntegerKey(Integer.parseInt(valueB));
				}
				else if (columnarFile.type[columnBNumber-1].attrType == AttrType.attrString)
				{
					// Create a key for the search value
					isStringKey = true;
					searchKey = new StringKey(valueB);
				}
				
				/*
				 * Convert the operator against Column B into a formatted scan
				 * range using the following key iterators
				 */
				if (operatorB.equals("<="))
				{
					cbitMapScan = cbitMap.new_scan(null, searchKey);
				}
				else if (operatorB.equals("<")){
					if (isStringKey) {
						cbitMapScan = cbitMap.new_scan(null, searchKey);
					} else {
						searchKey = new IntegerKey(Integer.parseInt(valueB)-1);
						cbitMapScan = cbitMap.new_scan(null, searchKey);
					}
				}
				else if (operatorB.equals(">="))
				{
					cbitMapScan = cbitMap.new_scan(searchKey, null);
				}
				else if (operatorB.equals(">"))
				{
					if (isStringKey) {
						cbitMapScan = cbitMap.new_scan(searchKey, null);
					} else {
						searchKey = new IntegerKey(Integer.parseInt(valueB)+1);
						cbitMapScan = cbitMap.new_scan(searchKey, null);
					}
				} 
				else if (operatorB.equals("!=") || operatorB.equals("NOT"))
				{
					if (isStringKey)
					{
						cbitMapScan = cbitMap.new_scan(null, searchKey);
					} else {
						searchKey = new IntegerKey(Integer.parseInt(valueB)-1);
						cbitMapScan = cbitMap.new_scan(null, searchKey);
					}
				}
				else
				{
					cbitMapScan = cbitMap.new_scan(searchKey, searchKey);
				}
				
				// Iterate through B-Tree index entires
				while ((entry = cbitMapScan.get_next()) != null)
				{
					//match on Column A criteria, check column B and constraint for match
					int pos = cbitMapScan.getLatestPositionMatch();
					
					//need to avoid duplicates in second column
					if( false == dupeFreeList.contains( pos ) )
					{
						dupeFreeList.add( 0, pos );
					}
				}
				
				//In a not equal to instance, we will now do a follow-up
				//iteration over the values after the passed argument
				if(operatorB.equals("!=") || operatorB.equals("NOT"))
				{
					if (isStringKey)
					{
						cbitMapScan.closeCbitmapScans();
						cbitMapScan = cbitMap.new_scan(searchKey, null);
					} else {
						cbitMapScan.closeCbitmapScans();
						searchKey = new IntegerKey(Integer.parseInt(valueB)+1);
						cbitMapScan = cbitMap.new_scan(searchKey, null);
					}
					
					while ((entry = cbitMapScan.get_next()) != null)
					{
						//match on Column A criteria, check column B and constraint for match
						int pos = cbitMapScan.getLatestPositionMatch();
						
						//need to avoid duplicates in second column
						if( false == dupeFreeList.contains( pos ) )
						{
							dupeFreeList.add( 0, pos );
						}
					}
				}
				//close the scan on Column A as we are done using it
				cbitMapScan.closeCbitmapScans();
				cbitMap.close();
			}
			
			//The string checks for >/</!= have not filtered out exact matches yet
			//so at this point it is necessary to do a little extra filtering
			KeyClass searchKeyA = null;
			boolean filterColumnAFurther = false;
			if( (columnarFile.type[columnANumber-1].attrType == AttrType.attrString) && 
				(operatorA.equals(">") || operatorA.equals("<") || operatorA.equals("!=")) )
			{
				filterColumnAFurther = true;
				searchKeyA = new StringKey(valueA);
			}
			boolean filterColumnBFurther = false;
			if( (OR_OP == constraint) &&
				(columnarFile.type[columnBNumber-1].attrType == AttrType.attrString) && 
				(operatorB.equals(">") || operatorB.equals("<") || operatorB.equals("!=")) )
			{
				filterColumnBFurther = true;
			}	
			//iterate and print the duplicate free results
			for( int i = 0; i < dupeFreeList.size(); i++ )
			{				
				int pos = dupeFreeList.elementAt(i);
				TID tid1 = columnarFile.getTidFromPosition(pos);
				Tuple tuple = columnarFile.getTuple(tid1);
				boolean validMatch = true;
				if( filterColumnAFurther || filterColumnBFurther )
				{
					//On an AND_OP we only need to review the first column, as the second
					//isn't in the dupeFreeList
					if( (AND_OP == constraint) && filterColumnAFurther )
					{
						if( (((StringKey)searchKeyA).getKey().equals(tuple.getStrFld(columnANumber))) )
						{
							validMatch = false;
						}
					}
					//On an OR_OP we exclude it if BOTH constraint rejects the string
					else
					{
						//both columns are strings to potentially filter further
						if( filterColumnAFurther && filterColumnBFurther )
						{
							if( (((StringKey)searchKeyA).getKey().equals(tuple.getStrFld(columnANumber))) &&
								(((StringKey)searchKey).getKey().equals(tuple.getStrFld(columnBNumber))) )
							{
								validMatch = false;
							}
						}
						//these next 2 will be reached if 1 column is not a string, and we'll need and since
						//those filter >/</!= completely by here we only need to reconfirm the string inclusion
						else if( filterColumnAFurther )
						{
							boolean altColInvalidToo = andCheckColumnBool( columnarFile, columnBNumber, tuple, operatorB, valueB);
							if( (((StringKey)searchKeyA).getKey().equals(tuple.getStrFld(columnANumber))) && !altColInvalidToo )
							{
								validMatch = false;
							}
						}
						else if( filterColumnBFurther )
						{
							boolean altColInvalidToo = andCheckColumnBool( columnarFile, columnANumber, tuple, operatorA, valueA);
							if( (((StringKey)searchKey).getKey().equals(tuple.getStrFld(columnBNumber))) && !altColInvalidToo )
							{
								validMatch = false;
							}
						}
					}
				}
				if( validMatch )
				{
					//for an AND_OP we still need to confirm the
					//values in the 2nd column match criteria
					if( AND_OP == constraint )
					{
						boolean andCondMet = andCheckColumnBool( columnarFile, columnBNumber, tuple, operatorB, valueB);
						if( andCondMet )
						{
							printTupleColumns( columnarFileName, tuple, targetColumns, tid1, isDelete );
						}
					}
					//else it's either or and we know it's adupe free list by now
					else
					{
						printTupleColumns( columnarFileName, tuple, targetColumns, tid1, isDelete );
					}
				}
			}

			SystemDefs.JavabaseBM.flushAllPages();

			// Print the number of disk pages read and written
			System.out.println("Number of disk pages read: " + PCounter.getReadCount());
			System.out.println("Number of disk pages written: " + PCounter.getWriteCount());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}


