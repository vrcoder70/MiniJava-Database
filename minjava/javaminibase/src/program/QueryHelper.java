package program;

import java.util.*;

import btree.*;
import global.*;
import bitmap.*;
import columnar.*;
import heap.Tuple;

public class QueryHelper {
  public static int count = 0;

  public static final int AND_OP = 1;
  public static final int OR_OP = 2;

  private static void printTupleColumns(String columnarFileName, Tuple tuple, int[] targetColumns, TID tid,
      boolean isDelete) {
    try {
      // get a reference for column types
      Columnarfile columnarFile = new Columnarfile(columnarFileName);
      if (columnarFile.isTupleMarkedDeleted(tid.position)) {
        return;
      }
      if (isDelete) {
        columnarFile.markTupleDeleted(tid);
      }

      System.out.print("{");
      for (int i = 0; i < targetColumns.length; i++) {
        // formatting print
        if (0 != i) {
          System.out.print(", ");
        }

        // parse the type of this column
        int printColumn = targetColumns[i];
        if (columnarFile.type[printColumn - 1].attrType == AttrType.attrInteger) {
          System.out.print(tuple.getIntFld(printColumn));
        } else if (columnarFile.type[printColumn - 1].attrType == AttrType.attrReal) {
          System.out.print(tuple.getFloFld(printColumn));
        } else if (columnarFile.type[printColumn - 1].attrType == AttrType.attrString) {
          System.out.print(tuple.getStrFld(printColumn));
        } else {
          System.out.print("<ERROR-PRINTING>");
        }
      }
      System.out.println("}");
      count++;
    } catch (Exception ex) {
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
  public static boolean andCheckColumnBool(Columnarfile columnarFile, int ColNo, int[] targetColumns,
      String columnarFileName, Tuple tuple, String compOp, String checkValue) {
    boolean validMatch = false;
    try {
      boolean colBStringComp = false;
      if (columnarFile.type[ColNo - 1].attrType == AttrType.attrString) {
        colBStringComp = true;
      }
      /*
       * Check Column B at this position for using it's operator criteria
       * and if the value also passes in Column B we will print the Tuple
       */
      if (compOp.equals("<=")) {
        if (colBStringComp) {
          if (0 <= checkValue.compareTo(tuple.getStrFld(ColNo)))
            validMatch = true;
        } else {
          if (tuple.getIntFld(ColNo) <= Integer.parseInt(checkValue))
            validMatch = true;
        }
      } else if (compOp.equals("<")) {
        if (colBStringComp) {
          if (0 < checkValue.compareTo(tuple.getStrFld(ColNo)))
            validMatch = true;
        } else {
          if (tuple.getIntFld(ColNo) < Integer.parseInt(checkValue))
            validMatch = true;
        }
      } else if (compOp.equals(">=")) {
        if (colBStringComp) {
          if (0 >= checkValue.compareTo(tuple.getStrFld(ColNo)))
            validMatch = true;
        } else {
          if (tuple.getIntFld(ColNo) >= Integer.parseInt(checkValue))
            validMatch = true;
        }
      } else if (compOp.equals(">")) {
        if (colBStringComp) {
          if (0 > checkValue.compareTo(tuple.getStrFld(ColNo)))
            validMatch = true;
        } else {
          if (tuple.getIntFld(ColNo) > Integer.parseInt(checkValue))
            validMatch = true;
        }
      } else if (compOp.equals("!=") || compOp.equals("NOT")) {
        if (colBStringComp) {
          if (0 != checkValue.compareTo(tuple.getStrFld(ColNo)))
            validMatch = true;
        } else {
          if (Integer.parseInt(checkValue) != tuple.getIntFld(ColNo))
            validMatch = true;
        }
      } else {
        if (colBStringComp) {
          if (0 == checkValue.compareTo(tuple.getStrFld(ColNo)))
            validMatch = true;
        } else {
          if (Integer.parseInt(checkValue) == tuple.getIntFld(ColNo))
            validMatch = true;
        }
      }
    } catch (Exception e) {
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
  /**
   * @param columnarFileName
   * @param targetColumns
   * @param valueConstraints
   * @param isDelete
   * @return
   */
  public static int executeBTreeQuery(
      String columnarFileName,
      int[] targetColumns,
      String[] valueConstraints,
      boolean isDelete) {
    // Create a BTreeFile and get the header heapfile for the columnar file
    try {
      // Parse value constraints
      String columnName = valueConstraints[0];
      String operator = valueConstraints[1];
      String value = valueConstraints[2];
      boolean isStringKey = false;

      Columnarfile columnarFile = new Columnarfile(columnarFileName);
      int columnNumber = Arrays.asList(columnarFile.columnNames).indexOf(columnName) + 1;

      if (!columnarFile.createBTreeIndex(columnNumber)) {
        System.out.println("BTree Index for column " + columnName + " already exists");
      }

      System.out.println(columnarFileName + ".btree" + Integer.toString(columnNumber));
      BTreeFile btf = new BTreeFile(columnarFileName + ".btree" + Integer.toString(columnNumber));

      // Create a ColumnarIndexScan
      // ColumnarIndexScan columnarIndexScan = new ColumnarIndexScan(columnName,
      // columnNumber, 1, columnName, columnarFile.type, columnarFile.strSizes,
      // columnarFile.numColumns, targetColumns.length, null, null, true);

      // Creates a BTreeFile object using the index column name
      IntegerKey intKey = null;
      StringKey strKey = null;

      // If value is a string, create a BTree with a string key, if its an integer,
      // create a BTree with an integer key
      if (columnarFile.type[columnNumber - 1].attrType == AttrType.attrInteger) {
        intKey = new IntegerKey(Integer.parseInt(value));
      } else if (columnarFile.type[columnNumber - 1].attrType == AttrType.attrString) {
        // Create a key for the search value
        isStringKey = true;
        strKey = new StringKey(value);
      }

      // Create a BTreeFileScan object
      BTFileScan btfScan = null;
      // System.out.println(operator);
      // if operator = range, use the range scan method
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
      } else if (operator.equals(">")) {
        if (isStringKey) {
          // Figure out how to increment string key
          btfScan = btf.new_scan(strKey, null);
        } else {
          intKey = new IntegerKey(Integer.parseInt(value) + 1);
          btfScan = btf.new_scan(intKey, null);
        }
      } else if (operator.equals("<")) {
        if (isStringKey) {
          btfScan = btf.new_scan(null, strKey);
        } else {
          intKey = new IntegerKey(Integer.parseInt(value) - 1);
          btfScan = btf.new_scan(null, intKey);
        }
      }
      // If operator is Not equal to, use the range scan method
      else if (operator.equals("!=") || operator.equals("NOT")) {
        if (isStringKey) {
          btfScan = btf.new_scan(null, strKey);
        } else {
          intKey = new IntegerKey(Integer.parseInt(value) - 1);
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
      while ((entry = btfScan.get_next()) != null) {
        // Print the tuple
        final int pos = columnarFile.getPositionFromRid(((LeafData) entry.data).getData(), columnNumber);
        TID tid1 = columnarFile.getTidFromPosition(pos);
        Tuple tuple = columnarFile.getTuple(tid1);
        if ((isStringKey) && (operator.equals(">") || operator.equals("<") || operator.equals("!="))) {
          // <, >, != check over a range, but we want to ignore cases where
          // the returned value matches the query condition
          if (!(strKey.getKey().equals(tuple.getStrFld(columnNumber)))) {
            // print only the requested column values
            printTupleColumns(columnarFileName, tuple, targetColumns, tid1, isDelete);
          }
        } else {
          // print only the requested column values
          printTupleColumns(columnarFileName, tuple, targetColumns, tid1, isDelete);
        }
      }

      // SPECIAL CASE -- In a not equal to instance, we will now do a follow-up
      // iteration over the values after the passed argument
      if (operator.equals("!=") || operator.equals("NOT")) {
        if (isStringKey) {
          btfScan.DestroyBTreeFileScan();
          btfScan = btf.new_scan(strKey, null);
        } else {
          btfScan.DestroyBTreeFileScan();
          intKey = new IntegerKey(Integer.parseInt(value) + 1);
          btfScan = btf.new_scan(intKey, null);
        }

        while ((entry = btfScan.get_next()) != null) {
          // Print the tuple
          final int pos = columnarFile.getPositionFromRid(((LeafData) entry.data).getData(), columnNumber);
          TID tid1 = columnarFile.getTidFromPosition(pos);
          Tuple tuple = columnarFile.getTuple(tid1);
          if (isStringKey) {
            // != checks over a range, but we want to ignore cases where
            // the returned value matches the query condition
            if (!(strKey.getKey().equals(tuple.getStrFld(columnNumber)))) {
              // print only the requested column values
              printTupleColumns(columnarFileName, tuple, targetColumns, tid1, isDelete);
            }
          } else {
            printTupleColumns(columnarFileName, tuple, targetColumns, tid1, isDelete);
          }
        }
      }

      // Close the BTreeFileScan
      btfScan.DestroyBTreeFileScan();
      btf.close();

    } catch (Exception e) {
      e.printStackTrace();
    }
    return count;
  }

  /*
   * Method to complete a query utilizing the BTree index
   * Takes a DB to query, a columnar file the BTree is created from, the
   * columns of information to return on match, & the conditions of the query
   * (in format "<column> <expression> <value>")
   */
  public static int executeBTreeQuery(
      String columnarFileName,
      int[] targetColumns,
      String[] valueConstraintsC1,
      String[] valueConstraintsC2,
      int constraint,
      boolean isDelete) {
    // Create a BTreeFile and get the header heapfile for the columnar file
    try {
      // Parse value constraints
      // Parse value constraints
      String columnAName = valueConstraintsC1[0];
      String operatorA = valueConstraintsC1[1];
      String valueA = valueConstraintsC1[2];
      boolean isStringKey = false;
      String columnBName = valueConstraintsC2[0];
      String operatorB = valueConstraintsC2[1];
      String valueB = valueConstraintsC2[2];

      // create counters for the columns of the complex query
      Columnarfile columnarFile = new Columnarfile(columnarFileName);
      int columnANumber = Arrays.asList(columnarFile.columnNames).indexOf(columnAName) + 1;
      int columnBNumber = Arrays.asList(columnarFile.columnNames).indexOf(columnBName) + 1;

      // we will need a linked list if the constraint is OR_OP
      Vector<Integer> dupeFreeList = new Vector<Integer>();

      if (!columnarFile.createBTreeIndex(columnANumber)) {
        System.out.println("BTree Index for column " + columnAName + " already exists");
      }

      System.out.println(columnarFileName + ".btree" + Integer.toString(columnANumber));
      BTreeFile btf = new BTreeFile(columnarFileName + ".btree" + Integer.toString(columnANumber));

      KeyClass searchKey = null;

      // If value is a string, create a BTree with a string key, if its an integer,
      // create a BTree with an integer key
      if (columnarFile.type[columnANumber].attrType == AttrType.attrInteger) {
        searchKey = new IntegerKey(Integer.parseInt(valueA));
      } else if (columnarFile.type[columnANumber].attrType == AttrType.attrString) {
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
      if (operatorA.equals("RANGE")) {
        btfScan = btf.new_scan(null, null);
      } else if (operatorA.equals("<=")) {
        btfScan = btf.new_scan(null, searchKey);
      } else if (operatorA.equals("<")) {
        if (isStringKey) {
          btfScan = btf.new_scan(null, searchKey);
        } else {
          searchKey = new IntegerKey(Integer.parseInt(valueA) - 1);
          btfScan = btf.new_scan(null, searchKey);
        }
      } else if (operatorA.equals(">=")) {
        btfScan = btf.new_scan(searchKey, null);
      } else if (operatorA.equals(">")) {
        if (isStringKey) {
          btfScan = btf.new_scan(searchKey, null);
        } else {
          searchKey = new IntegerKey(Integer.parseInt(valueA) + 1);
          btfScan = btf.new_scan(searchKey, null);
        }
      } else if (operatorA.equals("!=") || operatorA.equals("NOT")) {
        if (isStringKey) {
          btfScan = btf.new_scan(null, searchKey);
        } else {
          searchKey = new IntegerKey(Integer.parseInt(valueA) - 1);
          btfScan = btf.new_scan(null, searchKey);
        }
      } else {
        btfScan = btf.new_scan(searchKey, searchKey);
      }

      // Iterate through B-Tree index entires
      KeyDataEntry entry;
      while ((entry = btfScan.get_next()) != null) {
        // Print the tuple
        int pos = columnarFile.getPositionFromRid(((LeafData) entry.data).getData(), columnANumber);
        // when iterating the first column we will never get
        // duplicates and can immediately append the result
        dupeFreeList.add(0, pos);
      }
      // In a not equal to instance, we will now do a follow-up
      // iteration over the values after the passed argument
      // for Column A
      if (operatorA.equals("!=") || operatorA.equals("NOT")) {
        if (isStringKey) {
          btfScan.DestroyBTreeFileScan();
          btfScan = btf.new_scan(searchKey, null);
        } else {
          btfScan.DestroyBTreeFileScan();
          searchKey = new IntegerKey(Integer.parseInt(valueA) + 1);
          btfScan = btf.new_scan(searchKey, null);
        }

        while ((entry = btfScan.get_next()) != null) {
          // match on Column A criteria, check column B and constraint for match
          int pos = columnarFile.getPositionFromRid(((LeafData) entry.data).getData(), columnANumber);
          // when iterating the first column we will never get
          // duplicates and can immediately append the result
          dupeFreeList.add(0, pos);
        }
      }
      // close the scan on Column A as we are done using it
      btfScan.DestroyBTreeFileScan();
      btf.close();

      // now we need the values in column B matching the criteria
      // but in an OR case we will aim to avoid duplicate results to
      // what was matched in Column A
      if (OR_OP == constraint) {
        String btreeFileName = columnarFileName + ".btree" + Integer.toString(columnBNumber);
        try {
          btf = new BTreeFile(btreeFileName);
        } catch (Exception e) {
          System.out.println("Could not open " + btreeFileName);
          e.printStackTrace();
          return count;
        }

        // If value is a string, create a BTree with a string key, if its an integer,
        // create a BTree with an integer key
        isStringKey = false;
        if (columnarFile.type[columnBNumber].attrType == AttrType.attrInteger) {
          searchKey = new IntegerKey(Integer.parseInt(valueB));
        } else if (columnarFile.type[columnBNumber].attrType == AttrType.attrString) {
          // Create a key for the search value
          isStringKey = true;
          searchKey = new StringKey(valueB);
        }

        /*
         * Convert the operator against Column B into a formatted scan
         * range using the following key iterators
         */
        if (operatorB.equals("<=")) {
          btfScan = btf.new_scan(null, searchKey);
        } else if (operatorB.equals("<")) {
          if (isStringKey) {
            btfScan = btf.new_scan(null, searchKey);
          } else {
            searchKey = new IntegerKey(Integer.parseInt(valueB) - 1);
            btfScan = btf.new_scan(null, searchKey);
          }
        } else if (operatorB.equals(">=")) {
          btfScan = btf.new_scan(searchKey, null);
        } else if (operatorB.equals(">")) {
          if (isStringKey) {
            btfScan = btf.new_scan(searchKey, null);
          } else {
            searchKey = new IntegerKey(Integer.parseInt(valueB) + 1);
            btfScan = btf.new_scan(searchKey, null);
          }
        } else if (operatorB.equals("!=") || operatorB.equals("NOT")) {
          if (isStringKey) {
            btfScan = btf.new_scan(null, searchKey);
          } else {
            searchKey = new IntegerKey(Integer.parseInt(valueB) - 1);
            btfScan = btf.new_scan(null, searchKey);
          }
        } else {
          btfScan = btf.new_scan(searchKey, searchKey);
        }

        // Iterate through B-Tree index entires
        while ((entry = btfScan.get_next()) != null) {
          // match on Column A criteria, check column B and constraint for match
          int pos = columnarFile.getPositionFromRid(((LeafData) entry.data).getData(), columnBNumber);
          // need to avoid duplicates in second column
          if (false == dupeFreeList.contains(pos)) {
            dupeFreeList.add(0, pos);
          }
        }

        // In a not equal to instance, we will now do a follow-up
        // iteration over the values after the passed argument
        if (operatorB.equals("!=") || operatorB.equals("NOT")) {
          if (isStringKey) {
            btfScan.DestroyBTreeFileScan();
            btfScan = btf.new_scan(searchKey, null);
          } else {
            btfScan.DestroyBTreeFileScan();
            searchKey = new IntegerKey(Integer.parseInt(valueA) + 1);
            btfScan = btf.new_scan(searchKey, null);
          }

          while ((entry = btfScan.get_next()) != null) {
            // match on Column A criteria, check column B and constraint for match
            int pos = columnarFile.getPositionFromRid(((LeafData) entry.data).getData(), columnBNumber);
            // need to avoid duplicates in second column
            if (false == dupeFreeList.contains(pos)) {
              dupeFreeList.add(0, pos);
            }
          }
        }
        // close the scan on Column A as we are done using it
        btfScan.DestroyBTreeFileScan();
        btf.close();
      }

      // iterate and print the duplicate free results
      for (int i = 0; i < dupeFreeList.size(); i++) {
        int pos = dupeFreeList.elementAt(i);
        TID tid1 = columnarFile.getTidFromPosition(pos);
        Tuple tuple = columnarFile.getTuple(tid1);
        boolean validMatch = true;
        if ((isStringKey) && (operatorB.equals(">") || operatorB.equals("<") || operatorB.equals("!="))) {
          // <, >, != check over a range, but we want to ignore cases where
          // the returned value matches the query condition
          if ((((StringKey) searchKey).getKey().equals(tuple.getStrFld(columnANumber)))) {
            validMatch = false;
          }
        }
        if (validMatch) {
          // for an AND_OP we still need to confirm the
          // values in the 2nd column match criteria
          if (AND_OP == constraint) {
            boolean andCondMet = andCheckColumnBool(columnarFile, columnBNumber, targetColumns,
                columnarFileName, tuple, operatorB, valueB);
            if (andCondMet) {
              printTupleColumns(columnarFileName, tuple, targetColumns, tid1, isDelete);
            }
          }
          // else it's either or and we know it's adupe free list by now
          else {
            printTupleColumns(columnarFileName, tuple, targetColumns, tid1, isDelete);
          }
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
    return count;
  }

  /*
   * Method to complete a query utilizing the Bitmap index
   * Takes a DB to query, a columnar file the Bitmap is created from, the
   * columns of information to return on match, & the conditions of the query
   * (in format "<column> <expression> <value>")
   */
  public static int executeBitMapQuery(String columnarFileName,
      int[] targetColumns, String[] valueConstraints, boolean isDelete) {
    try {
      // Parse value constraints
      String columnName = valueConstraints[0];
      String operator = valueConstraints[1];
      String value = valueConstraints[2];
      boolean isStringKey = false;

      // Create a ColumnarFile object
      Columnarfile columnarFile = new Columnarfile(columnarFileName);
      int columnNumber = Arrays.asList(columnarFile.columnNames).indexOf(columnName) + 1;

      String bmFileName = columnarFileName + ".bitmap" + Integer.toString(columnNumber);
      BitMapFile bitMap;
      try {
        bitMap = new BitMapFile(bmFileName);
      } catch (Exception e) {
        System.out.println("Could not open " + bmFileName);
        e.printStackTrace();
        return count;
      }
      bitMap.setSrcColumnarFile(columnarFile);
      bitMap.setMappedColumn(columnNumber);

      IntegerKey intKey = null;
      StringKey strKey = null;

      System.out.println("colNo " + columnNumber + " & attr " + columnarFile.type[columnNumber - 1].attrType);

      // If value is a string, create a BTree with a string key, if its an integer,
      // create a BTree with an integer key
      if (columnarFile.type[columnNumber - 1].attrType == AttrType.attrInteger) {
        intKey = new IntegerKey(Integer.parseInt(value));
      } else if (columnarFile.type[columnNumber - 1].attrType == AttrType.attrString) {
        // Create a key for the search value
        isStringKey = true;
        strKey = new StringKey(value);
      }

      // Create a BitMapFileScan object
      BMFileScan bitMapScan = null;

      // CASE -- Searching over a RANGE
      if (operator.equals("RANGE")) {
        bitMapScan = bitMap.new_scan(null, null);
      }
      // CASE -- Searching for all values less-than-or-equal-to
      else if (operator.equals("<=")) {
        if (isStringKey) {
          bitMapScan = bitMap.new_scan(null, strKey);
        } else {
          bitMapScan = bitMap.new_scan(null, intKey);
        }
      }
      // CASE -- Searching for all values less-than
      else if (operator.equals("<")) {
        if (isStringKey) {
          // need means to check up to but not including
          bitMapScan = bitMap.new_scan(null, strKey);
        } else {
          intKey = new IntegerKey(Integer.parseInt(value) - 1);
          bitMapScan = bitMap.new_scan(null, intKey);
        }
      }
      // CASE -- Searching for all values greater-than-or-equal-to
      else if (operator.equals(">=")) {
        if (isStringKey) {
          bitMapScan = bitMap.new_scan(strKey, null);
        } else {
          bitMapScan = bitMap.new_scan(intKey, null);
        }
      }
      // CASE -- Searching for all values greater-than
      else if (operator.equals(">")) {
        if (isStringKey) {
          // need to figure out way to go to n+1 string index
          bitMapScan = bitMap.new_scan(strKey, null);
        } else {
          intKey = new IntegerKey(Integer.parseInt(value) + 1);
          bitMapScan = bitMap.new_scan(intKey, null);
        }
      }
      // CASE -- Searching for all values not-equal to
      else if (operator.equals("!=") || operator.equals("NOT")) {
        if (isStringKey) {
          bitMapScan = bitMap.new_scan(null, strKey);
        } else {
          intKey = new IntegerKey(Integer.parseInt(value) - 1);
          bitMapScan = bitMap.new_scan(null, intKey);
        }
      }
      // DEFAULT CASE -- Searching for all values equal to
      else {
        if (isStringKey) {
          bitMapScan = bitMap.new_scan(strKey, strKey);
        } else {
          bitMapScan = bitMap.new_scan(intKey, intKey);
        }
      }

      // Iterate through B-Tree index entires
      KeyDataEntry entry;
      while ((entry = bitMapScan.get_next()) != null) {
        // Print the tuple
        int pos = columnarFile.getPositionFromRid(((LeafData) entry.data).getData(), columnNumber);
        TID tid1 = columnarFile.getTidFromPosition(pos);
        Tuple tuple = columnarFile.getTuple(tid1);
        if ((isStringKey) && (operator.equals(">") || operator.equals("<") || operator.equals("!="))) {
          // <, >, != check over a range, but we want to ignore cases where
          // the returned value matches the query condition
          if (!(strKey.getKey().equals(tuple.getStrFld(columnNumber)))) {
            // print only the requested column values
            printTupleColumns(columnarFileName, tuple, targetColumns, tid1, isDelete);
          }
        } else {
          // print only the requested column values
          printTupleColumns(columnarFileName, tuple, targetColumns, tid1, isDelete);
        }
      }

      // SPECIAL CASE -- In a not equal to instance, we will now do a follow-up
      // iteration over the values after the passed argument
      if (operator.equals("!=") || operator.equals("NOT")) {
        if (isStringKey) {
          bitMapScan.closeBitmapScans();
          bitMapScan = bitMap.new_scan(strKey, null);
        } else {
          bitMapScan.closeBitmapScans();
          intKey = new IntegerKey(Integer.parseInt(value) + 1);
          bitMapScan = bitMap.new_scan(intKey, null);
        }

        while ((entry = bitMapScan.get_next()) != null) {
          // Print the tuple
          int pos = columnarFile.getPositionFromRid(((LeafData) entry.data).getData(), columnNumber);
          TID tid1 = columnarFile.getTidFromPosition(pos);
          Tuple tuple = columnarFile.getTuple(tid1);
          if (isStringKey) {
            // != checks over a range, but we want to ignore cases where
            // the returned value matches the query condition
            if (!(strKey.getKey().equals(tuple.getStrFld(columnNumber)))) {
              // print only the requested column values
              printTupleColumns(columnarFileName, tuple, targetColumns, tid1, isDelete);
            }
          } else {
            printTupleColumns(columnarFileName, tuple, targetColumns, tid1, isDelete);
          }
        }
      }

      // clean up allocations accordingly
      bitMapScan.closeBitmapScans();
      bitMap.close();

    } catch (Exception e) {
      e.printStackTrace();
    }
    return count;
  }

  /*
   * Method to complete a query utilizing the Bitmap index
   * Takes a DB to query, a columnar file the Bitmap is created from, the
   * columns of information to return on match, & the conditions of the query
   * (in format "<column> <expression> <value>")
   */
  public static int executeBitMapQuery(String columnarFileName, int[] targetColumns,
      String[] valueConstraintsC1, String[] valueConstraintsC2, int constraint, boolean isDelete) {
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
      int columnANumber = Arrays.asList(columnarFile.columnNames).indexOf(columnAName) + 1;
      int columnBNumber = Arrays.asList(columnarFile.columnNames).indexOf(columnBName) + 1;

      // we will need a linked list if the constraint is OR_OP
      Vector<Integer> dupeFreeList = new Vector<Integer>();

      // init bitmap for column 1 (in the case of "and" cosntraint we won't need to
      // open the 2nd bitmap file, so for now leave it be)
      String bmFileName = columnarFileName + ".bitmap" + Integer.toString(columnANumber);
      BitMapFile bitMap;
      try {
        bitMap = new BitMapFile(bmFileName);
      } catch (Exception e) {
        System.out.println("Could not open " + bmFileName);
        e.printStackTrace();
        return count;
      }
      bitMap.setSrcColumnarFile(columnarFile);
      bitMap.setMappedColumn(columnANumber);

      KeyClass searchKey = null;

      System.out.println("A: colNo " + columnANumber + " & attr " + columnarFile.type[columnANumber - 1].attrType);
      System.out.println("B: colNo " + columnBNumber + " & attr " + columnarFile.type[columnBNumber - 1].attrType);

      // If value is a string, create a BTree with a string key, if its an integer,
      // create a BTree with an integer key
      if (columnarFile.type[columnANumber - 1].attrType == AttrType.attrInteger) {
        searchKey = new IntegerKey(Integer.parseInt(valueA));
      } else if (columnarFile.type[columnANumber - 1].attrType == AttrType.attrString) {
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
      if (operatorA.equals("RANGE")) {
        bitMapScan = bitMap.new_scan(null, null);
      } else if (operatorA.equals("<=")) {
        bitMapScan = bitMap.new_scan(null, searchKey);
      } else if (operatorA.equals("<")) {
        if (isStringKey) {
          bitMapScan = bitMap.new_scan(null, searchKey);
        } else {
          searchKey = new IntegerKey(Integer.parseInt(valueA) - 1);
          bitMapScan = bitMap.new_scan(null, searchKey);
        }
      } else if (operatorA.equals(">=")) {
        bitMapScan = bitMap.new_scan(searchKey, null);
      } else if (operatorA.equals(">")) {
        if (isStringKey) {
          bitMapScan = bitMap.new_scan(searchKey, null);
        } else {
          searchKey = new IntegerKey(Integer.parseInt(valueA) + 1);
          bitMapScan = bitMap.new_scan(searchKey, null);
        }
      } else if (operatorA.equals("!=") || operatorA.equals("NOT")) {
        if (isStringKey) {
          bitMapScan = bitMap.new_scan(null, searchKey);
        } else {
          searchKey = new IntegerKey(Integer.parseInt(valueA) - 1);
          bitMapScan = bitMap.new_scan(null, searchKey);
        }
      } else {
        bitMapScan = bitMap.new_scan(searchKey, searchKey);
      }

      // Iterate through the indices scoped within the scan for Column A
      KeyDataEntry entry;
      while ((entry = bitMapScan.get_next()) != null) {
        // match on Column A criteria, check column B and constraint for match
        int pos = bitMapScan.getLatestPositionMatch();
        // when iterating the first column we will never get
        // duplicates and can immediately append the result
        dupeFreeList.add(0, pos);
      }

      // In a not equal to instance, we will now do a follow-up
      // iteration over the values after the passed argument
      // for Column A
      if (operatorA.equals("!=") || operatorA.equals("NOT")) {
        if (isStringKey) {
          bitMapScan.closeBitmapScans();
          bitMapScan = bitMap.new_scan(searchKey, null);
        } else {
          bitMapScan.closeBitmapScans();
          searchKey = new IntegerKey(Integer.parseInt(valueA) + 1);
          bitMapScan = bitMap.new_scan(searchKey, null);
        }

        while ((entry = bitMapScan.get_next()) != null) {
          // match on Column A criteria, check column B and constraint for match
          int pos = bitMapScan.getLatestPositionMatch();
          // when iterating the first column we will never get
          // duplicates and can immediately append the result
          dupeFreeList.add(0, pos);
        }
      }
      // close the scan on Column A as we are done using it
      bitMapScan.closeBitmapScans();
      bitMap.close();

      // now we need the values in column B matching the criteria
      // but in an OR case we will aim to avoid duplicate results to
      // what was matched in Column A
      if (OR_OP == constraint) {
        bmFileName = columnarFileName + ".bitmap" + Integer.toString(columnBNumber);
        try {
          bitMap = new BitMapFile(bmFileName);
        } catch (Exception e) {
          System.out.println("Could not open " + bmFileName);
          e.printStackTrace();
          return count;
        }
        bitMap.setSrcColumnarFile(columnarFile);
        bitMap.setMappedColumn(columnBNumber);

        // If value is a string, create a BTree with a string key, if its an integer,
        // create a BTree with an integer key
        isStringKey = false;
        if (columnarFile.type[columnBNumber - 1].attrType == AttrType.attrInteger) {
          searchKey = new IntegerKey(Integer.parseInt(valueB));
        } else if (columnarFile.type[columnBNumber - 1].attrType == AttrType.attrString) {
          // Create a key for the search value
          isStringKey = true;
          searchKey = new StringKey(valueB);
        }

        /*
         * Convert the operator against Column B into a formatted scan
         * range using the following key iterators
         */
        if (operatorB.equals("<=")) {
          bitMapScan = bitMap.new_scan(null, searchKey);
        } else if (operatorB.equals("<")) {
          if (isStringKey) {
            bitMapScan = bitMap.new_scan(null, searchKey);
          } else {
            searchKey = new IntegerKey(Integer.parseInt(valueB) - 1);
            bitMapScan = bitMap.new_scan(null, searchKey);
          }
        } else if (operatorB.equals(">=")) {
          bitMapScan = bitMap.new_scan(searchKey, null);
        } else if (operatorB.equals(">")) {
          if (isStringKey) {
            bitMapScan = bitMap.new_scan(searchKey, null);
          } else {
            searchKey = new IntegerKey(Integer.parseInt(valueB) + 1);
            bitMapScan = bitMap.new_scan(searchKey, null);
          }
        } else if (operatorB.equals("!=") || operatorB.equals("NOT")) {
          if (isStringKey) {
            bitMapScan = bitMap.new_scan(null, searchKey);
          } else {
            searchKey = new IntegerKey(Integer.parseInt(valueB) - 1);
            bitMapScan = bitMap.new_scan(null, searchKey);
          }
        } else {
          bitMapScan = bitMap.new_scan(searchKey, searchKey);
        }

        // Iterate through B-Tree index entires
        while ((entry = bitMapScan.get_next()) != null) {
          // match on Column A criteria, check column B and constraint for match
          int pos = bitMapScan.getLatestPositionMatch();

          // need to avoid duplicates in second column
          if (false == dupeFreeList.contains(pos)) {
            dupeFreeList.add(0, pos);
          }
        }

        // In a not equal to instance, we will now do a follow-up
        // iteration over the values after the passed argument
        if (operatorB.equals("!=") || operatorB.equals("NOT")) {
          if (isStringKey) {
            bitMapScan.closeBitmapScans();
            bitMapScan = bitMap.new_scan(searchKey, null);
          } else {
            bitMapScan.closeBitmapScans();
            searchKey = new IntegerKey(Integer.parseInt(valueB) + 1);
            bitMapScan = bitMap.new_scan(searchKey, null);
          }

          while ((entry = bitMapScan.get_next()) != null) {
            // match on Column A criteria, check column B and constraint for match
            int pos = bitMapScan.getLatestPositionMatch();

            // need to avoid duplicates in second column
            if (false == dupeFreeList.contains(pos)) {
              dupeFreeList.add(0, pos);
            }
          }
        }
        // close the scan on Column A as we are done using it
        bitMapScan.closeBitmapScans();
        bitMap.close();
      }

      // iterate and print the duplicate free results
      for (int i = 0; i < dupeFreeList.size(); i++) {
        int pos = dupeFreeList.elementAt(i);
        TID tid1 = columnarFile.getTidFromPosition(pos);
        Tuple tuple = columnarFile.getTuple(tid1);
        boolean validMatch = true;
        if ((isStringKey) && (operatorB.equals(">") || operatorB.equals("<") || operatorB.equals("!="))) {
          // <, >, != check over a range, but we want to ignore cases where
          // the returned value matches the query condition
          if ((((StringKey) searchKey).getKey().equals(tuple.getStrFld(columnANumber)))) {
            validMatch = false;
          }
        }
        if (validMatch) {
          // for an AND_OP we still need to confirm the
          // values in the 2nd column match criteria
          if (AND_OP == constraint) {
            boolean andCondMet = andCheckColumnBool(columnarFile, columnBNumber, targetColumns,
                columnarFileName, tuple, operatorB, valueB);
            if (andCondMet) {
              printTupleColumns(columnarFileName, tuple, targetColumns, tid1, isDelete);
            }
          }
          // else it's either or and we know it's adupe free list by now
          else {
            printTupleColumns(columnarFileName, tuple, targetColumns, tid1, isDelete);
          }
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
    return count;
  }

  /*
   * Method to complete a query utilizing the Compressed Bitmap index
   * Takes a DB to query, a columnar file the Compressed Bitmap is created from,
   * the
   * columns of information to return on match, & the conditions of the query
   * (in format "<column> <expression> <value>")
   */
  public static int executeCBitMapQuery(String columnarFileName,
      int[] targetColumns, String[] valueConstraints, boolean isDelete) {
    try {
      // Parse value constraints
      String columnName = valueConstraints[0];
      String operator = valueConstraints[1];
      String value = valueConstraints[2];
      boolean isStringKey = false;

      // Create a ColumnarFile object
      Columnarfile columnarFile = new Columnarfile(columnarFileName);
      int columnNumber = Arrays.asList(columnarFile.columnNames).indexOf(columnName) + 1;

      String bmFileName = columnarFileName + ".cbitmap" + Integer.toString(columnNumber);
      CBitMapFile cbitmap;
      try {
        cbitmap = new CBitMapFile(bmFileName);
      } catch (Exception e) {
        System.out.println("Could not open " + bmFileName);
        e.printStackTrace();
        return count;
      }
      cbitmap.setSrcColumnarFile(columnarFile);
      cbitmap.setMappedColumn(columnNumber);

      IntegerKey intKey = null;
      StringKey strKey = null;

      System.out.println("colNo " + columnNumber + " & attr " + columnarFile.type[columnNumber - 1].attrType);

      // If value is a string, create a BTree with a string key, if its an integer,
      // create a BTree with an integer key
      if (columnarFile.type[columnNumber - 1].attrType == AttrType.attrInteger) {
        intKey = new IntegerKey(Integer.parseInt(value));
      } else if (columnarFile.type[columnNumber - 1].attrType == AttrType.attrString) {
        // Create a key for the search value
        isStringKey = true;
        strKey = new StringKey(value);
      }

      // Create a BitMapFileScan object
      CBMFileScan cbitmapScan = null;

      // if operator = range, use the range scan method
      if (operator.equals("RANGE")) {
        cbitmapScan = cbitmap.new_scan(null, null);
      }
      // If operator is less than or equal to, use the range scan method
      else if (operator.equals("<=")) {
        if (isStringKey) {
          cbitmapScan = cbitmap.new_scan(null, strKey);
        } else {
          cbitmapScan = cbitmap.new_scan(null, intKey);
        }
      }
      // If operator is greater than or equal to, use the range scan method
      else if (operator.equals(">=")) {
        if (isStringKey) {
          cbitmapScan = cbitmap.new_scan(strKey, null);
        } else {
          cbitmapScan = cbitmap.new_scan(intKey, null);
        }
      }
      // greater than case
      else if (operator.equals(">")) {
        if (isStringKey) {
          cbitmapScan = cbitmap.new_scan(strKey, null);
        } else {
          intKey = new IntegerKey(Integer.parseInt(value) + 1);
          cbitmapScan = cbitmap.new_scan(intKey, null);
        }
      }
      // less than case
      else if (operator.equals("<")) {
        if (isStringKey) {
          cbitmapScan = cbitmap.new_scan(null, strKey);
        } else {
          intKey = new IntegerKey(Integer.parseInt(value) - 1);
          cbitmapScan = cbitmap.new_scan(null, intKey);
        }
      }
      // If operator is Not equal to, use the range scan method
      else if (operator.equals("!=") || operator.equals("NOT")) {
        if (isStringKey) {
          cbitmapScan = cbitmap.new_scan(null, strKey);
        } else {
          intKey = new IntegerKey(Integer.parseInt(value) - 1);
          cbitmapScan = cbitmap.new_scan(null, intKey);
        }
      }
      // DEFAULT -- operator is equals
      else {
        // Create a point scan
        if (isStringKey) {
          cbitmapScan = cbitmap.new_scan(strKey, strKey);
        } else {
          cbitmapScan = cbitmap.new_scan(intKey, intKey);
        }
      }

      // Iterate through B-Tree index entires
      KeyDataEntry entry;
      while ((entry = cbitmapScan.get_next()) != null) {
        // Print the tuple
        int pos = columnarFile.getPositionFromRid(((LeafData) entry.data).getData(), columnNumber);
        TID tid1 = columnarFile.getTidFromPosition(pos);
        Tuple tuple = columnarFile.getTuple(tid1);
        if ((isStringKey) && (operator.equals(">") || operator.equals("<") || operator.equals("!="))) {
          // <, >, != check over a range, but we want to ignore cases where
          // the returned value matches the query condition
          if (!(strKey.getKey().equals(tuple.getStrFld(columnNumber)))) {
            // print only the requested column values
            printTupleColumns(columnarFileName, tuple, targetColumns, tid1, isDelete);
          }
        } else {
          // print only the requested column values
          printTupleColumns(columnarFileName, tuple, targetColumns, tid1, isDelete);
        }
      }

      // SPECIAL CASE -- In a not equal to instance, we will now do a follow-up
      // iteration over the values after the passed argument
      if (operator.equals("!=") || operator.equals("NOT")) {
        if (isStringKey) {
          cbitmapScan.closeCbitmapScans();
          cbitmapScan = cbitmap.new_scan(strKey, null);
        } else {
          cbitmapScan.closeCbitmapScans();
          intKey = new IntegerKey(Integer.parseInt(value) + 1);
          cbitmapScan = cbitmap.new_scan(intKey, null);
        }

        while ((entry = cbitmapScan.get_next()) != null) {
          // Print the tuple
          int pos = columnarFile.getPositionFromRid(((LeafData) entry.data).getData(), columnNumber);
          TID tid1 = columnarFile.getTidFromPosition(pos);
          Tuple tuple = columnarFile.getTuple(tid1);
          if (isStringKey) {
            // != checks over a range, but we want to ignore cases where
            // the returned value matches the query condition
            if (!(strKey.getKey().equals(tuple.getStrFld(columnNumber)))) {
              // print only the requested column values
              printTupleColumns(columnarFileName, tuple, targetColumns, tid1, isDelete);
            }
          } else {
            printTupleColumns(columnarFileName, tuple, targetColumns, tid1, isDelete);
          }
        }
      }

      // clean up allocations accordingly
      cbitmapScan.closeCbitmapScans();
      cbitmap.close();

    } catch (Exception e) {
      e.printStackTrace();
    }
    return count;
  }

  /*
   * Method to complete a query utilizing the Compressed Bitmap index
   * Takes a DB to query, a columnar file the Compressed Bitmap is created from,
   * the
   * columns of information to return on match, & the conditions of the query
   * (in format "<column> <expression> <value>")
   */
  public static int executeCBitMapQuery(String columnarFileName, int[] targetColumns,
      String[] valueConstraintsC1, String[] valueConstraintsC2, int constraint, boolean isDelete) {
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
      int columnANumber = Arrays.asList(columnarFile.columnNames).indexOf(columnAName) + 1;
      int columnBNumber = Arrays.asList(columnarFile.columnNames).indexOf(columnBName) + 1;

      // we will need a linked list if the constraint is OR_OP
      Vector<Integer> dupeFreeList = new Vector<Integer>();

      // init bitmap for column 1 (in the case of "and" cosntraint we won't need to
      // open the 2nd bitmap file, so for now leave it be)
      String cbmFileName = columnarFileName + ".cbitmap" + Integer.toString(columnANumber);
      CBitMapFile cbitMap;
      try {
        cbitMap = new CBitMapFile(cbmFileName);
      } catch (Exception e) {
        System.out.println("Could not open " + cbmFileName);
        e.printStackTrace();
        return count;
      }
      cbitMap.setSrcColumnarFile(columnarFile);
      cbitMap.setMappedColumn(columnANumber);

      KeyClass searchKey = null;

      System.out.println("A: colNo " + columnANumber + " & attr " + columnarFile.type[columnANumber - 1].attrType);
      System.out.println("B: colNo " + columnBNumber + " & attr " + columnarFile.type[columnBNumber - 1].attrType);

      // If value is a string, create a BTree with a string key, if its an integer,
      // create a BTree with an integer key
      if (columnarFile.type[columnANumber - 1].attrType == AttrType.attrInteger) {
        searchKey = new IntegerKey(Integer.parseInt(valueA));
      } else if (columnarFile.type[columnANumber - 1].attrType == AttrType.attrString) {
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
      if (operatorA.equals("RANGE")) {
        cbitMapScan = cbitMap.new_scan(null, null);
      } else if (operatorA.equals("<=")) {
        cbitMapScan = cbitMap.new_scan(null, searchKey);
      } else if (operatorA.equals("<")) {
        if (isStringKey) {
          cbitMapScan = cbitMap.new_scan(null, searchKey);
        } else {
          searchKey = new IntegerKey(Integer.parseInt(valueA) - 1);
          cbitMapScan = cbitMap.new_scan(null, searchKey);
        }
      } else if (operatorA.equals(">=")) {
        cbitMapScan = cbitMap.new_scan(searchKey, null);
      } else if (operatorA.equals(">")) {
        if (isStringKey) {
          cbitMapScan = cbitMap.new_scan(searchKey, null);
        } else {
          searchKey = new IntegerKey(Integer.parseInt(valueA) + 1);
          cbitMapScan = cbitMap.new_scan(searchKey, null);
        }
      } else if (operatorA.equals("!=") || operatorA.equals("NOT")) {
        if (isStringKey) {
          cbitMapScan = cbitMap.new_scan(null, searchKey);
        } else {
          searchKey = new IntegerKey(Integer.parseInt(valueA) - 1);
          cbitMapScan = cbitMap.new_scan(null, searchKey);
        }
      } else {
        cbitMapScan = cbitMap.new_scan(searchKey, searchKey);
      }

      // Iterate through the indices scoped within the scan for Column A
      KeyDataEntry entry;
      while ((entry = cbitMapScan.get_next()) != null) {
        // match on Column A criteria, check column B and constraint for match
        int pos = cbitMapScan.getLatestPositionMatch();
        // when iterating the first column we will never get
        // duplicates and can immediately append the result
        dupeFreeList.add(0, pos);
      }

      // In a not equal to instance, we will now do a follow-up
      // iteration over the values after the passed argument
      // for Column A
      if (operatorA.equals("!=") || operatorA.equals("NOT")) {
        if (isStringKey) {
          cbitMapScan.closeCbitmapScans();
          cbitMapScan = cbitMap.new_scan(searchKey, null);
        } else {
          cbitMapScan.closeCbitmapScans();
          searchKey = new IntegerKey(Integer.parseInt(valueA) + 1);
          cbitMapScan = cbitMap.new_scan(searchKey, null);
        }

        while ((entry = cbitMapScan.get_next()) != null) {
          // match on Column A criteria, check column B and constraint for match
          int pos = cbitMapScan.getLatestPositionMatch();
          // when iterating the first column we will never get
          // duplicates and can immediately append the result
          dupeFreeList.add(0, pos);
        }
      }
      // close the scan on Column A as we are done using it
      cbitMapScan.closeCbitmapScans();
      cbitMap.close();

      // now we need the values in column B matching the criteria
      // but in an OR case we will aim to avoid duplicate results to
      // what was matched in Column A
      if (OR_OP == constraint) {
        cbmFileName = columnarFileName + ".cbitmap" + Integer.toString(columnBNumber);
        try {
          cbitMap = new CBitMapFile(cbmFileName);
        } catch (Exception e) {
          System.out.println("Could not open " + cbmFileName);
          e.printStackTrace();
          return count;
        }
        cbitMap.setSrcColumnarFile(columnarFile);
        cbitMap.setMappedColumn(columnBNumber);

        // If value is a string, create a BTree with a string key, if its an integer,
        // create a BTree with an integer key
        isStringKey = false;
        if (columnarFile.type[columnBNumber - 1].attrType == AttrType.attrInteger) {
          searchKey = new IntegerKey(Integer.parseInt(valueB));
        } else if (columnarFile.type[columnBNumber - 1].attrType == AttrType.attrString) {
          // Create a key for the search value
          isStringKey = true;
          searchKey = new StringKey(valueB);
        }

        /*
         * Convert the operator against Column B into a formatted scan
         * range using the following key iterators
         */
        if (operatorB.equals("<=")) {
          cbitMapScan = cbitMap.new_scan(null, searchKey);
        } else if (operatorB.equals("<")) {
          if (isStringKey) {
            cbitMapScan = cbitMap.new_scan(null, searchKey);
          } else {
            searchKey = new IntegerKey(Integer.parseInt(valueB) - 1);
            cbitMapScan = cbitMap.new_scan(null, searchKey);
          }
        } else if (operatorB.equals(">=")) {
          cbitMapScan = cbitMap.new_scan(searchKey, null);
        } else if (operatorB.equals(">")) {
          if (isStringKey) {
            cbitMapScan = cbitMap.new_scan(searchKey, null);
          } else {
            searchKey = new IntegerKey(Integer.parseInt(valueB) + 1);
            cbitMapScan = cbitMap.new_scan(searchKey, null);
          }
        } else if (operatorB.equals("!=") || operatorB.equals("NOT")) {
          if (isStringKey) {
            cbitMapScan = cbitMap.new_scan(null, searchKey);
          } else {
            searchKey = new IntegerKey(Integer.parseInt(valueB) - 1);
            cbitMapScan = cbitMap.new_scan(null, searchKey);
          }
        } else {
          cbitMapScan = cbitMap.new_scan(searchKey, searchKey);
        }

        // Iterate through B-Tree index entires
        while ((entry = cbitMapScan.get_next()) != null) {
          // match on Column A criteria, check column B and constraint for match
          int pos = cbitMapScan.getLatestPositionMatch();

          // need to avoid duplicates in second column
          if (false == dupeFreeList.contains(pos)) {
            dupeFreeList.add(0, pos);
          }
        }

        // In a not equal to instance, we will now do a follow-up
        // iteration over the values after the passed argument
        if (operatorB.equals("!=") || operatorB.equals("NOT")) {
          if (isStringKey) {
            cbitMapScan.closeCbitmapScans();
            cbitMapScan = cbitMap.new_scan(searchKey, null);
          } else {
            cbitMapScan.closeCbitmapScans();
            searchKey = new IntegerKey(Integer.parseInt(valueB) + 1);
            cbitMapScan = cbitMap.new_scan(searchKey, null);
          }

          while ((entry = cbitMapScan.get_next()) != null) {
            // match on Column A criteria, check column B and constraint for match
            int pos = cbitMapScan.getLatestPositionMatch();

            // need to avoid duplicates in second column
            if (false == dupeFreeList.contains(pos)) {
              dupeFreeList.add(0, pos);
            }
          }
        }
        // close the scan on Column A as we are done using it
        cbitMapScan.closeCbitmapScans();
        cbitMap.close();
      }

      // iterate and print the duplicate free results
      for (int i = 0; i < dupeFreeList.size(); i++) {
        int pos = dupeFreeList.elementAt(i);
        TID tid1 = columnarFile.getTidFromPosition(pos);
        Tuple tuple = columnarFile.getTuple(tid1);
        boolean validMatch = true;
        if ((isStringKey) && (operatorB.equals(">") || operatorB.equals("<") || operatorB.equals("!="))) {
          // <, >, != check over a range, but we want to ignore cases where
          // the returned value matches the query condition
          if ((((StringKey) searchKey).getKey().equals(tuple.getStrFld(columnANumber)))) {
            validMatch = false;
          }
        }
        if (validMatch) {
          // for an AND_OP we still need to confirm the
          // values in the 2nd column match criteria
          if (AND_OP == constraint) {
            boolean andCondMet = andCheckColumnBool(columnarFile, columnBNumber, targetColumns,
                columnarFileName, tuple, operatorB, valueB);
            if (andCondMet) {
              printTupleColumns(columnarFileName, tuple, targetColumns, tid1, isDelete);
            }
          }
          // else it's either or and we know it's adupe free list by now
          else {
            printTupleColumns(columnarFileName, tuple, targetColumns, tid1, isDelete);
          }
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
    return count;
  }
}
