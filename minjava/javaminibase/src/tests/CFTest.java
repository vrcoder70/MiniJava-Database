/*
 * File - CFTest.java
 *
 * Original Author - Vivian Roshan Adithan
 *
 * Description -
 *		...
 */
package tests;

import columnar.*;
import diskmgr.*;
import global.*;
import heap.*;
import index.ColumnarIndexScan;
import iterator.ColumnarFileScan;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;
import java.io.*;

/*
<pre>
 Note that in JAVA, methods can't be overridden to be more private.
 Therefore, the declaration of all private functions are now declared
 protected as opposed to the private type in C++.
</pre>
*/
class CFDriver extends TestDriver implements GlobalConst {

  private static final boolean OK = true;
  private static final boolean FAIL = false;

  private int choice;
  private int diskPages;
  private int bufPages;

  public CFDriver() {
    super("cftest");
    // choice = 1; // big enough for file to occupy > 1 data page
    // choice = 100; // big enough for file to occupy > 1 data page
    // choice = 2000; // big enough for file to occupy > 1 directory page
    choice = 20000;
    diskPages = 1000000;
    bufPages = 50;
  }

  public boolean runTests() {

    System.out.println("\n" + "Running " + testName() + " tests...." + "\n");

    new SystemDefs(dbpath, diskPages, bufPages, "Clock");

    // Kill anything that might be hanging around
    String newdbpath;
    String newlogpath;
    String remove_logcmd;
    String remove_dbcmd;
    String remove_cmd = "/bin/rm -rf ";

    newdbpath = dbpath;
    newlogpath = logpath;

    remove_logcmd = remove_cmd + logpath;
    remove_dbcmd = remove_cmd + dbpath;

    // Commands here is very machine dependent. We assume
    // user are on UNIX system here
    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    } catch (IOException e) {
      System.err.println("IO error: " + e);
    }

    remove_logcmd = remove_cmd + newlogpath;
    remove_dbcmd = remove_cmd + newdbpath;

    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    } catch (IOException e) {
      System.err.println("IO error: " + e);
    }

    // Run the tests. Return type different from C++
    boolean _pass = runAllTests();

    // Clean up again
    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    } catch (IOException e) {
      System.err.println("IO error: " + e);
    }

    System.out.print("\n" + "..." + testName() + " tests ");
    System.out.print(_pass == OK ? "completely successfully" : "failed");
    System.out.print(".\n\n");

    return _pass;
  }

  protected boolean createDummyColumnarFile(String name) {
    System.out.println(" - creating dummy columnar file: " + name);
    boolean status = OK;

    AttrType[] attrType = new AttrType[3];
    attrType[0] = new AttrType(AttrType.attrInteger);
    attrType[1] = new AttrType(AttrType.attrReal);
    attrType[2] = new AttrType(AttrType.attrString);

    short[] Ssizes = new short[1];
    Ssizes[0] = 30; // first elt. is 30

    String[] columnNames = new String[3];
    columnNames[0] = "column1:int";
    columnNames[1] = "column2:float";
    columnNames[2] = "column3:string";

    try {
      new Columnarfile(name, 3, attrType, Ssizes, columnNames);
    } catch (Exception e) {
      System.err.println(" [x] Error creating columnar file");
      e.printStackTrace();
      status = FAIL;
    }
    return status;
  }

  protected boolean insertDummyRecord(Columnarfile f, int numRecords) {
    System.out.println(" - inserting " + numRecords + " dummy records");
    boolean status = OK;
    for (int i = 0; (i < numRecords) && (status == OK); i++) {
      // fixed length record
      Tuple t = new Tuple();
      int intVal = i;
      float floatVal = (float) (i * 2.5);
      String strVal = "record" + i;
      // Ssizes[0] = (short) rec.name.length();
      try {
        t.setHdr((short) 3, f.type, f.strSizes);
        t.setIntFld(1, intVal);
        t.setFloFld(2, floatVal);
        t.setStrFld(3, strVal);
        f.insertTuple(t.getTupleByteArray());
      } catch (Exception e) {
        status = FAIL;
        System.err.println(" [x] Error inserting record " + i + "\n");
        e.printStackTrace();
        return status;
      }
    }
    return status;
  }

  protected boolean test1() {
    String testName = "Test1";
    boolean status = OK;
    System.out.println(testName + " - create");

    status = createDummyColumnarFile(testName);
    if (status != OK) {
      return status;
    }
    Columnarfile f = null;
    try {
      f = new Columnarfile(testName);
    } catch (Exception e) {
      System.err.println(" [x] Error retrieving columnar file");
      e.printStackTrace();
      status = FAIL;
      return status;
    }

    System.out.println(" - check if the columnar file is retrieved correctly");
    if (Columnarfile.numColumns != 3) {
      System.err.println(" [x] numColumns retrieved incorrectly");
      status = FAIL;
    }
    if (f.type.length != 3) {
      System.err.println(" [x] type length retrieved incorrectly");
      status = FAIL;
    }
    if (f.type[0].attrType != AttrType.attrInteger
        || f.type[1].attrType != AttrType.attrReal
        || f.type[2].attrType != AttrType.attrString) {
      System.err.println(" [x] type retrieved incorrectly");
      status = FAIL;
    }
    if (f.strSizes.length != 1) {
      System.err.println(" [x] strSizes length retrieved incorrectly");
      status = FAIL;
    }
    if (f.strSizes[0] != 30) {
      System.err.println(" [x] strSizes retrieved incorrectly");
      status = FAIL;
    }
    if (f.columnNames.length != 3) {
      System.err.println(" [x] columnNames length retrieved incorrectly");
      status = FAIL;
    }
    if (!f.columnNames[0].equals("column1:int")
        || !f.columnNames[1].equals("column2:float")
        || !f.columnNames[2].equals("column3:string")) {
      System.err.println(" [x] columnNames retrieved incorrectly");
      status = FAIL;
    }
    try {
      if (f.getTupleCnt() != 0) {
        System.err.println(" [x] getTupleCnt retrieved incorrectly");
        status = FAIL;
      }
    } catch (Exception e) {
      status = FAIL;
      System.err.println(" [x] unable to retrieve getTupleCnt");
      e.printStackTrace();
    }
    return status;
  }

  protected boolean test2() {
    String testName = "Test2";
    boolean status = OK;
    System.out.println(testName + " - insert");

    status = createDummyColumnarFile(testName);
    if (status != OK) {
      return status;
    }
    Columnarfile f = null;
    try {
      f = new Columnarfile(testName);
    } catch (Exception e) {
      System.err.println(" [x] Error retrieving columnar file");
      e.printStackTrace();
      status = FAIL;
      return status;
    }

    status = insertDummyRecord(f, choice);
    if (status != OK) {
      return status;
    }

    try {
      int tupleCount = f.getTupleCnt();
      if (tupleCount != choice) {
        System.err.println(" [x] getTupleCnt retrieved incorrectly");
        status = FAIL;
      }
      System.out.println(" - inserted Columns " + tupleCount);
    } catch (Exception e) {
      status = FAIL;
      System.err.println(" [x] unable to retrieve getTupleCnt");
      e.printStackTrace();
      return status;
    }
    return status;
  }

  protected boolean test3() {
    String testName = "Test3";
    boolean status = OK;
    System.out.println(testName + " - TupleScan");

    status = createDummyColumnarFile(testName);
    if (status != OK) {
      return status;
    }
    Columnarfile f = null;
    try {
      f = new Columnarfile(testName);
    } catch (Exception e) {
      System.err.println(" [x] Error retrieving columnar file");
      e.printStackTrace();
      status = FAIL;
      return status;
    }

    status = insertDummyRecord(f, choice);
    if (status != OK) {
      return status;
    }

    System.out.println(" - opening tuple scan");
    TupleScan tupleScan;
    try {
      tupleScan = f.openTupleScan();
    } catch (Exception e) {
      status = FAIL;
      System.err.println(" [x] unable opening tuple scan");
      e.printStackTrace();
      return status;
    }

    System.out.println(" - scanning");
    TID tid = new TID(Columnarfile.numColumns);
    Tuple t;
    int i = 0;
    try {
      t = tupleScan.getNext(tid);
      while (t != null) {
        // t.print(f.type);
        status = (t.getIntFld(1) != i) ? FAIL : status;
        status = (t.getFloFld(2) != (float) (i * 2.5)) ? FAIL : status;
        status = (!("record" + i).equals(t.getStrFld(3))) ? FAIL : status;
        t = tupleScan.getNext(tid);
        i++;
      }
    } catch (Exception e) {
      status = FAIL;
      System.err.println(" [x] unable getting next tuple");
      e.printStackTrace();
      return status;
    }

    int position = 2;
    System.out.println(" - getting TID from position: " + position);
    TID positionTID;
    try {
      positionTID = f.getTidFromPosition(position);
    } catch (Exception e) {
      status = FAIL;
      System.err.println(" [x] unable getting TID from position");
      e.printStackTrace();
      return status;
    }

    System.out.println(" - positioning tuple scan to the TID");
    try {
      status = tupleScan.position(positionTID);
    } catch (Exception e) {
      status = FAIL;
      System.err.println(" [x] unable to position to the TID tuple scan");
      e.printStackTrace();
      return status;
    }
    if (!status) {
      System.err.println(" [x] TupleScan.position returned false");
      status = FAIL;
      return status;
    }

    System.out.println(" - checking position");
    try {
      t = tupleScan.getNext(tid);
      // t.print(f.type);
      status = (t.getIntFld(1) != position) ? FAIL : status;
      status = (t.getFloFld(2) != (float) (position * 2.5)) ? FAIL : status;
      status = (!("record" + position).equals(t.getStrFld(3))) ? FAIL : status;
    } catch (Exception e) {
      status = FAIL;
      System.err.println(" [x] unable to get next tuple in position");
      e.printStackTrace();
      return status;
    }
    System.out.println(" - closing tuple scan");
    try {
      tupleScan.closetuplescan();
    } catch (Exception e) {
      status = FAIL;
      System.err.println(" [x] unable to close tuple scan");
      e.printStackTrace();
      return status;
    }

    if (status != OK) {
      System.err.println(" [x] Incorrect values retrieved");
    }

    return status;
  }

  protected boolean test4() {
    String testName = "Test4";
    boolean status = OK;
    System.out.println(testName + " - getValue");

    status = createDummyColumnarFile(testName);
    if (status != OK) {
      return status;
    }
    Columnarfile f = null;
    try {
      f = new Columnarfile(testName);
    } catch (Exception e) {
      System.err.println(" [x] Error retrieving columnar file");
      e.printStackTrace();
      status = FAIL;
      return status;
    }

    status = insertDummyRecord(f, choice);
    if (status != OK) {
      return status;
    }

    int position = 2;
    System.out.println(" - getting TID from position: " + position);
    TID positionTID;
    try {
      positionTID = f.getTidFromPosition(position);
    } catch (Exception e) {
      status = FAIL;
      System.err.println(" [x] unable getting TID from position");
      e.printStackTrace();
      return status;
    }

    System.out.println(" - getting value from position: " + position);
    try {
      IntegerValueClass intValueClass = (IntegerValueClass) f.getValue(positionTID, 1);
      FloatValueClass floValueClass = (FloatValueClass) f.getValue(positionTID, 2);
      StringValueClass strValueClass = (StringValueClass) f.getValue(positionTID, 3);
      status = (intValueClass.classValue != position) ? FAIL : status;
      status = (floValueClass.classValue != (float) (position * 2.5)) ? FAIL : status;
      status = (!("record" + position).equals(strValueClass.classValue)) ? FAIL : status;
    } catch (Exception e) {
      status = FAIL;
      System.err.println("[x] Error getting value");
      e.printStackTrace();
      return status;
    }
    if (status != OK) {
      System.err.println(" [x] Incorrect values retrieved");
    }

    return status;
  }

  protected boolean test5() {
    String testName = "Test5";
    boolean status = OK;
    String testString;
    testString = "getPositionFromRid, getRidFromPosition, getTidFromPosition";
    System.out.println(testName + " - " + testString);
    status = createDummyColumnarFile(testName);
    if (status != OK) {
      return status;
    }
    Columnarfile f = null;
    try {
      f = new Columnarfile(testName);
    } catch (Exception e) {
      System.err.println(" [x] Error retrieving columnar file");
      e.printStackTrace();
      status = FAIL;
      return status;
    }

    status = insertDummyRecord(f, choice);
    if (status != OK) {
      return status;
    }

    int position = 2;
    int testPosition;
    System.out.println(" - getting RID from position: " + position);
    RID positionRID;
    try {
      positionRID = f.getRidFromPosition(position, 1);
      testPosition = f.getPositionFromRid(positionRID, 1);
    } catch (Exception e) {
      status = FAIL;
      System.err.println(" [x] unable getting RID from position");
      e.printStackTrace();
      return status;
    }
    System.out.println(" - checking position from RID: " + testPosition);
    if (testPosition != position) {
      status = FAIL;
      System.err.println(" [x] position from RID is incorrect");
    }

    System.out.println(" - getting TID from position: " + position);
    TID positionTID;
    try {
      positionTID = f.getTidFromPosition(position);
    } catch (Exception e) {
      status = FAIL;
      System.err.println(" [x] unable getting TID from position");
      e.printStackTrace();
      return status;
    }
    System.out.println(" - checking position in TID: " + positionTID.position);
    if (positionTID.position != position) {
      status = FAIL;
      System.err.println(" [x] position from TID is incorrect");
    }

    return status;
  }

  protected boolean test6() {
    String testName = "Test6";
    boolean status = OK;
    System.out.println(testName + " - purgeAllDeletedTuples");

    status = createDummyColumnarFile(testName);
    if (status != OK) {
      return status;
    }
    Columnarfile f = null;
    try {
      f = new Columnarfile(testName);
    } catch (Exception e) {
      System.err.println(" [x] Error retrieving columnar file");
      e.printStackTrace();
      status = FAIL;
      return status;
    }

    status = insertDummyRecord(f, choice);
    if (status != OK) {
      return status;
    }

    int tupuleCountBeforeDelete;
    System.out.println(" - getting Tuple count before mark as deleted");
    try {
      tupuleCountBeforeDelete = f.getTupleCnt();
    } catch (Exception e) {
      status = FAIL;
      System.err.println(" [x] unable to get tuple count");
      e.printStackTrace();
      return status;
    }

    int x = 3;
    int noOfTuplesDeleted = 0;
    System.out.println(
        " - marking every tuple at position divisible by " //
            + x
            + " as deleted");
    try {
      for (int i = 0; i < f.getTupleCnt(); i++) {
        if (i % x == 0) {
          f.markTupleDeleted(f.getTidFromPosition(i));
          f.markTupleDeleted(f.getTidFromPosition(i));
          noOfTuplesDeleted++;
        }
      }
    } catch (Exception e) {
      status = FAIL;
      System.err.println(" [x] unable to mark tuple deleted");
      e.printStackTrace();
      return status;
    }

    System.out.println(" - purging all deleted tuples");
    try {
      f.purgeAllDeletedTuples();
    } catch (Exception e) {
      status = FAIL;
      System.err.println(" [x] unable to purge deleted tuples");
      e.printStackTrace();
      return status;
    }

    System.out.println(" - checking tuple count after delete");
    try {
      if (f.getTupleCnt() != tupuleCountBeforeDelete - noOfTuplesDeleted) {
        System.err.println(" [x] tuple count after delete is incorrect");
        status = FAIL;
      }
    } catch (Exception e) {
      status = FAIL;
      System.err.println(" [x] unable to get tuple count");
      e.printStackTrace();
      return status;
    }

    return status;
  }

  protected boolean test7() {
    String testName = "Test7";
    boolean status = OK;
    System.out.println(testName + " - ColumnarFileScan");

    status = createDummyColumnarFile(testName);
    if (status != OK) {
      return status;
    }
    Columnarfile f = null;
    try {
      f = new Columnarfile(testName);
    } catch (Exception e) {
      System.err.println(" [x] Error retrieving columnar file");
      e.printStackTrace();
      status = FAIL;
      return status;
    }

    status = insertDummyRecord(f, choice);
    if (status != OK) {
      return status;
    }

    // Columnarfile columnarFile, String columnDBName, String columnarFileName,
    // String[] targetColumns, String[] valueConstraints

    return status;
  }

  protected boolean test0() {
    String testName = "Test0";
    boolean status = OK;
    System.out.println(testName + " - TupleScan");

    status = createDummyColumnarFile(testName);
    if (status != OK) {
      return status;
    }
    Columnarfile f = null;
    try {
      f = new Columnarfile(testName);
    } catch (Exception e) {
      System.err.println(" [x] Error retrieving columnar file");
      e.printStackTrace();
      status = FAIL;
      return status;
    }

    status = insertDummyRecord(f, choice);
    if (status != OK) {
      return status;
    }

    FldSpec[] projlist = new FldSpec[3];
    RelSpec rel = new RelSpec(RelSpec.outer);
    projlist[0] = new FldSpec(rel, 1);
    projlist[1] = new FldSpec(rel, 2);
    projlist[2] = new FldSpec(rel, 3);

    CondExpr[] expr = new CondExpr[2];
    expr[0] = new CondExpr();
    expr[0].op = new AttrOperator(AttrOperator.aopEQ);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrInteger);
    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
    expr[0].operand2.integer = 3;
    expr[0].next = null;
    expr[1] = null;

    try {
      System.out.println("ColumnarFileScan");
      ColumnarFileScan scan = new ColumnarFileScan(
          "Test0", // filename
          f.type, // in_types[]
          f.strSizes, // strSizes
          (short) 3, // in_tuple size
          3, // out_tuple size
          projlist, // projlist
          expr // expr
      );
      Tuple t = scan.get_next();
      while (t != null) {
        t.print(f.type);
        t = scan.get_next();
      }
      scan.close();
    } catch (Exception e) {
      status = FAIL;
      System.err.println("*** ColumnarFileScan\n");
      e.printStackTrace();
    }

    try {
      System.out.println("ColumnarIndexScan: B_Tree");
      f.createBTreeIndex(1);
      f.createBTreeIndex(3);
      ColumnarIndexScan iscan;
      String[] indNames = new String[3];
      indNames[0] = "Test0.btree1";
      indNames[1] = "Test0.btree2";
      indNames[2] = "Test0.btree3";
      expr = new CondExpr[2];
      expr[0] = new CondExpr();
      expr[0].op = new AttrOperator(AttrOperator.aopGE);
      expr[0].type1 = new AttrType(AttrType.attrSymbol);
      expr[0].type2 = new AttrType(AttrType.attrInteger);
      expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
      expr[0].operand2.integer = 3;
      expr[0].next = null;
      expr[1] = null;
      int[] fldNum = new int[1];
      fldNum[0] = 1;
      FldSpec[] projlist2 = new FldSpec[2];
      projlist2[0] = new FldSpec(new RelSpec(RelSpec.outer), 2);
      projlist2[1] = new FldSpec(new RelSpec(RelSpec.outer), 3);
      System.out.println("projlist2 length: " + projlist2.length);
      AttrType[] attrType2 = new AttrType[projlist2.length];
      for (int i = 0; i < projlist2.length; i++) {
        attrType2[i] = f.type[projlist2[i].offset - 1];
        System.out.println("AttrType: " + attrType2[i].attrType);
      }
      iscan = new ColumnarIndexScan(
          "Test0", // filename
          fldNum, // fldNum
          new IndexType(IndexType.B_Index), // indexType
          indNames, // indNames[]
          f.type, // in_types[]
          f.strSizes, // in_str_sizes[]
          3, // in_tuple size
          2, // out_tuple size
          projlist2, // projlist
          expr, // expr
          false // indexOnly
      );
      Tuple t = iscan.get_next();
      while (t != null) {
        t.print(attrType2);
        t = iscan.get_next();
      }
      iscan.close();
    } catch (Exception e) {
      status = FAIL;
      System.err.println("*** ColumnarIndexScan: B_Tree\n");
      e.printStackTrace();
    }

    try {
      System.out.println("ColumnarIndexScan: BitMap");
      f.createBitMapIndex(1, new IntegerValueClass(), false);
      f.createBitMapIndex(3, new StringValueClass(), false);
      ColumnarIndexScan iscan;
      String[] indNames = new String[3];
      indNames[0] = "Test0.bitmap1";
      indNames[1] = "Test0.bitmap2";
      indNames[2] = "Test0.bitmap3";
      expr = new CondExpr[2];
      expr[0] = new CondExpr();
      expr[0].op = new AttrOperator(AttrOperator.aopGE);
      expr[0].type1 = new AttrType(AttrType.attrSymbol);
      expr[0].type2 = new AttrType(AttrType.attrInteger);
      expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
      expr[0].operand2.integer = 3;
      expr[0].next = null;
      expr[1] = null;
      int[] fldNum = new int[1];
      fldNum[0] = 1;
      iscan = new ColumnarIndexScan(
          "Test0", // filename
          fldNum, // fldNum
          new IndexType(IndexType.Bitmap), // indexType
          indNames, // indNames[]
          f.type, // in_types[]
          f.strSizes, // in_str_sizes[]
          3, // in_tuple size
          3, // out_tuple size
          projlist, // projlist
          expr, // expr
          false // indexOnly
      );
      Tuple t = iscan.get_next();
      while (t != null) {
        t.print(f.type);
        t = iscan.get_next();
      }
    } catch (Exception e) {
      status = FAIL;
      System.err.println("*** ColumnarIndexScan: Bitmap\n");
      e.printStackTrace();
    }

    try {
      f.deleteColumnarFile();
      f = new Columnarfile("Test0", 3, f.type, f.strSizes, f.columnNames);
      System.out.println("Tuple count: " + f.getTupleCnt());
      f.deleteColumnarFile();
    } catch (Exception e) {
      status = FAIL;
      System.err.println("*** Error deleting and recreating file\n");
      e.printStackTrace();
    }

    return status;
  }

  protected boolean runTest(TestFunc testFunction) {
    PCounter.initialize();
    boolean status = testFunction.execute();
    if (status != OK) {
      System.out.println("[x] fail");
    } else {
      System.out.println("pass");
    }
    try {
      SystemDefs.JavabaseBM.flushAllPages();
    } catch (Exception e) {
      System.err.println("[x] error flushing pages");
      e.printStackTrace();
    }
    String read = "read: " + PCounter.getReadCount();
    String write = "write: " + PCounter.getWriteCount();
    System.out.println("Pcounter => " + read + " " + write);
    System.out.println();
    return status;
  }

  @FunctionalInterface
  protected interface TestFunc {
    boolean execute();
  }

  protected boolean runAllTests() {

    boolean _passAll = OK;

    _passAll = (!runTest(this::test1)) ? FAIL : _passAll;
    _passAll = (!runTest(this::test2)) ? FAIL : _passAll;
    _passAll = (!runTest(this::test3)) ? FAIL : _passAll;
    _passAll = (!runTest(this::test4)) ? FAIL : _passAll;
    _passAll = (!runTest(this::test5)) ? FAIL : _passAll;
    _passAll = (!runTest(this::test6)) ? FAIL : _passAll;
    // _passAll = (!runTest(this::test7)) ? FAIL : _passAll;
    // _passAll = (!runTest(this::test0)) ? FAIL : _passAll;

    return _passAll;
  }

  protected String testName() {
    return "Columnar File";
  }
}

public class CFTest {

  public static void main(String argv[]) {

    CFDriver cd = new CFDriver();
    boolean dbstatus;

    dbstatus = cd.runTests();

    if (dbstatus != true) {
      System.err.println("Error encountered during columnar file tests:\n");
      Runtime.getRuntime().exit(1);
    }

    Runtime.getRuntime().exit(0);
  }
}
