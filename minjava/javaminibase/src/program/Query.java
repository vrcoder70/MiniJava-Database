package program;

import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.PinPageException;
import columnar.*;
import diskmgr.PCounter;
import global.*;
import heap.*;
import index.ColumnarIndexScan;
import index.IndexException;
import index.UnknownIndexTypeException;
import iterator.*;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class Query {

  public static boolean DEBUG = false;

  public static void main(String[] args) throws CFException {
    boolean isDelete = false;
    String command = args[0].trim().toUpperCase();
    if (command.equals("QUERY")) {
      isDelete = false;
    } else if (command.equals("DELETE_QUERY")) {
      isDelete = true;
    } else {
      System.err.println("[x] Invalid command: " + command);
      System.exit(1);
    }
    String debug = args[1].trim().toUpperCase();
    if (debug.equals("DEBUG")) {
      DEBUG = true;
      System.err.println("[*] Debug mode enabled");
    } else if (debug.equals("NODEBUG")) {
      DEBUG = false;
    } else {
      System.err.println("[x] Invalid debug: " + debug);
      System.exit(1);
    }
    if ((isDelete == false && args.length != 8) ||
        (isDelete == true && args.length != 9)) {
      System.err.println("[x] Invalid number of arguments: " + args.length);
      System.exit(1);
    }
    String[] slicedArgs = new String[args.length - 2];
    System.arraycopy(args, 2, slicedArgs, 0, args.length - 2);

    boolean isErr = false;

    int count = 0;
    PCounter.initialize();
    try {
      count = query(slicedArgs, isDelete);
    } catch (Exception e) {
      isErr = true;
      System.err.println("[x] Error executing query");
      e.printStackTrace();
    }
    try {
      SystemDefs.JavabaseBM.flushAllPages();
    } catch (Exception e) {
      System.err.println("[x] Error flushing pages");
      e.printStackTrace();
    }

    System.err.println();
    System.err.println("No of Tuples: " + count);
    System.err.println("Page Reads: " + PCounter.rcounter);
    System.err.println("Page Writes: " + PCounter.wcounter);

    if (isErr) {
      System.exit(1);
    }
  }

  public static int query(String[] args, boolean isDelete)
      throws CFException,
      IllegalArgumentException {
    String dbName = args[0].trim() + ".minibase-db";
    String columnarFileName = args[1].trim();
    String[] targetColumns = args[2]
        .replaceAll("\\s+|\\[|\\]", "") // s+ | [ | ] -> ""
        .split(",");
    String[] vc = args[3]
        .replaceAll("\\s+", " ")
        .replaceAll("\\) AND \\(", "\\) and \\(")
        .replaceAll("\\) OR \\(", "\\) or \\(")
        .replaceAll("\\{|\\}|\\(|\\)", "") // { | } | ( | ) -> ""
        .replaceAll("\\s+", " ")
        .split(" ");
    int numBuf = Integer.parseInt(args[4].trim());
    String accessType = args[5].trim().toUpperCase();
    boolean isPurge = false;
    if (isDelete) {
      String isPurgeStr = args[6].toLowerCase().trim();
      if (!isPurgeStr.equals("true") && !isPurgeStr.equals("false")) {
        System.err.println("[x] Invalid isPurge: " + isPurgeStr);
        throw new IllegalArgumentException("Invalid isPurge");
      }
      isPurge = Boolean.parseBoolean(isPurgeStr);
    }

    if (vc.length != 3 && vc.length != 7) {
      System.err.println("vc.length: " + vc.length);
      System.err.println("[x] Invalid value constraint: length not 3 or 7");
      throw new IllegalArgumentException("Invalid value constraint");
    }
    if (vc.length == 7 && !(vc[3].equals("and") || vc[3].equals("or"))) {
      System.err.println("[x] Invalid value constraint: logical operator");
      throw new IllegalArgumentException("Invalid value constraint");
    }

    String currentDirectory = System.getProperty("user.dir");
    File file = new File(currentDirectory, dbName);
    if (!file.exists()) {
      System.err.println("[x] Database does not exist");
      throw new CFException(null, "Database does not exist");
    }

    try {
      SystemDefs.MINIBASE_RESTART_FLAG = true;
      new SystemDefs(dbName, 50000, numBuf, "Clock");
    } catch (Exception e) {
      System.err.println("[x] Error opening database");
      throw new CFException(e, "Error opening database");
    }

    Columnarfile f = null;
    try {
      f = new Columnarfile(columnarFileName);
    } catch (Exception e) {
      System.err.println("[x] Error retrieving columnar file");
      throw new CFException(e, "Error retrieving columnar file");
    }

    for (String targetColumn : targetColumns) {
      if (!Arrays.asList(f.columnNames).contains(targetColumn)) {
        System.err.println("[x] Invalid target column: " + targetColumn);
      }
    }
    String[] vcColNames = null;
    if (vc.length == 3) {
      vcColNames = new String[] { vc[0] };
    }
    if (vc.length == 7) {
      vcColNames = new String[] { vc[0], vc[4] };
    }
    for (String vcColName : vcColNames) {
      if (!Arrays.asList(f.columnNames).contains(vcColName)) {
        System.err.println("[x] Invalid valueConstraint column:" + vcColName);
        throw new IllegalArgumentException("Invalid valueConstraint column");
      }
    }

    FldSpec[] projlist = new FldSpec[targetColumns.length];
    RelSpec rel = new RelSpec(RelSpec.outer);
    for (int i = 0; i < projlist.length; i++) {
      int colNum = Arrays.asList(f.columnNames).indexOf(targetColumns[i]) + 1;
      projlist[i] = new FldSpec(rel, colNum);
    }

    AttrType[] projtypes = new AttrType[projlist.length];
    for (int i = 0; i < projlist.length; i++) {
      projtypes[i] = new AttrType(f.type[projlist[i].offset - 1].attrType);
    }

    int colNum1 = Arrays.asList(f.columnNames).indexOf(vc[0]) + 1;
    CondExpr[] expr = new CondExpr[3];
    expr[0] = getExpr(vc[1], vc[2], colNum1, f.type);
    expr[1] = null;
    expr[2] = null;
    if (vc.length == 7) {
      int colNum2 = Arrays.asList(f.columnNames).indexOf(vc[4]) + 1;
      CondExpr exprTemp = getExpr(vc[5], vc[6], colNum2, f.type);
      if (vc[3].equals("or")) {
        expr[0].next = exprTemp;
      } else if (vc[3].equals("and")) {
        expr[1] = exprTemp;
      } else {
        System.err.println("[x] Invalid value constraint");
        throw new IllegalArgumentException("Invalid value constraint");
      }
    }

    if (DEBUG) {
      debug_input_parsing(
          dbName,
          columnarFileName,
          projlist, // Target columns - FldSpec[]
          projtypes, // attr types of projlist
          expr, // value constraint - CondExpr[]
          numBuf,
          accessType,
          isDelete,
          isPurge);
    }

    // for index scans
    int[] projColumnIndexes = new int[projlist.length];
    for (int i = 0; i < projlist.length; i++) {
      projColumnIndexes[i] = projlist[i].offset;
    }
    String[] vc1 = new String[3];
    vc1[0] = new String(vc[0]);
    vc1[1] = new String(vc[1]);
    vc1[2] = new String(vc[2]);
    String[] vc2 = new String[3];
    int op = -1;
    if (vc.length == 7) {
      vc2[0] = new String(vc[4]);
      vc2[1] = new String(vc[5]);
      vc2[2] = new String(vc[6]);
      op = (vc[3].equals("and")) ? QueryHelper.AND_OP : QueryHelper.OR_OP;
    }

    int count = 0;
    switch (accessType) {
      case "FILESCAN":
        count = execColumnarFileScan(f, projlist, projtypes, expr, isDelete);
        break;
      case "COLUMNSCAN":
        count = execColumnScan(f, projlist, projtypes, expr, isDelete);
        break;
      case "BTREE":
        // System.err.println("[x] BTREE not implemented");
        // count = execBtreeScan(f, projlist, projtypes, expr, isDelete);
        if (vc.length == 3) {
          count = QueryHelper.executeBTreeQuery(f.get_fileName(), projColumnIndexes,
              vc1, isDelete);
        } else {
          count = QueryHelper.executeBTreeQuery(f.get_fileName(), projColumnIndexes,
              vc1, vc2, op, isDelete);
        }
        break;
      case "BITMAP":
        if (vc.length == 3) {
          count = QueryHelper.executeBitMapQuery(f.get_fileName(), projColumnIndexes,
              vc1, isDelete);
        } else {
          count = QueryHelper.executeBitMapQuery(f.get_fileName(), projColumnIndexes,
              vc1, vc2, op, isDelete);
        }
        break;
      case "CBITMAP":
        if (vc.length == 3) {
          count = QueryHelper.executeCBitMapQuery(f.get_fileName(), projColumnIndexes,
              vc1, isDelete);
        } else {
          count = QueryHelper.executeCBitMapQuery(f.get_fileName(), projColumnIndexes,
              vc1, vc2, op, isDelete);
        }
        break;
      default:
        System.err.println("[x] Invalid access type: " + accessType);
        throw new IllegalArgumentException("Invalid access type");
    }

    if (isDelete && isPurge) {
      try {
        f.purgeAllDeletedTuples();
      } catch (Exception e) {
        System.err.println("[x] Error purging deleted tuples");
        throw new CFException(e, "Error purging deleted tuples");
      }
    }
    return count;
  }

  private static CondExpr getExpr(
      String operator, // = | < | > | <= | >= | != |
      // aopEQ | aopLT | aopGT | aopLE | aopGE | aopNE
      String value,
      int colNum,
      AttrType[] colTypes)
      throws IllegalArgumentException {
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

  private static void debug_input_parsing(
      String dbName,
      String columnarFileName,
      FldSpec[] projlist,
      AttrType[] projtypes,
      CondExpr[] valueConstraint,
      int numBuf,
      String accessType,
      boolean isDelete,
      boolean isPurge) {
    System.err.println("DB Name: " + dbName);
    System.err.println("Columnar File Name: " + columnarFileName);
    System.err.print("Projection List: ");
    for (FldSpec proj : projlist) {
      System.err.print(proj.offset + "-" + proj.relation.key + " ");
    }
    System.err.println();
    System.err.print("Projection Types: ");
    for (AttrType type : projtypes) {
      System.err.print(type.toString() + " ");
    }
    System.err.println();
    System.err.println("Value Constraint: ");
    printCondExprArray(valueConstraint);
    System.err.println("Num of Buffers: " + numBuf);
    System.err.println("Access Type: " + accessType);
    System.err.println("Is Delete: " + isDelete);
    System.err.println("Is Purge: " + isPurge);
  }

  private static void printCondExprArray(CondExpr[] expr) {
    for (int i = 0; i < expr.length; i++) {
      if (expr[i] == null) {
        break;
      }
      if (i != 0) {
        System.err.println(" -AND-");
      }
      printCondExpr(expr[i]);
    }
  }

  private static void printCondExpr(CondExpr expr) {
    CondExpr temp = expr;
    while (true) {
      System.err.println(" Operator: " + temp.op);
      System.err.println(" Type1: " + temp.type1);
      System.err.println(" Type2: " + temp.type2);
      System.err.println(" Operand1: " + temp.operand1.symbol.offset);
      if (temp.type2.attrType == AttrType.attrInteger) {
        System.err.println(" Operand2: " + temp.operand2.integer);
      } else if (temp.type2.attrType == AttrType.attrReal) {
        System.err.println(" Operand2: " + temp.operand2.real);
      } else if (temp.type2.attrType == AttrType.attrString) {
        System.err.println(" Operand2: " + temp.operand2.string);
      } else {
        System.err.println("[x] Invalid column type: Columnar file Corrupted");
        throw new IllegalArgumentException("Invalid column type");
      }
      temp = temp.next;
      if (temp == null) {
        break;
      }
      System.err.println(" -OR-");
    }
  }

  private static int execColumnarFileScan(
      Columnarfile f,
      FldSpec[] projlist,
      AttrType[] projtypes,
      CondExpr[] expr,
      boolean isDelete)
      throws CFException {
    int count = 0;

    // open columnar file scan
    ColumnarFileScan cfscan;
    BTreeFile btf = null;
    try {
      cfscan = new ColumnarFileScan(
          f.get_fileName(),
          f.type,
          f.strSizes,
          (short) Columnarfile.numColumns,
          (short) projlist.length,
          projlist,
          expr);
      btf = new BTreeFile(f.get_fileName() + ".deletedBTree");
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
          f.markTupleDeleted(tid);
        }
        count++;
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
    return count;
  }

  static Columnarfile colscan_f = null;
  static Scan[] colscan_scans = null;
  static AttrType[] colscan_evalTypes = null;
  static short[] colscan_evalStrSizes = null;
  static CondExpr[] colscan_expr = null;
  static Integer colscan_pos = null;

  // rid is modified in this function
  private static Integer get_next_colscan(RID[] rids)
      throws CFException,
      InvalidTupleSizeException,
      IOException,
      FieldNumberOutOfBoundException,
      InvalidTypeException,
      UnknowAttrType,
      PredEvalException {
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

  private static int execColumnScan(
      Columnarfile f,
      FldSpec[] projlist,
      AttrType[] projtypes,
      CondExpr[] expr,
      boolean isDelete)
      throws CFException {
    int count = 0;

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
        scans[i] = f.openColumnScan(colNumToScan[i]);
      }
      btf = new BTreeFile(f.get_fileName() + ".deletedBTree");
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
        evalType[i] = new AttrType(f.type[colNumToScan[i] - 1].attrType);
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
          evalStrSizes[i] = f.strSizes[evalStrIndex];
          evalStrIndex++;
        }
      }

      RID[] rids = new RID[colNumToScan.length];
      for (int i = 0; i < colNumToScan.length; i++) {
        rids[i] = new RID();
      }

      // column scan setup
      colscan_f = f;
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
          f.type,
          Columnarfile.numColumns,
          f.strSizes,
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
          tid = f.getTidFromPosition(pos, colRids, null);
          f.markTupleDeleted(tid);
        } else {
          tid = f.getTidFromPosition(pos, colRids, projlist);
        }
        Tuple t1 = f.getTupleProj(tid, projlist);
        Projection.Project(t1, f.type, Jtuple, projlist, projlist.length);
        Jtuple.print(projtypes);
        count++;
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
        btf.close();
      } catch (Exception e) {
        System.err.println("[x] Error closing iterator");
        throw new CFException(e, "Error closing iterator");
      }
    }
    return count;
  }

  private static int execBtreeScan(
      Columnarfile f,
      FldSpec[] projlist,
      AttrType[] projtypes,
      CondExpr[] expr,
      boolean isDelete)
      throws CFException {
    // columnNum to scan
    int[] colNumToScan = null;
    if (expr[0].next == null && expr[1] == null) {
      colNumToScan = new int[1];
    } else {
      colNumToScan = new int[2];
    }
    colNumToScan[0] = expr[0].operand1.symbol.offset;
    if (colNumToScan.length == 2 && expr[1] == null) {
      colNumToScan[1] = expr[0].next.operand1.symbol.offset;
    } else if (colNumToScan.length == 2 && expr[1] != null) {
      colNumToScan[1] = expr[1].operand1.symbol.offset;
    }

    // construct btree columnNames
    String[] btreeColumnNames = new String[Columnarfile.numColumns];
    for (int i = 0; i < Columnarfile.numColumns; i++) {
      btreeColumnNames[i] = f.get_fileName() + ".btree" + Integer.toString(i + 1);
    }

    ColumnarIndexScan ciscan;
    BTreeFile btf = null;
    try {
      ciscan = new ColumnarIndexScan(
          f.get_fileName(), // relName
          colNumToScan, // fldNum
          new IndexType(IndexType.B_Index), // index
          btreeColumnNames, // indName
          f.type, // types
          f.strSizes, // str_sizes
          Columnarfile.numColumns, // noInFlds
          projlist.length, // noOutFlds
          projlist, // outFlds
          expr, // selects
          false); // indexOnly
      btf = new BTreeFile(f.get_fileName() + ".deletedBTree");
    } catch (Exception e) {
      System.err.println("[x] Error opening index scan");
      throw new CFException(e, "Error opening ColumnarIndexScan");
    }

    int count = 0;
    try {
      Tuple tuple = ciscan.get_next();
      while (tuple != null) {
        tuple.print(projtypes);
        count++;
        tuple = ciscan.get_next();
      }
    } catch (Exception e) {
      System.err.println("[x] Error getting next tuple");
      e.printStackTrace();
      throw new CFException(e, "Error getting next tuple");
    } finally {
      // close columnar file scan
      try {
        btf.close();
        ciscan.close();
      } catch (Exception e) {
        System.err.println("[x] Error closing ColumnarFileScan");
        throw new CFException(e, "Error closing ColumnarFileScan");
      }
    }
    return count;
  }
}
