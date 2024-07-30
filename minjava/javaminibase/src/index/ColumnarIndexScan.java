package index;

import java.io.IOException;

import btree.KeyDataEntry;
import btree.LeafData;
import columnar.Columnarfile;
import global.*;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import iterator.*;

public class ColumnarIndexScan extends Iterator {

  private ColumnIndexScan[] _columnIndexScan;
  private Sort[] _sort;
  private boolean _indexOnly;
  private String _relName;
  private int[] fldNum;
  private int noInFlds;
  private int noOutFlds;
  private TIDJoin _tidJoin;
  FldSpec[] outFlds;
  AttrType[] types;
  short[] strSizes;
  Tuple tuple1;
  Tuple tuple2;
  Tuple Jtuple;

  int[] colNumToScan;

  enum TIDJoin {
    AND, OR, NONE;
  }

  public ColumnarIndexScan(
      java.lang.String relName,
      int[] fldNum,
      IndexType index,
      String[] indName,
      AttrType[] types,
      short[] str_sizes,
      int noInFlds,
      int noOutFlds,
      FldSpec[] outFlds,
      CondExpr[] selects,
      boolean indexOnly)
      throws IndexException,
      InvalidTypeException,
      InvalidTupleSizeException,
      UnknownIndexTypeException,
      IOException,
      SortException, TupleUtilsException, InvalidRelation {

    this.noInFlds = noInFlds;
    this.noOutFlds = noOutFlds;
    this.types = types;
    this.colNumToScan = fldNum;
    this._relName = relName;
    this.outFlds = outFlds;
    Jtuple = new Tuple();
    AttrType[] Jtypes = new AttrType[noOutFlds];
    short[] ts_size;
    ts_size = TupleUtils.setup_op_tuple(
        Jtuple, Jtypes, types, noInFlds, str_sizes, outFlds, noOutFlds);

    // column number to scan
    if (selects[0].next == null && selects[1] == null) {
      fldNum = new int[1];
      fldNum[0] = fldNum[0];
    } else {
      fldNum = new int[2];
      fldNum[0] = fldNum[0];
      fldNum[1] = fldNum[1];
    }

    // setup cond expr
    CondExpr[][] expr = new CondExpr[colNumToScan.length][2];
    expr[0][0] = copyExpr(selects[0]);
    expr[0][1] = null;
    _tidJoin = TIDJoin.NONE;
    if (colNumToScan.length == 2) {
      if (selects[0].next != null) {
        expr[1][0] = copyExpr(selects[0].next);
        expr[1][1] = null;
        _tidJoin = TIDJoin.OR;
      } else {
        expr[1][0] = copyExpr(selects[1]);
        expr[1][1] = null;
        _tidJoin = TIDJoin.AND;
      }
    }

    // open column index scans
    _columnIndexScan = new ColumnIndexScan[colNumToScan.length];
    for (int i = 0; i < colNumToScan.length; i++) {
      _columnIndexScan[i] = new ColumnIndexScan(
          index,
          relName + "." + Integer.toString(colNumToScan[i]),
          indName[colNumToScan[i] - 1],
          types[colNumToScan[i] - 1],
          (short)0,
          expr[i],
          indexOnly);
    }

    if (colNumToScan.length == 1) {
      return;
    }

    _sort = new Sort[colNumToScan.length];
    for (int i = 0; i < colNumToScan.length; i++) {
      _sort[i] = new Sort(
          new AttrType[] { types[colNumToScan[i] - 1] },
          (short) 1,
          null,
          _columnIndexScan[i],
          1,
          new TupleOrder(TupleOrder.Ascending),
          4,
          10);
    }
  }

  public Tuple get_next()
      throws Exception {
    TID tid = get_next_TID();
    if (tid == null) {
      return null;
    }
    Columnarfile f = new Columnarfile(_relName);
    Tuple t = f.getTuple(tid);
    Projection.Project(t, types, Jtuple, outFlds, noOutFlds);
    return Jtuple;
  }

  public TID get_next_TID()
      throws Exception {
    Integer pos = get_next_pos();
    // System.err.println(pos);
    if (pos == null) {
      return null;
    }
    Columnarfile f = new Columnarfile(_relName);
    return f.getTidFromPosition(pos);
  }

  public Integer get_next_pos()
      throws Exception {
    Columnarfile f = new Columnarfile(_relName);
    if (colNumToScan.length == 1) {
      KeyDataEntry entry = _columnIndexScan[0].get_next_KeyDataEntry();
      if (entry == null) {
        return null;
      }
      RID rid = ((LeafData) entry.data).getData();
      int position = f.getPositionFromRid(rid, this.colNumToScan[0]);
      return position;
    }
    if (_tidJoin == TIDJoin.AND) {
      if (tuple1 == null || tuple2 == null) {
        tuple1 = _sort[0].get_next();
        tuple2 = _sort[1].get_next();
      }
      while (tuple1 != null && tuple2 != null) {
        int value1 = tuple1.getIntFld(1);
        int value2 = tuple2.getIntFld(1);
        if (value1 == value2) {
          tuple1 = _sort[0].get_next();
          tuple2 = _sort[1].get_next();
          return value1;
        } else if (value1 < value2) {
          tuple1 = _sort[0].get_next();
        } else {
          tuple2 = _sort[1].get_next();
        }
      }
      return null;
    }
    if (_tidJoin == TIDJoin.OR) {
      if (tuple1 == null || tuple2 == null) {
        tuple1 = _sort[0].get_next();
        tuple2 = _sort[1].get_next();
      }
      while (tuple1 != null || tuple2 != null) {
        if (tuple1 == null) {
          int value2 = tuple2.getIntFld(1);
          tuple2 = _sort[1].get_next();
          return value2;
        } else if (tuple2 == null) {
          int value1 = tuple1.getIntFld(1);
          tuple1 = _sort[0].get_next();
          return value1;
        } else {
          int value1 = tuple1.getIntFld(1);
          int value2 = tuple2.getIntFld(1);
          if (value1 == value2) {
            tuple1 = _sort[0].get_next();
            tuple2 = _sort[1].get_next();
            return value1;
          } else if (value1 < value2) {
            tuple1 = _sort[0].get_next();
            return value1;
          } else {
            tuple2 = _sort[1].get_next();
            return value2;
          }
        }
      }
      return null;
    }
    throw new Exception("Invalid TID join type");
  }

  public void close() throws IOException, IndexException, SortException {
    if (!closeFlag) {
      for (int i = 0; i < _columnIndexScan.length; i++) {
        _columnIndexScan[i].close();
      }
      if (_sort != null) {
        for (int i = 0; i < _sort.length; i++) {
          _sort[i].close();
        }
      }
    }
  }

  /**
   * Copy a CondExpr
   * but the next field is not copied
   * it's set to null
   * 
   * @param expr
   * @return
   */
  private CondExpr copyExpr(CondExpr expr) {
    CondExpr newExpr = new CondExpr();
    newExpr.op = new AttrOperator(expr.op.attrOperator);
    newExpr.type1 = new AttrType(expr.type1.attrType);
    newExpr.type2 = new AttrType(expr.type2.attrType);
    newExpr.operand1 = copyOperand(expr.operand1, expr.type1);
    newExpr.operand2 = copyOperand(expr.operand2, expr.type2);
    return newExpr;
  }

  private Operand copyOperand(Operand operand, AttrType type) {
    Operand newOperand = new Operand();
    switch (type.attrType) {
      case AttrType.attrInteger:
        newOperand.integer = operand.integer;
        break;
      case AttrType.attrReal:
        newOperand.real = operand.real;
        break;
      case AttrType.attrString:
        newOperand.string = new String(operand.string);
        break;
      case AttrType.attrSymbol:
        RelSpec rel = new RelSpec(operand.symbol.relation.key);
        newOperand.symbol = new FldSpec(rel, operand.symbol.offset);
        break;
    }
    return newOperand;
  }

}
