/*
 * File - TupleScan.java
 *
 * Original Author - Vivian Roshan Adithan, Vedanti
 *
 * Description - 
 *		...
 */
package columnar;

import java.io.IOException;

import global.RID;
import global.TID;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;

public class TupleScan {

  private Columnarfile cf;
  private int position = -1;
  private Scan scanArray[];

  // Constructor Summary
  public TupleScan(Columnarfile cf)
      throws HFException,
      HFBufMgrException,
      HFDiskMgrException,
      IOException,
      InvalidTupleSizeException,
      IOException {
    this.scanArray = new Scan[Columnarfile.numColumns];
    this.cf = cf;
    for (int i = 0; i < Columnarfile.numColumns; i++) {
      String fileName = cf.get_fileName() + "." + Integer.toString(i + 1);
      Heapfile hf = new Heapfile(fileName);
      scanArray[i] = hf.openScan();
    }
  }

  // Closes the TupleScan object
  public void closetuplescan() {
    for (int i = 0; i < Columnarfile.numColumns; i++)
      this.scanArray[i].closescan();
  }

  // Retrieve the next tuple in a sequential scan
  public Tuple getNext(TID tid)
      throws InvalidTupleSizeException,
      IOException,
      Exception {
    this.position++;
    RID[] rid = new RID[Columnarfile.numColumns];
    Tuple[] columnTuple = new Tuple[Columnarfile.numColumns];
    for (int i = 0; i < Columnarfile.numColumns; i++) {
      rid[i] = new RID();
      columnTuple[i] = this.scanArray[i].getNext(rid[i]);
    }
    if(columnTuple[0] == null) {
      return null;
    }
    tid.copyTid(new TID(Columnarfile.numColumns, this.position, rid));
    return cf.getTupleFromColTuples(columnTuple);
  }

  // Position all scan cursors to the records with the given rids
  public boolean position(TID tid)
      throws IOException,
      InvalidTupleSizeException {
    this.position = tid.position;
    boolean isPositionChanged = true;
    for (int i = 0; i < Columnarfile.numColumns; i++) {
      boolean isRidPositionChanged = this.scanArray[i].position(tid.recordIDs[i]);
      if (!isRidPositionChanged) {
        isPositionChanged = false;
      }
    }
    return isPositionChanged;
  }
}
