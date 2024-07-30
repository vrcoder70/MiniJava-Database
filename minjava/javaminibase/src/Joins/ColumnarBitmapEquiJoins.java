package Joins;

import java.io.*;
import java.lang.*;
import java.util.*;
import driver.*;
import btree.*;
import bitmap.BMFileScan;
import bitmap.BitMapFile;
import bitmap.CBMFileScan;
import bitmap.CBitMapFile;
import btree.KeyDataEntry;
import columnar.CFException;
import columnar.Columnarfile;
import global.AttrType;
import global.IndexType;
import global.ValueClass;
import heap.InvalidSlotNumberException;
import heap.Tuple;
import iterator.ColumnarFileScan;
import iterator.*;

public class ColumnarBitmapEquiJoins  {

    public ColumnarBitmapEquiJoins( 
        String leftColumnarFileName, 
        String leftJoinFeild, 
        String rightColumnarFileName,
        String rightJoinFeild,
        String[] targetLeftColumnNames,
        String[] targetRightColumnNames,
        String indexType
    ){
        try {
            Columnarfile leftColumnarfile = loadColumnarFile(leftColumnarFileName);
            Columnarfile rightColumnarfile = loadColumnarFile(rightColumnarFileName);
            
            String[] leftColumns = leftColumnarfile.columnNames;
            String[] rightColums = rightColumnarfile.columnNames;

            int[] targetleftCols = getTargetCols(targetLeftColumnNames, leftColumns);
            int[] targetrightsCols = getTargetCols(targetRightColumnNames, rightColums);
            
            int leftJoinFeildIndex = Arrays.asList(leftColumns).indexOf(leftJoinFeild)+1;
            int rightJoinFeildIndex = Arrays.asList(rightColums).indexOf(rightJoinFeild)+1;
            
            // ColumnarFileScan leftScan = getColumnScan(leftColumnarfile, leftColumnarFileName, targetLeftColumnNames);
            // Tuple t = leftScan.get_next();
            // t.print(leftColumnarfile.type);
            if(indexType.equals("BITMAP")){
                String leftBmFileName = leftColumnarFileName + ".bitmap" + Integer.toString(leftJoinFeildIndex);
                // System.out.println("Bitmap File Name:- "+leftBmFileName);
                BitMapFile leftBitMapFile = loadBitmapIndexes(leftBmFileName, leftColumnarfile, leftJoinFeildIndex);
                    
                String rightBmFileName = rightColumnarFileName + ".bitmap" + Integer.toString(rightJoinFeildIndex);
                BitMapFile righBitMapFile = loadBitmapIndexes(rightBmFileName, rightColumnarfile, rightJoinFeildIndex);
                performBitmapEquiJoin(leftBitMapFile, righBitMapFile, leftColumnarfile, rightColumnarfile, leftJoinFeildIndex, rightJoinFeildIndex, targetleftCols, targetrightsCols);
            }else{
                String leftCBmFileName = leftColumnarFileName + ".cbitmap" + Integer.toString(leftJoinFeildIndex);
                String rightCBmFileName = rightColumnarFileName + ".cbitmap" + Integer.toString(leftJoinFeildIndex);

                CBitMapFile leftCBitMapScanFile = loadCBitmapIndexes(leftCBmFileName, leftColumnarfile, leftJoinFeildIndex);
                CBitMapFile rightCBitMapScanFile = loadCBitmapIndexes(rightCBmFileName, rightColumnarfile, rightJoinFeildIndex);
                performCBitmapEquiJoin(leftCBitMapScanFile, rightCBitMapScanFile, leftColumnarfile, rightColumnarfile, leftJoinFeildIndex, rightJoinFeildIndex, targetleftCols, targetrightsCols);
            }

            
        } catch (Exception e) {
            System.err.println("Error in Joins");
            e.printStackTrace();
        }
        

    }

    public int[] getTargetCols( String[] targetColumnNames, String[] columnsNamea){
        int[] targetCols = new int[targetColumnNames.length];
        for( int j=0; j < targetColumnNames.length; j++){
            for (int i = 0; i < columnsNamea.length; i++) {
                if(targetColumnNames[j].equals(columnsNamea[i])){
                    targetCols[j] = i+1;
                    break;
                }
            }
        }  
        return targetCols;
    }

    

    private Columnarfile loadColumnarFile(String columnarFileName) throws CFException{
        // Method to load columnar files into memory
        Columnarfile columnarFile = null;
        
        try{
            columnarFile = new Columnarfile(columnarFileName);
            System.out.println("Columnar File "+ columnarFileName + " exist. Opening the file...");
        }catch (Exception e){
            System.out.println("Columnar File " + columnarFileName + " does not exist.");
            throw new CFException(null, "file does not exist");
        }
        
        
        return columnarFile;


    }

    private CBitMapFile loadCBitmapIndexes(String bitmapFileName, Columnarfile columnarfile, int colNum) throws Exception{
        try {
            ValueClass value = columnarfile.getClassType(colNum);
            CBitMapFile cbitmap = null;

            try {
                cbitmap = new CBitMapFile(bitmapFileName, columnarfile, colNum, value);
            } catch (Exception e) {
                System.out.println("Error in creating or opening in bitMapFile...");
                throw new GetFileEntryException(null, "file does not exist");
            }
            return cbitmap;
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            throw e;
        }
        
    }


    private BitMapFile loadBitmapIndexes(String bitmapFileName, Columnarfile columnarfile, int colNum) throws Exception {
        try {
            ValueClass value = columnarfile.getClassType(colNum);
            BitMapFile bitmap = null;

            try {
                bitmap = new BitMapFile(bitmapFileName, columnarfile, colNum, value);
            } catch (Exception e) {
                System.out.println("Error in creating or opening in bitMapFile...");
                throw new GetFileEntryException(null, "file does not exist");
            }            

            return bitmap;
            
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            throw e;
        }
        
    }

    private void performCBitmapEquiJoin(CBitMapFile leftBitmap, 
    CBitMapFile rightBitmap, 
    Columnarfile leftColumnarfile, 
    Columnarfile rightColumnarfile, 
    int leftJoinField, 
    int rightJoinField,
    int[] leftCols,
    int[] rightCols
    ) {
        try {
           
            int leftJoinFieldType = leftColumnarfile.type[leftJoinField-1].attrType;
            CBMFileScan left = leftBitmap.new_scan(null, null);
            AttrType att = leftColumnarfile.type[leftJoinField - 1];
            KeyDataEntry leftEntry;
            while ((leftEntry = left.get_next()) != null) {
                KeyDataEntry rightEntry;
                CBMFileScan right = rightBitmap.new_scan(null, null);
                while ((rightEntry = right.get_next()) != null) {
                    Tuple leftTuple = leftColumnarfile.getTuple(leftColumnarfile.getTidFromPosition(leftColumnarfile.getPositionFromRid(((LeafData) leftEntry.data).getData(), leftJoinField)));
                    Tuple rightTuple = rightColumnarfile.getTuple(rightColumnarfile.getTidFromPosition(rightColumnarfile.getPositionFromRid(((LeafData) rightEntry.data).getData(), rightJoinField)));
                
                    int comp_res = TupleUtils.CompareTupleWithTuple(att, leftTuple, leftJoinField, rightTuple, rightJoinField);
                    if(comp_res == 0){
                        
                        System.out.print("{");
                        printTupleColumns(leftColumnarfile, leftTuple, leftCols);
                        System.out.print(", ");
                        printTupleColumns(rightColumnarfile, rightTuple, rightCols);
                        System.out.println("}");
                    }
                }
                right.closeCbitmapScans();
            }
            // Reset left bitmap scan for next iteration
            left.closeCbitmapScans();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void performBitmapEquiJoin(BitMapFile leftbitmap, 
    BitMapFile rightbitmap, 
    Columnarfile leftColumnarfile, 
    Columnarfile rightColumnarfile, 
    int leftJoinField, 
    int rightJoinField,
    int[] leftCols,
    int[] rightCols){
        try {
            BMFileScan left = leftbitmap.new_scan(null, null);
            int leftJoinFieldType = leftColumnarfile.type[leftJoinField - 1].attrType;
            AttrType att = leftColumnarfile.type[leftJoinField - 1];
            KeyDataEntry leftEntry;
            System.out.println(leftJoinFieldType);
            while ((leftEntry = left.get_next()) != null) {
                
                KeyDataEntry rightEntry;
                BMFileScan right = rightbitmap.new_scan(null, null);
                while ((rightEntry = right.get_next()) != null) {
                    Tuple leftTuple = leftColumnarfile.getTuple(leftColumnarfile.getTidFromPosition(leftColumnarfile.getPositionFromRid(((LeafData) leftEntry.data).getData(), leftJoinField)));
                    Tuple rightTuple = rightColumnarfile.getTuple(rightColumnarfile.getTidFromPosition(rightColumnarfile.getPositionFromRid(((LeafData) rightEntry.data).getData(), rightJoinField)));
                    
                    int comp_res = TupleUtils.CompareTupleWithTuple(att, leftTuple, leftJoinField, rightTuple, rightJoinField);
                    
                    if(comp_res == 0){
                        
                        System.out.print("{");
                        printTupleColumns(leftColumnarfile, leftTuple, leftCols);
                        System.out.print(", ");
                        printTupleColumns(rightColumnarfile, rightTuple, rightCols);
                        System.out.println("}");
                    }
                }
                right.closeBitmapScans();
            }
            left.closeBitmapScans();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    

    public void printTupleColumns(Columnarfile columnarFile, Tuple tuple, int[] targetColumns){
        try
		{	
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
		}
		catch( Exception ex )
		{
			System.out.println("<ERROR-PRINTING>");
			ex.printStackTrace();
		}
    }

}
