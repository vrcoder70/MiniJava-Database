/*
 * File - DBDriver.java
 *
 * Description - 
 *		Top level driver for DB controls. Currently supports ability to:
 1) Perform Batch Insert
 2) 
 */

package driver;

import java.io.*;
import java.util.*;
import java.lang.*;
import columnar.*;
import diskmgr.PCounter;
import heap.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import btree.*;
import Joins.*;

/*
** Command Line Argument retrieval support tools
*/
class cliReader
{
	cliReader() {}
	
	//known integer return from menu selection
	public static int getChoice ()
	{
		BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
		int choice = -1;
		
		try
		{
			choice = Integer.parseInt(in.readLine());
		}
		catch (NumberFormatException e)
		{
			return -1;
		}
		catch (IOException e)
		{
			return -1;
		}
		
		return choice;
	} 
	
	//other user inputs
	public static String getReturn()
	{
		BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
		
		try
		{
			String ret = in.readLine();
			return ret;
		}
		catch (IOException e)
		{
			return null;
		}
	} 
}

public class DBDriver
{
	
	private static void printMenu()
	{
		System.out.println("\n"); //formatting separation
		System.out.println("===========================");
		System.out.println("---DB Supported Commands---");
		System.out.println("===========================");
		System.out.println("---Configuration Commands---");
		System.out.println("[1] Set DB Name");
		System.out.println("[2] Set Columnar File Name");
		System.out.println("[3] Print available Indexes");
		System.out.println("---Operational Commands---");
		System.out.println("[4] Batch Insert");
		System.out.println("[5] Create an Index");
		System.out.println("[6] DB Query");
		System.out.println("[7] Complex DB Query");
		System.out.println("[8] Query_Delete DB");
		System.out.println("[9] Column Bitmap Equi Join");
		System.out.println("------");
		System.out.println("[10] Exit this Program");
		System.out.print("Make your choice: ");
	}
	
	private static void getAvailableIndexes( String columnDB, String columnar )
	{
		String columnDBName = columnDB + ".minibase-db";
		String currentDirectory = System.getProperty("user.dir");
		File file = new File(currentDirectory, columnDBName);
		boolean dbExists = false;
		try
		{
            if (file.exists())
			{
                System.out.println("DB exists. Opening the DB");
                SystemDefs.MINIBASE_RESTART_FLAG = true;
				SystemDefs sysdef = new SystemDefs( columnDBName, 10000, GlobalConst.NUMBUF, "Clock");
				dbExists = true;
            }
			else
			{
                System.out.println("DB doesn't exists, No indexes can exist.");
            }
        }
		catch (Exception ex)
		{   
            ex.printStackTrace();
        }
		
		if( dbExists )
		{
			try
			{
				Columnarfile columnarFile = new Columnarfile(columnar);
				int colCount = columnarFile.type.length;
				int asciiBase = 64; // or the ascii value for A - 1
				//print all Btrees
				System.out.println("BTrees");
				for(int i = 1; i <= colCount; i++)
				{
					String checkName = columnar + ".btree" + Integer.toString(i);
					PageId existence = SystemDefs.JavabaseDB.get_file_entry( checkName );
					if( null != existence )
					{
						System.out.print("   > Column " + ((char)(asciiBase + i)) );
						if (columnarFile.type[i-1].attrType == AttrType.attrInteger)
						{
							System.out.println(" (int)");
						}
						else if(columnarFile.type[i-1].attrType == AttrType.attrString)
						{
							System.out.println(" (str)");
						}
					}
				}
				//print all bitmaps
				System.out.println("Bitmaps");
				for(int i = 1; i <= colCount; i++)
				{
					String checkName = columnar + ".bitmap" + Integer.toString(i);
					PageId existence = SystemDefs.JavabaseDB.get_file_entry( checkName );
					if( null != existence )
					{
						System.out.print("   > Column " + ((char)(asciiBase + i)) );
						if (columnarFile.type[i-1].attrType == AttrType.attrInteger)
						{
							System.out.println(" (int)");
						}
						else if(columnarFile.type[i-1].attrType == AttrType.attrString)
						{
							System.out.println(" (str)");
						}
					}
				}
				//print all compressed bitmaps
				System.out.println("Compressed Bitmaps");
				for(int i = 1; i <= colCount; i++)
				{
					String checkName = columnar + ".cbitmap" + Integer.toString(i);
					PageId existence = SystemDefs.JavabaseDB.get_file_entry( checkName );
					if( null != existence )
					{
						System.out.print("   > Column " + ((char)(asciiBase + i)) );
						if (columnarFile.type[i-1].attrType == AttrType.attrInteger)
						{
							System.out.println(" (int)");
						}
						else if(columnarFile.type[i-1].attrType == AttrType.attrString)
						{
							System.out.println(" (str)");
						}
					}
				}
			}
			catch( Exception ex )
			{
				ex.printStackTrace();
			}
		}
	}
	
    public static void main(String[] args)
	{
        int choice = 0;
		boolean moreCommands = true;
		
		String columnDB = "";
		String columnarName = "";
		
		while( moreCommands )
		{
			printMenu(); 
			
			try{
				choice = cliReader.getChoice();
				
				switch( choice )
				{
					case 1: //change DB being operated on
						String tmpDB = "";
						while( tmpDB.equals("") )
						{
							System.out.println("Please enter a DB name to use (\".minibase-db\" will be postfixed): ");
							tmpDB = cliReader.getReturn();
						}
						columnDB = tmpDB;
						break;
						
					case 2: //change columnar file being operate on
						String tmpColumnar = "";
						while( tmpColumnar.equals("") )
						{
							System.out.println("Please enter the name of the columnar file being inserted to: ");
							tmpColumnar = cliReader.getReturn();
						}
						columnarName = tmpColumnar;
						break;
					
					case 3: //print the configured indexes for this columnar file
						if( columnDB.equals("") )
						{
							System.out.println("Unspecified DB to operate on (command [1])");
						}
						else if( columnarName.equals("") )
						{
							System.out.println("Unspecified ColumnarFile to index (command [2])");
						}
						else
						{
							getAvailableIndexes( columnDB, columnarName );
						}
						break;
					
					case 4: //Batch Insert
						if( columnDB.equals("") )
						{
							System.out.println("Unspecified DB to operate on(command [1])");
						}
						else
						{
							String fileName = "";
							int colCount = -1;
							
							while( fileName.equals("") )
							{
								System.out.println("Please enter a name of the file to read in (\".txt\" will be postfixed): ");
								fileName = cliReader.getReturn() + ".txt";
							}
							while( columnarName.equals("") ) //if one hasn't been specified yet
							{
								System.out.println("Please enter the name of the columnar file being inserted to: ");
								columnarName = cliReader.getReturn();
							}
							while( -1 == colCount )
							{
								System.out.println("Please enter the expected number of columns: ");
								colCount = cliReader.getChoice();
								if( -1 == colCount )
									System.out.println("Invalid column number");
							}
							//perform insert operation
							BatchInsert.batchInsert(fileName, columnDB, columnarName, colCount);
						}
						break;
						
					case 5: //Create Index
						if( columnDB.equals("") )
						{
							System.out.println("Unspecified DB to operate on (command [1])");
						}
						else if( columnarName.equals("") )
						{
							System.out.println("Unspecified ColumnarFile to index (command [2])");
						}
						else
						{
							String colIndex = "";
							String indexType = "";
							
							while( colIndex.equals("") )
							{
								System.out.println("Please specify column to index (ie, A/B/C/...): ");
								colIndex = cliReader.getReturn().toUpperCase().substring(0,1);
							}
							int idxPick = -1;
							while( -1 == idxPick )
							{
								System.out.println("Pick Index to create:");
								System.out.println("[1] BTree");
								System.out.println("[2] Bitmap");
								System.out.println("[3] Compressed Bitmap");
								idxPick = cliReader.getChoice();
								switch( idxPick )
								{
									case 1: indexType = "BTREE";
										break;
									case 2: indexType = "BITMAP";
										break;
									case 3: indexType = "CBITMAP";
										break;
									default: System.out.println("Invalid/Unsupported Index");
										break;
								}
							}
							//perform insert operation
							Index.createIndex(columnDB, columnarName, colIndex, indexType);
						}
						break;
						
					case 6: //Query DB
						if( columnDB.equals("") )
						{
							System.out.println("Unspecified DB to operate on (command [1])");
						}
						else if( columnarName.equals("") )
						{
							System.out.println("Unspecified ColumnarFile to index (command [2])");
						}
						else
						{
							String targetColumns = "";
							String queryColumn = "";
							String queryType = "";
							String compValue = "";
							String indexType = "";
							int bufCount = 0;
							
							System.out.println("QUERY = [condition] [selection]");
							while( targetColumns.equals("") )
							{
								System.out.println("Please specify columns (comma delimited) to return on match (ie, A,B,E,...): ");
								targetColumns = cliReader.getReturn().toUpperCase();
								//remove any whitespaces user may have added
								targetColumns.replaceAll("\\s+","");
							}
							//Formulate the query syntax
							System.out.println("QUERY = [condition] o[" + targetColumns + "]");
							while( queryColumn.equals("") )
							{
								System.out.println("Please specify column to query (ie, A/B/C/...): ");
								queryColumn = cliReader.getReturn().toUpperCase().substring(0,1);
							}
							System.out.println("QUERY = \"" + queryColumn + " ?= [?]\" o[" + targetColumns + "]");
							int idxPick = -1;
							while( -1 == idxPick )
							{
								System.out.println("Pick comparator to use:");
								System.out.println("[1] =");
								System.out.println("[2] <");
								System.out.println("[3] <=");
								System.out.println("[4] >");
								System.out.println("[5] >=");
								System.out.println("[6] !=");
								idxPick = cliReader.getChoice();
								switch( idxPick )
								{
									case 1: queryType = "=";
										break;
									case 2: queryType = "<";
										break;
									case 3: queryType = "<=";
										break;
									case 4: queryType = ">";
										break;
									case 5: queryType = ">=";
										break;
									case 6: queryType = "!=";
										break;
									default: System.out.println("Invalid/Unsupported Comparator");
										break;
								}
							}
							System.out.println("QUERY = \"" + queryColumn + " " + queryType + " [?]\" o[" + targetColumns + "]");
							while( compValue.equals("") )
							{
								System.out.println("Please declare value to compare against: ");
								compValue = cliReader.getReturn();
							}
							System.out.println("QUERY = \"" + queryColumn + " " + queryType + " " + compValue + "\" o[" + targetColumns + "]");
							//select an index or scan to resolve the query
							idxPick = -1;
							while( -1 == idxPick )
							{
								System.out.println("Pick query resolution approach:");
								System.out.println("[1] FileScan");
								System.out.println("[2] ColumnScan");
								System.out.println("[3] BTree");
								System.out.println("[4] Bitmap");
								System.out.println("[5] Compressed Bitmap");
								idxPick = cliReader.getChoice();
								switch( idxPick )
								{
									case 1: indexType = "FILESCAN";
										break;
									case 2: indexType = "COLUMNSCAN";
										break;
									case 3: indexType = "BTREE";
										break;
									case 4: indexType = "BITMAP";
										break;
									case 5: indexType = "CBITMAP";
										break;
									default: System.out.println("Invalid/Unsupported Index");
										break;
								}
							}
							//specify a buffer count
							while( 0 >= bufCount )
							{
								System.out.println("Specify number of buffers to use to resolve query:");
								bufCount = cliReader.getChoice();
							}
							//Query DB (ending "false" is we won't delete on match)
							QueryProgram.queryDB(columnDB, columnarName, targetColumns, queryColumn,
													queryType, compValue, bufCount, indexType, false );
						}
						break;
					
					case 7: //Complex Query DB
						if( columnDB.equals("") )
						{
							System.out.println("Unspecified DB to operate on (command [1])");
						}
						else if( columnarName.equals("") )
						{
							System.out.println("Unspecified ColumnarFile to index (command [2])");
						}
						else
						{
							String targetColumns = "";
							String queryColumn1 = "";
							String queryType1 = "";
							String compValue1 = "";
							String queryColumn2 = "";
							String queryType2 = "";
							String compValue2 = "";
							int constraintOp = -1;
							String indexType = "";
							int bufCount = 0;
							
							System.out.println("   QUERY = {[condition 1] [constraint] [condition 2]} [selection]");
							while( targetColumns.equals("") )
							{
								System.out.println("Please specify columns (comma delimited) to return on match (ie, A,B,E,...): ");
								targetColumns = cliReader.getReturn().toUpperCase();
								//remove any whitespaces user may have added
								targetColumns.replaceAll("\\s+","");
							}
							//define the first query
							System.out.println("   QUERY = {[condition 1] [constraint] [condition 2]} o[" + targetColumns + "]");
							while( queryColumn1.equals("") )
							{
								System.out.println("Please specify 1st column to query (ie, A/B/C/...): ");
								queryColumn1 = cliReader.getReturn().toUpperCase().substring(0,1);
							}
							System.out.println("   QUERY = {\"" + queryColumn1 + " ?= [?]\" [constraint] [condition 2]} o[" + targetColumns + "]");
							int idxPick = -1;
							while( -1 == idxPick )
							{
								System.out.println("Pick 1st comparator to use:");
								System.out.println("[1] =");
								System.out.println("[2] <");
								System.out.println("[3] <=");
								System.out.println("[4] >");
								System.out.println("[5] >=");
								System.out.println("[6] !=");
								idxPick = cliReader.getChoice();
								switch( idxPick )
								{
									case 1: queryType1 = "=";
										break;
									case 2: queryType1 = "<";
										break;
									case 3: queryType1 = "<=";
										break;
									case 4: queryType1 = ">";
										break;
									case 5: queryType1 = ">=";
										break;
									case 6: queryType1 = "!=";
										break;
									default: System.out.println("Invalid/Unsupported Comparator");
										break;
								}
							}
							System.out.println("   QUERY = {\"" + queryColumn1 + " " + queryType1 + " [?]\" [constraint] [condition2]} o[" + targetColumns + "]");
							while( compValue1.equals("") )
							{
								System.out.println("Please declare 1st value to compare against: ");
								compValue1 = cliReader.getReturn();
							}
							//define the second query
							System.out.println("   QUERY = {\"" + queryColumn1 + " " + queryType1 + " " + compValue1 + "\" [constraint] [condition 2]} o[" + targetColumns + "]");
							while( queryColumn2.equals("") )
							{
								System.out.println("Please specify 2nd column to query (ie, A/B/C/...): ");
								queryColumn2 = cliReader.getReturn().toUpperCase().substring(0,1);
							}
							System.out.println("   QUERY = {\"" + queryColumn1 + " " + queryType1 + " " + compValue1 + "\" [constraint] \""+ queryColumn2 +" ?= [?]\"} o[" + targetColumns + "]");
							idxPick = -1;
							while( -1 == idxPick )
							{
								System.out.println("Pick 2nd comparator to use:");
								System.out.println("[1] =");
								System.out.println("[2] <");
								System.out.println("[3] <=");
								System.out.println("[4] >");
								System.out.println("[5] >=");
								System.out.println("[6] !=");
								idxPick = cliReader.getChoice();
								switch( idxPick )
								{
									case 1: queryType2 = "=";
										break;
									case 2: queryType2 = "<";
										break;
									case 3: queryType2 = "<=";
										break;
									case 4: queryType2 = ">";
										break;
									case 5: queryType2 = ">=";
										break;
									case 6: queryType2 = "!=";
										break;
									default: System.out.println("Invalid/Unsupported Comparator");
										break;
								}
							}
							System.out.println("   QUERY = {\"" + queryColumn1 + " " + queryType1 + " " + compValue1 + "\" [constraint] \""+ queryColumn2 + " " + queryType2 + " [?]\"} o[" + targetColumns + "]");
							while( compValue2.equals("") )
							{
								System.out.println("Please declare 2nd value to compare against: ");
								compValue2 = cliReader.getReturn();
							}
							System.out.println("   QUERY = {\"" + queryColumn1 + " " + queryType1 + " " + compValue1 + "\" [constraint] \""+ queryColumn2 + " " + queryType2 + " " + compValue2 + "\"} o[" + targetColumns + "]");
							String tmpConstraint = "none";
							while( constraintOp < 0 )
							{
								System.out.println("Specify the constraint on the queries:");
								System.out.println("[1] and");
								System.out.println("[2] or");
								constraintOp = cliReader.getChoice();
								if( 1 == constraintOp )
								{
									tmpConstraint = "and";
								}
								else if(2 == constraintOp)
								{
									tmpConstraint = "or";
								}
								else
								{
									System.out.println("Invalid Selection");
									constraintOp = -1;
								}
							}
							System.out.println("   QUERY = {\"" + queryColumn1 + " " + queryType1 + " " + compValue1 + "\" " + tmpConstraint + " \""+ queryColumn2 + " " + queryType2 + " " + compValue2 + "\"} o[" + targetColumns + "]");
							//select an index or scan to resolve the query
							idxPick = -1;
							while( -1 == idxPick )
							{
								System.out.println("Pick query resolution approach:");
								System.out.println("[1] FileScan");
								System.out.println("[2] ColumnScan");
								System.out.println("[3] BTree");
								System.out.println("[4] Bitmap");
								System.out.println("[5] Compressed Bitmap");
								idxPick = cliReader.getChoice();
								switch( idxPick )
								{
									case 1: indexType = "FILESCAN";
										break;
									case 2: indexType = "COLUMNSCAN";
										break;
									case 3: indexType = "BTREE";
										break;
									case 4: indexType = "BITMAP";
										break;
									case 5: indexType = "CBITMAP";
										break;
									default: System.out.println("Invalid/Unsupported Index");
										break;
								}
							}
							//specify a buffer count
							while( 0 >= bufCount )
							{
								System.out.println("Specify number of buffers to use to resolve query:");
								bufCount = cliReader.getChoice();
							}
							//Query DB (ending "false" is we won't delete on match)
							ComplexQueryProgram.queryDB(columnDB, columnarName, targetColumns, 
													queryColumn1, queryType1, compValue1, 
													queryColumn2, queryType2, compValue2, 
													constraintOp, bufCount, indexType, false );
						}
						break;
					
					case 8: //Query Delete DB
						if( columnDB.equals("") )
						{
							System.out.println("Unspecified DB to operate on (command [1])");
						}
						else if( columnarName.equals("") )
						{
							System.out.println("Unspecified ColumnarFile to index (command [2])");
						}
						else
						{
							String targetColumns = "";
							String queryColumn = "";
							String queryType = "";
							String compValue = "";
							String indexType = "";
							int bufCount = 0;
							
							System.out.println("QUERY = [condition] [selection]");
							while( targetColumns.equals("") )
							{
								System.out.println("Please specify columns (comma delimited) to return on match (ie, A,B,E,...): ");
								targetColumns = cliReader.getReturn().toUpperCase();
								//remove any whitespaces user may have added
								targetColumns.replaceAll("\\s+","");
							}
							//Formulate the query syntax
							System.out.println("QUERY = [condition] o[" + targetColumns + "]");
							while( queryColumn.equals("") )
							{
								System.out.println("Please specify column to query (ie, A/B/C/...): ");
								queryColumn = cliReader.getReturn().toUpperCase().substring(0,1);
							}
							System.out.println("QUERY = \"" + queryColumn + " ?= [?]\" o[" + targetColumns + "]");
							int idxPick = -1;
							while( -1 == idxPick )
							{
								System.out.println("Pick comparator to use:");
								System.out.println("[1] =");
								System.out.println("[2] <");
								System.out.println("[3] <=");
								System.out.println("[4] >");
								System.out.println("[5] >=");
								System.out.println("[6] !=");
								idxPick = cliReader.getChoice();
								switch( idxPick )
								{
									case 1: queryType = "=";
										break;
									case 2: queryType = "<";
										break;
									case 3: queryType = "<=";
										break;
									case 4: queryType = ">";
										break;
									case 5: queryType = ">=";
										break;
									case 6: queryType = "!=";
										break;
									default: System.out.println("Invalid/Unsupported Comparator");
										break;
								}
							}
							System.out.println("QUERY = \"" + queryColumn + " " + queryType + " [?]\" o[" + targetColumns + "]");
							while( compValue.equals("") )
							{
								System.out.println("Please declare value to compare against: ");
								compValue = cliReader.getReturn();
							}
							System.out.println("QUERY = \"" + queryColumn + " " + queryType + " " + compValue + "\" o[" + targetColumns + "]");
							//select an index or scan to resolve the query
							idxPick = -1;
							while( -1 == idxPick )
							{
								System.out.println("Pick query resolution approach:");
								System.out.println("[1] FileScan");
								System.out.println("[2] ColumnScan");
								System.out.println("[3] BTree");
								System.out.println("[4] Bitmap");
								System.out.println("[5] Compressed Bitmap");
								idxPick = cliReader.getChoice();
								switch( idxPick )
								{
									case 1: indexType = "FILESCAN";
										break;
									case 2: indexType = "COLUMNSCAN";
										break;
									case 3: indexType = "BTREE";
										break;
									case 4: indexType = "BITMAP";
										break;
									case 5: indexType = "CBITMAP";
										break;
									default: System.out.println("Invalid/Unsupported Index");
										break;
								}
							}
							//specify a buffer count
							while( 0 >= bufCount )
							{
								System.out.println("Specify number of buffers to use to resolve query:");
								bufCount = cliReader.getChoice();
							}
							//Query DB (ending "true" is we will flag for delete on match)
							QueryProgram.queryDB(columnDB, columnarName, targetColumns, queryColumn,
													queryType, compValue, bufCount, indexType, true );
						}
						break;
					
					case 9: // Equi Joins
						if( columnDB.equals("") )
						{
							System.out.println("Unspecified DB to operate on (command [1])");
						}else{
							performColumnarBitmapEquiJoin(columnDB);
						}
						break;
					case 10: //Exit CMD
						System.out.println("Exiting");
						moreCommands = false;
						break;
						
					default:
						System.out.println("Invalid menu selection. Try again.");
						break;
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
				System.out.println("       !!         Something is wrong                    !!");
				System.out.println("       !!     Is your DB full? then exit. rerun it!     !!");
				System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			}
		}
    }

	private static void performColumnarBitmapEquiJoin(String columnDB){
		String columnDBName = columnDB+".minibase-db";
		String currentDirectory = System.getProperty("user.dir");
        
        File file = new File(currentDirectory, columnDBName);


		try {
            if (file.exists()) {
                System.out.println("DB exists. Opening the DB");
                SystemDefs.MINIBASE_RESTART_FLAG = true;
            } else {
                System.out.println("DB doesn't exists, Creating new DB.");
                SystemDefs.MINIBASE_RESTART_FLAG = false;
            }
            SystemDefs sysdef = new SystemDefs( columnDBName, 10000, GlobalConst.NUMBUF, "Clock");
        } catch (Exception e) {
        }

		System.out.println("Please enter Outer left columnar file: ");
		String leftColumnarFileName = cliReader.getReturn();

		System.out.println("Please enter Outer right columnar file: ");
		String rightColumnarFileName = cliReader.getReturn();

		String indexType = "";
		int idxPick = -1;
		while( -1 == idxPick )
		{
			System.out.println("Pick Index to create:");
			System.out.println("[1] Bitmap");
			System.out.println("[2] Compressed Bitmap");
			idxPick = cliReader.getChoice();
			switch( idxPick )
			{
				case 1: indexType = "BITMAP";
					break;
				case 2: indexType = "CBITMAP";
					break;
				default: System.out.println("Invalid/Unsupported Index");
					break;
			}
		}

		String leftJoinFeild = "";

		while( leftJoinFeild.equals("") )
		{
			System.out.println("Please specify columns to join from left columnar file:");
			leftJoinFeild = cliReader.getReturn().toUpperCase();
			leftJoinFeild.replaceAll("\\s+","");
		}

		String rightJoinFeild = "";

		while( rightJoinFeild.equals("") )
		{
			System.out.println("Please specify columns to join from right columnar file:");
			rightJoinFeild = cliReader.getReturn().toUpperCase();
			rightJoinFeild.replaceAll("\\s+","");
		}

		String targetColumnsleft = "";
		while( targetColumnsleft.equals("") )
		{
			System.out.println("Please specify columns (comma delimited) to return on match (ie, A,B,E,...) for left columnar file: ");
			targetColumnsleft = cliReader.getReturn().toUpperCase();
			//remove any whitespaces user may have added
			targetColumnsleft.replaceAll("\\s+","");
		}

		String targetColumnsright = "";
		while( targetColumnsright.equals("") )
		{
			System.out.println("Please specify columns (comma delimited) to return on match (ie, A,B,E,...) for right columnar file: ");
			targetColumnsright = cliReader.getReturn().toUpperCase();
			//remove any whitespaces user may have added
			targetColumnsright.replaceAll("\\s+","");
		}

		String[] targetLeftColumnNames = targetColumnsleft.split(",");
		String[] targetRightColumnNames = targetColumnsright.split(",");
		ColumnarBitmapEquiJoins join = new ColumnarBitmapEquiJoins(leftColumnarFileName,
		 leftJoinFeild, 
		 rightColumnarFileName, 
		 rightJoinFeild, 
		 targetLeftColumnNames, 
		 targetRightColumnNames, 
		 indexType);

		// SystemDefs.JavabaseBM.flushAllPages();
        System.out.println("Batch Insert Read Count:- "+PCounter.rcounter);
        System.out.println("Batch Insert Write Count:- "+PCounter.wcounter);
	}

}
