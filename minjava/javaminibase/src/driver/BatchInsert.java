package driver;

import columnar.*;
import diskmgr.PCounter;
import global.*;
import heap.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;


public class BatchInsert{
	
	//do nothing constructor
	public BatchInsert(){}
	
    public static void batchInsert(String fileName, String columnDB, String columnarName, int colCount) {
        
        // Extracting command line arguments
        String dataFileName = fileName;
        String columnDBName = columnDB+".minibase-db";
        String columnarFileName = columnarName;
        //int numColumns = Integer.parseInt(args[3]);
        int numColumns = colCount;

        // Get the current working directory
        String currentDirectory = System.getProperty("user.dir");
        
        File file = new File(currentDirectory, columnDBName);

        // Displaying the extracted arguments
        System.out.println("Data File Name: " + dataFileName);
        System.out.println("Column DB Name: " + columnDBName);
        System.out.println("Columnar File Name: " + columnarFileName);
        System.out.println("Number of Columns: " + numColumns);
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
           
            // SystemDefs.MINIBASE_RESTART_FLAG = false;
            // SystemDefs sysdef = new SystemDefs( columnDBName, 10000, GlobalConst.NUMBUF,"Clock"); 
            // TODO: handle exception
        }

        try (BufferedReader br = new BufferedReader(new FileReader(dataFileName)))
		{
            String line;
            // Read the first line for attribute names and types
            String[] attributeInfo = br.readLine().split("\\s+");

            // Initialize attribute types array
            AttrType[] attributeTypes = new AttrType[numColumns];
            String[] columnNames = new String[numColumns];

            int sSize = 0;
            // Parse attribute types from the first line of the data file
            for (int i = 0; i < numColumns; i++)
			{
                String[] attrInfo = attributeInfo[i].split(":");

                // Validate attribute info format
                if (attrInfo.length != 2)
				{
                    // Handle error if attribute info format is incorrect
                    throw new IllegalArgumentException("Invalid attribute info format: " + attributeInfo[i]);
                }

                 // Extract attribute name and type
                columnNames[i] = attrInfo[0];
                String attributeType = attrInfo[1];

                switch (attributeType)
				{
                    case "int":
                        attributeTypes[i] = new AttrType(AttrType.attrInteger);
                        break;
                    case "float":
                        attributeTypes[i] = new AttrType(AttrType.attrReal);
                        break;
                    case "string":
                        attributeTypes[i] = new AttrType(AttrType.attrString);
                        sSize++;
                        break;
                    default:
                        attributeTypes[i] = new AttrType(AttrType.attrString);
                        sSize++;
                        break;
                }
            }
			

            // Initialize array to store maximum string sizes for each column
            short[] strSizes = null;
            if (sSize != 0)
			{
                strSizes = new short[sSize];
                // Set all attribute sizes to the value of MAX_NAME
                for(int i = 0; i < strSizes.length; i++)
				{
                    strSizes[i] = (short) GlobalConst.MAX_NAME;
                }
            }
			
            
            Columnarfile columnarFile = null;
            try {
                columnarFile = new Columnarfile(columnarFileName);
                System.out.println("Columnar File "+ columnarFileName + " exist. Opening the file...");
				//re-opening a columnar file doesn't carry over defined attribute types, so we must reset them
				columnarFile.setColumnTypes( attributeTypes );
            } catch (Exception e) {
                System.out.println("Columnar File " + columnarFileName + " does not exist. Creating a new one...");
                try {
                    columnarFile = new Columnarfile(columnarFileName, numColumns, attributeTypes, strSizes, columnNames);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            
			//go over each record in the input file column-by-column
			//reseting to the start of the input file once each column
			//has been inserted
			//This is not faster than going row-by-row in practice, but
			//requires fewer page reads/writes than a row-by-row approach
            int recordCount = 0;
			for( int i = 0; i < numColumns; i++ )
			{
				System.out.println("Working Column " + i);
				BufferedReader br2 = new BufferedReader(new FileReader(dataFileName));
				//skip first line of column specifiers
				br2.readLine();
				
				while ((line = br2.readLine()) != null)
				{
					// Split the line into values using a delimiter (e.g., space)
					String[] values = line.split("\\s+");
						
					try
					{
						byte [] data;
						switch (attributeTypes[i].attrType)
						{
							case AttrType.attrInteger:
								data = new byte[4];
								Convert.setIntValue(Integer.parseInt(values[i]), 0, data);
								columnarFile.addToColumnarColumn(i, data);
								break;
							case AttrType.attrReal:
								data = new byte[4];
								Convert.setFloValue(Float.parseFloat(values[i]), 0, data);
								columnarFile.addToColumnarColumn(i, data);
								break;
							default:
								data = new byte[GlobalConst.MAX_NAME];
								Convert.setStrValue(values[i], 0, data);
								columnarFile.addToColumnarColumn(i, data);
								break;       
						}
						
						recordCount++;
						
						if(recordCount%5000 == 0)
						{
							System.out.println("Column " + i + " inserted " + recordCount + " records...");
						}
						
					} catch (Exception e) {
						// Handle any errors that occur during insertion
						e.printStackTrace();
					}
				}
				//reset record counter
				recordCount = 0;
				//close and release this buffered reader
				br2.close();
			}
            
            SystemDefs.JavabaseBM.flushAllPages();
            System.out.println("Batch Insert Read Count:- "+PCounter.rcounter);
            System.out.println("Batch Insert Write Count:- "+PCounter.wcounter);
            System.out.println("Tuple Count:- "+ columnarFile.getTupleCnt());
			
			//close the top level buffered reader
			br.close();
		   
        } catch (Exception e) {
            e.printStackTrace();
        }
		
    }
}
