


import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;

import org.somatic.som.SOMatic;
import org.somatic.trainer.Global;

import edu.sdsu.datavis.trispace.tsprep.db.PostgreSQLJDBC;
import edu.sdsu.datavis.trispace.tsprep.io.img.ImageManager;
import edu.sdsu.datavis.trispace.tsprep.io.img.Pixel;
import edu.sdsu.datavis.trispace.tsprep.xform.CSVManager;
import processing.core.PApplet;
import processing.data.Table;
import processing.data.TableRow;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ThesisWorkflow extends PApplet {
	int numBands;
	int numPixels;

	// 0 - Actual Values
	// 1 - Normalized Values
	int dataOption = 1;

	String[] images;
	String[] csvs;
	String[] time;

	String[] cols;
	String[] cols2;
	String[] bands;
	
	String[] perspectives;

//	ArrayList<Integer> xList = new ArrayList<Integer>();
//	ArrayList<Integer> yList = new ArrayList<Integer>();
	ArrayList<Double> xList = new ArrayList<Double>();
	ArrayList<Double> yList = new ArrayList<Double>();
	ArrayList<ArrayList<Integer>> bList = new ArrayList<ArrayList<Integer>>();
	ArrayList<String> uniqueIDList = new ArrayList<String>();

	Pixel[] pixelData;

	Table inputTable;
	Table outputTable;
	Table keyTable;
	Table[] mergeTables;

	public void setup() {
		
		File f = new File("./data/inputCSV");
		String outDir = "./data/inputCSV2";
		
		File f2 = new File("./data/inputCSV/1993_SanElijo.csv");
//		File f = new File("./data/inputImagery");
		
		File f3 = new File("./data3/Non_Normalized/L_AT.csv");
		File f6 = new File("./data3/Non_NormalizedAT_L_SanE.csv");
		File f7 = new File("./data3/Non_NormalizedA_LT_SanE.csv");
		File f8 = new File("./data3/Non_NormalizedT_LA_SanE.csv");
		File f9 = new File("./data3/Non_NormalizedLT_A.csv");
		File f10 = new File("./data3/LA_T.csv");
		
//		ImageManager.performGDAL2XYZ(f,outDir);
		
//		ImageManager.cleanData(f, -9999, "./data/inputCSV/clean/fix",true);
		
		int[] pk = {0,1};
		
//		CSVManager.convertToL_AT("L_A_T", f3, pk);
		
//		CSVManager.fromL_AT2AT_L(f3, f3.getParent() + "AT_L_SanE.csv");
//		CSVManager.fromAT_L2L_AT(f6, f3.getParent() + "L_AT_SanE.csv");
//		CSVManager.fromA_LT2L_AT(f7, f3.getParent() + "L_AT_SanE212.csv");
		
//		CSVManager.fromLA_T2L_AT(f10, f3.getParent() + "L_AT_WOOT.csv");
		
		CSVManager.normalizeCSV(f10, f3.getParent() + "L_AT_WOOT32.csv", "LA_T", 1.0E-16f);
		
		//start
//        long lStartTime = System.nanoTime();
		
//		CSVManager.fromT_LA2L_AT(f8, f3.getParent() + "L_AT_SanE216NEW2.csv");
		
		//end
//        long lEndTime = System.nanoTime();
        
        //time elapsed
//        long output = lEndTime - lStartTime;

//        System.out.println("Process 1 Elapsed time in milliseconds: " + output / 1000000);
        
      //start
//        long lStartTime2 = System.nanoTime();
		
//		CSVManager.fromT_LA2L_AT_OLD(f8, f3.getParent() + "L_AT_SanE216NEW3.csv");
		
		//end
//        long lEndTime2 = System.nanoTime();
        
        //time elapsed
//        long output2 = lEndTime2 - lStartTime2;

//        System.out.println("Process 2 Elapsed time in milliseconds: " + output2 / 1000000);
		
//		File f4 = new File("./data3/inputImagery");
//		ImageManager.performGDAL2XYZ(f4);

//		initialize();

		// Convert TIFs to CSVs for all datasets in data/inputImagery
//		for (int i = 0; i < images.length; i++) {
//			runPython(i, images[i], csvs[i]);
//		}
//
//		// Remove noData values + add column headers
//		fixTables();
//		populateTableArray();
//		convert2PixelClass();
		
//		for (int i = 0; i < perspectives.length; i++) {
//			normalize2Perspective(perspectives[i],dataOption);
//			Trispace.createTrispace(this,"L_AT", "./data/" + perspectives[i] + "_Normalized/L_AT.csv",
//					numPixels, time.length, numBands, "./data/" + perspectives[i] + "_Normalized/output_TS", false, true, true);
//			println("Trispace created for " + perspectives[i]);
//		}
//		
//		normalize2Perspective(perspectives[0],0);
//		Trispace.createTrispace(this,"L_AT", "./data/Non_Normalized/L_AT.csv",
//				numPixels, time.length, numBands, "./data/Non_Normalized/output_TS", false, true, true);
		
		String normalization = "AT_L";

//		int xSize = 5;
////		// change these file paths here if you want to test somatic locally
		String filePath = new File("").getAbsolutePath();
//		File file = new File(filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticOut");
//		if (!file.exists()) {
//			file.mkdir();
//		}
//		File file2 = new File(filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticOut/" + targetData);
//		if (!file2.exists()) {
//			file2.mkdir();
//		}
		
		int [] somSizes = new int[6];
		somSizes[0] = 36;
		somSizes[1] = 5;
		somSizes[2] = 5;
		somSizes[3] = 87;
		somSizes[4] = 61;
		somSizes[5] = 5;
		
		perspectives = new String[6];
		perspectives[0] = "L_AT";
		perspectives[1] = "A_LT";
		perspectives[2] = "T_LA";
		perspectives[3] = "LA_T";
		perspectives[4] = "LT_A";
		perspectives[5] = "AT_L";
		
//		for (int i = 0; i < perspectives.length; i++) {
//			normalization = perspectives[5];
//			int k = 0;
//			 Euclidean Distance Similarity
//			for (int k = 0; k < perspectives.length; k++) {
//				runSOM(filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticIn/" + perspectives[k] + ".dat",
//						filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticOut/" + perspectives[k], somSizes[k], somSizes[k],
//						(float) 0.04, somSizes[k], 10000, 1, 1, true, 2);
//			}
//		}
			
			// If using Cosine Similarity
//			for (int k = 0; k < perspectives.length; k++) {
//				if (i == 3) {
//					if (k != 4) {
//						runSOM(filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticIn/" + perspectives[k] + ".dat",
//								filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticOut/" + perspectives[k], somSizes[k], somSizes[k],
//								(float) 0.04, somSizes[k], 10000, 2, 1, true, 2);
//					}						
//				} else if (i == 4) {
//					if (k != 1 && k != 3 && k != 5) {
//						runSOM(filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticIn/" + perspectives[k] + ".dat",
//								filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticOut/" + perspectives[k], somSizes[k], somSizes[k],
//								(float) 0.04, somSizes[k], 10000, 2, 1, true, 2);
//					}
//				} else {
//					runSOM(filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticIn/" + perspectives[k] + ".dat",
//							filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticOut/" + perspectives[k], somSizes[k], somSizes[k],
//							(float) 0.04, somSizes[k], 10000, 2, 1, true, 2);
//				}
//			}				
		
		
//		normalization = perspectives[4];
//		int k = 1;
//		runSOM(filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticIn/" + perspectives[k] + ".dat",
//				filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticOut/" + perspectives[k], somSizes[k], somSizes[k],
//				(float) 0.04, somSizes[k], 10000, 2, 1, true, 2);	
		
//		runSOM(filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticIn/" + targetData + ".dat",
//				filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticOut/" + targetData, xSize, xSize,
//				(float) 0.04, xSize, 10000, 2, 6, true, 2);		

		int kMeansVal = 20;
		
//		for (int i = 0; i < perspectives.length; i++) {
//			normalization = perspectives[i];
//			for (int j = 1; j < perspectives.length; j++) {
//				String targetData = perspectives[0];
//				File oldFile1 = new File(filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticOut/" + targetData + "/trainedSom.cod");
//				File newFile1 = new File(filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticOut/" + targetData + "/" + targetData + "image.cod");
//				if(oldFile1.renameTo(newFile1)){
//					System.out.println("Rename succesful");
//				}else{
//					System.out.println("Rename failed");
//				}
//				File oldFile2 = new File(filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticIn/" + targetData + ".dat");
//				File newFile2 = new File(filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticIn/" + targetData + "image.dat");
//				if(oldFile2.renameTo(newFile2)){
//					System.out.println("Rename succesful");
//				}else{
//					System.out.println("Rename failed");
//				}
//								
//				Som som = new Som("data/" + normalization + "_Normalized/output_TS/SOMaticIn/" + targetData + "image.dat",
//									"data/" + normalization + "_Normalized/output_TS/SOMaticOut/" + targetData + "/" + targetData + "image.cod",
//									targetData, 0, "data/" + normalization + "_Normalized/output_TS/SOMaticOut/" + targetData + "/" + targetData + "iv2bmu.csv",
//									"data/" + normalization + "_Normalized/output_TS/SOMaticOut/" + targetData + "/" + targetData + "bmu2iv.csv",
//									"data/" + normalization + "_Normalized/output_TS/SOMaticOut/" + targetData + "/" + targetData + "SSE.csv", 
//									kMeansVal);
//				
//				println("normalization: " + normalization);
//				println("targetData: " + targetData);
//				println("Complete");
//			}
//		}
		
//		File oldFile1 = new File(filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticOut/" + targetData + "/trainedSom.cod");
//		File newFile1 = new File(filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticOut/" + targetData + "/" + targetData + "image.cod");
//		if(oldFile1.renameTo(newFile1)){
//			System.out.println("Rename succesful");
//		}else{
//			System.out.println("Rename failed");
//		}
//		File oldFile2 = new File(filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticIn/" + targetData + ".dat");
//		File newFile2 = new File(filePath + "/data/" + normalization + "_Normalized/output_TS/SOMaticIn/" + targetData + "image.dat");
//		if(oldFile2.renameTo(newFile2)){
//			System.out.println("Rename succesful");
//		}else{
//			System.out.println("Rename failed");
//		}
//		
//		
		Som som = new Som("data/" + normalization + "_Normalized/output_TS/SOMaticIn/" + targetData + "image.dat",
							"data/" + normalization + "_Normalized/output_TS/SOMaticOut/" + targetData + "/" + targetData + "image.cod",
							targetData, 0, "data/" + normalization + "_Normalized/output_TS/SOMaticOut/" + targetData + "/iv2bmu.csv",
							"data/" + normalization + "_Normalized/output_TS/SOMaticOut/" + targetData + "/bmu2iv.csv");

		String url = "jdbc:postgresql://localhost:5432/schempp17";
		String user = "schempp_admin";
		String pw = "rectre43";
		PostgreSQLJDBC db = null;
		try {
			db = new PostgreSQLJDBC(url,user,pw);
//			createTest(db);
//			insertionTest(db);
			
//			db.createCentroidsTable();
			String fileP = "./data/loci_key/loci_key.csv";
//			try {
//				db.insertCentroidCoordsFromCSV(fileP);
//			} catch (IOException e) {
//				e.printStackTrace();
//				System.out.println("CANNOT FIND FILE OR SOME ISSUE");
//			}
//			
//			try {
//				db.createAttrAliasTable();
//				db.createLocusAliasTable();
//				db.createTimeAliasTable();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
			
//			String fileP = "./data/Non_Normalized/L_AT.csv";
//			try {
//				db.createTSTable("L_AT", fileP);
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//			
//			fileP = "./data";
//			try {
//				db.insertTSFromCSV("L_AT", fileP);
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
			
//			String fileP2 = "./data/Non_Normalized/output_TS/transformations/AT_L.csv";
//			try {
//				db.createTSTable("AT_L", fileP2);
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
			
//			System.out.println(Integer.MAX_VALUE);
//			db.queryTSObject(1, "LT_A", 0);
//			db.queryL_ATObject(1, 6);
//			db.queryLT_AObject(1, 3, 0);
//			db.queryLT_AObject(1, 3, 6);
//			db.queryLA_TObject(1, 1, 0);
//			db.queryLA_TObject(1, 1, 1);
//			db.queryLA_TObject(1, 2, 0);
//			db.queryLA_TObject(1, 3, 0);
//			db.queryLA_TObject(1, 6, 6);
//			db.queryT_LAObject(1, 0);
//			db.queryA_LTObject(1, 0);
//			db.queryAT_LObject(6, 3, 6);
//			String[] colIds = db.getTableColumnIds(tableName);
//			String[] colTypes = db.getTableColumnTypes(tableName);
//			
//			for (int i = 0; i < colIds.length; i++) {
//				System.out.println(colIds[i] + " - " + colTypes[i]);
//			}
			
			
			
//			String sql = "CREATE TABLE COMPANY " +
//		            "(ID INT PRIMARY KEY     NOT NULL," +
//		            " NAME           TEXT    NOT NULL, " +
//		            " AGE            INT     NOT NULL, " +
//		            " ADDRESS        CHAR(50), " +
//		            " SALARY         REAL)";
//			
//			db.executeSQL(sql);
			
			
			db.disconnect();
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		System.exit(0);
	}
//		}
//	}
	
	void insertionTest(PostgreSQLJDBC pgsql) throws SQLException {
		String tableName = "final_project.COMPANY";
		
//		String[][] tableData = new String[5][4];
//		String[] data0 = {"1","2","3","4"};
//		String[] data1 = {"'Paul'", "'Allen'", "'Teddy'", "'Mark'"};
//		String[] data2 = {"32","25","23","25"};
//		String[] data3 = {"'California'", "'Texas'", "'Norway'", "'Rich-Mond'"};
//		String[] data4 = {"20000.00","15000.00","20000.00","65000.00"};
//		tableData[4] = data4;
		
		String[][] tableData = new String[4][5];
		String[] data0 = {"1","'Paul'","32","'California'","20000.00"};
		String[] data1 = {"2", "'Allen'", "25", "'Texas'", "15000.00"};		
		String[] data2 = {"3", "'Teddy'","23","'Norway'","20000.00"};		
		String[] data3 = {"4", "'Mark'", "25", "'Rich-Mond'","65000.00"};		
		tableData[0] = data0;
		tableData[1] = data1;
		tableData[2] = data2;
		tableData[3] = data3;


		
//		for (int i = 0; i < tableData.length; i++) {
//			pgsql.insertIntoTable(tableName, tableData[i]);
//		}
		pgsql.insertIntoTable(tableName, tableData);
	}
	
	void createTest(PostgreSQLJDBC pgsql) throws SQLException {
		String tableName = "final_project.COMPANY";
		String[] tableCols = new String[5];
		tableCols[0] = "(ID INT PRIMARY KEY NOT NULL";
		tableCols[1] = "NAME TEXT NOT NULL";
		tableCols[2] = "AGE INT NOT NULL";
		tableCols[3] = "ADDRESS CHAR(50)";
		tableCols[4] = "SALARY REAL)";
		pgsql.createTable(tableName, tableCols);
	}

	void initialize() {
		perspectives = new String[6];
		perspectives[0] = "L_AT";
		perspectives[1] = "A_LT";
		perspectives[2] = "T_LA";
		perspectives[3] = "LA_T";
		perspectives[4] = "LT_A";
		perspectives[5] = "AT_L";
		
		// reads inputImagery directory to capture TIF files
		File dir = new File("./data/inputImagery");
		File[] files = dir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".tif");
			}
		});

		// Initialize images array
		String fPath = null;
		images = new String[files.length];
		time = new String[files.length];
		csvs = new String[files.length];
		int counter = 0;

		// Populate images + csvs arrays
		for (File tifFile : files) {
			images[counter] = tifFile.getName();
			csvs[counter++] = tifFile.getName().substring(0, tifFile.getName().length() - 4) + ".csv";
			fPath = tifFile.getPath();
		}

		// Get number of bands from data
		numBands = getNumBands(fPath);

		// Initialize bands, cols, cols2, time arrays
		bands = new String[numBands];
		cols = new String[numBands + 1];
		cols2 = new String[numBands + 2];
		cols[0] = "UID";
		cols2[0] = "X";
		cols2[1] = "Y";
		for (int i = 0; i < numBands; i++) {
			bands[i] = String.valueOf((i + 1));
			cols[i + 1] = String.valueOf((i + 1));
			cols2[i + 2] = String.valueOf((i + 1));
			bList.add(new ArrayList<Integer>());
		}
		for (int i = 0; i < time.length; i++) {
//			println(images[i]);
			String[] parts = images[i].split("_");
//			println(parts[0]);
			time[i] = parts[0];
//			time[i] = "t" + (i + 1);
		}
	}

	// GDAL Method to get the number of bands from input TIF files via
	// rasterCount.py
	// Returns -1 if file cannot be read
	int getNumBands(String path) {
		int bands = -1;
		String argument = path;
		String filePath = ".";
		String argScript1 = filePath + "/lib/rasterCount.py";
		String[] cmd = { "python", argScript1, argument, };

		try {
			Runtime rt = Runtime.getRuntime();
			Process pr = rt.exec(cmd);
			BufferedReader bfr = new BufferedReader(new InputStreamReader(pr.getInputStream()));
			String line = "";
			while ((line = bfr.readLine()) != null) {
				bands = Integer.parseInt(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			bands = -1;
		}
		return (bands);
	}

	// Performs the gdal2xyz.py script with dynamic parameters
	// Usage: gdal2xyz.py [-skip factor] [-srcwin xoff yoff width height]
	// [-band b] [-csv] srcfile [dstfile]
	void runPython(int num, String imagery, String csv) {
		println("Running gdal2xyz.py script on " + imagery);
		String filePath = ".";
		String argBand = "-band";
		String argCSV = "-csv";
		String argsrc = filePath + "/data/inputImagery/" + imagery;
		String argdest = filePath + "/data/inputCSV/" + csv;
		String argScript = filePath + "/lib/gdal2xyz.py";
		int numBands2 = this.numBands * 2;

		// String array to hold all arguments
		String[] cmd = new String[5 + numBands2];

		// Call python
		cmd[0] = "python";

		// Call gdal2xys.py
		cmd[1] = argScript;

		// [-band b] argument(s)
		int counter = 1;
		int counter2 = 2;
		for (int i = 0; i < numBands2; i++) {
			if (i == 0 || i % 2 == 0) {
				cmd[i + 2] = argBand;
			} else {
				cmd[i + 2] = bands[i - counter++];
			}
			counter2++;
		}
		// [-csv] argument
		cmd[counter2++] = argCSV;
		// srcfile argument
		cmd[counter2++] = argsrc;
		// [dstfile] argument
		cmd[counter2] = argdest;

		// Execute .py script
		try {
			Runtime rt = Runtime.getRuntime();
			Process pr = rt.exec(cmd);
			pr.waitFor();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// Remove noData pixels + add column headers
	void fixTables() {
		for (int k = 0; k < csvs.length; k++) {
			String csvName = "./data/inputCSV/" + csvs[k];
			inputTable = loadTable(csvName);
			for (TableRow row : inputTable.rows()) {
				int test = row.getInt(3);
				if (test >= 0) {
					xList.add(row.getDouble(0));
					yList.add(row.getDouble(1));
					for (int i = 0; i < bList.size(); i++) {
						bList.get(i).add(row.getInt(i + 2));
					}
				}
			}
			if (k == 0) {
				keyTable = new Table();
				keyTable.addColumn("UID");
				keyTable.addColumn("X");
				keyTable.addColumn("Y");
				numPixels = xList.size();
				pixelData = new Pixel[numPixels];
				for (int j = 0; j < numPixels; j++) {
					TableRow newRow1 = keyTable.addRow();
					int pixVal = j + 1;
					uniqueIDList.add("P" + pixVal);
					newRow1.setString("UID", "P" + pixVal);
					newRow1.setDouble("X", xList.get(j));
					newRow1.setDouble("Y", yList.get(j));
				}
				saveTable(keyTable, "./data/loci_key/loci_key.csv");
			}

			outputTable = new Table();
			for (int j = 0; j < cols.length; j++) {
				outputTable.addColumn(cols[j]);
			}
			for (int i = 0; i < numPixels; i++) {
				TableRow newRow = outputTable.addRow();
				newRow.setString("UID", uniqueIDList.get(i));
				for (int j = 0; j < numBands; j++) {
					newRow.setInt(bands[j], bList.get(j).get(i));
				}
			}

			String newCSV = "./data/fixed_CSV/" + csvs[k];
			saveTable(outputTable, newCSV);

			// Reset Variables for next CSV
			xList = new ArrayList<Double>();
			yList = new ArrayList<Double>();
			bList = new ArrayList<ArrayList<Integer>>();

			for (int i = 0; i < numBands; i++) {
				bList.add(new ArrayList<Integer>());
			}

			println(csvs[k] + " done");
		}
		// System.exit(0);
	}

	// Populate mergeTables array
	void populateTableArray() {
		mergeTables = new Table[time.length];
		for (int i = 0; i < csvs.length; i++) {
			String dataset = "./data/fixed_CSV/" + csvs[i];
			Table datasetTable = loadTable(dataset, "header");
			mergeTables[i] = datasetTable;
		}
		println("Table Array Populated");
	}

	// Combine CSV files into L_AT perspective Table
	void convert2L_AT_Table(int option, String type) {
		Table tableL_AT = new Table();
		TableRow addRow = tableL_AT.addRow();
		addRow.setString(0, "L_AT");
		int colCount = 1;
		for (int i = 0; i < this.numBands; i++) {
			for (int j = 0; j < time.length; j++) {
				addRow.setString(colCount++, "Band" + (i + 1));
			}
		}

		addRow = tableL_AT.addRow();
		colCount = 1;
		for (int i = 0; i < this.numBands; i++) {
			for (int j = 0; j < time.length; j++) {
				addRow.setString(colCount++, time[j]);
			}
		}

		for (int i = 0; i < pixelData.length; i++) {
			TableRow newRow = tableL_AT.addRow();
			newRow.setString(0, pixelData[i].name);
			int rowCount = 1;
			for (int k = 0; k < pixelData[i].bandData[0].length; k++) {
				for (int j = 0; j < pixelData[i].bandData.length; j++) {
					if (option == 0) {
						newRow.setInt(rowCount++, pixelData[i].bandData[j][k]);
					} else if (option == 1) {
						newRow.setFloat(rowCount++, pixelData[i].bandDataNormalized[j][k]);
					} else {
						println("Set option to either 0 or 1");
					}

				}
			}
		}
		if (option == 1) {
			saveTable(tableL_AT, "./data/" + type + "_Normalized/L_AT.csv");
		} else {
			saveTable(tableL_AT, "./data/Non_Normalized/L_AT.csv");
		}
		
		println("L_AT Table Saved");
	}

	// Combine CSV files into A_LT perspective Table
	void convert2A_LT_Table(int option) {
		Table tableA_LT = new Table();
		TableRow addRow = tableA_LT.addRow();
		addRow.setString(0, "A_LT");
		int colCount = 1;
		for (int j = 0; j < pixelData.length; j++) {
			for (int i = 0; i < time.length; i++) {
				addRow.setString(colCount++, pixelData[j].name);
			}
		}

		addRow = tableA_LT.addRow();
		colCount = 1;
		for (int j = 0; j < pixelData.length; j++) {
			for (int i = 0; i < time.length; i++) {
				addRow.setString(colCount++, time[i]);
			}

		}

		for (int i = 0; i < numBands; i++) {
			TableRow newRow = tableA_LT.addRow();
			newRow.setString(0, "Band" + (i + 1));
			int rowCount = 1;
			for (int k = 0; k < pixelData.length; k++) {
				for (int j = 0; j < pixelData[k].bandData.length; j++) {
					if (option == 0) {
						newRow.setInt(rowCount++, pixelData[k].bandData[j][i]);
					} else if (option == 1) {
						newRow.setFloat(rowCount++, pixelData[k].bandDataNormalized[j][i]);
					} else {
						println("Set option to either 0 or 1");
					}
				}
			}
		}
		saveTable(tableA_LT, "./data/A_LT/A_LT.csv");
		println("A_LT Table Saved");
	}

	// Combine CSV files into T_LA perspective Table
	void convert2T_LA_Table(int option) {
		Table tableT_LA = new Table();
		TableRow addRow = tableT_LA.addRow();
		addRow.setString(0, "T_LA");
		int colCount = 1;
		for (int j = 0; j < pixelData.length; j++) {
			for (int i = 0; i < this.numBands; i++) {
				addRow.setString(colCount++, pixelData[j].name);
			}
		}

		addRow = tableT_LA.addRow();
		colCount = 1;
		for (int j = 0; j < pixelData.length; j++) {
			for (int i = 0; i < this.numBands; i++) {
				addRow.setString(colCount++, "Band" + (i + 1));
			}
		}

		for (int i = 0; i < time.length; i++) {
			TableRow newRow = tableT_LA.addRow();
			newRow.setString(0, time[i]);
			int rowCount = 1;
			for (int k = 0; k < pixelData.length; k++) {
				for (int j = 0; j < pixelData[k].bandData[i].length; j++) {
					if (option == 0) {
						newRow.setInt(rowCount++, pixelData[k].bandData[i][j]);
					} else if (option == 1) {
						newRow.setFloat(rowCount++, pixelData[k].bandDataNormalized[i][j]);
					} else {
						println("Set option to either 0 or 1");
					}

				}
			}
		}

		saveTable(tableT_LA, "./data/T_LA/T_LA.csv");
		println("T_LA Table Saved");
	}

	// Combine CSV files into LA_T perspective Table
	void convert2LA_T_Table(int option) {
		Table tableLA_T = new Table();
		tableLA_T.addColumn("Loci");
		tableLA_T.addColumn("Attribute");
		for (int i = 0; i < time.length; i++) {
			tableLA_T.addColumn(time[i]);
		}

		for (int i = 0; i < pixelData.length; i++) {
			for (int k = 0; k < pixelData[i].bandData[0].length; k++) {
				TableRow newRow = tableLA_T.addRow();
				newRow.setString("Loci", pixelData[i].name);
				newRow.setString("Attribute", "Band" + (k + 1));
				for (int j = 0; j < time.length; j++) {
					if (option == 0) {
						newRow.setInt(time[j], pixelData[i].bandData[j][k]);
					} else if (option == 1) {
						newRow.setFloat(time[j], pixelData[i].bandDataNormalized[j][k]);
					} else {
						println("Set option to either 0 or 1");
					}

				}
			}
			// println(pixelData[i].name);
		}
		saveTable(tableLA_T, "./data/LA_T/LA_T.csv");
		println("LA_T Table Saved");
	}

	// Combine CSV files into LT_A perspective Table
	void convert2LT_A_Table(int option) {
		Table tableLT_A = new Table();
		TableRow addRow = tableLT_A.addRow();
		addRow.setString(0, "LT_A");
		for (int i = 0; i < numBands; i++) {
			addRow.setString(i + 2, "Bands" + (i + 1));
		}

		for (int i = 0; i < pixelData.length; i++) {
			for (int j = 0; j < time.length; j++) {
				addRow = tableLT_A.addRow();
				addRow.setString(0, pixelData[i].name);
				addRow.setString(1, time[j]);
				int rowCount = 2;
				for (int k = 0; k < numBands; k++) {
					if (option == 0) {
						addRow.setInt(rowCount++, pixelData[i].bandData[j][k]);
					} else if (option == 1) {
						addRow.setFloat(rowCount++, pixelData[i].bandDataNormalized[j][k]);
					} else {
						println("Set option to either 0 or 1");
					}
				}
			}
		}

		saveTable(tableLT_A, "./data/LT_A/LT_A.csv");
		println("LT_A Table Saved");
	}

	// Combine CSV files into AT_L perspective Table
	void convert2AT_L_Table(int option) {
		Table tableAT_L = new Table();
		TableRow addRow = tableAT_L.addRow();
		addRow.setString(0, "AT_L");
		for (int i = 0; i < pixelData.length; i++) {
			addRow.setString(i + 2, pixelData[i].name);
		}

		for (int i = 0; i < numBands; i++) {
			for (int j = 0; j < time.length; j++) {
				addRow = tableAT_L.addRow();
				addRow.setString(0, "Band" + (i + 1));
				addRow.setString(1, time[j]);
				int rowCount = 2;
				for (int k = 0; k < pixelData.length; k++) {
					if (option == 0) {
						addRow.setInt(rowCount++, pixelData[k].bandData[j][i]);
					} else if (option == 1) {
						addRow.setFloat(rowCount++, pixelData[k].bandDataNormalized[j][i]);
					} else {
						println("Set option to either 0 or 1");
					}
				}
			}
		}

		saveTable(tableAT_L, "./data/AT_L/AT_L.csv");
		println("AT_L Table Saved");
	}

	// Populate L_AT Normalized Data
	void convert2L_AT() {
		ArrayList<ArrayList<Integer>> tempData = new ArrayList<ArrayList<Integer>>();
		ArrayList<ArrayList<Float>> normData = new ArrayList<ArrayList<Float>>();

		for (int i = 0; i < pixelData.length; i++) {
			ArrayList<Integer> rowData = new ArrayList<Integer>();
			pixelData[i].clearNormData();
			for (int k = 0; k < pixelData[i].bandData[0].length; k++) {
				for (int j = 0; j < pixelData[i].bandData.length; j++) {
					rowData.add(pixelData[i].bandData[j][k]);
				}
			}
			tempData.add(rowData);
		}

		for (int i = 0; i < tempData.size(); i++) {
			ArrayList<Float> rowData = new ArrayList<Float>();
			int min = Collections.min(tempData.get(i));
			int max = Collections.max(tempData.get(i));
			for (int j = 0; j < tempData.get(i).size(); j++) {
				float value;
				if (min == max) {
					value = (float) 1.0;
				} else {
					value = map(tempData.get(i).get(j), min, max, 0, 1);
				}				
				rowData.add(value);	
			}
			normData.add(rowData);
			
			for (int j = 0; j < normData.get(i).size(); j++) {
				int yrCount = j % 3;
				int bandCount = j / 3;
				pixelData[i].setNormData(yrCount, bandCount, normData.get(i).get(j));
			}			
		}
	}
	
	// Populate A_LT Normalized Data
	void convert2A_LT() {
		ArrayList<ArrayList<Integer>> tempData = new ArrayList<ArrayList<Integer>>();
		ArrayList<ArrayList<Float>> normData = new ArrayList<ArrayList<Float>>();
		
		for (int i = 0; i < numBands; i++) {
			ArrayList<Integer> rowData = new ArrayList<Integer>();
			for (int k = 0; k < pixelData.length; k++) {
				pixelData[i].clearNormData();
				for (int j = 0; j < pixelData[k].bandData.length; j++) {
					rowData.add(pixelData[k].bandData[j][i]);
				}
			}
			tempData.add(rowData);
		}
		
		for (int i = 0; i < tempData.size(); i++) {
			ArrayList<Float> rowData = new ArrayList<Float>();
			int min = Collections.min(tempData.get(i));
			int max = Collections.max(tempData.get(i));
			for (int j = 0; j < tempData.get(i).size(); j++) {
				float value;
				if (min == max) {
					value = (float) 1.0;
				} else {
					value = map(tempData.get(i).get(j), min, max, 0, 1);
				}				
				rowData.add(value);	
			}
			normData.add(rowData);
			
			for (int j = 0; j < normData.get(i).size(); j++) {
				int yrCount = j % 3;
				int pixelCount = j / 3;
				pixelData[pixelCount].setNormData(yrCount, i, normData.get(i).get(j));
			}
		}		
	}

	// Populate T_LA Normalized Data
	void convert2T_LA() {
		ArrayList<ArrayList<Integer>> tempData = new ArrayList<ArrayList<Integer>>();
		ArrayList<ArrayList<Float>> normData = new ArrayList<ArrayList<Float>>();
		
		for (int i = 0; i < time.length; i++) {
			ArrayList<Integer> rowData = new ArrayList<Integer>();
			for (int k = 0; k < pixelData.length; k++) {
				pixelData[k].clearNormData();
				for (int j = 0; j < pixelData[k].bandData[i].length; j++) {
					rowData.add(pixelData[k].bandData[i][j]);
				}
			}
			tempData.add(rowData);
		}
		
		for (int i = 0; i < tempData.size(); i++) {
			ArrayList<Float> rowData = new ArrayList<Float>();
			int min = Collections.min(tempData.get(i));
			int max = Collections.max(tempData.get(i));
			for (int j = 0; j < tempData.get(i).size(); j++) {
				float value;
				if (min == max) {
					value = (float) 1.0;
				} else {
					value = map(tempData.get(i).get(j), min, max, 0, 1);
				}				
				rowData.add(value);	
			}
			normData.add(rowData);
			
			for (int j = 0; j < normData.get(i).size(); j++) {
				int bandCount = j % 6;
				int pixelCount = j / 6;
				pixelData[pixelCount].setNormData(i, bandCount, normData.get(i).get(j));
			}
		}		
	}

	// Populate LA_T Normalized Data
	void convert2LA_T() {
		ArrayList<ArrayList<Integer>> tempData = new ArrayList<ArrayList<Integer>>();
		ArrayList<ArrayList<Float>> normData = new ArrayList<ArrayList<Float>>();

		for (int i = 0; i < pixelData.length; i++) {
			pixelData[i].clearNormData();
			for (int k = 0; k < pixelData[i].bandData[0].length; k++) {
				ArrayList<Integer> rowData = new ArrayList<Integer>();
				for (int j = 0; j < time.length; j++) {
					rowData.add(pixelData[i].bandData[j][k]);
				}
				tempData.add(rowData);
			}
		}
		
		for (int i = 0; i < tempData.size(); i++) {
			ArrayList<Float> rowData = new ArrayList<Float>();
			int min = Collections.min(tempData.get(i));
			int max = Collections.max(tempData.get(i));
			for (int j = 0; j < tempData.get(i).size(); j++) {
				float value;
				if (min == max) {
					value = (float) 1.0;
				} else {
					value = map(tempData.get(i).get(j), min, max, 0, 1);
				}				
				rowData.add(value);				
			}
			normData.add(rowData);
			
			for (int j = 0; j < normData.get(i).size(); j++) {
				int bandCount = i % 6;
				int pixelCount = i / 6;
				pixelData[pixelCount].setNormData(j, bandCount, normData.get(i).get(j));
			}
		}
	}

	// Populate LT_A Normalized Data
	void convert2LT_A() {
		ArrayList<ArrayList<Integer>> tempData = new ArrayList<ArrayList<Integer>>();
		ArrayList<ArrayList<Float>> normData = new ArrayList<ArrayList<Float>>();

		for (int i = 0; i < pixelData.length; i++) {
			pixelData[i].clearNormData();
			for (int j = 0; j < time.length; j++) {
				ArrayList<Integer> rowData = new ArrayList<Integer>();
				for (int k = 0; k < numBands; k++) {
					rowData.add(pixelData[i].bandData[j][k]);
				}
				tempData.add(rowData);
			}
		}
		
		for (int i = 0; i < tempData.size(); i++) {
			ArrayList<Float> rowData = new ArrayList<Float>();
			int min = Collections.min(tempData.get(i));
			int max = Collections.max(tempData.get(i));
			for (int j = 0; j < tempData.get(i).size(); j++) {
				float value;
				if (min == max) {
					value = (float) 1.0;
				} else {
					value = map(tempData.get(i).get(j), min, max, 0, 1);
				}				
				rowData.add(value);				
			}
			normData.add(rowData);
			
			for (int j = 0; j < normData.get(i).size(); j++) {
				int timeCount = i % 3;
				int pixelCount = i / 3;
				pixelData[pixelCount].setNormData(timeCount, j, normData.get(i).get(j));
			}
		}
	}

	// Populate AT_L Normalized Data
	void convert2AT_L() {
		ArrayList<ArrayList<Integer>> tempData = new ArrayList<ArrayList<Integer>>();
		ArrayList<ArrayList<Float>> normData = new ArrayList<ArrayList<Float>>();
		
		for (int i = 0; i < numBands; i++) {
			for (int j = 0; j < time.length; j++) {
				ArrayList<Integer> rowData = new ArrayList<Integer>();
				for (int k = 0; k < pixelData.length; k++) {
					pixelData[k].clearNormData();
					rowData.add(pixelData[k].bandData[j][i]);
				}
				tempData.add(rowData);
			}
		}
		
		for (int i = 0; i < tempData.size(); i++) {
			ArrayList<Float> rowData = new ArrayList<Float>();
			int min = Collections.min(tempData.get(i));
			int max = Collections.max(tempData.get(i));
			for (int j = 0; j < tempData.get(i).size(); j++) {
				float value;
				if (min == max) {
					value = (float) 1.0;
				} else {
					value = map(tempData.get(i).get(j), min, max, 0, 1);
				}				
				rowData.add(value);				
			}
			normData.add(rowData);
			
			for (int j = 0; j < normData.get(i).size(); j++) {
				int timeCount = i % 3;
				int bandCount = i / 3;
				pixelData[j].setNormData(timeCount, bandCount, normData.get(i).get(j));
			}
		}
	}

	// Read all CSV files into memory - pixelData array
	void convert2PixelClass() {
		int count;
		for (int i = 0; i < mergeTables.length; i++) {
			count = 0;
			for (TableRow rows : mergeTables[i].rows()) {
				if (i == 0) {
					String name = "P" + (count + 1);
					Pixel newPix = new Pixel(this, name, time.length, numBands);
					pixelData[count] = newPix;
				}
				for (int j = 0; j < bands.length; j++) {
					int newData = rows.getInt(bands[j]);
					pixelData[count].setData(i, j, newData);
				}
				count++;
			}
		}
		println("Pixel Data Populated");
	}

	void normalize2Perspective(String type, int option) {
		if (type == "L_AT") {
			convert2L_AT();
			convert2L_AT_Table(option, type);
			println("Normalized to " + type);
		} else if (type == "A_LT") {
			convert2A_LT();
			convert2L_AT_Table(option, type);
			println("Normalized to " + type);
		} else if (type == "T_LA") {
			convert2T_LA();
			convert2L_AT_Table(option, type);
			println("Normalized to " + type);
		} else if (type == "LA_T") {
			convert2LA_T();
			convert2L_AT_Table(option, type);
			println("Normalized to " + type);
		} else if (type == "LT_A") {
			convert2LT_A();
			convert2L_AT_Table(option, type);
			println("Normalized to " + type);
		} else if (type == "AT_L") {
			convert2AT_L();
			convert2L_AT_Table(option, type);
			println("Normalized to " + type);
		} else {
			println("No Known Format");
		}		
	}
	
	static void runSOM(String sominputFilePath, String somoutputFilePath, 
			int neuronNumberX, int neuronNumberY,float alphaValue, int radius, 
			int iterations, int simMeasure, int nThreads, boolean rounding, int scalingFactor) {
		
		SOMatic s = new SOMatic();
		Global g = Global.getInstance();

		// setting of global values
		g.similarityMeasure = simMeasure;
		g.nrOfThreads = nThreads;
		g.rounding = rounding;
		g.scalingFactor = scalingFactor;

		s.setTrainingDataFilePath(new File(sominputFilePath));
		try {
			s.readFile();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// set the normalization method for each attribute (NULL, LINEAR BOOLEAN
		// or ZSCORE)
		for (int i = 0; i < s.getAttributes().length; i++) {
			try {
				s.setNormalizationMethod(s.getAttributes()[i], g.NULL);
			} catch (Exception e) {
			}
		}
		// normalize the training data
		s.normalizeTrainingVectors();

		s.setNumberOfNeuronsX(neuronNumberX);
		s.setNumberOfNeuronsY(neuronNumberY);
		s.setRandomSOMInitialization(true);

		try {
			s.setTopologyOfTheSOM(g.HEXA);
		} catch (Exception e) {
			println("Error with setTopology(); Topology =" + g.HEXA);
		}

		s.initializeSOM();

		try {
			s.setInitialAlphaValue(alphaValue);
			s.setInitialNeighborhoodRadius(radius);
			s.setNumberOfTrainingRuns(iterations);
		} catch (Exception e) {
			e.printStackTrace();
		}
		s.doTraining();

		try {
			s.setInitialAlphaValue(0.04);
			// second stage: half the radius
			s.setInitialNeighborhoodRadius((int) (radius / 2));
			s.setNumberOfTrainingRuns(20000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		s.doTraining();

		try {
			s.setInitialAlphaValue(0.03);
			// third stage: fifth the radius
			s.setInitialNeighborhoodRadius((int) (radius / 5)); 
			s.setNumberOfTrainingRuns(50000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		s.doTraining();

		s.setSOMFilePath(new File(somoutputFilePath));

		println("Now writing trained SOM to file");
		s.writeSomToAFile();
		println("Finished writing SOM file");
//		System.exit(0);
	}

	public static void main(String[] args) {
		PApplet.main("ThesisWorkflow");
	}

}
