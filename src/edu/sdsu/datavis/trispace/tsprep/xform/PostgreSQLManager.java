package edu.sdsu.datavis.trispace.tsprep.xform;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.bytedeco.javacv.FrameRecorder.Exception;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import edu.sdsu.datavis.trispace.tsprep.db.PostgreSQLJDBC;
import edu.sdsu.datavis.trispace.tsprep.dr.SOMaticManager;
import edu.sdsu.datavis.trispace.tsprep.io.img.ImageManager;
import edu.sdsu.datavis.trispace.tsprep.som.InputVector;
import edu.sdsu.datavis.trispace.tsprep.som.Neuron;
import edu.sdsu.datavis.trispace.tsprep.utils.JSONUtil;

/**
 * CSVManager is an abstract class for all Tri-Space
 * conversions that include transformation and formatting methods.
 * 
 * @author      Tim Schempp
 * @version     %I%, %G%
 * @since       1.0
 */
public abstract class PostgreSQLManager {

	private final static String TEMP_POLY_TABLE = "tmp_fishnet";
	final static String[] PERSPECTIVES = {"L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L"};
	final static String[] NORMALIZATIONS = {"Unnormalized", "L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L"};
	final static int BATCH_SIZE = 50000; // constant for batch size
	final static int MAX_COLUMNS = 1598; // constant for max columns in DB

	static class Polygon {
		String uid;
		Coordinate centroid;
		ArrayList<Coordinate> coords;

		Polygon(String uid, Coordinate centroid, ArrayList<Coordinate> polyCoords, boolean closed) {
			this.uid = uid;
			this.centroid = centroid;
			this.coords = polyCoords;
			if (!closed) {
				closePolygon();
			}
		}

		//		Polygon(ArrayList<DoubleCoordinate> polyCoords) {
		//			for (int i = 0; i < polyCoords.size(); i++) {
		//				this.coords.add(new Coordinate(polyCoords.x,polyCoords.y));
		//			}
		//			this.coords = new Coordinate(polyCoords.x,polyCoords.y);
		//		}

		void closePolygon() {
			this.coords.add(this.coords.get(0));
		}
	}

	static class Coordinate {
		String x;
		String y;

		Coordinate(String x, String y) {
			this.x = x;
			this.y = y;
		}

		Coordinate(double x, double y) {
			this.x = x + "";
			this.y = y + "";
		}

		Coordinate(float x, float y) {
			this.x = x + "";
			this.y = y + "";
		}
	}

	static class DoubleCoordinate {
		double x;
		double y;

		DoubleCoordinate(String x, String y) {
			this.x = Double.parseDouble(x);
			this.y = Double.parseDouble(y);
		}

		DoubleCoordinate(Coordinate coord) {
			this.x = Double.parseDouble(coord.x);
			this.y = Double.parseDouble(coord.y);
		}
	}

	static class FloatCoordinate {
		float x;
		float y;

		FloatCoordinate(String x, String y) {
			this.x = Float.parseFloat(x);
			this.y = Float.parseFloat(y);
		}

		FloatCoordinate(Coordinate coord) {
			this.x = Float.parseFloat(coord.x);
			this.y = Float.parseFloat(coord.y);
		}
	}

	//	public static boolean createLocusDictionary(PostgreSQLJDBC db, File[] files, String schema, int[] primaryKey, int headers) {
	//		
	//		String[] tableCols = new String[2];
	//		tableCols[0] = "(ID TEXT PRIMARY KEY NOT NULL";
	//		tableCols[1] = "ALIAS TEXT NOT NULL)";
	//		String table = schema;
	//		table = table + ".locus_key";
	//		try {
	//			db.createTable(table, tableCols);
	//			
	//			BufferedReader br = new BufferedReader(new FileReader(files[0].getPath()));
	//			
	//			while (headers > 0) {
	//				System.out.println("HEADERS");
	//				System.out.println(br.readLine());
	//				headers--;
	//			}
	//			
	//			
	//			String line = "";
	//			int lociCount = 1;
	//			
	//			while ((line = br.readLine()) != null) {
	//				String[] split = line.split(",");
	//				String[] insertData = new String[2];
	//				
	//				insertData[0] = "'l" + lociCount + "'";
	//				
	//				if (primaryKey.length == 1) {
	//					insertData[1] = "'" + split[primaryKey[0]] + "'";
	//				} else if (primaryKey.length > 1) {
	//					insertData[1] = "'" + split[primaryKey[0]] + "'";
	//					for (int i = 1; i < primaryKey.length; i++) {
	//						insertData[1] = insertData[1] + "_" + split[primaryKey[i]];
	//					}
	//				} else {
	//					br.close();
	//					return false;
	//				}
	//				if (!db.insertIntoTable(schema + ".locus_key",insertData)) {
	//					br.close();
	//					return false;
	//				}
	//				lociCount++;
	//			}
	//			
	//			br.close();
	//			return true;
	//		} catch (IOException | SQLException e1) {
	//			// TODO Auto-generated catch block
	//			e1.printStackTrace();
	//			return false;
	//		}
	//	}

	public static boolean createFinalPixelPolyTable(PostgreSQLJDBC db, String schema, boolean batch) {

		// return false if either temp poly or locus (point geometry) table don't exist
		if (!db.tableExists(schema, TEMP_POLY_TABLE) || !db.tableExists(schema,"locus_pt")) {
			return false;
		}

		int tLength = db.getTableLength(schema, "locus_pt");

		// return false if table is emptry
		if (tLength == -1) {
			return false;
		}

		// extract epsg from the locus (point geometry) table
		String epsg = db.getESPG(schema, "locus_pt");
		
		// Set default type to SERIAL
		String idType = "SERIAL";
		
		// If number of objects is less than the range for SMALLSERIAL
		if (tLength > 0 && tLength <= 32767) {			
			// Use SMALLSERIAL instead of SERIAL
			idType = "SMALLSERIAL";
		} else if (tLength > 2147483647) {
			// Use BIGSERIAL instead of SERIAL
			idType = "BIGSERIAL";
		}	
		
		String[] tableCols = new String[3];
		tableCols[0] = "(id " + idType;
		tableCols[1] = "geom geometry(POLYGON," + epsg + ")";		
		tableCols[2] = "PRIMARY KEY(id))";
//		db.createTable(table, tableCols);

//		String[] tableCols = new String[2];
//		tableCols[0] = "(UID TEXT PRIMARY KEY NOT NULL";
//		tableCols[1] = "geom geometry(POLYGON," + epsg + "))";		


		try {
			if (!db.createTable(schema + "." + "locus_poly", tableCols)) {
				return false;
			}
			if (!extractCleanPolygons(db,schema,tLength, batch)) {
				return false;
			}
			return db.executeSQL("DROP TABLE " + schema + "." + TEMP_POLY_TABLE + ";");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	public static boolean polyFromTable(PostgreSQLJDBC db, String schema, String tsElement, String epsg, File f, boolean batch, int batchSize, boolean header, String column, boolean multiPolygon) {

		String polyTable = tsElement + "_poly";
		ArrayList<String> batchInsertion = new ArrayList<String>();
		
		// return false if either temp poly or locus (point geometry) table don't exist
		if (db.tableExists(schema, polyTable)) {
			return false;
		}
		
		// dictionary to hold each object and its coordinates
		Map<String, ArrayList<ArrayList<String>>> xDictionary = new HashMap<String, ArrayList<ArrayList<String>>>();
		Map<String, ArrayList<ArrayList<String>>> yDictionary = new HashMap<String, ArrayList<ArrayList<String>>>();
		Map<String, String> idDictionary = new HashMap<String, String>();
		Map<String, Integer> idxDictionary = new HashMap<String, Integer>();
		
		
		// variable to store each line
		String line = "";
		
		
		try {
			
			// Create BR to read in file
			BufferedReader br = new BufferedReader(new FileReader(f));
			
			// remove header if necessary
			if (header) line = br.readLine();
			
			// iterate thru file
			while ((line = br.readLine()) != null) {
				
//				System.out.println(line);
				
				// split the line data into a String array
				String[] split = line.split(",");
				
				if (!xDictionary.containsKey(split[0])) { // if dictionary doen't have key
					
					// create empty list
					ArrayList<ArrayList<String>> tmpListX = new ArrayList<ArrayList<String>>();				
					ArrayList<String> tmpX = new ArrayList<String>();
					
					tmpListX.add(tmpX);
					
					ArrayList<ArrayList<String>> tmpListY = new ArrayList<ArrayList<String>>();
					ArrayList<String> tmpY = new ArrayList<String>();
					
					tmpListY.add(tmpY);
					
					// add key and value (empty list) to dictionary
					xDictionary.put(split[0], tmpListX);
					yDictionary.put(split[0], tmpListY);
					idxDictionary.put(split[0], 0);
				}
				
				
				int currIdx = idxDictionary.get(split[0]);
				if (split[0].equalsIgnoreCase("HI")) {
					System.out.println("current index1: " + currIdx);
				}
				
				
				String origX = "";
				String recentX = "";
				String origY = "";
				String recentY = "";
				
				if (xDictionary.get(split[0]).get(currIdx).size() > 1) {
					origX = xDictionary.get(split[0]).get(currIdx).get(0);
					origY = yDictionary.get(split[0]).get(currIdx).get(0);
					
					recentX = xDictionary.get(split[0]).get(currIdx).get(xDictionary.get(split[0]).get(currIdx).size() - 1);
					recentY = yDictionary.get(split[0]).get(currIdx).get(yDictionary.get(split[0]).get(currIdx).size() - 1);
					
					if (origX.equals(recentX) && origY.equals(recentY)) {
						currIdx++;
						idxDictionary.put(split[0], currIdx);
//						idxDictionary.set(split[0], new Integer(currIdx));
						ArrayList<String> tmpX = new ArrayList<String>();
						ArrayList<String> tmpY = new ArrayList<String>();
						
						xDictionary.get(split[0]).add(tmpX);
						yDictionary.get(split[0]).add(tmpY);
					}
				}
								
//				if (origX.equals(xDictionary.get(split[0]).get(currIdx).get) && origY.equals(split[2])) {
//					currIdx++;
//					ArrayList<String> tmpX = new ArrayList<String>();
//					ArrayList<String> tmpY = new ArrayList<String>();
//					
//					xDictionary.get(split[0]).add(tmpX);
//					yDictionary.get(split[0]).add(tmpY);
//				} else {
//					
//				}
				
				if (split[0].equalsIgnoreCase("HI")) {
					System.out.println("current index2: " + currIdx);
				}
				
				
				
//				if (xDictionary.get(split[0].get(idxDictionary.get(split[0]))[0])
				
				// append coordinate to dictionaries
				xDictionary.get(split[0]).get(currIdx).add(split[1]);
				yDictionary.get(split[0]).get(currIdx).add(split[2]);
				
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
//		 return false if table is empty
		if (xDictionary.size() == 0) {
			return false;
		}			
		
		// Set default type to SERIAL
		String idType = "SERIAL";
		
		// If number of objects is less than the range for SMALLSERIAL
		if (xDictionary.size() > 0 && xDictionary.size() <= 32767) {			
			// Use SMALLSERIAL instead of SERIAL
			idType = "SMALLSERIAL";
		} else if (xDictionary.size() > 2147483647) {
			// Use BIGSERIAL instead of SERIAL
			idType = "BIGSERIAL";
		}	
		
		String[] tableCols = new String[3];
		tableCols[0] = "(id " + idType;
		if (epsg.equals("")) {
			if (multiPolygon) {
				tableCols[1] = "geom geometry(MULTIPOLYGON)";				
			} else {
				tableCols[1] = "geom geometry(POLYGON)";
			}			
		} else {
			if (multiPolygon) {
				tableCols[1] = "geom geometry(MULTIPOLYGON," + epsg + ")";	
			} else {
				tableCols[1] = "geom geometry(POLYGON," + epsg + ")";	
			}
					
		}
		
		tableCols[2] = "PRIMARY KEY(id))";		


		try {
			
			if (!db.createTable(schema + "." + polyTable, tableCols)) {
				return false;
			}
			
			for (String key : xDictionary.keySet()) {
				String tmpId = db.getObjectColumnValue(schema, tsElement + "_key", column, key, "id");
				idDictionary.put(key, tmpId);
//				System.out.println("Key: " + key + " |Value: " + tmpId);
			}
			
			
			
			if (multiPolygon) {
				for (String key : idDictionary.keySet()) {
					String[] insertData = new String[2];
					insertData[0] = "'" + idDictionary.get(key) + "'";
					String geometry = "ST_GeomFromText('MULTIPOLYGON(((";
					
					boolean firstTime = true;
					
					for (int i = 0; i < xDictionary.get(key).size(); i++) {
//					for (int i = 0; i < 2; i++) {
//						int startIdx = 0;
//						if (firstTime) startIdx = 1; 					
						
						for (int j = 0; j < xDictionary.get(key).get(i).size(); j++) {
							geometry += xDictionary.get(key).get(i).get(j) + " -" + yDictionary.get(key).get(i).get(j);
							if (j < xDictionary.get(key).get(i).size() - 1) {
								geometry += ",";
							} else {
								geometry += ")";
								if (i < xDictionary.get(key).size() - 1) {
									geometry += ",(";									
								}
							}
						}
						
//						geometry += xDictionary.get(key).get(0).get(0) + " " + yDictionary.get(key).get(0).get(0);
//						if (i == xDictionary.get(key).size() - 1) {
//							geometry += ")";
//							
//						}
					}
					
//					for (int i = 1; i < xDictionary.get(key).size(); i++) {
//						geometry += "," + xDictionary.get(key).get(i) + " " + yDictionary.get(key).get(i);
//					}
					
					geometry += "))')";
					
					insertData[1] = geometry;
					
					if (!batch) {
//						db.insertIntoTable(schema + "." + polyTable, insertData);
//						System.out.println("Key: " + key + " |Value: " + tmpId)
					} else {
						batchInsertion.add(db.insertQueryBuilder(schema + "." + polyTable, insertData));
						System.out.println(batchInsertion.get(batchInsertion.size()-1));
					}
				}
			} else {
				for (String key : idDictionary.keySet()) {
					String[] insertData = new String[2];
					insertData[0] = "'" + idDictionary.get(key) + "'";
					String geometry = "ST_GeomFromText('POLYGON((";			
						
					for (int i = 0; i < xDictionary.get(key).get(0).size(); i++) {
						geometry += xDictionary.get(key).get(0).get(i) + " -" + yDictionary.get(key).get(0).get(i);
//						geometry += yDictionary.get(key).get(0).get(i) + " " + xDictionary.get(key).get(0).get(i);
						if (i < xDictionary.get(key).get(0).size() - 1) {
							geometry += ",";
						}
					}
					
					geometry += "))')";
					
					insertData[1] = geometry;
					
					if (!batch) {
						db.insertIntoTable(schema + "." + polyTable, insertData);
					} else {
						batchInsertion.add(db.insertQueryBuilder(schema + "." + polyTable, insertData));
						System.out.println(batchInsertion.get(batchInsertion.size()-1));
					}
				}
			}
			
			
			
			if (batch) {
				String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
				try {
					db.insertIntoTableBatch(schema + "." + polyTable + "_geom", batchInsertionAr, batchSize);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
								
			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	private static boolean extractCleanPolygons(PostgreSQLJDBC db, String schema, int tableLength, boolean batch) throws SQLException {

		if (!batch) {
			for (int i = 0; i < tableLength; i++) {
//				String uid = "l" + (i+1);
				String uid = "" + (i+1);
				if (!db.extractFishnetGeom(schema, uid)) {
					return false;
				}
			}
		} else {
			ArrayList<String> batchInsertion = new ArrayList<String>(); //extractFishnetGeomQuery
			for (int i = 0; i < tableLength; i++) {
//				String uid = "l" + (i+1);
				String uid = "" + (i+1);
				batchInsertion.add(db.extractFishnetGeomQuery(schema,uid));
			}
			String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
			db.insertIntoTableBatch(schema + ".locus_poly", batchInsertionAr);
		}
		
		return true;
	}

	public static boolean createPixelPolyTable(PostgreSQLJDBC db, String schema, File f, String epsg) {

		File targetFile;

		if (f.isDirectory()) {
			File[] children = f.listFiles();
			targetFile = children[0];
		} else {
			targetFile = f;
		}

		String[] tableCols = new String[4];
		tableCols[0] = "(UID TEXT PRIMARY KEY NOT NULL";
		tableCols[1] = "X NUMERIC";
		tableCols[2] = "Y NUMERIC";
		tableCols[3] = "geom geometry(POLYGON," + epsg + "))";		

		try {
			if (!db.createTable(schema + "." + TEMP_POLY_TABLE, tableCols)) {
				return false;
			}

			System.out.println("path: " + targetFile.getPath());

			int numRows = ImageManager.getNumRows(targetFile.getPath());
			int numCols = ImageManager.getNumColumns(targetFile.getPath());

			System.out.println("numRows: " + numRows);
			System.out.println("numCols: " + numCols);

			BufferedReader br = new BufferedReader(new FileReader(targetFile.getPath()));

			ArrayList<Coordinate> row1 = new ArrayList<Coordinate>();
			ArrayList<Coordinate> row2 = new ArrayList<Coordinate>();
			ArrayList<Coordinate> row3 = new ArrayList<Coordinate>();

			String line = "";

			//			System.out.println("COLUMNS: " + numCols);
			//			System.out.println("ROWS: " + numRows);

			// store row 1 coordinates in memory
			for (int i = 0; i < numCols; i++) {
				line = br.readLine();
				String[] split = line.split(",");

				row1.add(new Coordinate(split[0],split[1]));
			}

			// store row 2 coordinates in memory
			for (int i = 0; i < numCols; i++) {
				line = br.readLine();
				String[] split = line.split(",");

				row2.add(new Coordinate(split[0],split[1]));
			}

			int idCounter = 1;

			insertPolyExtentDouble(db,schema,epsg,row1,row2,true,idCounter);
			idCounter = idCounter + numCols;

			// store row 3 coordinates in memory
			for (int i = 0; i < numCols; i++) {
				line = br.readLine();
				String[] split = line.split(",");

				row3.add(new Coordinate(split[0],split[1]));
			}

			int colCounter = 0;

			while ((line = br.readLine()) != null) {
				if (colCounter == 0 || colCounter % numCols == 0) {
					insertPolyExtentDouble(db,schema,epsg,row1,row2,row3,idCounter);
					idCounter = idCounter + numCols;
					row1 = row2;
					row2 = row3;
					row3 = new ArrayList<Coordinate>();
				}

				String[] split = line.split(",");

				row3.add(new Coordinate(split[0],split[1]));
				colCounter++;

			}

			insertPolyExtentDouble(db,schema,epsg,row1,row2,row3,idCounter);
			idCounter = idCounter + numCols;

			insertPolyExtentDouble(db,schema,epsg,row2,row3,false,idCounter);
			idCounter = idCounter + numCols;

			br.close();
			return true;
		} catch (IOException | SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return false;
		}
	}

	public static boolean createPixelPolyTableBatch(PostgreSQLJDBC db, String schema, File f, String epsg) {

		File targetFile;

		if (f.isDirectory()) {
			File[] children = f.listFiles();
			targetFile = children[0];
		} else {
			targetFile = f;
		}

		String[] tableCols = new String[4];
		tableCols[0] = "(UID TEXT PRIMARY KEY NOT NULL";
		tableCols[1] = "X NUMERIC";
		tableCols[2] = "Y NUMERIC";
		tableCols[3] = "geom geometry(POLYGON," + epsg + "))";		

		try {
			if (!db.createTable(schema + "." + TEMP_POLY_TABLE, tableCols)) {
				return false;
			}

			System.out.println("path: " + targetFile.getPath());

			int numRows = ImageManager.getNumRows(targetFile.getPath());
			int numCols = ImageManager.getNumColumns(targetFile.getPath());

			System.out.println("numRows: " + numRows);
			System.out.println("numCols: " + numCols);

			BufferedReader br = new BufferedReader(new FileReader(targetFile.getPath()));

			ArrayList<Coordinate> row1 = new ArrayList<Coordinate>();
			ArrayList<Coordinate> row2 = new ArrayList<Coordinate>();
			ArrayList<Coordinate> row3 = new ArrayList<Coordinate>();

			String line = "";

			//			System.out.println("COLUMNS: " + numCols);
			//			System.out.println("ROWS: " + numRows);

			// store row 1 coordinates in memory
			for (int i = 0; i < numCols; i++) {
				line = br.readLine();
				String[] split = line.split(",");

				row1.add(new Coordinate(split[0],split[1]));
			}

			// store row 2 coordinates in memory
			for (int i = 0; i < numCols; i++) {
				line = br.readLine();
				String[] split = line.split(",");

				row2.add(new Coordinate(split[0],split[1]));
			}

			int idCounter = 1;

			insertPolyExtentDoubleBatch(db,schema,epsg,row1,row2,true,idCounter);
			idCounter = idCounter + numCols;

			// store row 3 coordinates in memory
			for (int i = 0; i < numCols; i++) {
				line = br.readLine();
				String[] split = line.split(",");

				row3.add(new Coordinate(split[0],split[1]));
			}

			int colCounter = 0;

			while ((line = br.readLine()) != null) {
				if (colCounter == 0 || colCounter % numCols == 0) {
					insertPolyExtentDoubleBatch(db,schema,epsg,row1,row2,row3,idCounter);
					idCounter = idCounter + numCols;
					row1 = row2;
					row2 = row3;
					row3 = new ArrayList<Coordinate>();
				}

				String[] split = line.split(",");

				row3.add(new Coordinate(split[0],split[1]));
				colCounter++;

			}

			insertPolyExtentDoubleBatch(db,schema,epsg,row1,row2,row3,idCounter);
			idCounter = idCounter + numCols;

			insertPolyExtentDoubleBatch(db,schema,epsg,row2,row3,false,idCounter);
			idCounter = idCounter + numCols;

			br.close();
			return true;
		} catch (IOException | SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return false;
		}
	}

	public static boolean createPixelPolyTableBatch2(PostgreSQLJDBC db, String schema, File f, String epsg) {

		ArrayList<String> batchInsertion = new ArrayList<String>();
		File targetFile;

		if (f.isDirectory()) {
			File[] children = f.listFiles();
			targetFile = children[0];
		} else {
			targetFile = f;
		}
		
		System.out.println("path: " + targetFile.getPath());

		int numRows = ImageManager.getNumRows(targetFile.getPath());
		int numCols = ImageManager.getNumColumns(targetFile.getPath());

		System.out.println("Number of Rows: " + numRows);
		System.out.println("Number of Columns: " + numCols);
		
		int numPixels = numRows * numCols;
		System.out.println("Number of Pixels: " + numPixels);
		
		String[] tableCols = new String[5];
		
		// Set default type to SERIAL
		String idType = "SERIAL";
		
		// If number of pixels is less than the range for SMALLSERIAL
		if (numPixels > 0 && numPixels <= 32767) {			
			// Use SMALLSERIAL instead of SERIAL
			idType = "SMALLSERIAL";
		} else if (numPixels > 2147483647) {
			// Use BIGSERIAL instead of SERIAL
			idType = "BIGSERIAL";
		}

		
		tableCols[0] = "(id " + idType;
		tableCols[1] = "X NUMERIC";
		tableCols[2] = "Y NUMERIC";
		tableCols[3] = "geom geometry(POLYGON," + epsg + ")";
		tableCols[4] = "PRIMARY KEY(id))";		

		try {
			if (!db.createTable(schema + "." + TEMP_POLY_TABLE, tableCols)) {
				return false;
			}

			

			BufferedReader br = new BufferedReader(new FileReader(targetFile.getPath()));

			ArrayList<Coordinate> row1 = new ArrayList<Coordinate>();
			ArrayList<Coordinate> row2 = new ArrayList<Coordinate>();
			ArrayList<Coordinate> row3 = new ArrayList<Coordinate>();

			String line = "";

			//			System.out.println("COLUMNS: " + numCols);
			//			System.out.println("ROWS: " + numRows);

			// store row 1 coordinates in memory
			for (int i = 0; i < numCols; i++) {
				line = br.readLine();
				String[] split = line.split(",");

				row1.add(new Coordinate(split[0],split[1]));
			}

			// store row 2 coordinates in memory
			for (int i = 0; i < numCols; i++) {
				line = br.readLine();
				String[] split = line.split(",");

				row2.add(new Coordinate(split[0],split[1]));
			}

			int idCounter = 1;

			batchInsertion.addAll(insertPolyExtentDoubleBatch2(db,schema,epsg,row1,row2,true,idCounter));

			idCounter = idCounter + numCols;

			// store row 3 coordinates in memory
			for (int i = 0; i < numCols; i++) {
				line = br.readLine();
				String[] split = line.split(",");

				row3.add(new Coordinate(split[0],split[1]));
			}

			int colCounter = 0;

			while ((line = br.readLine()) != null) {
				if (colCounter == 0 || colCounter % numCols == 0) {
					batchInsertion.addAll(insertPolyExtentDoubleBatch2(db,schema,epsg,row1,row2,row3,idCounter));
					idCounter = idCounter + numCols;
					row1 = row2;
					row2 = row3;
					row3 = new ArrayList<Coordinate>();
				}

				String[] split = line.split(",");

				row3.add(new Coordinate(split[0],split[1]));
				colCounter++;

			}

			batchInsertion.addAll(insertPolyExtentDoubleBatch2(db,schema,epsg,row1,row2,row3,idCounter));
			idCounter = idCounter + numCols;

			batchInsertion.addAll(insertPolyExtentDoubleBatch2(db,schema,epsg,row2,row3,false,idCounter));
			idCounter = idCounter + numCols;

			br.close();

			String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);			
			db.insertIntoTableBatch(schema + "." + TEMP_POLY_TABLE, batchInsertionAr);

			return true;
		} catch (IOException | SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return false;
		}
	}

	public static boolean insertPolyExtentDoubleBatch(PostgreSQLJDBC db, String schema, String epsg, 
			ArrayList<Coordinate> row1, ArrayList<Coordinate> row2, boolean writeFirstRow, int counter) throws SQLException {

		ArrayList<Coordinate> editRow;
		ArrayList<Coordinate> compareRow;

		if (writeFirstRow) {
			editRow = row1;
			compareRow = row2;
		} else {
			editRow = row2;
			compareRow = row1;
		}

		ArrayList<String> batchInsertion = new ArrayList<String>();

		Polygon testPoly = calculateDoubleCornerExtent(counter + "",editRow.get(0),editRow.get(1),compareRow.get(0));		
		batchInsertion.add(insertPolygonQuery(db,schema,TEMP_POLY_TABLE,epsg,testPoly));

		counter++;

		for (int i = 1; i < editRow.size()-1; i++) {
			testPoly = calculateDoubleExtent(counter + "",editRow.get(i),editRow.get(i-1),editRow.get(i+1),null,compareRow.get(i));		
			batchInsertion.add(insertPolygonQuery(db,schema,TEMP_POLY_TABLE,epsg,testPoly));			
			counter++;
		}

		testPoly = calculateDoubleCornerExtent(counter + "",editRow.get(editRow.size()-1),editRow.get(editRow.size()-2),compareRow.get(editRow.size()-1));		
		batchInsertion.add(insertPolygonQuery(db,schema,TEMP_POLY_TABLE,epsg,testPoly));

		String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);

		db.insertIntoTableBatch(schema + "." + TEMP_POLY_TABLE, batchInsertionAr);

		return true;			
	}

	public static boolean insertPolyExtentDoubleBatch (PostgreSQLJDBC db, String schema, String epsg, 
			ArrayList<Coordinate> row1, ArrayList<Coordinate> row2, ArrayList<Coordinate> row3, int counter) throws SQLException {

		ArrayList<Coordinate> editRow = row2;
		ArrayList<Coordinate> topRow = row1;
		ArrayList<Coordinate> botRow = row3;

		ArrayList<String> batchInsertion = new ArrayList<String>();

		Polygon testPoly = calculateDoubleExtent(counter + "",editRow.get(0),null,editRow.get(1),topRow.get(0),botRow.get(0));		
		batchInsertion.add(insertPolygonQuery(db,schema,TEMP_POLY_TABLE,epsg,testPoly));

		counter++;

		for (int i = 1; i < editRow.size()-1; i++) {
			testPoly = calculateDoubleExtent(counter + "",editRow.get(i),editRow.get(i-1),editRow.get(i+1),topRow.get(i),botRow.get(i));		
			batchInsertion.add(insertPolygonQuery(db,schema,TEMP_POLY_TABLE,epsg,testPoly));			
			counter++;
		}

		testPoly = calculateDoubleExtent(counter + "",editRow.get(editRow.size()-1),editRow.get(editRow.size()-2),null,topRow.get(editRow.size()-1),botRow.get(editRow.size()-1));		
		batchInsertion.add(insertPolygonQuery(db,schema,TEMP_POLY_TABLE,epsg,testPoly));

		String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);

		db.insertIntoTableBatch(schema + "." + TEMP_POLY_TABLE, batchInsertionAr);
		return true;			
	}

	public static ArrayList<String> insertPolyExtentDoubleBatch2 (PostgreSQLJDBC db, String schema, String epsg, 
			ArrayList<Coordinate> row1, ArrayList<Coordinate> row2, boolean writeFirstRow, int counter) throws SQLException {

		ArrayList<Coordinate> editRow;
		ArrayList<Coordinate> compareRow;

		if (writeFirstRow) {
			editRow = row1;
			compareRow = row2;
		} else {
			editRow = row2;
			compareRow = row1;
		}

		ArrayList<String> batchInsertion = new ArrayList<String>();

		Polygon testPoly = calculateDoubleCornerExtent(counter + "",editRow.get(0),editRow.get(1),compareRow.get(0));		
		batchInsertion.add(insertPolygonQuery(db,schema,TEMP_POLY_TABLE,epsg,testPoly));

		counter++;

		for (int i = 1; i < editRow.size()-1; i++) {
			testPoly = calculateDoubleExtent(counter + "",editRow.get(i),editRow.get(i-1),editRow.get(i+1),null,compareRow.get(i));		
			batchInsertion.add(insertPolygonQuery(db,schema,TEMP_POLY_TABLE,epsg,testPoly));			
			counter++;
		}

		testPoly = calculateDoubleCornerExtent(counter + "",editRow.get(editRow.size()-1),editRow.get(editRow.size()-2),compareRow.get(editRow.size()-1));		
		batchInsertion.add(insertPolygonQuery(db,schema,TEMP_POLY_TABLE,epsg,testPoly));

		//		String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);

		//		db.insertIntoTableBatch(schema + "." + TEMP_POLY_TABLE, batchInsertionAr);

		//		return true;			
		return batchInsertion;
	}

	public static ArrayList<String> insertPolyExtentDoubleBatch2 (PostgreSQLJDBC db, String schema, String epsg, 
			ArrayList<Coordinate> row1, ArrayList<Coordinate> row2, ArrayList<Coordinate> row3, int counter) throws SQLException {

		ArrayList<Coordinate> editRow = row2;
		ArrayList<Coordinate> topRow = row1;
		ArrayList<Coordinate> botRow = row3;

		ArrayList<String> batchInsertion = new ArrayList<String>();

		Polygon testPoly = calculateDoubleExtent(counter + "",editRow.get(0),null,editRow.get(1),topRow.get(0),botRow.get(0));		
		batchInsertion.add(insertPolygonQuery(db,schema,TEMP_POLY_TABLE,epsg,testPoly));

		counter++;

		for (int i = 1; i < editRow.size()-1; i++) {
			testPoly = calculateDoubleExtent(counter + "",editRow.get(i),editRow.get(i-1),editRow.get(i+1),topRow.get(i),botRow.get(i));		
			batchInsertion.add(insertPolygonQuery(db,schema,TEMP_POLY_TABLE,epsg,testPoly));			
			counter++;
		}

		testPoly = calculateDoubleExtent(counter + "",editRow.get(editRow.size()-1),editRow.get(editRow.size()-2),null,topRow.get(editRow.size()-1),botRow.get(editRow.size()-1));		
		batchInsertion.add(insertPolygonQuery(db,schema,TEMP_POLY_TABLE,epsg,testPoly));

		//		String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);

		//		db.insertIntoTableBatch(schema + "." + TEMP_POLY_TABLE, batchInsertionAr);
		//		return true;		
		return batchInsertion;
	}

	public static boolean insertPolyExtentDouble(PostgreSQLJDBC db, String schema, String epsg, 
			ArrayList<Coordinate> row1, ArrayList<Coordinate> row2, boolean writeFirstRow, int counter) {

		ArrayList<Coordinate> editRow;
		ArrayList<Coordinate> compareRow;

		if (writeFirstRow) {
			editRow = row1;
			compareRow = row2;
		} else {
			editRow = row2;
			compareRow = row1;
		}

		Polygon testPoly = calculateDoubleCornerExtent(counter + "",editRow.get(0),editRow.get(1),compareRow.get(0));		
		insertPolygon(db,schema,TEMP_POLY_TABLE,epsg,testPoly);

		counter++;

		for (int i = 1; i < editRow.size()-1; i++) {
			testPoly = calculateDoubleExtent(counter + "",editRow.get(i),editRow.get(i-1),editRow.get(i+1),null,compareRow.get(i));		
			insertPolygon(db,schema,TEMP_POLY_TABLE,epsg,testPoly);			
			counter++;
		}

		testPoly = calculateDoubleCornerExtent(counter + "",editRow.get(editRow.size()-1),editRow.get(editRow.size()-2),compareRow.get(editRow.size()-1));		
		insertPolygon(db,schema,TEMP_POLY_TABLE,epsg,testPoly);
		return true;			
	}

	public static boolean insertPolyExtentDouble(PostgreSQLJDBC db, String schema, String epsg, 
			ArrayList<Coordinate> row1, ArrayList<Coordinate> row2, ArrayList<Coordinate> row3, int counter) {

		ArrayList<Coordinate> editRow = row2;
		ArrayList<Coordinate> topRow = row1;
		ArrayList<Coordinate> botRow = row3;

		Polygon testPoly = calculateDoubleExtent(counter + "",editRow.get(0),null,editRow.get(1),topRow.get(0),botRow.get(0));		
		insertPolygon(db,schema,TEMP_POLY_TABLE,epsg,testPoly);

		counter++;

		for (int i = 1; i < editRow.size()-1; i++) {
			testPoly = calculateDoubleExtent(counter + "",editRow.get(i),editRow.get(i-1),editRow.get(i+1),topRow.get(i),botRow.get(i));		
			insertPolygon(db,schema,TEMP_POLY_TABLE,epsg,testPoly);			
			counter++;
		}

		testPoly = calculateDoubleExtent(counter + "",editRow.get(editRow.size()-1),editRow.get(editRow.size()-2),null,topRow.get(editRow.size()-1),botRow.get(editRow.size()-1));		
		insertPolygon(db,schema,TEMP_POLY_TABLE,epsg,testPoly);
		return true;			
	}

	public static Polygon calculateDoubleExtent(String uid, Coordinate centroid, 
			Coordinate leftNbr, Coordinate rightNbr, Coordinate upNbr, Coordinate dwnNbr) {

		DoubleCoordinate doubleCentroid = new DoubleCoordinate(centroid);

		int nullCount = 0;

		if (upNbr == null) {
			nullCount++;
		}
		if (dwnNbr == null) {
			nullCount++;
		}
		if (rightNbr == null) {
			nullCount++;
		}
		if (leftNbr == null) {
			nullCount++;
		}

		if (nullCount > 1) {
			return null;
		}

		double leftWidth;		
		double rightWidth;		
		double dwnHeight;		
		double upHeight;

		if (upNbr == null) {
			leftWidth = getDoubleDistance(centroid,leftNbr);
			leftWidth = leftWidth / 2;

			rightWidth = getDoubleDistance(centroid,rightNbr);
			rightWidth = rightWidth / 2;

			dwnHeight = getDoubleDistance(centroid,dwnNbr);
			dwnHeight = dwnHeight / 2;

			upHeight = dwnHeight + 0;

		} else if (dwnNbr == null) {

			leftWidth = getDoubleDistance(centroid,leftNbr);
			leftWidth = leftWidth / 2;

			rightWidth = getDoubleDistance(centroid,rightNbr);
			rightWidth = rightWidth / 2;

			upHeight = getDoubleDistance(centroid,upNbr);
			upHeight = upHeight / 2;

			dwnHeight = upHeight + 0;

		} else if (rightNbr == null) {
			leftWidth = getDoubleDistance(centroid,leftNbr);
			leftWidth = leftWidth / 2;

			rightWidth = leftWidth + 0;

			upHeight = getDoubleDistance(centroid,upNbr);
			upHeight = upHeight / 2;

			dwnHeight = getDoubleDistance(centroid,dwnNbr);
			dwnHeight = dwnHeight / 2;

		} else if (leftNbr == null) {						
			rightWidth = getDoubleDistance(centroid,rightNbr);
			rightWidth = rightWidth / 2;

			leftWidth = rightWidth + 0;

			upHeight = getDoubleDistance(centroid,upNbr);
			upHeight = upHeight / 2;

			dwnHeight = getDoubleDistance(centroid,dwnNbr);
			dwnHeight = dwnHeight / 2;

		} else if (nullCount == 0) {
			leftWidth = getDoubleDistance(centroid,leftNbr);
			leftWidth = leftWidth / 2;

			rightWidth = getDoubleDistance(centroid,rightNbr);
			rightWidth = rightWidth / 2;

			upHeight = getDoubleDistance(centroid,upNbr);
			upHeight = upHeight / 2;

			dwnHeight = getDoubleDistance(centroid,dwnNbr);
			dwnHeight = dwnHeight / 2;
		} else {
			return null;
		}

		ArrayList<Coordinate> coords = new ArrayList<Coordinate>();

		coords.add(new Coordinate(doubleCentroid.x-leftWidth, doubleCentroid.y+upHeight));
		coords.add(new Coordinate(doubleCentroid.x+rightWidth, doubleCentroid.y+upHeight));
		coords.add(new Coordinate(doubleCentroid.x+rightWidth, doubleCentroid.y-dwnHeight));
		coords.add(new Coordinate(doubleCentroid.x-leftWidth, doubleCentroid.y-dwnHeight));

		return new Polygon(uid,centroid,coords,false);
	}

	public static Polygon calculateFloatExtent(String uid, Coordinate centroid, 
			Coordinate leftNbr, Coordinate rightNbr, Coordinate upNbr, Coordinate dwnNbr) {

		FloatCoordinate floatCentroid = new FloatCoordinate(centroid);

		int nullCount = 0;

		if (upNbr == null) {
			nullCount++;
		}
		if (dwnNbr == null) {
			nullCount++;
		}
		if (rightNbr == null) {
			nullCount++;
		}
		if (leftNbr == null) {
			nullCount++;
		}

		if (nullCount > 1) {
			return null;
		}

		float leftWidth;		
		float rightWidth;		
		float dwnHeight;		
		float upHeight;

		if (upNbr == null) {
			leftWidth = getFloatDistance(centroid,leftNbr);
			leftWidth = leftWidth / 2;

			rightWidth = getFloatDistance(centroid,rightNbr);
			rightWidth = rightWidth / 2;

			dwnHeight = getFloatDistance(centroid,dwnNbr);
			dwnHeight = dwnHeight / 2;

			upHeight = dwnHeight + 0;

		} else if (dwnNbr == null) {

			leftWidth = getFloatDistance(centroid,leftNbr);
			leftWidth = leftWidth / 2;

			rightWidth = getFloatDistance(centroid,rightNbr);
			rightWidth = rightWidth / 2;

			upHeight = getFloatDistance(centroid,upNbr);
			upHeight = upHeight / 2;

			dwnHeight = upHeight + 0;

		} else if (rightNbr == null) {
			leftWidth = getFloatDistance(centroid,leftNbr);
			leftWidth = leftWidth / 2;

			rightWidth = leftWidth + 0;

			upHeight = getFloatDistance(centroid,upNbr);
			upHeight = upHeight / 2;

			dwnHeight = getFloatDistance(centroid,dwnNbr);
			dwnHeight = dwnHeight / 2;

		} else if (leftNbr == null) {						
			rightWidth = getFloatDistance(centroid,rightNbr);
			rightWidth = rightWidth / 2;

			leftWidth = rightWidth + 0;

			upHeight = getFloatDistance(centroid,upNbr);
			upHeight = upHeight / 2;

			dwnHeight = getFloatDistance(centroid,dwnNbr);
			dwnHeight = dwnHeight / 2;

		} else if (nullCount == 0) {
			leftWidth = getFloatDistance(centroid,leftNbr);
			leftWidth = leftWidth / 2;

			rightWidth = getFloatDistance(centroid,rightNbr);
			rightWidth = rightWidth / 2;

			upHeight = getFloatDistance(centroid,upNbr);
			upHeight = upHeight / 2;

			dwnHeight = getFloatDistance(centroid,dwnNbr);
			dwnHeight = dwnHeight / 2;
		} else {
			return null;
		}

		ArrayList<Coordinate> coords = new ArrayList<Coordinate>();

		coords.add(new Coordinate(floatCentroid.x-leftWidth, floatCentroid.y+upHeight));
		coords.add(new Coordinate(floatCentroid.x+rightWidth, floatCentroid.y+upHeight));
		coords.add(new Coordinate(floatCentroid.x+rightWidth, floatCentroid.y-dwnHeight));
		coords.add(new Coordinate(floatCentroid.x-leftWidth, floatCentroid.y-dwnHeight));

		return new Polygon(uid,centroid,coords,false);
	}

	public static Polygon calculateDoubleCornerExtent(String uid, Coordinate centroid, Coordinate xNeighbor, Coordinate yNeighbor) {

		DoubleCoordinate doubleCentroid = new DoubleCoordinate(centroid);

		double width = getDoubleDistance(centroid,xNeighbor);
		width = width / 2;

		double height = getDoubleDistance(centroid,yNeighbor);
		height = height / 2;

		ArrayList<Coordinate> coords = new ArrayList<Coordinate>();

		coords.add(new Coordinate(doubleCentroid.x-width, doubleCentroid.y+height));
		coords.add(new Coordinate(doubleCentroid.x+width, doubleCentroid.y+height));
		coords.add(new Coordinate(doubleCentroid.x+width, doubleCentroid.y-height));
		coords.add(new Coordinate(doubleCentroid.x-width, doubleCentroid.y-height));

		return new Polygon(uid,centroid,coords,false);
	}

	public static Polygon calculateFloatCornerExtent(String uid, Coordinate centroid, Coordinate xNeighbor, Coordinate yNeighbor) {

		FloatCoordinate floatCentroid = new FloatCoordinate(centroid);

		float width = getFloatDistance(centroid,xNeighbor);
		width = width / 2;

		float height = getFloatDistance(centroid,yNeighbor);
		height = height / 2;

		ArrayList<Coordinate> coords = new ArrayList<Coordinate>();

		coords.add(new Coordinate(floatCentroid.x-width, floatCentroid.y+height));
		coords.add(new Coordinate(floatCentroid.x+width, floatCentroid.y+height));
		coords.add(new Coordinate(floatCentroid.x+width, floatCentroid.y-height));
		coords.add(new Coordinate(floatCentroid.x-width, floatCentroid.y-height));

		Polygon tmpPoly = new Polygon(uid,centroid,coords,false);

		return tmpPoly;		
	}

	public static boolean insertPolygon(PostgreSQLJDBC db, String schema, String table, String epsg, Polygon tmpPoly) {

		String[] insertData = new String[4];
		insertData[0] = "'" + tmpPoly.uid + "'";
		insertData[1] = tmpPoly.centroid.x;
		insertData[2] = tmpPoly.centroid.y;
		insertData[3] = "ST_GeomFromText('POLYGON((" + tmpPoly.coords.get(0).x + " " + tmpPoly.coords.get(0).y;

		for (int i = 1; i < tmpPoly.coords.size(); i++) {
			insertData[3] = insertData[3] + "," + tmpPoly.coords.get(i).x + " " + tmpPoly.coords.get(i).y;
		}

		insertData[3] = insertData[3] + "))'," + epsg + ")";

		//		System.out.println(insertData[3]);

		try {
			if (schema.equals("")) {
				return db.insertIntoTable(table,insertData);
			} else {
				//				System.out.println("SCHEMA: " + schema);
				//				System.out.println("TABLE: " + table);
				//				System.out.println("Full Query: " + schema + "." + table);
				return db.insertIntoTable(schema + "." + table,insertData);
			}			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public static String insertPolygonQuery(PostgreSQLJDBC db, String schema, String table, String epsg, Polygon tmpPoly) {

		String[] insertData = new String[4];
		insertData[0] = "'" + tmpPoly.uid + "'";
		insertData[1] = tmpPoly.centroid.x;
		insertData[2] = tmpPoly.centroid.y;
		insertData[3] = "ST_GeomFromText('POLYGON((" + tmpPoly.coords.get(0).x + " " + tmpPoly.coords.get(0).y;

		for (int i = 1; i < tmpPoly.coords.size(); i++) {
			insertData[3] = insertData[3] + "," + tmpPoly.coords.get(i).x + " " + tmpPoly.coords.get(i).y;
		}

		insertData[3] = insertData[3] + "))'," + epsg + ")";

		//		System.out.println(insertData[3]);

		try {
			if (schema.equals("")) {
				return db.insertQueryBuilder(table,insertData);
			} else {
				//				System.out.println("SCHEMA: " + schema);
				//				System.out.println("TABLE: " + table);
				//				System.out.println("Full Query: " + schema + "." + table);
				return db.insertQueryBuilder(schema + "." + table,insertData);
			}			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	private static ArrayList<DoubleCoordinate> coord2DoubleCoord (ArrayList<Coordinate> coords) {
		ArrayList<DoubleCoordinate> doubleCoord = new ArrayList<DoubleCoordinate>();

		for (int i = 0; i < coords.size(); i++) {
			doubleCoord.add(new DoubleCoordinate(coords.get(i)));
		}

		return doubleCoord;
	}

	private static double getDoubleDistance(Coordinate pt1, Coordinate pt2) {

		DoubleCoordinate p1 = new DoubleCoordinate(pt1.x,pt1.y);
		DoubleCoordinate p2 = new DoubleCoordinate(pt2.x,pt2.y);


		return Math.hypot(p1.x - p2.x, p1.y - p2.y);
	}

	private static double getDoubleDistance(DoubleCoordinate pt1, DoubleCoordinate pt2) {		

		return Math.hypot(pt1.x - pt2.x, pt1.y - pt2.y);
	}

	private static ArrayList<FloatCoordinate> coord2FloatCoord (ArrayList<Coordinate> coords) {
		ArrayList<FloatCoordinate> floatCoord = new ArrayList<FloatCoordinate>();

		for (int i = 0; i < coords.size(); i++) {
			floatCoord.add(new FloatCoordinate(coords.get(i)));
		}

		return floatCoord;
	}

	private static float getFloatDistance(Coordinate pt1, Coordinate pt2) {

		FloatCoordinate p1 = new FloatCoordinate(pt1.x,pt1.y);
		FloatCoordinate p2 = new FloatCoordinate(pt2.x,pt2.y);


		return (float) Math.hypot(p1.x - p2.x, p1.y - p2.y);
	}

	private static float getFloatDistance(FloatCoordinate pt1, FloatCoordinate pt2) {		

		return (float) Math.hypot(pt1.x - pt2.x, pt1.y - pt2.y);
	}

	public static boolean insertPoly(PostgreSQLJDBC db, String schema, 
			ArrayList<String> row1X, ArrayList<String> row1Y, ArrayList<String> row2X, ArrayList<String> row2Y, 
			boolean row2Compare, String epsg, int count) {

		ArrayList<String> editRowX;
		ArrayList<String> editRowY;
		ArrayList<String> compareRowX;
		ArrayList<String> compareRowY;

		if (row2Compare) {
			editRowX = row1X;
			editRowY = row1Y;
			compareRowX = row2X;
			compareRowY = row2Y;
		} else {
			editRowX = row2X;
			editRowY = row2Y;
			compareRowX = row1X;
			compareRowY = row1Y;
		}

		String[] polyX = new String[4];
		String[] polyY = new String[4];

		double width = Double.parseDouble(row1X.get(0)) - Double.parseDouble(row1X.get(1));
		width = width / 2;

		double height = Double.parseDouble(row1Y.get(0)) - Double.parseDouble(row2Y.get(1));
		height = height / 2;

		//		polyX
		//		
		String[] insertData = new String[3];
		insertData[0] = "'" + count + "'";
		insertData[1] = row1X.get(0);
		insertData[2] = row1Y.get(0);
		//		insertData[3] = "ST_GeomFromText('POLYGON(" + row1X.get(0) + " " + row1Y.get(0) + ")'" + epsg + ")";

		for (int i = 0; i < row1X.size(); i++) {

		}

		for (int i = 1; i < row1X.size(); i++) {

		}

		try {
			if (db.insertIntoTable(schema + ".tmp_locus_poly",insertData)) {
				//			br.close();
				//				return false;
				count++;
			} else {
				return false;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}


		return true;

		//		while ((line = br.readLine()) != null) {
		//			String[] data = line.split(",");
		//			String[] insertData = new String[2];
		//			insertData[0] = "'l" + elementCount + "'";
		//			if (coordinates.length == 2) {
		//				insertData[1] = "ST_GeomFromText('POINT(" + data[coordinates[0]];
		//			} else if (coordinates.length == 3) {
		//				insertData[1] = "ST_GeomFromText('POINTZ(" + data[coordinates[0]];
		//			} else {
		//				br.close();
		//				return false;
		//			}
		//			
		//			
		//			for (int i = 1; i < coordinates.length; i++) {
		//				insertData[1] = insertData[1] + " " + data[coordinates[i]];
		//			}
		//			
		//			insertData[1] = insertData[1] + ")'," + epsg + ")";
		//			if (!db.insertIntoTable(schema + "." + element,insertData)) {
		//				br.close();
		//				return false;
		//			}
		//		}
	}

	public static boolean createTableL_AT(PostgreSQLJDBC db, String format, String schema, File dir, int[] primaryKey, int headers, boolean batch) {
		// ensure f is a Directory or

		if (!dir.isDirectory()) {	
			System.out.println("ERROR: f is not a directory!");
			return false;
		} 

		if (format.equalsIgnoreCase("L_A_T")) {			

			if (!exportL_A_T2L_AT(db, schema, dir, primaryKey, headers, batch)) {
				return false;
			}
		}

		return true;
	}
	
	public static boolean updateKeyTable(PostgreSQLJDBC db, String schema, String table, String dictionaryPath, boolean batch) {
		
		// table path (merges schema and tablename)
		String tablePath = schema + "." + table;
		
		// list to hold the update statements
		ArrayList<String> batchUpdate = new ArrayList<String>();
		
		try {
			
			ArrayList<String> tableIds = new ArrayList<String>(Arrays.asList(db.getTableColumnIds(tablePath)));
			
			BufferedReader br = new BufferedReader(new FileReader(dictionaryPath));
			
			String line = "";
			int lineCount = 0;
			
			// get the columns from first line of dictionary file
			String[] columns = br.readLine().split(",");
			
			for (int i = 0; i < columns.length; i++) {
				
				if (!tableIds.contains(columns[i])) {
					
					// add abbreviations to locus_key
					if (!db.addColumn(schema + "." + table, columns[i], "TEXT")) {
						
						System.out.println("Unable to add column " + columns[i] + " to " + tablePath);
						br.close();
						return false;
						
					}
					
				}
				
			}
			
			while ((line = br.readLine()) != null) {
				
				lineCount++;
				String[] split = line.split(",");
				
				for (int i = 0; i < columns.length; i++) {
					
					
					if (batch) { // if batch
					
						// append insertion statement to ArrayList
						batchUpdate.add(db.updateQueryBuilder(schema, table, "id", lineCount + "", columns[i], "'" + split[i] + "'"));
				
					} else {
						
						// otherwise update table one at a time
						db.updateTable(schema, table, "id", lineCount + "", columns[i], "'" + split[i] + "'");
					
					}
				}
			}
			
			br.close();
		} catch (IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
		// If using batch mode, now we submit insertion statements to database
		if (batch) {
			
			// convert ArrayList to array
			String[] batchUpdateAr = batchUpdate.toArray(new String[batchUpdate.size()]);
			
			try {
				
				// insert into table
				db.insertIntoTableBatch(schema + "." + table, batchUpdateAr, BATCH_SIZE);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * Creates the Tri-Space project tables. This includes the key tables (locus_key, time_key, etc.) 
	 * and the Tri-Space perspective tables (l_at, a_lt, t_la, la_t, lt_a, at_l) for a particular Tri-Space
	 * perspective
	 * 
	 *
	 * @param db
	 *            PostgreSQLJDBC object.
	 * @param schema
	 *            The schema in the DB to write to.
	 * @param tsDirectory
	 *            Folder that contains the Tri-Space perspectives (any normalization).
	 * @param dictionaryDirectory
	 *            Folder that contains the dictionaries (loci.txt, attributes.txt, times.txt).
	 * @param headers
	 *            The number of headers in the non-composite perspectives (l_at, a_lt, t_la) CSV files.
	 * @param batch
	 *            Whether or not to use the postgresSQL batch execution approach.
	 * @param somThreshold     
	 * 			  The maximum number of objects before switching from MDS to SOM
	 * @param perspectives
	 * 			  The Tri-Space perspectives being analyzed.
	 * @param somPerspectives
	 * 			  The perspectives that will utilize a SOM.
	 * @param normalizations
	 * 			  The normalizations being analyzed.	
	 * @param normLabels
	 * 			  The labels for the normalizations
	 */			
	public static boolean createTables(PostgreSQLJDBC db, String schema, File tsDirectory, File dictionaryDirectory, 
										int headers, boolean batch,	int[] perspectives, int[] somPerspectives, 
										int[] normalizations, String[] normLabels) {		
		
		// ensure tsDirectory is a Directory or return false
		if (!tsDirectory.isDirectory()) {	
			System.out.println("ERROR: tsDirectory is not a directory!");
			return false;
		} 
		
		// create TS-perspective tables & key tables
		if (!createTrispaceTables(db, schema, tsDirectory, headers, batch, perspectives, 
								somPerspectives, normalizations, normLabels)) {
			return false;
		}
		
		updateKeyTable(db, schema, "locus_key", dictionaryDirectory.getAbsolutePath() + "/loci.txt", batch);		
		updateKeyTable(db, schema, "attribute_key", dictionaryDirectory.getAbsolutePath() + "/attributes.txt", batch);		
		updateKeyTable(db, schema, "time_key", dictionaryDirectory.getAbsolutePath() + "/times.txt", batch);

		return true;
	}
	
	public static boolean createTrispaceTables(PostgreSQLJDBC db, String schema, File dir, int headers, boolean batch, 
						int[] perspectives, int[] somPerspectives, int[] normalizations, String[] normLabels) {
	
		
		// get the csv files in the directory
		File[] files = dir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".csv");
			}
		});
		
		// return false if there are no files in directory
		if (files.length == 0) {
			return false;
		}	
		
		// dictionary for TriSpace files
		// key = tsperspective; value = File
		Map<String, File> tsFiles = new HashMap<String, File>();
		
		// iterate thru all files
		for (int i = 0; i < files.length; i++) {
			
			// get the file name for comparison
			String tmpFileName = files[i].getName().toLowerCase();
			
			if (tmpFileName.contains("l_at")) {

				// map the l_at file
				tsFiles.put("l_at", files[i]);				
			} else if (tmpFileName.contains("t_la")) {
				
				// map the t_la file
				tsFiles.put("t_la", files[i]);				
			} else if (tmpFileName.contains("a_lt")) {
				
				// map the a_lt file
				tsFiles.put("a_lt", files[i]);				
			}
		}
		
		try {
			if (!createAllDictionaries(db, tsFiles, schema, headers, batch, normalizations, normLabels)) {
				return false;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		
		
		
		for (int i = 0; i < perspectives.length; i++) {
			boolean somPerspective = false;
			for (int j = 0; j < somPerspectives.length; j++) {
				if (perspectives[i] == somPerspectives[j]) {
					somPerspective = true;
					break;
				}
			}
			if (!createTSTable(db, schema, PERSPECTIVES[perspectives[i]], somPerspective)) {
				return false;
			}				
		}

		return true;

	}
	
//	public static boolean exportL_A_T2L_AT_new(PostgreSQLJDBC db, String schema, File dir, int[] primaryKey, int headers, String alias, boolean batch, int somThreshold) {
//		
//		// get the csv files in the directory
//		File[] files = dir.listFiles(new FilenameFilter() {
//			@Override
//			public boolean accept(File dir, String name) {
//				return name.endsWith(".csv");
//			}
//		});
//
//		// return false if there are no files in directory OR loci not common b/w files
//		if (files.length == 0 || !CSVManager.commonLociCheck(dir.getPath(), primaryKey)) {
//			return false;
//		}	
//
//		if (!createAllDictionaries(db,files,schema,primaryKey,headers,alias,batch)) {
//			return false;
//		}			
//		
//		for (int i = 1; i < NORMALIZATIONS.length; i++) {
//			if (!createTSTable(db, schema, NORMALIZATIONS[i], somThreshold)) {
//				return false;
//			}
////			if (!createTSTablePCA(db, schema, NORMALIZATIONS[i], somThreshold, pcaThreshold, classes[i-1])) {
////				return false;
////			}
//		}
//		
//		return true;
//	}

	public static boolean exportL_A_T2L_AT(PostgreSQLJDBC db, String schema, File dir, int[] primaryKey, int headers, String alias, boolean batch, boolean landcover, int somThreshold) {
		
		// constant for max columns in DB
		int MAX_COLUMNS = 1598;
		
		// get the csv files in the directory
		File[] files = dir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".csv");
			}
		});

		// return false if there are no files in directory OR loci not common b/w files
		if (files.length == 0 || !CSVManager.commonLociCheck(dir.getPath(), primaryKey)) {
			return false;
		}	

		if (!createAllDictionaries(db,files,schema,primaryKey,headers,alias,batch)) {
			return false;
		}

		int numLoci = db.getTableLength(schema, "locus_key");

		int numAttributes = db.getTableLength(schema, "attribute_key");

		int numNormalizations = db.getTableLength(schema, "normalization_key");

		int numTimes = db.getTableLength(schema, "time_key");

//		System.out.println("Number of loci: " + numLoci);
//		System.out.println("SOM Threshold: " + somThreshold);
		// L_AT Perspective
		// test if SOM or MDS
		if (numLoci > somThreshold) { // if SOM
			
			// Test if number of columns requires PCA
			if (numAttributes * numTimes < MAX_COLUMNS) { // if enough columns
				
				// create SOM table with actual attributes
				if (!createTable_L_AT(db, schema, numLoci, false, true)) {
					return false;
				}		
				
			} else { // if not enough columns
				
				// create SOM table with PCA attributes
				if (!createTable_L_AT(db, schema, numLoci, true, true)) {
					return false;
				}	
			}
		} else { // if MDS
			
			// Test if number of columns requires PCA
			if (numAttributes * numTimes < MAX_COLUMNS) { // if enough columns

				// create MDS table with actual attributes
				if (!createTable_L_AT(db, schema, numLoci, false, false)) {
					return false;
				}		
				
			} else { // if not enough columns
				
				// create MDS table with PCA attributes
				if (!createTable_L_AT(db, schema, numLoci, true, false)) {
					return false;
				}	
			}
		}
		
		// A_LT Perspective
		// test if SOM or MDS
		if (numAttributes > somThreshold) { // if SOM
			
			// Test if number of columns requires PCA
			if (numLoci * numTimes < MAX_COLUMNS) { // if enough columns
				
				// create SOM table with actual attributes
				if (!createTable_A_LT(db, schema, false, true)) {
					return false;
				}
				
			} else { // if not enough columns
				
				// create SOM table with PCA attributes
				if (!createTable_A_LT(db, schema, true, true)) {
					return false;
				}
			}
		} else { // if MDS
			
			// Test if number of columns requires PCA			
			if (numLoci * numTimes < MAX_COLUMNS) { // if enough columns
				
				// create MDS table with actual attributes
				if (!createTable_A_LT(db, schema, false, false)) {
					return false;
				}
			} else { // if not enough columns
				
				// create MDS table with PCA attributes
				if (!createTable_A_LT(db, schema, true, false)) {
					return false;
				}
			}
		}

		

		if (numAttributes < MAX_COLUMNS) {
			if (!createTable_LT_A(db,schema,landcover)) {
				return false;
			}
		}
		
		// T_LA Perspective
		// test if SOM or MDS
		if (numTimes > somThreshold) { // if SOM
			
			// Test if number of columns requires PCA
			if (numLoci * numAttributes < MAX_COLUMNS) { // if enough columns
				
				// create SOM table with actual attributes
				if (!createTable_T_LA(db, schema, false, true)) {
					return false;
				}		
				
			} else { // if not enough columns
				
				// create SOM table with PCA attributes
				if (!createTable_T_LA(db, schema, true, true)) {
					return false;
				}	
			}
		} else { // if MDS
			
			// Test if number of columns requires PCA
			if (numLoci * numAttributes < MAX_COLUMNS) { // if enough columns

				// create MDS table with actual attributes
				if (!createTable_T_LA(db, schema, false, false)) {
					return false;
				}		
				
			} else { // if not enough columns
				
				// create MDS table with PCA attributes
				if (!createTable_T_LA(db,schema, true, false)) {
					return false;
				}	
			}
		}

		if (numTimes < MAX_COLUMNS) {
			if (!createTable_LA_T(db,schema)) {
				return false;
			}
		}
		
		// AT_L Perspective
		// test if SOM or MDS
		if (numAttributes * numTimes > somThreshold) { // if SOM
			
			// Test if number of columns requires PCA
			if (numLoci < MAX_COLUMNS) { // if enough columns
				
				// create SOM table with actual attributes
				if (!createTable_AT_L(db, schema, false, true)) {
					return false;
				}
				
			} else { // if not enough columns
				
				// create SOM table with PCA attributes
				if (!createTable_AT_L(db, schema, true, true)) {
					return false;
				}
			}
		} else { // if MDS
			
			// Test if number of columns requires PCA			
			if (numLoci < MAX_COLUMNS) { // if enough columns
				
				// create MDS table with actual attributes
				if (!createTable_AT_L(db, schema, false, false)) {
					return false;
				}
			} else { // if not enough columns
				
				// create MDS table with PCA attributes
				if (!createTable_AT_L(db, schema, true, false)) {
					return false;
				}
			}
		}

		return true;

	}
	
	public static boolean createTable(PostgreSQLJDBC db, String schema, String tableName, String filePath, String[] dataTypes) throws IOException, SQLException {
		// list to hold the insertion statements
//		ArrayList<String> batchUpdate = new ArrayList<String>();
		
		// BufferedReader to read in file (csv)
		BufferedReader br = new BufferedReader(new FileReader(filePath));

		// line variable to store each line from file
		String line = br.readLine();
		
		// primary key is columnArray[0]
		String[] columnArray = line.split(",");
		// the rest are insert columns
		
		if (columnArray.length != dataTypes.length) {
			br.close();
			return false;
		}
		
		
		String[] tableCols = new String[columnArray.length];
//		tableCols[0] = "(ID TEXT PRIMARY KEY NOT NULL";
		tableCols[0] = "(" + columnArray[0] + " " + dataTypes[0] + " PRIMARY KEY NOT NULL";
		
		int idxCounter = 1;
		
		for (int i = 1; i < columnArray.length; i++) {
			tableCols[idxCounter++] = columnArray[i] + " " + dataTypes[i];
		}


		tableCols[tableCols.length-1] = tableCols[tableCols.length-1] + ")";

		try {
			db.createTable(schema + "." + tableName, tableCols);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			br.close();
			return false;
		}
		
		
		
		// iterate thru the rest of the file
		while ((line = br.readLine()) != null) {
			
			// split based on comma delimiter
			String[] lineSplit = line.split(",");
			
			for (int i = 0; i < lineSplit.length; i++) {
				if (dataTypes[i] == "TEXT") {
					String quoteChar = "'";
					String tmpData = lineSplit[i] + "";
					lineSplit[i] = quoteChar + tmpData + quoteChar;
				}
			}
			
//			db.update
			
//			String test = db.insertQueryBuilder(schema + "." + tableName, lineSplit);
//			System.out.println(test);
			
//			String test = db.updateQueryBuilder(schema, tableName, columnArray[0], lineSplit[0], );
			
			if (db.insertIntoTable(schema + "." + tableName, lineSplit) == false) {
				br.close();
				return false;
			}
		}
		
		// close the BufferedReader
		br.close();
	
		return true;		
	}
	
	/**
	 * Creates a table for a particular Tri-Space perspective
	 *
	 * @param db
	 *            PostgreSQLJDBC object
	 * @param schema
	 *            the schema in the DB to write to
	 * @param ts
	 *            the Tri-Space perspective
	 * @deprecated
	 */
	public static boolean createTSTablePCA(PostgreSQLJDBC db, String schema, String ts, int somThreshold, int pcaThreshold, String[] classes) {
				
		// get the number of loci from key
		int numLoci = db.getTableLength(schema, "locus_key");
		// get the number of attributes from key
		int numAttributes = db.getTableLength(schema, "attribute_key");
		// get the number of times from key
		int numTimes = db.getTableLength(schema, "time_key");
		
		// initialize with 2 columns (for id, normalization, & geometry) and extra columns for any classes
		int numCols = 2 + classes.length;
		
		// the number of objects in the TS perspective		
		int numObjects = -1;
		// the number of dimensions in the TS perspective
		int numDimensions = -1;
		
		// list to hold the objects individually
		ArrayList<Integer> objectList = new ArrayList<Integer>();
		// list to hold the objects individually
		ArrayList<Integer> dimensionList = new ArrayList<Integer>();
		// list to hold the dimension names
		ArrayList<String> dimensionNames = new ArrayList<String>();		
		
		if (ts.equalsIgnoreCase("l_at")) {
			
			numObjects = numLoci;			
			numDimensions = numAttributes * numTimes;
			
			objectList.add(numLoci);
			dimensionList.add(numAttributes);
			dimensionList.add(numTimes);
			dimensionNames.add("a");
			dimensionNames.add("t");
			
		} else if (ts.equalsIgnoreCase("a_lt")) {		
			
			numObjects = numAttributes;
			numDimensions = numLoci * numTimes;
			
			objectList.add(numAttributes);
			dimensionList.add(numLoci);
			dimensionList.add(numTimes);
			dimensionNames.add("l");
			dimensionNames.add("t");
			
		} else if (ts.equalsIgnoreCase("t_la")) {
			
			numObjects = numTimes;
			numDimensions = numLoci * numAttributes;
			
			objectList.add(numTimes);
			dimensionList.add(numLoci);
			dimensionList.add(numAttributes);
			dimensionNames.add("l");
			dimensionNames.add("a");
			
		} else if (ts.equalsIgnoreCase("la_t")) {
			
			numObjects = numLoci * numAttributes;
			numDimensions = numTimes;
			
			objectList.add(numLoci);
			objectList.add(numAttributes);
			dimensionList.add(numTimes);
			dimensionNames.add("t");
			
			numCols += 1;
			
		} else if (ts.equalsIgnoreCase("lt_a")) {			
			numObjects = numLoci * numTimes;
			numDimensions = numAttributes;
			
			objectList.add(numLoci);
			objectList.add(numTimes);
			dimensionList.add(numAttributes);
			dimensionNames.add("a");
			
			numCols += 1;
			
		} else if (ts.equalsIgnoreCase("at_l")) {
			
			numObjects = numAttributes * numTimes;
			numDimensions = numLoci;
			
			objectList.add(numAttributes);
			objectList.add(numTimes);
			dimensionList.add(numLoci);
			dimensionNames.add("l");
			
			numCols += 1;
			
		} else {
			return false;
		}
		
		boolean som;
		boolean pca;
		
		// test if SOM or MDS
		if (numObjects > somThreshold) {			
			som = true;
		} else {
			som = false;
		}
		
		if (numDimensions > pcaThreshold || numDimensions > MAX_COLUMNS) {
			pca = true;
		} else {
			pca = false;
		}
		
		// if SOM add additional column for BMU & polygon geometry
		if (som) numCols += 1;
		
		// initialize number of PCA attributes to -1
		int numPCA = -1;
		
		if (!pca) { // if not using PCA
			
			// add the number of dimensions to the number of columns
			numCols += numDimensions;
			
		} else { // if using PCA
									
			if (numObjects < numDimensions) { // if number of objects is less than number of dimensions
				
				// then PCA will use the number of objects as its maximum dimensionality 
				numCols += numObjects;
				numPCA = numObjects;
				
			} else { // if the number of dimensions are less than the number of objects
				
				// then PCA will use the number of dimensions as its maximum dimensionality
				numCols += numDimensions;
				numPCA = numDimensions;
			}
		}
		
		// store the table columns in an array
		String[] tableCols = new String[numCols+1];
		
		// keep track of column index
		int colIdx = 0;
		
		if (objectList.size() == 1) {
			
			// Set default type to SERIAL
			String idType = "SERIAL";
			
			// If number of objects is less than the range for SMALLSERIAL
			if (numObjects > 0 && numObjects <= 32767) {			
				// Use SMALLSERIAL instead of SERIAL
				idType = "SMALLSERIAL";
			} else if (numObjects > 2147483647) {
				// Use BIGSERIAL instead of SERIAL
				idType = "BIGSERIAL";
			}
			
			// id column
			tableCols[colIdx++] = "(id " + idType;
			
			// normalization column
			tableCols[colIdx++] = "normalization SMALLINT";
			
			// assign primary key
			tableCols[tableCols.length-1] = "PRIMARY KEY(id,normalization))";
			
		} else {
			
			// set default type to INTEGER
			String idType1 = "INTEGER";
			String idType2 = "INTEGER";
			
			// if number of objects is less than the range for SMALLINT
			if (objectList.get(0) > 0 && objectList.get(0) <= 32767) {			
				// use SMALLINT instead of INTEGER
				idType1 = "SMALLINT";
			} else if (objectList.get(0) > 2147483647) {
				// use BIGINT instead of INTEGER
				idType1 = "BIGINT";
			}	
			
			// if number of objects is less than the range for SMALLINT
			if (objectList.get(1) > 0 && objectList.get(1) <= 32767) {			
				// use SMALLINT instead of INTEGER
				idType2 = "SMALLINT";
			} else if (objectList.get(1) > 2147483647) {
				// use BIGINT instead of INTEGER
				idType2 = "BIGINT";
			}	
			
			// id1 column
			tableCols[colIdx++] = "(id1 " + idType1;
			
			// id2 column
			tableCols[colIdx++] = "id2 " + idType2;
			
			// normalization column
			tableCols[colIdx++] = "normalization SMALLINT";
			
			// assign primary key
			tableCols[tableCols.length-1] = "PRIMARY KEY(id1,id2,normalization))";
		}
		
		if (numPCA == -1) { // if using regular attributes
			if (dimensionList.size() == 1) {
				for (int i = 1; i <= dimensionList.get(0); i++) {
					String colName = dimensionNames.get(0) + i;
					tableCols[colIdx++] = colName + " NUMERIC";
				}
			} else {
				for (int i = 1; i <= dimensionList.get(0); i++) {
					for (int j = 1; j <= dimensionList.get(1); j++) {
						String colName = dimensionNames.get(0) + i + "_" + dimensionNames.get(1) + j;
						tableCols[colIdx++] = colName + " NUMERIC";
					}
				}
			}			
		} else { // if using PCA components for attributes
			for (int i = 1; i <= numPCA; i++) {
				String colName = "pc" + i;
				tableCols[colIdx++] = colName + " NUMERIC";
			}
		}
		
		if (som) { // if SOM need to add BMU column
			
			int somSize = SOMaticManager.findSOMDimension(numObjects);
			somSize *= somSize;
			
			// Set default type to INTEGER
			String bmuDataType = "INTEGER";
			
			// If number of objects is less than the range for SMALLINT
			if (somSize > 0 && somSize <= 32767) {			
				// Use SMALLINT instead of INTEGER
				bmuDataType = "SMALLINT";
			} else if (somSize > 2147483647) {
				// Use BIGINT instead of INTEGER
				bmuDataType = "BIGINT";
			}	

			tableCols[colIdx++] = "bmu " + bmuDataType;
		}
		
		for (int i = 0; i < classes.length; i++) {
			tableCols[colIdx++] = classes[i] + " TEXT";
		}
		
//		if (som) {
//			tableCols[colIdx++] = "som_pt geometry(POINT)";
//			tableCols[colIdx++] = "som_poly geometry(POLYGON)";
//		} else {
//			tableCols[colIdx++] = "mds_pt geometry(POINT)";
//		}
		
		try {
			return db.createTable(schema + "." + ts.toLowerCase(), tableCols);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	/**
	 * Creates a PostgreSQL table for a particular Tri-Space perspective.
	 *
	 * @param perspective
	 *            the CSV file's current perspective.
	 * @param csv
	 *            the CSV file.
	 * @param output
	 *            the output path including .dat extension.
	 * 
	 */
	public static boolean createTSTable(PostgreSQLJDBC db, String schema, String ts, boolean som) {
						
		int numLoci 		= db.getTableLength(schema, "locus_key");					// number of loci in locus_key table	
		int numAttributes 	= db.getTableLength(schema, "attribute_key");				// number of attributes in attribute_key table				
		int numTimes 		= db.getTableLength(schema, "time_key");					// number of times in time_key table
		
		int numCols 		= 2;			// initialize with 2 columns (id, normalization & ?geometry?) and additional columns for classes
				
		int numObjects 		= -1;														// number of objects in the TS perspective
		int numDimensions 	= -1;														// number of dimensions in the TS perspective
				
		ArrayList<Integer> objectList = new ArrayList<Integer>();						// list to hold number of objects				
		ArrayList<Integer> dimensionList = new ArrayList<Integer>();					// list to hold number of dimensions		
		ArrayList<String> dimensionNames = new ArrayList<String>();						// list to hold the dimension names
		
		if (ts.equalsIgnoreCase("l_at")) {
			
			numObjects = numLoci;			
			numDimensions = numAttributes * numTimes;
			
			objectList.add(numLoci);
			dimensionList.add(numAttributes);
			dimensionList.add(numTimes);
			dimensionNames.add("a");
			dimensionNames.add("t");
			
		} else if (ts.equalsIgnoreCase("a_lt")) {		
			
			numObjects = numAttributes;
			numDimensions = numLoci * numTimes;
			
			objectList.add(numAttributes);
			dimensionList.add(numLoci);
			dimensionList.add(numTimes);
			dimensionNames.add("l");
			dimensionNames.add("t");
			
		} else if (ts.equalsIgnoreCase("t_la")) {
			
			numObjects = numTimes;
			numDimensions = numLoci * numAttributes;
			
			objectList.add(numTimes);
			dimensionList.add(numLoci);
			dimensionList.add(numAttributes);
			dimensionNames.add("l");
			dimensionNames.add("a");
			
		} else if (ts.equalsIgnoreCase("la_t")) {
			
			numObjects = numLoci * numAttributes;
			numDimensions = numTimes;
			
			objectList.add(numLoci);
			objectList.add(numAttributes);
			dimensionList.add(numTimes);
			dimensionNames.add("t");
			
			numCols += 1;
			
		} else if (ts.equalsIgnoreCase("lt_a")) {			
			numObjects = numLoci * numTimes;
			numDimensions = numAttributes;
			
			objectList.add(numLoci);
			objectList.add(numTimes);
			dimensionList.add(numAttributes);
			dimensionNames.add("a");
			
			numCols += 1;
			
		} else if (ts.equalsIgnoreCase("at_l")) {
			
			numObjects = numAttributes * numTimes;
			numDimensions = numLoci;
			
			objectList.add(numAttributes);
			objectList.add(numTimes);
			dimensionList.add(numLoci);
			dimensionNames.add("l");
			
			numCols += 1;
			
		} else {
			return false;
		}		
		
		boolean ignoreData;
		
		// test if SOM or MDS
//		if (numObjects > somThreshold) {			
//			som = true;
//		} else {
//			som = false;
//		}
		
		// test if input vector data will be written to table
		if (numDimensions + numCols > MAX_COLUMNS) {
			ignoreData = true;
		} else {
			ignoreData = false;
			numCols += numDimensions;  					// add the number of dimensions to the number of columns
		}		
								
		if (som) numCols += 1;							// if SOM add additional column for BMU
				
		String[] tableCols = new String[numCols+1];		// store the table columns in an array				
		int colIdx = 0;									// keep track of column index
		
		if (objectList.size() == 1) {												// if NOT composite perspective (L_AT, A_LT, or T_LA)
			
			String idType = "SERIAL";												// set default type to SERIAL
						
			if (numObjects > 0 && numObjects <= 32767) {							// if number of objects is less than the range for SMALLSERIAL						
				idType = "SMALLSERIAL";												// use SMALLSERIAL instead of SERIAL
			} else if (numObjects > 2147483647) {									// otherwise		
				idType = "BIGSERIAL";												// use BIGSERIAL instead of SERIAL
			}
						
			tableCols[colIdx++] 			= "(id " + idType;						// id column					
			tableCols[colIdx++] 			= "normalization SMALLINT";				// normalization column						
			tableCols[tableCols.length-1] 	= "PRIMARY KEY(id,normalization))";		// assign primary key
			
		} else {																		// otherwise it's a composite perspective (LA_T, LT_A, or AT_L)
						
			String idType1 = "INTEGER";													// set default type to INTEGER
			String idType2 = "INTEGER";													// set default type to INTEGER
						
			if (objectList.get(0) > 0 && objectList.get(0) <= 32767) {					// if number of objects is less than the range for SMALLINT	
				idType1 = "SMALLINT";													// use SMALLINT instead of INTEGER
			} else if (objectList.get(0) > 2147483647) {								// otherwise
				idType1 = "BIGINT";														// use BIGINT instead of INTEGER
			}	
						
			if (objectList.get(1) > 0 && objectList.get(1) <= 32767) {					// if number of objects is less than the range for SMALLINT				
				idType2 = "SMALLINT";													// use SMALLINT instead of INTEGER
			} else if (objectList.get(1) > 2147483647) {								// otherwise
				idType2 = "BIGINT";														// use BIGINT instead of INTEGER
			}	
						
			tableCols[colIdx++] 			= "(id1 " + idType1;						// id1 column						
			tableCols[colIdx++] 			= "id2 " + idType2;							// id2 column						
			tableCols[colIdx++] 			= "normalization SMALLINT";					// normalization column						
			tableCols[tableCols.length-1] 	= "PRIMARY KEY(id1,id2,normalization))";	// assign primary key
		}
		
		if (!ignoreData) { 																				// if using regular attributes
			if (dimensionList.size() == 1) {															// if composite perspective (LT_A, LA_T, or AT_L)
				for (int i = 1; i <= dimensionList.get(0); i++) {										// iterate thru dimension names
					String colName = dimensionNames.get(0) + i;											// get dimension name
					tableCols[colIdx++] = colName + " NUMERIC";											// set column name and type to NUMERIC
				}
			} else {																					// if NOT composite perspective (L_AT, A_LT, or T_LA)
				for (int i = 1; i <= dimensionList.get(0); i++) {										// iterate thru first of composite-dimensions
					for (int j = 1; j <= dimensionList.get(1); j++) {									// iterate thru second of composite-dimensions
						String colName = dimensionNames.get(0) + i + "_" + dimensionNames.get(1) + j;	// get composite dimension name
						tableCols[colIdx++] = colName + " NUMERIC";										// set column name and type to NUMERIC
					}
				}
			}			
		}
		
		if (som) { 															// if SOM need to add BMU column
			
			int somSize = SOMaticManager.findSOMDimension(numObjects);		// compute length of SOM
			somSize *= somSize;													// multiply by 2 for square SOM
						
			String bmuDataType = "INTEGER";									// Set default type to INTEGER
						
			if (somSize > 0 && somSize <= 32767) {							// if size of SOM is less than the range for SMALLINT							
				bmuDataType = "SMALLINT";									// Use SMALLINT instead of INTEGER
			} else if (somSize > 2147483647) {								// otherwise
				bmuDataType = "BIGINT";										// Use BIGINT instead of INTEGER
			}	

			tableCols[colIdx++] = "bmu " + bmuDataType;						// set BMU column name and type to NUMERIC
		}
		
//		for (int i = 0; i < classes.length; i++) {							// iterate thru classes
//			tableCols[colIdx++] = classes[i] + " TEXT";						// set column name and type to TEXT
//		}
		
//		if (som) {
//			tableCols[colIdx++] = "som_pt geometry(POINT)";
//			tableCols[colIdx++] = "som_poly geometry(POLYGON)";
//		} else {
//			tableCols[colIdx++] = "mds_pt geometry(POINT)";
//		}
		
		try {
			return db.createTable(schema + "." + ts.toLowerCase(), tableCols);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public static boolean createTable_L_AT(PostgreSQLJDBC db, String schema, int numLoci, boolean pca, boolean som) {
				
		// mode to determine if the A_LT table will utilize PCA instead of regular attributes
		boolean mode = true;
		
		// get the number of attributes from key
		int numAttributes = db.getTableLength(schema, "attribute_key");
		// get the number of times from key
		int numTimes = db.getTableLength(schema, "time_key");

		// initialize with 2 columns (for id & normalization)
		int numCols = 2;
		
		// initialize number of PCA attributes to -1
		int numPCA = -1;
		
		// if SOM add additional column for BMU
		if (som) numCols += 1;

		if (!pca) { // if not using PCA
			
			// set the number of columns to:
			// number of Loci * number of Times * number of normalizations
			numCols += numAttributes * numTimes;
			
		} else { // if using PCA
									
			if (numLoci < numAttributes * numTimes) { // if number of objects is less than number of dimensions
				mode = false;
				
				// then PCA will use the number of objects as its maximum dimensionality 
				numCols += numLoci;
				numPCA = numLoci;
				
			} else { // if the number of dimensions are less than the number of objects
				
				// then PCA will use the number of dimensions as its maximum dimensionality
				numCols += numAttributes * numTimes;
				numPCA = numAttributes * numTimes;
			}
		}
		
		// Set default type to SERIAL
		String idType = "SERIAL";
		
		// If number of objects is less than the range for SMALLSERIAL
		if (numLoci > 0 && numLoci <= 32767) {			
			// Use SMALLSERIAL instead of SERIAL
			idType = "SMALLSERIAL";
		} else if (numLoci > 2147483647) {
			// Use BIGSERIAL instead of SERIAL
			idType = "BIGSERIAL";
		}	
		
		// store the table columns in an array
		String[] tableCols = new String[numCols + 1];
		
		// column for the locus id
		tableCols[0] = "(id " + idType;
		// column for the normalization
		tableCols[1] = "normalization SMALLINT";

		int idxCounter = 2;
		
		if (mode) { // if using regular attributes
			for (int i = 1; i <= numAttributes; i++) {
				for (int j = 1; j <= numTimes; j++) {
					String colName = "a" + i + "_t" + j;
					tableCols[idxCounter++] = colName + " NUMERIC";
				}
			}

			
		} else { // if using PCA components for attributes
			for (int i = 1; i <= numPCA; i++) {
				String colName = "pc" + i;
				tableCols[idxCounter++] = colName + " NUMERIC";
			}
		}
		
		if (som) { // if SOM need to add BMU column
			
			int somSize = SOMaticManager.findSOMDimension(numLoci);
			somSize *= somSize;
			
			// Set default type to INTEGER
			String bmuDataType = "INTEGER";
			
			// If number of objects is less than the range for SMALLINT
			if (somSize > 0 && somSize <= 32767) {			
				// Use SMALLINT instead of INTEGER
				bmuDataType = "SMALLINT";
			} else if (somSize > 2147483647) {
				// Use BIGINT instead of INTEGER
				bmuDataType = "BIGINT";
			}	

			tableCols[idxCounter++] = "bmu " + bmuDataType;
		}

		tableCols[tableCols.length-1] = "PRIMARY KEY(id,normalization))";

		try {
			return db.createTable(schema + ".l_at", tableCols);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public void createTSTable(PostgreSQLJDBC db, String schema, String ts, String filePath) throws IOException, SQLException {

		if (ts.equals("L_AT")) {
			BufferedReader br = new BufferedReader(new FileReader(filePath));			
			String line = br.readLine();
			String[] header = line.split(",");
			line = br.readLine();
			String[] header2 = line.split(",");			
			String[] tableCols = new String[1 + (header.length-1)*7];			
			tableCols[0] = "(LID TEXT PRIMARY KEY NOT NULL";
			int index = 1;
			for (int j = 0; j < 7; j++) {
				for (int i = 1; i < header.length; i++) {
					if (j == 0) {
						tableCols[index] = header[i]+"_"+header2[i] + "_" + j + " INT NOT NULL"; 
						index++;
					} else {
						tableCols[index] = header[i]+"_"+header2[i] + "_" + j + " NUMERIC NOT NULL"; 
						index++;
					}					
				}	
			}

			for (int i = 0; i < tableCols.length; i++) {
				System.out.println(tableCols[i]);
			}
			tableCols[tableCols.length-1] = tableCols[tableCols.length-1] + ")";
			db.createTable(schema + ".l_at", tableCols);
			br.close();
		} else if (ts.equals("AT_L")) {
			BufferedReader br = new BufferedReader(new FileReader(filePath));			
			String line = br.readLine();
			String[] header = line.split(",");
			line = br.readLine();
			String[] header2 = line.split(",");			
			String[] tableCols = new String[1 + (header.length-1)*7];			
			tableCols[0] = "(AID TEXT PRIMARY KEY NOT NULL";
			int index = 1;
			for (int j = 0; j < 7; j++) {
				for (int i = 1; i < header.length; i++) {
					if (j == 0) {
						tableCols[index] = header[i] + "_" + header2[i] + "_" + j + " INT NOT NULL"; 
						index++;
					} else {
						tableCols[index] = header[i] + "_" + header2[i] + "_" + j + " NUMERIC NOT NULL"; 
						index++;
					}					
				}	
			}

			for (int i = 0; i < tableCols.length; i++) {
				System.out.println(tableCols[i]);
			}
			tableCols[tableCols.length-1] = tableCols[tableCols.length-1] + ")";
			db.createTable(schema + ".at_l", tableCols);
			br.close();
		}
	}

	public static boolean createSSETable(PostgreSQLJDBC db, String schema, int kMax) {
		int numCols = (kMax - 1) * 6;

		numCols++;

		String[] tableCols = new String[numCols];
		tableCols[0] = "(ID TEXT PRIMARY KEY NOT NULL";


		int kVal = 2;
		int normVal = 1;
		for (int i = 1; i < numCols; i++) {
			tableCols[i] = "k" + kVal++ + "_n" + normVal + " NUMERIC";
			if (kVal > kMax) {
				kVal = 2;
				normVal++;
			}
		}

		tableCols[numCols - 1] = tableCols[numCols - 1] + ")";

		try {
			return db.createTable(schema + ".sse", tableCols);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public static boolean insert2SSETable(PostgreSQLJDBC db, String schema, int kMax, boolean batch) {
		String[] perspectives = {"l_at", "a_lt", "t_la", "la_t", "lt_a", "at_l"};

		if (!batch) {
			for (int i = 0; i < perspectives.length; i++) {
				try {
					String[] insertData = new String[db.getTableColumnIds(schema + ".sse").length];
					insertData[0] = "'" + perspectives[i] + "'";
					for (int j = 1; j < insertData.length; j++) {
						insertData[j] = "-1";
					}
					if (!db.insertIntoTable(schema + ".sse", insertData)) {
						return false;
					}
					//				insertData[insertData.length-1] = insertData[insertData.length-1] + ")";
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return false;
				}
			}
		} else {
			ArrayList<String> batchInsertion = new ArrayList<String>();
			for (int i = 0; i < perspectives.length; i++) {
				try {
					String[] insertData = new String[db.getTableColumnIds(schema + ".sse").length];
					insertData[0] = "'" + perspectives[i] + "'";
					for (int j = 1; j < insertData.length; j++) {
						insertData[j] = "-1";
					}
					batchInsertion.add(db.insertQueryBuilder(schema + ".sse",insertData));
//					if (!db.insertIntoTable(schema + ".sse", insertData)) {
//						return false;
//					}
					//				insertData[insertData.length-1] = insertData[insertData.length-1] + ")";
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return false;
				}
			}
			
			String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
			try {
				db.insertIntoTableBatch(schema + ".sse", batchInsertionAr);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		}
		
		return true;

	}
	
	
	/**
	 * Add geometry columns to a Tri-Space perspective table (a_lt, t_la, at_l, etc.).
	 *
	 * @param db
	 *            - An instance of the PostgreSQLJDBC class.
	 * @param schema
	 *            - The schema in Postgres to write to.
	 * @param perspective
	 *            - The Tri-Space perspective to write to.
	 * @param column
	 *            - The name of the geometry table.
	 * @param geomType
	 *            - The type of geometry (POINT, POLYGON, etc.)
	 */
	public static boolean addGeometryColumn(PostgreSQLJDBC db, String schema, String perspective, String columnName, String geomType) {
		
		if (!db.addColumn(schema + "." + perspective.toLowerCase(), columnName, "geometry(" + geomType + ")")) {
			return false;
		}
		
		return true;
	}

	public static boolean createMDSTable(PostgreSQLJDBC db, String schema, String tsType, int kMax) {
		
		int numPrincipalComponents = 0;
		int numLoci = db.getTableLength(schema, "locus_key");
		int numTimes = db.getTableLength(schema, "time_key");
		int numAttributes = db.getTableLength(schema, "attribute_key");
		
		int maxColumns = 1599;
		int defaultColumns = 7;
		
		// Set default type to INTEGER
		String idType1 = "INTEGER";
		String idType2 = "";
		
		if (tsType.equalsIgnoreCase("A_LT")) {
			
			numPrincipalComponents = numAttributes;
			
			if (numTimes * numLoci < numPrincipalComponents) {
				numPrincipalComponents = numTimes * numLoci;
			}
			
			// If number of objects is less than the range for SMALLSERIAL
			if (numAttributes > 0 && numAttributes <= 32767) {			
				// Use SMALLSERIAL instead of SERIAL
				idType1 = "SMALLINT";
			} else if (numAttributes > 2147483647) {
				// Use BIGSERIAL instead of SERIAL
				idType1 = "BIGINT";
			}	
			
		} else if (tsType.equalsIgnoreCase("T_LA")) {
			
			numPrincipalComponents = numTimes;
			
			if (numAttributes * numLoci < numPrincipalComponents) {
				numPrincipalComponents = numAttributes * numLoci;
			}
			
			// If number of objects is less than the range for SMALLSERIAL
			if (numTimes > 0 && numTimes <= 32767) {			
				// Use SMALLSERIAL instead of SERIAL
				idType1 = "SMALLINT";
			} else if (numTimes > 2147483647) {
				// Use BIGSERIAL instead of SERIAL
				idType1 = "BIGINT";
			}	
		} else if (tsType.equalsIgnoreCase("AT_L")) {
			
			maxColumns--;
			defaultColumns++;
			
			numPrincipalComponents = numAttributes * numTimes;
			if (numLoci < numPrincipalComponents) {
				numPrincipalComponents = numLoci;
			}
			
			idType2 = "INTEGER";
			
			// If number of objects is less than the range for SMALLSERIAL
			if (numAttributes > 0 && numAttributes <= 32767) {			
				// Use SMALLSERIAL instead of SERIAL
				idType1 = "SMALLINT";
			} else if (numAttributes > 2147483647) {
				// Use BIGSERIAL instead of SERIAL
				idType1 = "BIGINT";
			}	
			
			// If number of objects is less than the range for SMALLSERIAL
			if (numTimes > 0 && numTimes <= 32767) {			
				// Use SMALLSERIAL instead of SERIAL
				idType2 = "SMALLINT";
			} else if (numTimes > 2147483647) {
				// Use BIGSERIAL instead of SERIAL
				idType2 = "BIGINT";
			}
		}  else if (tsType.equalsIgnoreCase("L_AT")) {
			
			numPrincipalComponents = numLoci;
			
			if (numAttributes * numTimes < numPrincipalComponents) {
				numPrincipalComponents = numAttributes * numTimes;
			}
			
			// If number of objects is less than the range for SMALLSERIAL
			if (numLoci > 0 && numLoci <= 32767) {			
				// Use SMALLSERIAL instead of SERIAL
				idType1 = "SMALLINT";
			} else if (numLoci > 2147483647) {
				// Use BIGSERIAL instead of SERIAL
				idType1 = "BIGINT";
			}	
		} else return false;
		
		int totalNumPCA = numPrincipalComponents * 6;
		
		
		maxColumns -= 7;
//		maxColumns -= totalNumPCA;
		
		int totalKCols = (kMax - 1) * 6;
		
		if (totalKCols > maxColumns) {
			return false;
		}
		

//		int numCols = 7 + totalKCols + totalNumPCA;
		int numCols = defaultColumns + totalKCols; 
		
		String[] tableCols = null;
		int startIdx = 2;
		if (idType2.equals("")) {
			startIdx = 1;
			tableCols = new String[numCols + 1];
			tableCols[0] = "(id " + idType1;	
		} else {
			tableCols = new String[numCols + 1];
			tableCols[0] = "(id1 " + idType1;
			tableCols[1] = "id2 " + idType2;	
		}
		
		

		

//		String[] tableCols = new String[numCols];
//		tableCols[0] = "(ID TEXT PRIMARY KEY NOT NULL";

		//		for (int i = 1; i < 7; i++) {
		//			tableCols[i] = "IVs_n" + i + " INTEGER";
		//		}

		int normCount = 1;
		int kCount = 2;
		for (int i = startIdx; i < numCols - 6; i++) {
			tableCols[i] = "k" + (kCount++) + "_n" + normCount + " SMALLINT";
			if (kCount > kMax) {
				kCount = 2;
				normCount++;
			}
		}

//		normCount = 1;
//		int pcaCount = 1;
//		for (int i = numCols - 6 - totalNumPCA; i < numCols - 6; i++) {
//			tableCols[i] = "pca" + (pcaCount++) + "_n" + normCount + " SMALLINT";
//			if (pcaCount > numPrincipalComponents) {
//				pcaCount = 1;
//				normCount++;
//			}
//		}
		
		int geomIdx = 1;
		for (int i = numCols - 6; i < numCols; i++) {
			tableCols[i] = "geom_n" + geomIdx++ + " geometry(POINT)";
		}


//		tableCols[numCols-1] = tableCols[numCols-1] + ")";
		
		if (idType2.equals("")) {
			tableCols[tableCols.length-1] = "PRIMARY KEY(id))";
		} else {
			tableCols[tableCols.length-1] = "PRIMARY KEY(id1,id2))";
		}
		

		try {
			//			return db.createTable(schema + ".l_at", tableCols);
			return db.createTable(schema + "." + tsType.toLowerCase() + "_geom", tableCols);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	public static boolean createPCATable(PostgreSQLJDBC db, String schema) {		

		int numCols = 7; 

		String[] tableCols = new String[numCols];
		tableCols[0] = "(ts_type SMALLINT";
		tableCols[1] = "pca_id SMALLINT";
		tableCols[2] = "normalization SMALLINT";
		tableCols[3] = "variance NUMERIC";
		tableCols[4] = "cumulative_variance NUMERIC";
		tableCols[5] = "std_dev NUMERIC";
		tableCols[6] = "PRIMARY KEY(ts_type, pca_id, normalization))";

		try {
			return db.createTable(schema + ".pca", tableCols);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	public static boolean populatePCATable(PostgreSQLJDBC db, String schema, int perspectiveIdx, String folder, boolean batch) {		
		
		String[] normalizations = {"L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L"};

		int dataLength = 0;
		try {
			dataLength = db.getTableColumnIds(schema + ".pca").length;
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return false;
		}
		
		File f = new File(folder);
		// test if file is a directory
		if (f.isDirectory()) {
			
			// store children to array
			File[] children = f.listFiles();
			
			// loop thru each child-combination to perform common loci test
			for (int i = 0; i < children.length; i++) {
				ArrayList<String> batchInsertion = new ArrayList<String>();
				String[] nameSplit = children[i].getName().split("\\.");
				int normIdx = -1;
				for (int j = 0; j < normalizations.length; j++) {
					if (nameSplit[0].equalsIgnoreCase(normalizations[j])) {
						normIdx = j + 1;
						break;
					}
				}
//				System.out.println(children[i].getName());
				try {
//					String[] insertData = new String[dataLength];
					BufferedReader br = new BufferedReader(new FileReader(children[i].getPath()));
					String line = br.readLine();
					String[] lineSplit = line.split(",");
					
					String[][] csvData = new String[4][lineSplit.length-1];
					
					for (int j = 1; j < lineSplit.length; j++) {
						csvData[0][j-1] = lineSplit[j];
					}
					
					
					for (int j = 1; j < 4; j++) {
						line = br.readLine();
						lineSplit = line.split(",");
						for (int k = 1; k < lineSplit.length; k++) {
							csvData[j][k-1] = lineSplit[k];
						}						
					}
					
					for (int j = 0; j < csvData[0].length; j++) {
						String[] insertData = new String[dataLength];
						insertData[0] = "'" + perspectiveIdx + "'";
						insertData[1] = "'" + csvData[0][j].substring(2) + "'";
						insertData[2] = "'" +  normIdx + "'";
						insertData[5] = csvData[1][j];
						insertData[3] = csvData[2][j];
						insertData[4] = csvData[3][j];
						
						
						if (!batch) {
							db.insertIntoTable(schema + ".pca",insertData);
						} else {
							batchInsertion.add(db.insertQueryBuilder(schema + ".pca",insertData));
						}											
					}
					
					if (batch) {
						String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
						db.insertIntoTableBatch(schema + ".pca", batchInsertionAr);
					}				
					
					br.close();
				} catch (IOException | SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} else {
			return false;
		}
		return true;
	}
	
	public static boolean populateTSPCATable(PostgreSQLJDBC db, String schema, String tsPerspective, String folder, boolean batch) {		
		
		String[] normalizations = {"L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L"};

		int dataLength = 0;
		try {
			dataLength = db.getTableColumnIds(schema + ".pca").length;
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return false;
		}
		
		File f = new File(folder);
		// test if file is a directory
		if (f.isDirectory()) {
			
			// store children to array
			File[] children = f.listFiles();
			BufferedReader[] br = new BufferedReader[children.length];
			int[] normOrder = new int[6];
			for (int i = 0; i < children.length; i++) {
				BufferedReader tmpBR = null;
				try {
					tmpBR = new BufferedReader(new FileReader(children[i].getPath()));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				br[i] = tmpBR;
				
				String[] nameSplit = children[i].getName().split("\\.");
//				int normIdx = -1;
				for (int j = 0; j < normalizations.length; j++) {
					if (nameSplit[0].equalsIgnoreCase(normalizations[j])) {
//						normIdx = j;
						normOrder[i] = j;
						break;
					}
				}
			}
			
			BufferedReader[] brSorted = new BufferedReader[children.length];
			for (int i = 0; i < normOrder.length; i++) {
				brSorted[normOrder[i]] = br[i];
			}
			
			// remove headers and metadata
			for (int i = 0; i < brSorted.length; i++) {
				try {
					brSorted[i].readLine();
					brSorted[i].readLine();
					brSorted[i].readLine();
					brSorted[i].readLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return false;
				}				
			}
			
			ArrayList<String> batchInsertion = new ArrayList<String>();
			
			String[] line =  new String[6];
			try {
				while ((line[0] = brSorted[0].readLine()) != null) {
					
					//					System.out.println(line[0]);
					for (int i = 1; i < brSorted.length; i++) {
						line[i] = brSorted[i].readLine();
					}
					String[][] data = new String[6][];
					for (int i = 0; i < data.length; i++) {
						data[i] = line[i].split(",");
					}
					//					String[] data = line.split(",");
					String[] insertData;
					
					try {
						insertData = new String[db.getTableColumnIds(schema + "." + tsPerspective).length];
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						
						e.printStackTrace();
						return false;
					}
//				String uid = data[0][0] + "_" + data[0][1];
					//					uid = uid.substring(1, uid.length());
					int counterStart = 2;
					if (tsPerspective.equalsIgnoreCase("at_l") || tsPerspective.equalsIgnoreCase("lt_a") || tsPerspective.equalsIgnoreCase("la_t")) {
						String[] dataSplit = data[0][0].split("_");
//						insertData[0] = "'"+data[0][0]+"'";
						insertData[0] = "'" + dataSplit[0].substring(1) + "'";
						insertData[1] = "'" + dataSplit[1].substring(1) + "'";
					} else if (tsPerspective.equalsIgnoreCase("l_at") || tsPerspective.equalsIgnoreCase("a_lt") || tsPerspective.equalsIgnoreCase("t_la") ) {
						insertData[0] = "'" + data[0][0].substring(1) + "'";
						counterStart = 1;
					}
					
//					System.out.println(insertData[0]);
					int counter1 = 0;
					int counter2 = 1;

					for (int i = counterStart; i < insertData.length; i++) {
						insertData[i] = data[counter1][counter2];
						if (counter2 >= data[counter1].length-1) {
							counter1++;
							counter2 = 1;
						} else {
							counter2++;
						}
					}
					
					if (!batch) {
						db.insertIntoTable(schema + "." + tsPerspective,insertData);
					} else {
						batchInsertion.add(db.insertQueryBuilder(schema + "." + tsPerspective,insertData));
					}	
					//					db.insertIntoTable(schema + ".la_t",insertData);
//				String queryStatement = db.insertQueryBuilder(schema + "." + tsPerspective,insertData);
//				batchInsertion.add(queryStatement);
				}
			} catch (IOException | SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
			for (int i = 0; i < brSorted.length; i++) {
				try {
					brSorted[i].close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return false;
				}
			}	
			
			if (batch) {
				String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
				try {
					db.insertIntoTableBatch(schema + "." + tsPerspective, batchInsertionAr);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return false;
				}
			}		
		}
		return true;
	}

	
	/**
	 * Create table to store the geometry of the SOM.
	 *
	 * @param db
	 *            - An instance of the PostgreSQLJDBC class.
	 * @param schema
	 *            - The schema in Postgres to write to.
	 * @param perspective
	 *            - The Tri-Space perspective to write to.
	 * @param numObjects
	 * 			  - The number of neurons in the SOM.
	 */
	public static boolean createSOMTable(PostgreSQLJDBC db, String schema, String perspective, int numNeurons) {	
		
		// default number of columns
		// id, geom
		int numCols = 2;			

		String[] tableCols = new String[numCols+1];
		
		// Set default type to INTEGER		
		String idType = "SERIAL";
		
		// If number of objects is less than the range for SMALLSERIAL
		if (numNeurons > 0 && numNeurons <= 32767) {			
			// Use SMALLSERIAL instead of SERIAL
			idType = "SMALLSERIAL";
		} else if (numNeurons > 2147483647) {
			// Use BIGSERIAL instead of SERIAL
			idType = "BIGSERIAL";
		}	
				
		tableCols[0] = "(id " + idType;
		tableCols[1] = "geom geometry(POLYGON)";		
		tableCols[2] = "PRIMARY KEY(id))";

		try {
			return db.createTable(schema + "." + perspective.toLowerCase() + "_som", tableCols);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	
	/**
	 * Create a table to store the attributes of the SOM neurons.
	 *
	 * @param db
	 *            - An instance of the PostgreSQLJDBC class.
	 * @param schema
	 *            - The schema in Postgres to write to.
	 * @param perspective
	 *            - The Tri-Space perspective to write to.
	 * @param numObjects
	 * 			  - The number of neurons in the SOM.
	 */
	public static boolean createNeuronTable(PostgreSQLJDBC db, String schema, String ts, int numObjects) {
				
		int numLoci 		= db.getTableLength(schema, "locus_key");		// get the number of loci				
		int numAttributes 	= db.getTableLength(schema, "attribute_key");	// get the number of attributes				
		int numTimes 		= db.getTableLength(schema, "time_key");		// get the number of times
		
		int numCols 		= 4;											// initialize with 3 columns (id, normalization, ivs & num_ivs) and additional columns for classes
		
		int numDimensions 	= -1;											// number of dimensions in the TS perspective
		
		ArrayList<Integer> objectList = new ArrayList<Integer>();			// list to hold number of objects				
		ArrayList<Integer> dimensionList = new ArrayList<Integer>();		// list to hold number of dimensions		
		ArrayList<String> dimensionNames = new ArrayList<String>();			// list to hold the dimension names
		
		if (ts.equalsIgnoreCase("l_at")) {
						
			numDimensions = numAttributes * numTimes;
			
			objectList.add(numLoci);
			dimensionList.add(numAttributes);
			dimensionList.add(numTimes);
			dimensionNames.add("a");
			dimensionNames.add("t");
			
		} else if (ts.equalsIgnoreCase("a_lt")) {		
			
			numDimensions = numLoci * numTimes;
			
			objectList.add(numAttributes);
			dimensionList.add(numLoci);
			dimensionList.add(numTimes);
			dimensionNames.add("l");
			dimensionNames.add("t");
			
		} else if (ts.equalsIgnoreCase("t_la")) {
			
			numDimensions = numLoci * numAttributes;
			
			objectList.add(numTimes);
			dimensionList.add(numLoci);
			dimensionList.add(numAttributes);
			dimensionNames.add("l");
			dimensionNames.add("a");
			
		} else if (ts.equalsIgnoreCase("la_t")) {
			
			numDimensions = numTimes;
			
			objectList.add(numLoci);
			objectList.add(numAttributes);
			dimensionList.add(numTimes);
			dimensionNames.add("t");
			
		} else if (ts.equalsIgnoreCase("lt_a")) {			

			numDimensions = numAttributes;
			
			objectList.add(numLoci);
			objectList.add(numTimes);
			dimensionList.add(numAttributes);
			dimensionNames.add("a");
			
		} else if (ts.equalsIgnoreCase("at_l")) {
			
			numDimensions = numLoci;
			
			objectList.add(numAttributes);
			objectList.add(numTimes);
			dimensionList.add(numLoci);
			dimensionNames.add("l");
			
		} else {
			
			return false;
			
		}		
	
		if (numDimensions + numCols > MAX_COLUMNS) {
			System.out.println("COLUMNS EXCEED POSTGRES LIMITS");
			return false;			
		}

		numCols += numDimensions;  					// add the number of dimensions to the number of columns
		
		int colIdx = 0;

		String[] tableCols = new String[numCols+1];
		
		// Set default type to INTEGER		
		String idType = "INTEGER";
		
		// If number of objects is less than the range for SMALLSERIAL
		if (numObjects > 0 && numObjects <= 32767) {			
			// Use SMALLSERIAL instead of SERIAL
			idType = "SMALLINT";
		} else if (numObjects > 2147483647) {
			// Use BIGSERIAL instead of SERIAL
			idType = "BIGINT";
		}	
		
		
		tableCols[colIdx++] = "(id " + idType;
		tableCols[colIdx++] = "normalization SMALLINT";
		tableCols[colIdx++] = "ivs TEXT";				
		tableCols[colIdx++] = "num_ivs INTEGER";

//		for (int i = 0; i < classes.length; i++) {
//			tableCols[colIdx++] = classes[i] + " TEXT";
//		}
		
		if (dimensionList.size() == 1) {															// if composite perspective (LT_A, LA_T, or AT_L)
			for (int i = 1; i <= dimensionList.get(0); i++) {										// iterate thru dimension names
				String colName = dimensionNames.get(0) + i;											// get dimension name
				tableCols[colIdx++] = colName + " NUMERIC";											// set column name and type to NUMERIC
			}
		} else {																					// if NOT composite perspective (L_AT, A_LT, or T_LA)
			for (int i = 1; i <= dimensionList.get(0); i++) {										// iterate thru first of composite-dimensions
				for (int j = 1; j <= dimensionList.get(1); j++) {									// iterate thru second of composite-dimensions
					String colName = dimensionNames.get(0) + i + "_" + dimensionNames.get(1) + j;	// get composite dimension name
					tableCols[colIdx++] = colName + " NUMERIC";										// set column name and type to NUMERIC
				}
			}
		}	

		tableCols[numCols] = "PRIMARY KEY(id, normalization))";

		try {
			//			return db.createTable(schema + ".l_at", tableCols);
			return db.createTable(schema + "." + ts.toLowerCase() + "_neurons", tableCols);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	

	public static void insert2SOM(PostgreSQLJDBC db, String schema, String ts, String filePath, int kMax, boolean batch) {
		try {
			
			// get the number of loci
			int numLoci = db.getTableLength(schema, "locus_key");
			
			// get the number of attributes
			int numAttributes = db.getTableLength(schema, "attribute_key");
			
			// get the number of times
			int numTimes = db.getTableLength(schema, "time_key");	

			// default number of columns
			// id, ivs_n1-6, num_ivs_n1-6, geom
			int numCols = 14;
			
			// additional kmeans columns
			int totalKCols = (kMax - 1) * 6;
			
			if (ts.equalsIgnoreCase("l_at")) {
				int totalAttrCols = numAttributes * numTimes * 6;
				numCols += totalAttrCols;			
			} else if (ts.equalsIgnoreCase("la_t")) {
				int totalAttrCols = numTimes * 6;
				numCols += totalAttrCols + 6;						
			} else if (ts.equalsIgnoreCase("lt_a")) {
				int totalAttrCols = numAttributes * 6;
				numCols += totalAttrCols + 12;			
			} else if (ts.equalsIgnoreCase("a_lt")) {
				int totalAttrCols = numLoci * numTimes * 6;
				numCols += totalAttrCols;			
			}  else if (ts.equalsIgnoreCase("t_la")) {
				int totalAttrCols = numLoci * numAttributes * 6;
				numCols += totalAttrCols;			
			}  else if (ts.equalsIgnoreCase("at_l")) {
				int totalAttrCols = numLoci * 6;
				numCols += totalAttrCols;						
			}  else {
				System.out.println("Trispace Type not implemented, see CSVManager.createSOMTable()");
				return;
			}
			
			numCols += totalKCols;
				
			if ((1599 - numCols) < 0) {
				System.out.println("COLUMNS EXCEED POSTGRES LIMITS");
				return;	
			}
			

			JSONObject jsonObj;
			try {
				jsonObj = JSONUtil.parseJSONFile(filePath);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				jsonObj = null;
			}
			JSONArray features = jsonObj.getJSONArray("features");
			ArrayList<String> batchInsertion = new ArrayList<String>();
			
			String[] insertData = new String[numCols];
			insertData[0] = "";
			insertData[numCols-1] = "";
			
			// initialize each of the input vector columns (ivs_n1, ivs_n2, ...) to -1
			for (int j = 1; j < 7; j++) {
				insertData[j] = "-1";
			}
			
			// initialize each of the IV count columns (num_ivs_n1, num_ivs_n2, ...) to 0
			for (int j = 7; j < 13; j++) {
				insertData[j] = "0";
			}

			// initialize all remaining columns (kmeans, atomic values, class)
			for (int j = 13; j < numCols-1; j++) {
				insertData[j] = "-1";
			}

			for (int i = 0; i < features.length(); i++) {
				JSONObject geometry = features.getJSONObject(i).getJSONObject("geometry");
				JSONArray coordinates = geometry.getJSONArray("coordinates");
				JSONArray coordinates2 = coordinates.getJSONArray(0);
				JSONObject properties = features.getJSONObject(i).getJSONObject("properties");
				//				JSONArray coordi

				int tmpID = (int) properties.get("id");

				String id = tmpID + "";

				// String array to hold the data for postgres insert query
//				String[] insertData = new String[numCols];
				
				// get the id from GeoJSON feature
				insertData[0] = "'" + id + "'";

				// initialize each of the input vector columns (ivs_n1, ivs_n2, ...) to -1
//				for (int j = 1; j < 7; j++) {
//					insertData[j] = "-1";
//				}
//				
//				// initialize each of the IV count columns (num_ivs_n1, num_ivs_n2, ...) to 0
//				for (int j = 7; j < 13; j++) {
//					insertData[j] = "0";
//				}
//
//				// initialize all remaining columns (kmeans, atomic values, class)
//				for (int j = 13; j < numCols-1; j++) {
//					insertData[j] = "-1";
//				}
				
				// store geometry
				JSONArray coords3 = coordinates2.getJSONArray(0);
				insertData[numCols-1] = "ST_GeomFromText('POLYGON((" + coords3.getString(0) + " " + coords3.getString(1);


				for (int j = 1; j < coordinates2.length(); j++) {
					coords3 = coordinates2.getJSONArray(j);

					insertData[numCols-1] = insertData[numCols-1] + "," + coords3.getString(0) + " " + coords3.getString(1);
				}

				insertData[numCols-1] = insertData[numCols-1] + "))')";


				if (!batch) {
					db.insertIntoTable(schema + "." + ts.toLowerCase() + "_geom",insertData);
				} else {
					batchInsertion.add(db.insertQueryBuilder(schema + "." + ts.toLowerCase() + "_geom",insertData));					
				}				
			}
			
			if (batch) {
				String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
				db.insertIntoTableBatch(schema + "." + ts.toLowerCase() + "_geom", batchInsertionAr);
			}

		} catch (JSONException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void insert2SOMArray(PostgreSQLJDBC db, String schema, String ts, String filePath, int kMax, boolean batch) {
		try {
			
			// get the number of loci
			int numLoci = db.getTableLength(schema, "locus_key");
			
			// get the number of attributes
			int numAttributes = db.getTableLength(schema, "attribute_key");
			
			// get the number of times
			int numTimes = db.getTableLength(schema, "time_key");	

			// default number of columns
			// id, ivs_n1-6, num_ivs_n1-6, geom
			int numCols = 14;
			
			// additional kmeans columns
			int totalKCols = (kMax - 1) * 6;
			
			if (ts.equalsIgnoreCase("l_at")) {
				int totalAttrCols = numAttributes * numTimes * 6;
				numCols += totalAttrCols;			
			} else if (ts.equalsIgnoreCase("la_t")) {
				int totalAttrCols = numTimes * 6;
				numCols += totalAttrCols + 6;						
			} else if (ts.equalsIgnoreCase("lt_a")) {
				int totalAttrCols = numAttributes * 6;
				numCols += totalAttrCols + 12;			
			} else if (ts.equalsIgnoreCase("a_lt")) {
				int totalAttrCols = numLoci * numTimes * 6;
				numCols += totalAttrCols;			
			}  else if (ts.equalsIgnoreCase("t_la")) {
				int totalAttrCols = numLoci * numAttributes * 6;
				numCols += totalAttrCols;			
			}  else if (ts.equalsIgnoreCase("at_l")) {
				int totalAttrCols = numLoci * 6;
				numCols += totalAttrCols;						
			}  else {
				System.out.println("Trispace Type not implemented, see CSVManager.createSOMTable()");
				return;
			}
			
			numCols += totalKCols;
				
			if ((1599 - numCols) < 0) {
				System.out.println("COLUMNS EXCEED POSTGRES LIMITS");
				return;	
			}
			

			JSONObject jsonObj;
			try {
				jsonObj = JSONUtil.parseJSONFile(filePath);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				jsonObj = null;
			}
			JSONArray features = jsonObj.getJSONArray("features");
			String[] batchInsertion = new String[features.length()];
			int arrayIdx = 0;
			
			String[] insertData = new String[numCols];
			insertData[0] = "";
			insertData[numCols-1] = "";
			
			// initialize each of the input vector columns (ivs_n1, ivs_n2, ...) to -1
			for (int j = 1; j < 7; j++) {
				insertData[j] = "-1";
			}
			
			// initialize each of the IV count columns (num_ivs_n1, num_ivs_n2, ...) to 0
			for (int j = 7; j < 13; j++) {
				insertData[j] = "0";
			}

			// initialize all remaining columns (kmeans, atomic values, class)
			for (int j = 13; j < numCols-1; j++) {
				insertData[j] = "-1";
			}
			
//			ArrayList<String> batchInsertion = new ArrayList<String>();

			for (int i = 0; i < features.length(); i++) {
				JSONObject geometry = features.getJSONObject(i).getJSONObject("geometry");
				JSONArray coordinates = geometry.getJSONArray("coordinates");
				JSONArray coordinates2 = coordinates.getJSONArray(0);
				JSONObject properties = features.getJSONObject(i).getJSONObject("properties");
				//				JSONArray coordi

				int tmpID = (int) properties.get("id");

				String id = tmpID + "";

				// String array to hold the data for postgres insert query
//				String[] insertData = new String[numCols];
				
				// get the id from GeoJSON feature
				insertData[0] = "'" + id + "'";

				// initialize each of the input vector columns (ivs_n1, ivs_n2, ...) to -1
//				for (int j = 1; j < 7; j++) {
//					insertData[j] = "-1";
//				}
//				
//				// initialize each of the IV count columns (num_ivs_n1, num_ivs_n2, ...) to 0
//				for (int j = 7; j < 13; j++) {
//					insertData[j] = "0";
//				}
//
//				// initialize all remaining columns (kmeans, atomic values, class)
//				for (int j = 13; j < numCols-1; j++) {
//					insertData[j] = "-1";
//				}
				
				// store geometry
				JSONArray coords3 = coordinates2.getJSONArray(0);
				insertData[numCols-1] = "ST_GeomFromText('POLYGON((" + coords3.getString(0) + " " + coords3.getString(1);


				for (int j = 1; j < coordinates2.length(); j++) {
					coords3 = coordinates2.getJSONArray(j);

					insertData[numCols-1] = insertData[numCols-1] + "," + coords3.getString(0) + " " + coords3.getString(1);
				}

				insertData[numCols-1] = insertData[numCols-1] + "))')";


				if (!batch) {
					db.insertIntoTable(schema + "." + ts.toLowerCase() + "_geom",insertData);
				} else {
					batchInsertion[arrayIdx++] = (db.insertQueryBuilder(schema + "." + ts.toLowerCase() + "_geom",insertData));					
				}				
			}
			
			if (batch) {
//				String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
				db.insertIntoTableBatch(schema + "." + ts.toLowerCase() + "_geom", batchInsertion);
			}

		} catch (JSONException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void insert2SOMArray(PostgreSQLJDBC db, String schema, String ts, String filePath, int kMax, boolean batch, int batchSize) {
		try {
			
			// get the number of loci
			int numLoci = db.getTableLength(schema, "locus_key");
			
			// get the number of attributes
			int numAttributes = db.getTableLength(schema, "attribute_key");
			
			// get the number of times
			int numTimes = db.getTableLength(schema, "time_key");	

			// default number of columns
			// id, ivs_n1-6, num_ivs_n1-6, geom
			int numCols = 14;
			
			// additional kmeans columns
			int totalKCols = (kMax - 1) * 6;
			
			if (ts.equalsIgnoreCase("l_at")) {
				int totalAttrCols = numAttributes * numTimes * 6;
				numCols += totalAttrCols;			
			} else if (ts.equalsIgnoreCase("la_t")) {
				int totalAttrCols = numTimes * 6;
				numCols += totalAttrCols + 6;						
			} else if (ts.equalsIgnoreCase("lt_a")) {
				int totalAttrCols = numAttributes * 6;
				numCols += totalAttrCols + 12;			
			} else if (ts.equalsIgnoreCase("a_lt")) {
				int totalAttrCols = numLoci * numTimes * 6;
				numCols += totalAttrCols;			
			}  else if (ts.equalsIgnoreCase("t_la")) {
				int totalAttrCols = numLoci * numAttributes * 6;
				numCols += totalAttrCols;			
			}  else if (ts.equalsIgnoreCase("at_l")) {
				int totalAttrCols = numLoci * 6;
				numCols += totalAttrCols;						
			}  else {
				System.out.println("Trispace Type not implemented, see CSVManager.createSOMTable()");
				return;
			}
			
			numCols += totalKCols;
				
			if ((1599 - numCols) < 0) {
				System.out.println("COLUMNS EXCEED POSTGRES LIMITS");
				return;	
			}
			

			JSONObject jsonObj;
			try {
				jsonObj = JSONUtil.parseJSONFile(filePath);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				jsonObj = null;
			}
			JSONArray features = jsonObj.getJSONArray("features");
//			ArrayList<String> batchInsertion = new ArrayList<String>();
			String[] batchInsertion = new String[batchSize];
			
			int remainder = features.length() % batchSize;
			
			int batchIdx = 0;
			
			String[] insertData = new String[numCols];
			insertData[0] = "";
			insertData[numCols-1] = "";
			
			// initialize each of the input vector columns (ivs_n1, ivs_n2, ...) to -1
			for (int j = 1; j < 7; j++) {
				insertData[j] = "-1";
			}
			
			// initialize each of the IV count columns (num_ivs_n1, num_ivs_n2, ...) to 0
			for (int j = 7; j < 13; j++) {
				insertData[j] = "0";
			}

			// initialize all remaining columns (kmeans, atomic values, class)
			for (int j = 13; j < numCols-1; j++) {
				insertData[j] = "-1";
			}
			
//			int batchCycle = 0;
			
//			float batchCycles = (float) features.length() / (float) batchSize;
			
//			if (batchCycles == Math.ceil(batchCycles)) {
//				
//			}

			for (int i = 0; i < features.length(); i++) {
				JSONObject geometry = features.getJSONObject(i).getJSONObject("geometry");
				JSONArray coordinates = geometry.getJSONArray("coordinates");
				JSONArray coordinates2 = coordinates.getJSONArray(0);
				JSONObject properties = features.getJSONObject(i).getJSONObject("properties");
				//				JSONArray coordi

				int tmpID = (int) properties.get("id");

				String id = tmpID + "";
//				insertData

				// String array to hold the data for postgres insert query
//				String[] insertData = new String[numCols];
				
				// get the id from GeoJSON feature
				insertData[0] = "'" + id + "'";

//				// initialize each of the input vector columns (ivs_n1, ivs_n2, ...) to -1
//				for (int j = 1; j < 7; j++) {
//					insertData[j] = "-1";
//				}
//				
//				// initialize each of the IV count columns (num_ivs_n1, num_ivs_n2, ...) to 0
//				for (int j = 7; j < 13; j++) {
//					insertData[j] = "0";
//				}
//
//				// initialize all remaining columns (kmeans, atomic values, class)
//				for (int j = 13; j < numCols-1; j++) {
//					insertData[j] = "-1";
//				}
				
				// store geometry
				JSONArray coords3 = coordinates2.getJSONArray(0);			
				insertData[numCols-1] = "ST_GeomFromText('POLYGON((" + coords3.getString(0) + " " + coords3.getString(1);


				for (int j = 1; j < coordinates2.length(); j++) {
					coords3 = coordinates2.getJSONArray(j);

					insertData[numCols-1] = insertData[numCols-1] + "," + coords3.getString(0) + " " + coords3.getString(1);
				}

				insertData[numCols-1] = insertData[numCols-1] + "))')";


				if (!batch) { 
					
					// insert into table if not using batch
					db.insertIntoTable(schema + "." + ts.toLowerCase() + "_geom",insertData);
					
				} else { 
					
					// otherwise add to the batch ArrayList
//					batchInsertion.add(db.insertQueryBuilder(schema + "." + ts.toLowerCase() + "_geom",insertData));
					batchInsertion[batchIdx++] = db.insertQueryBuilder(schema + "." + ts.toLowerCase() + "_geom",insertData);
					System.out.println("batchIdx: " + batchIdx);
					System.out.println("i: " + i);
					
//					if (batchInsertion.size() == batchSize) { // if size of batch ArrayList reaches the batchSize parameter
					if (batchIdx == batchSize) { // if size of batch ArrayList reaches the batchSize parameter
						
//						batchIdx++;
						
						// convert to String array
//						String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
						
						// insert batch data into table
//						db.insertIntoTableBatch(schema + "." + ts.toLowerCase() + "_geom", batchInsertionAr);
						db.insertIntoTableBatch(schema + "." + ts.toLowerCase() + "_geom", batchInsertion);
						
						// get the remainder of features minus the index
						// compensate for the index starting at 0
						int currRemainder = features.length() - 1 - i;
						
						if (currRemainder < batchSize) {
							batchInsertion = new String[currRemainder];
						} 
//						else {
//							batchInsertion = new String[batchSize];
//						}
						
						// re-initialize the batch ArrayList
//						batchInsertion = new ArrayList<String>();
						
						batchIdx = 0;
					}
				}
				
				insertData[0] = "";
				insertData[numCols - 1] = "";
			}
			
			if (batch && remainder > 0) { // if batch and un-inserted items after parsing all content
				
				// convert to String array
//				String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
				
				// insert batch data into table
				db.insertIntoTableBatch(schema + "." + ts.toLowerCase() + "_geom", batchInsertion);
			}

		} catch (JSONException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void insert2SOM(PostgreSQLJDBC db, String schema, String ts, String filePath, int kMax, boolean batch, int batchSize) {
		try {
						
			int numLoci 		= db.getTableLength(schema, "locus_key");		// get the number of loci						
			int numAttributes 	= db.getTableLength(schema, "attribute_key");	// get the number of attributes						
			int numTimes 		= db.getTableLength(schema, "time_key");		// get the number of times	

			// default number of columns
			// id, ivs_n1-6, num_ivs_n1-6, geom
			int numCols = 14;
			
			// additional kmeans columns
			int totalKCols = (kMax - 1) * 6;
			
			if (ts.equalsIgnoreCase("l_at")) {
				int totalAttrCols = numAttributes * numTimes * 6;
				numCols += totalAttrCols;			
			} else if (ts.equalsIgnoreCase("la_t")) {
				int totalAttrCols = numTimes * 6;
				numCols += totalAttrCols + 6;						
			} else if (ts.equalsIgnoreCase("lt_a")) {
				int totalAttrCols = numAttributes * 6;
				numCols += totalAttrCols + 12;			
			} else if (ts.equalsIgnoreCase("a_lt")) {
				int totalAttrCols = numLoci * numTimes * 6;
				numCols += totalAttrCols;			
			}  else if (ts.equalsIgnoreCase("t_la")) {
				int totalAttrCols = numLoci * numAttributes * 6;
				numCols += totalAttrCols;			
			}  else if (ts.equalsIgnoreCase("at_l")) {
				int totalAttrCols = numLoci * 6;
				numCols += totalAttrCols;						
			}  else {
				System.out.println("Trispace Type not implemented, see CSVManager.createSOMTable()");
				return;
			}
			
			numCols += totalKCols;
				
			if ((1599 - numCols) < 0) {
				System.out.println("COLUMNS EXCEED POSTGRES LIMITS");
				return;	
			}
			

			JSONObject jsonObj;
			try {
				jsonObj = JSONUtil.parseJSONFile(filePath);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				jsonObj = null;
			}
			JSONArray features = jsonObj.getJSONArray("features");
//			ArrayList<String> batchInsertion = new ArrayList<String>();
//			String[] batchInsertion = new String[batchSize];
			
			int remainder = features.length() % batchSize;
			
			int completeCycles = features.length() / batchSize;
			
			System.out.println(completeCycles + " Complete Cycles of " + batchSize);
			System.out.println("with a remainder of " + remainder);
			
			String[] insertData = new String[numCols];
			insertData[0] = "";
			insertData[numCols-1] = "";
			
			// initialize each of the input vector columns (ivs_n1, ivs_n2, ...) to -1
			for (int j = 1; j < 7; j++) {
				insertData[j] = "-1";
			}
			
			// initialize each of the IV count columns (num_ivs_n1, num_ivs_n2, ...) to 0
			for (int j = 7; j < 13; j++) {
				insertData[j] = "0";
			}

			// initialize all remaining columns (kmeans, atomic values, class)
			for (int j = 13; j < numCols-1; j++) {
				insertData[j] = "-1";
			}
			
			
//			for (int i = 0; i < completeCycles; i++) {
//				
//				int startIdx = i * batchSize;
//				int endIdx = startIdx + batchSize;
				
//				ArrayList<String> batchInsertion = new ArrayList<String>();
			String[] batchInsertion = new String[features.length()];
				
				
				
//				System.out.println("Cycle: " + i);
//				System.out.println("Start Index: " + startIdx);
//				System.out.println("End Index: " + endIdx);
//				for (int j = startIdx; j < endIdx; j++) {
				for (int j = 0; j < features.length(); j++) {
//					System.out.println("Processing " + j);
					JSONObject geometry = features.getJSONObject(j).getJSONObject("geometry");
					JSONArray coordinates = geometry.getJSONArray("coordinates");
					JSONArray coordinates2 = coordinates.getJSONArray(0);
					JSONObject properties = features.getJSONObject(j).getJSONObject("properties");
					//				JSONArray coordi

					int tmpID = (int) properties.get("id");

					String id = tmpID + "";
					
					// get the id from GeoJSON feature
					insertData[0] = "'" + id + "'";

					
					// store geometry
					JSONArray coords3 = coordinates2.getJSONArray(0);			
					insertData[numCols-1] = "ST_GeomFromText('POLYGON((" + coords3.getString(0) + " " + coords3.getString(1);


					for (int k = 1; k < coordinates2.length(); k++) {
						coords3 = coordinates2.getJSONArray(k);

						insertData[numCols-1] = insertData[numCols-1] + "," + coords3.getString(0) + " " + coords3.getString(1);
					}

					insertData[numCols-1] = insertData[numCols-1] + "))')";
					
					batchInsertion[j] = db.insertQueryBuilder(schema + "." + ts.toLowerCase() + "_geom",insertData);
//					batchInsertion.add(db.insertQueryBuilder(schema + "." + ts.toLowerCase() + "_geom",insertData));
					
					insertData[0] = "";
					insertData[numCols - 1] = "";
				}
				
//				System.out.println("Beginning Database Insertion");
				
				// convert to String array
//				String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
				
				// insert batch data into table
//				db.insertIntoTableBatch(schema + "." + ts.toLowerCase() + "_geom", batchInsertionAr, batchSize);
				db.insertIntoTableBatch(schema + "." + ts.toLowerCase() + "_geom", batchInsertion, batchSize);
				
//				System.out.println("Cycle " + (i+1) + " complete out of " + completeCycles);
				
//			}
			
//			int remainderStartIdx = completeCycles * batchSize;
//			int remainderEndIdx = features.length();
//			System.out.println("Final Cycle: " + completeCycles);
//			System.out.println("Start Index: " + remainderStartIdx);
//			System.out.println("End Index: " + remainderEndIdx);
			
//			ArrayList<String> batchInsertion = new ArrayList<String>();
//			for (int i = remainderStartIdx; i < features.length(); i++) {
//				JSONObject geometry = features.getJSONObject(i).getJSONObject("geometry");
//				JSONArray coordinates = geometry.getJSONArray("coordinates");
//				JSONArray coordinates2 = coordinates.getJSONArray(0);
//				JSONObject properties = features.getJSONObject(i).getJSONObject("properties");
//				//				JSONArray coordi
//
//				int tmpID = (int) properties.get("id");
//
//				String id = tmpID + "";
//				
//				// get the id from GeoJSON feature
//				insertData[0] = "'" + id + "'";
//
//				
//				// store geometry
//				JSONArray coords3 = coordinates2.getJSONArray(0);			
//				insertData[numCols-1] = "ST_GeomFromText('POLYGON((" + coords3.getString(0) + " " + coords3.getString(1);
//
//
//				for (int k = 1; k < coordinates2.length(); k++) {
//					coords3 = coordinates2.getJSONArray(k);
//
//					insertData[numCols-1] = insertData[numCols-1] + "," + coords3.getString(0) + " " + coords3.getString(1);
//				}
//
//				insertData[numCols-1] = insertData[numCols-1] + "))')";
//
//				batchInsertion.add(db.insertQueryBuilder(schema + "." + ts.toLowerCase() + "_geom",insertData));
//				
//				insertData[0] = "";
//				insertData[numCols - 1] = "";
//			}
//			
//			// convert to String array
//			String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
//			
//			// insert batch data into table
//			db.insertIntoTableBatch(schema + "." + ts.toLowerCase() + "_geom", batchInsertionAr);
			
			
//			for (int i = )
			
//			int batchIdx = 0;
			
			
			
//			int batchCycle = 0;
			
//			float batchCycles = (float) features.length() / (float) batchSize;
			
//			if (batchCycles == Math.ceil(batchCycles)) {
//				
//			}

//			for (int j = 0; j < features.length(); j++) {
//				JSONObject geometry = features.getJSONObject(j).getJSONObject("geometry");
//				JSONArray coordinates = geometry.getJSONArray("coordinates");
//				JSONArray coordinates2 = coordinates.getJSONArray(0);
//				JSONObject properties = features.getJSONObject(j).getJSONObject("properties");
//				//				JSONArray coordi
//
//				int tmpID = (int) properties.get("id");
//
//				String id = tmpID + "";
//				
//				// get the id from GeoJSON feature
//				insertData[0] = "'" + id + "'";
//
//				
//				// store geometry
//				JSONArray coords3 = coordinates2.getJSONArray(0);			
//				insertData[numCols-1] = "ST_GeomFromText('POLYGON((" + coords3.getString(0) + " " + coords3.getString(1);
//
//
//				for (int j = 1; j < coordinates2.length(); j++) {
//					coords3 = coordinates2.getJSONArray(j);
//
//					insertData[numCols-1] = insertData[numCols-1] + "," + coords3.getString(0) + " " + coords3.getString(1);
//				}
//
//				insertData[numCols-1] = insertData[numCols-1] + "))')";
//
//
//				if (!batch) { 
//					
//					// insert into table if not using batch
//					db.insertIntoTable(schema + "." + ts.toLowerCase() + "_geom",insertData);
//					
//				} else { 
//					
//					// otherwise add to the batch ArrayList
//					batchInsertion.add(db.insertQueryBuilder(schema + "." + ts.toLowerCase() + "_geom",insertData));
////					batchInsertion[batchIdx++] = db.insertQueryBuilder(schema + "." + ts.toLowerCase() + "_geom",insertData);
//					System.out.println("batchIdx: " + batchInsertion.size());
//					System.out.println("j: " + j);
//					
//					if (batchInsertion.size() == batchSize) { // if size of batch ArrayList reaches the batchSize parameter
////					if (batchIdx == batchSize) { // if size of batch ArrayList reaches the batchSize parameter
//						
////						batchIdx++;
//						
//						// convert to String array
//						String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
//						
//						// insert batch data into table
//						db.insertIntoTableBatch(schema + "." + ts.toLowerCase() + "_geom", batchInsertionAr);
////						db.insertIntoTableBatch(schema + "." + ts.toLowerCase() + "_geom", batchInsertion);
//						
//						// get the remainder of features minus the index
//						// compensate for the index starting at 0
////						int currRemainder = features.length() - 1 - j;
//						
////						if (currRemainder < batchSize) {
////							batchInsertion = new String[currRemainder];
////						} else {
////							batchInsertion = new String[batchSize];
////						}
//						
//						// re-initialize the batch ArrayList
//						batchInsertion = new ArrayList<String>();
//						
////						batchIdx = 0;
//					}
//				}
//				
//				insertData[0] = "";
//				insertData[numCols - 1] = "";
//			}
//			
//			if (batch && remainder > 0) { // if batch and un-inserted items after parsing all content
//				
//				// convert to String array
//				String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
//				
//				// insert batch data into table
//				db.insertIntoTableBatch(schema + "." + ts.toLowerCase() + "_geom", batchInsertionAr);
////				db.insertIntoTableBatch(schema + "." + ts.toLowerCase() + "_geom", batchInsertion);
//			}

		} catch (JSONException | SQLException e) {
//		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void insertSOM(PostgreSQLJDBC db, String schema, String ts, String geoJSONFilePath, boolean batch, int batchSize) {
		try {			

			JSONObject jsonObj;
			try {
				jsonObj = JSONUtil.parseJSONFile(geoJSONFilePath);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				jsonObj = null;
			}
			JSONArray features = jsonObj.getJSONArray("features");
			
			int remainder = features.length() % batchSize;			
			int completeCycles = features.length() / batchSize;
			
			System.out.println(completeCycles + " Complete Cycles of " + batchSize);
			System.out.println("with a remainder of " + remainder);
			
			// initialize the id & normalization to empty strings
			String[] insertData = new String[2];
			insertData[0] = "";
			insertData[1] = "";
			
			String[] batchInsertion = new String[features.length()];
				
				
			for (int j = 0; j < features.length(); j++) {
				JSONObject geometry = features.getJSONObject(j).getJSONObject("geometry");
				JSONArray coordinates = geometry.getJSONArray("coordinates");
				JSONArray coordinates2 = coordinates.getJSONArray(0);
				JSONObject properties = features.getJSONObject(j).getJSONObject("properties");

				int tmpID = (int) properties.get("id");

				String id = tmpID + "";
				
				// get the id from GeoJSON feature
				insertData[0] = "'" + id + "'";

				
				// store geometry
				JSONArray coords3 = coordinates2.getJSONArray(0);			
				insertData[1] = "ST_GeomFromText('POLYGON((" + coords3.getString(0) + " " + coords3.getString(1);


				for (int k = 1; k < coordinates2.length(); k++) {
					coords3 = coordinates2.getJSONArray(k);

					insertData[1] = insertData[1] + "," + coords3.getString(0) + " " + coords3.getString(1);
				}

				insertData[1] = insertData[1] + "))')";
				
				batchInsertion[j] = db.insertQueryBuilder(schema + "." + ts.toLowerCase() + "_som",insertData);
				
				// reset the id & normalization to empty strings, make sure we don't overwrite something accidentally
				insertData[0] = "";
				insertData[1] = "";
			}
				
			// insert batch data into table
			db.insertIntoTableBatch(schema + "." + ts.toLowerCase() + "_som", batchInsertion, batchSize);				

		} catch (JSONException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void initializeNeurons(PostgreSQLJDBC db, String schema, String ts, int[] normalizations) {
		
		try {			
			ArrayList<String> batchInsertion = new ArrayList<String>();		// ArrayList to hold the insertion statements
			
			int numNeurons 		= db.getTableLength(schema, ts.toLowerCase() + "_som");	// get the number of neurons in the SOM
			String[] columns 	= db.getTableColumnIds(schema + "." + ts.toLowerCase() + "_neurons");
			
			for (int i = 0; i < normalizations.length; i++) {
				for (int j = 1; j <= numNeurons; j++) {
					// array to store each attribute to be inserted
					String[] insertData = new String[columns.length];
					insertData[0] = "" + j;
					insertData[1] = "" + normalizations[i];
					for (int k = 2; k < columns.length; k++) {
						insertData[k] = "0";
					}
					// build query statement of array of attributes
					String queryStatement = db.insertQueryBuilder(schema + "." + ts.toLowerCase() + "_neurons",insertData);									
					batchInsertion.add(queryStatement);	// add query statement to the batch insertion ArrayList
				}
			}
			
			String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);		// convert ArrayList to an array
			db.insertIntoTableBatch(schema + "." + ts.toLowerCase(), batchInsertionAr, BATCH_SIZE);		// try to insert the data into the table
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}		
	}

	public static void insertTSFromCSV(PostgreSQLJDBC db, String schema, String ts, String parentPath, String scene) {
		if (ts.equals("L_AT")) {
			String miniPath = "L_AT";
			
			if (!scene.equals("")) {
				if (!scene.contains("_")) {
					miniPath += "_";
				}
			}
			
			miniPath = miniPath + scene + ".csv";
			BufferedReader[] br = new BufferedReader[7];
			try {
				br[0] = new BufferedReader(new FileReader(parentPath + "/Non_Normalized/" + miniPath));
				br[1] = new BufferedReader(new FileReader(parentPath + "/L_AT_Normalized/" + miniPath));
				br[2] = new BufferedReader(new FileReader(parentPath + "/A_LT_Normalized/" + miniPath));
				br[3] = new BufferedReader(new FileReader(parentPath + "/T_LA_Normalized/" + miniPath));
				br[4] = new BufferedReader(new FileReader(parentPath + "/LA_T_Normalized/" + miniPath));
				br[5] = new BufferedReader(new FileReader(parentPath + "/LT_A_Normalized/" + miniPath));
				br[6] = new BufferedReader(new FileReader(parentPath + "/AT_L_Normalized/" + miniPath));

				// remove headers
				for (int i = 0; i < br.length; i++) {
					br[i].readLine();
					br[i].readLine();
				}

				String[] line =  new String[7];
				while ((line[0] = br[0].readLine()) != null) {
					//					System.out.println(line[0]);
					for (int i = 1; i < br.length; i++) {
						line[i] = br[i].readLine();
					}
					String[][] data = new String[7][];
					for (int i = 0; i < data.length; i++) {
						data[i] = line[i].split(",");
					}
					//					String[] data = line.split(",");
					String[] insertData = new String[db.getTableColumnIds(schema + ".l_at").length];
					String uid = data[0][0];
					//					uid = uid.substring(1, uid.length());
					insertData[0] = "'"+uid+"'";
					int counter1 = 0;
					int counter2 = 1;

					for (int i = 1; i < insertData.length; i++) {
						insertData[i] = data[counter1][counter2];
						if (counter2 >= data[counter1].length-1) {
							counter1++;
							counter2 = 1;
						} else {
							counter2++;
						}
					}
					db.insertIntoTable(schema + ".l_at",insertData);
				}
				for (int i = 0; i < br.length; i++) {
					br[i].close();
				}
			} catch (IOException | SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else if (ts.equals("LA_T")) {
			String miniPath = "LA_T";
			if (!scene.equals("")) {
				if (!scene.contains("_")) {
					miniPath += "_";
				}
			}
			miniPath = miniPath + scene + ".csv";
			BufferedReader[] br = new BufferedReader[7];
			try {
				br[0] = new BufferedReader(new FileReader(parentPath + "/Non_Normalized/" + miniPath));
				br[1] = new BufferedReader(new FileReader(parentPath + "/L_AT_Normalized/" + miniPath));
				br[2] = new BufferedReader(new FileReader(parentPath + "/A_LT_Normalized/" + miniPath));
				br[3] = new BufferedReader(new FileReader(parentPath + "/T_LA_Normalized/" + miniPath));
				br[4] = new BufferedReader(new FileReader(parentPath + "/LA_T_Normalized/" + miniPath));
				br[5] = new BufferedReader(new FileReader(parentPath + "/LT_A_Normalized/" + miniPath));
				br[6] = new BufferedReader(new FileReader(parentPath + "/AT_L_Normalized/" + miniPath));

				// remove headers
				for (int i = 0; i < br.length; i++) {
					br[i].readLine();
					//					br[i].readLine();
				}

				String[] line =  new String[7];
				while ((line[0] = br[0].readLine()) != null) {
					//					System.out.println(line[0]);
					for (int i = 1; i < br.length; i++) {
						line[i] = br[i].readLine();
					}
					String[][] data = new String[7][];
					for (int i = 0; i < data.length; i++) {
						data[i] = line[i].split(",");
					}
					//					String[] data = line.split(",");
					String[] insertData = new String[db.getTableColumnIds(schema + ".la_t").length];
					String uid = data[0][0] + "_" + data[0][1];
					//					uid = uid.substring(1, uid.length());
					insertData[0] = "'"+uid+"'";
					int counter1 = 0;
					int counter2 = 2;

					for (int i = 1; i < insertData.length; i++) {
						insertData[i] = data[counter1][counter2];
						if (counter2 >= data[counter1].length-1) {
							counter1++;
							counter2 = 2;
						} else {
							counter2++;
						}
					}
					db.insertIntoTable(schema + ".la_t",insertData);
				}
				for (int i = 0; i < br.length; i++) {
					br[i].close();
				}	
			} catch (IOException | SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else if (ts.equals("LT_A")) {
			String miniPath = "LT_A";
			if (!scene.equals("")) {
				if (!scene.contains("_")) {
					miniPath += "_";
				}
			}
			miniPath = miniPath + scene + ".csv";
			BufferedReader[] br = new BufferedReader[7];
			try {
				br[0] = new BufferedReader(new FileReader(parentPath + "/Non_Normalized/" + miniPath));
				br[1] = new BufferedReader(new FileReader(parentPath + "/L_AT_Normalized/" + miniPath));
				br[2] = new BufferedReader(new FileReader(parentPath + "/A_LT_Normalized/" + miniPath));
				br[3] = new BufferedReader(new FileReader(parentPath + "/T_LA_Normalized/" + miniPath));
				br[4] = new BufferedReader(new FileReader(parentPath + "/LA_T_Normalized/" + miniPath));
				br[5] = new BufferedReader(new FileReader(parentPath + "/LT_A_Normalized/" + miniPath));
				br[6] = new BufferedReader(new FileReader(parentPath + "/AT_L_Normalized/" + miniPath));

				// remove headers
				for (int i = 0; i < br.length; i++) {
					br[i].readLine();
					//					br[i].readLine();
				}

				String[] line =  new String[7];
				while ((line[0] = br[0].readLine()) != null) {
					//					System.out.println(line[0]);
					for (int i = 1; i < br.length; i++) {
						line[i] = br[i].readLine();
					}
					String[][] data = new String[7][];
					for (int i = 0; i < data.length; i++) {
						data[i] = line[i].split(",");
					}
					//					String[] data = line.split(",");
					String[] insertData = new String[db.getTableColumnIds(schema + ".lt_a").length];
					String uid = data[0][0] + "_" + data[0][1];
					//					uid = uid.substring(1, uid.length());
					insertData[0] = "'"+uid+"'";
					int counter1 = 0;
					int counter2 = 2;
					int totalCount = 1;
					
					for (int i = 1; i < insertData.length - 6; i++) {
						insertData[i] = data[counter1][counter2];
						if (counter2 >= data[counter1].length-1) {
							counter1++;
							counter2 = 2;
						} else {
							counter2++;
						}
						totalCount++;
					}
					
					for (int i = 1; i < 7; i++) {
						insertData[totalCount++] = "-1";
					}
					db.insertIntoTable(schema + ".lt_a",insertData);
				}
				for (int i = 0; i < br.length; i++) {
					br[i].close();
				}	
			} catch (IOException | SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
	
	public static boolean insertTSPerspectives(PostgreSQLJDBC db, String schema, String perspective, int[] normalizations,
												String[] inputPaths, boolean removeLeadingChar, boolean som, boolean writeData) {
				
		ArrayList<String> batchInsertion = new ArrayList<String>();		// ArrayList to hold the insertion statements
				
		boolean compositePerspective;									// test if composite perspective: LA_T, LT_A, or AT_L
		if (perspective.equalsIgnoreCase("l_at") || perspective.equalsIgnoreCase("a_lt") || perspective.equalsIgnoreCase("t_la")) {
			compositePerspective = false;
		} else if (perspective.equalsIgnoreCase("at_l") || perspective.equalsIgnoreCase("lt_a") || perspective.equalsIgnoreCase("la_t")) {
			compositePerspective = true;
		} else {
			return false;												// return false (fail)
		}
				
//		String miniPath = perspective.toUpperCase();								// File name to be processed
//		
//		if (!scene.equals("")) {
//			if (!scene.contains("_")) {
//				miniPath += "_";
//			}
//		}
//		
//		miniPath = miniPath + scene + ".csv";
//		
//		// array to store full paths to File (one for each normalization) 
//		String[] inputFiles = {
//				parentPath + "/Non_Normalized/" + miniPath,
//				parentPath + "/L_AT_Normalized/" + miniPath,
//				parentPath + "/A_LT_Normalized/" + miniPath,
//				parentPath + "/T_LA_Normalized/" + miniPath,
//				parentPath + "/LA_T_Normalized/" + miniPath,
//				parentPath + "/LT_A_Normalized/" + miniPath,
//				parentPath + "/AT_L_Normalized/" + miniPath
//		};
//		
		
		for (int i = 0; i < normalizations.length; i++) {									// iterate thru all input files/normalizations
			
			try {
				
				BufferedReader br = new BufferedReader(new FileReader(inputPaths[i]));	// open file								
				br.readLine();															// remove top header				
				if (!compositePerspective) br.readLine();								// remove second header (if necessary)																					
								
				String line = "";														// variable to store the content of a line
								
				while ((line = br.readLine()) != null) {								// iterate thru all lines in file
										
					String[] lineData = line.split(",");								// store line data as an array
					
					// array to store each attribute to be inserted
					String[] insertData = new String[db.getTableColumnIds(schema + "." + perspective.toLowerCase()).length];
										
					int startIdx = 0;													// index where the quantitative data begins			
										
					String id1 = lineData[startIdx];									// store the id (index 0)
					if (removeLeadingChar) id1 = id1.substring(1, id1.length());		// remove prefix (if necessary)
										
					insertData[startIdx++] = id1;										// place id1 in array of data to be inserted
					
					if (compositePerspective) { 										// if composite (2 identifiers)
												
						String id2 = lineData[startIdx];								// store the second id (index 1) 						
						if (removeLeadingChar) id2 = id2.substring(1, id2.length());	// remove prefix (if necessary)							
												
						insertData[startIdx++] = id2;									// place id2 in array of data to be inserted
					}
					
					insertData[startIdx++] = normalizations[i] + "";									// place normalization in array of data to be inserted
					
					if (writeData) {
						for (int j = startIdx-1; j < lineData.length; j++) {				// iterate thru the rest of the line's data														
							insertData[j+1] = lineData[j];									// insert value into the array
						}
					} else {
						for (int j = startIdx; j < insertData.length; j++) {				// iterate thru the rest of the line's data														
							insertData[j] = null;											// insert value into the array
						}
					}										

										
					if (som) insertData[insertData.length - 1] = "-1";					// if using SOM initialize BMU to -1
					
					// build query statement of array of attributes
					String queryStatement = db.insertQueryBuilder(schema + "." + perspective.toLowerCase(), insertData);									
					batchInsertion.add(queryStatement);									// add query statement to the batch insertion ArrayList
				}				
				
				br.close();																// close the BufferedReader
			} catch (IOException | SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}						
		}
				
		String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);		// convert ArrayList to an array

		try {
			db.insertIntoTableBatch(schema + "." + perspective.toLowerCase(), batchInsertionAr, BATCH_SIZE);	// try to insert the data into the table
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	public static boolean insertTSPerspectives(PostgreSQLJDBC db, String schema, String ts, String parentPath, String scene, File ts_class, File class_dict, boolean removeLeadingChar, boolean som, boolean writeData) {
		
		ArrayList<String> batchInsertion = new ArrayList<String>();		// ArrayList to hold the insertion statements
				
		boolean compositePerspective;									// test if composite perspective: LA_T, LT_A, or AT_L
		if (ts.equalsIgnoreCase("l_at") || ts.equalsIgnoreCase("a_lt") || ts.equalsIgnoreCase("t_la")) {
			compositePerspective = false;
		} else if (ts.equalsIgnoreCase("at_l") || ts.equalsIgnoreCase("lt_a") || ts.equalsIgnoreCase("la_t")) {
			compositePerspective = true;
		} else {
			return false;												// return false (fail)
		}
				
		String miniPath = ts.toUpperCase();								// File name to be processed
		
		if (!scene.equals("")) {
			if (!scene.contains("_")) {
				miniPath += "_";
			}
		}
		
		miniPath = miniPath + scene + ".csv";
		
		ArrayList<String[]> classTypes = new ArrayList<String[]>();
		BufferedReader classDictionary;
		try {
			classDictionary = new BufferedReader(new FileReader(class_dict.getAbsolutePath()));
			String classLine = "";
			while ((classLine = classDictionary.readLine()) != null) {
				classTypes.add(classLine.split(","));
			}
			classLine = "";
		} catch (IOException e) {
			e.printStackTrace();
		}

		
		// array to store full paths to File (one for each normalization) 
		String[] inputFiles = {
				parentPath + "/Non_Normalized/" + miniPath,
				parentPath + "/L_AT_Normalized/" + miniPath,
				parentPath + "/A_LT_Normalized/" + miniPath,
				parentPath + "/T_LA_Normalized/" + miniPath,
				parentPath + "/LA_T_Normalized/" + miniPath,
				parentPath + "/LT_A_Normalized/" + miniPath,
				parentPath + "/AT_L_Normalized/" + miniPath
		};
		
		
		for (int i = 0; i < inputFiles.length; i++) {									// iterate thru all input files/normalizations
			
			try {
				
				BufferedReader brClass = new BufferedReader(new FileReader(ts_class.getAbsolutePath()));
				BufferedReader br = new BufferedReader(new FileReader(inputFiles[i]));	// open file
				
				brClass.readLine();														// remove top header
				br.readLine();															// remove top header
				if (!compositePerspective) br.readLine();								// remove second header (if necessary)
								
				String classLine = "";													// variable to store the content of a line
				String line = "";														// variable to store the content of a line
								
				while ((line = br.readLine()) != null) {								// iterate thru all lines in file
					
					classLine = brClass.readLine();
										
					String[] classData = classLine.split(",");
					String[] lineData = line.split(",");								// store line data as an array
					
					// array to store each attribute to be inserted
					String[] insertData = new String[db.getTableColumnIds(schema + "." + ts.toLowerCase()).length];
										
					int startIdx = 0;													// index where the quantitative data begins			
										
					String id1 = lineData[startIdx];									// store the id (index 0)
					if (removeLeadingChar) id1 = id1.substring(1, id1.length());		// remove prefix (if necessary)
										
					insertData[startIdx++] = id1;										// place id1 in array of data to be inserted
					
					if (compositePerspective) { 										// if composite (2 identifiers)
												
						String id2 = lineData[startIdx];								// store the second id (index 1) 						
						if (removeLeadingChar) id2 = id2.substring(1, id2.length());	// remove prefix (if necessary)							
												
						insertData[startIdx++] = id2;									// place id2 in array of data to be inserted
					}
					
					insertData[startIdx++] = i + "";									// place normalization in array of data to be inserted
					
					if (writeData) {
						for (int j = startIdx-1; j < lineData.length; j++) {				// iterate thru the rest of the line's data														
							insertData[j+1] = lineData[j];									// insert value into the array
							startIdx++;
						}
					} else {
						for (int j = startIdx; j < insertData.length; j++) {				// iterate thru the rest of the line's data														
							insertData[j] = null;									// insert value into the array
						}
					}
					
					if (som) insertData[startIdx++] = "-1";								// if using SOM initialize BMU to -1
					
					for (int j = 1; j < classData.length; j++) {
						String tmpClass = "";
						for (int k = 0; k < classTypes.size(); k++) {
							if (classTypes.get(k)[0].equals(classData[j])) {
								tmpClass = classTypes.get(k)[1];
								break;
							}
						}
						insertData[startIdx++] = "'" + tmpClass + "'";
					}																															
					
					// build query statement of array of attributes
					String queryStatement = db.insertQueryBuilder(schema + "." + ts.toLowerCase(),insertData);									
					batchInsertion.add(queryStatement);									// add query statement to the batch insertion ArrayList
				}				
				
				brClass.close();														// close the class BufferedReader
				br.close();																// close the BufferedReader
			} catch (IOException | SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}						
		}
				
		String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);		// convert ArrayList to an array

		try {
			db.insertIntoTableBatch(schema + "." + ts.toLowerCase(), batchInsertionAr, BATCH_SIZE);	// try to insert the data into the table
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	
	/**
	 * Write a class to the input vectors. Assumes only 1 header.
	 *
	 * @param db
	 *            - An instance of the PostgreSQLJDBC class.
	 * @param schema
	 *            - The schema in Postgres to write to.
	 * @param perspective
	 *            - The Tri-Space perspective to write to.
	 * @param dataTypes
	 *			  - An array datatypes for each columns (TEXT, INT, SMALLINT, etc.). Pass in 1-element array if all same dataType
	 * @param classCSV
	 *			  - A CSV of classified input vectors.
	 * @param classLabels
	 *			  - A CSV of class labels (e.g. "1, Vegetation", "2, Urban", etc.), is nullable.
	 * @param removeLeadingChar
	 * 			  - If classData uses a prefix set to yet (l1, a1, t1, etc.)
	 * @param normalization
	 *      	  - The normalization to classify. 
	 */
	public static boolean classifyIVs(PostgreSQLJDBC db, String schema, String perspective, String[] dataTypes, 
								 File classCSV, File classLabels, boolean removeLeadingChar, int normalization) {
		
		ArrayList<String> batchUpdate = new ArrayList<String>();		// ArrayList to hold the insertion statements
		
		boolean compositePerspective;									// test if composite perspective: LA_T, LT_A, or AT_L
		if (perspective.equalsIgnoreCase("l_at") || perspective.equalsIgnoreCase("a_lt") || perspective.equalsIgnoreCase("t_la")) {
			compositePerspective = false;
		} else if (perspective.equalsIgnoreCase("at_l") || perspective.equalsIgnoreCase("lt_a") || perspective.equalsIgnoreCase("la_t")) {
			compositePerspective = true;
		} else {
			return false;												// return false (fail)
		}		
		
		String writeTable = schema + "." + perspective.toLowerCase();			
		
		ArrayList<String[]> classTypes = new ArrayList<String[]>();
		
		if (classLabels != null) {
			
			BufferedReader classDictionary;
			try {
				classDictionary = new BufferedReader(new FileReader(classLabels.getAbsolutePath()));
				String classLine = "";
				while ((classLine = classDictionary.readLine()) != null) {
					classTypes.add(classLine.split(","));
				}
				classLine = "";
			} catch (IOException e) {
				e.printStackTrace();
			}			
		}
		
			
		try {
			
			BufferedReader brClass = new BufferedReader(new FileReader(classCSV.getAbsolutePath()));  //read the class file
			String[] headers = brClass.readLine().split(",");  // get the headers from the first line
			
			for (int i = 1; i < headers.length; i++) {
				if (!db.columnExists(schema, perspective.toLowerCase(), headers[i])) {
					String dataType;
					if (dataTypes.length == 1) {
						dataType = dataTypes[0];
					} else if (dataTypes.length == (headers.length - 1)) {
						dataType = dataTypes[i - 1];
					} else {
						System.out.println("Number of headers in CSV does not match the number of dataTypes provided in input parameter.");
						return false;
					}
					if (!db.addColumn(writeTable, headers[i], dataType)) {
						return false;
					}
				}
			}			
			
							
			String classLine = "";													// variable to store the content of a line
			String line = "";														// variable to store the content of a line					
			
			while ((line = brClass.readLine()) != null) {							// iterate thru all lines in file				
									
				String[] classDataLine = line.split(",");							// store line data as an array
				
				ArrayList<String> objectList = new ArrayList<String>();				// list to hold number of objects
		
									
				String[] idArray = classDataLine[0].split("_");						// store the id (index 0)
				String id1 = idArray[0];
				String id2 = "";
				if (removeLeadingChar) id1 = id1.substring(1, id1.length());		// remove prefix (if necessary)
				objectList.add(id1);									
				
				if (compositePerspective) { 										// if composite (2 identifiers)
											
					id2 = idArray[1];												// store the second id (index 1) 						
					if (removeLeadingChar) id2 = id2.substring(1, id2.length());	// remove prefix (if necessary)							
											
					objectList.add(id2);											// place id2 in array of data to be inserted
				}
				
				for (int i = 1; i < classDataLine.length; i++) {
					String setData = classDataLine[i];
					if (classLabels != null) {
						for (int j = 0; j < classTypes.size(); j++) {
							if (classTypes.get(j)[0].equalsIgnoreCase(classDataLine[i])) {
								String dataType;
								if (classTypes.get(j).length == 1) {
									setData = classTypes.get(j)[1];
								} else if (classTypes.get(j).length == headers.length) {
									setData = classTypes.get(j)[1];
								} else {
									System.out.println("Number of headers in CSV does not match the number of dataTypes provided in input parameter.");
									return false;
								}
								setData = classTypes.get(j)[1];
							}							
						}
					}
					
					String query;
					if (!compositePerspective) {
						query = "UPDATE " + writeTable + " SET " + headers[i]
								+ " = '" +  setData + "' WHERE id = " + id1 + " AND normalization = " + normalization; 
					} else {
						query = "UPDATE " + writeTable + " SET " + headers[i]
								+ " = '" +  setData + "' WHERE id1 = " + id1 + " AND id2 = " + id2 
								+ " AND normalization = " + normalization; 
					}
					
					batchUpdate.add(query);	
				}				
			}				
			
			// build query statement of array of attributes
			String[] batchInsertionAr = batchUpdate.toArray(new String[batchUpdate.size()]);						// convert ArrayList to an array			
			db.insertIntoTableBatch(schema + "." + perspective.toLowerCase(), batchInsertionAr, BATCH_SIZE);		// try to insert the data into the table
			
			brClass.close();														// close the class BufferedReader
		} catch (IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}						
		
		return true;
		
	}
	
	/**
	 * Write a class to the neurons. Assumes only 1 header.
	 *
	 * @param db
	 *            - An instance of the PostgreSQLJDBC class.
	 * @param schema
	 *            - The schema in Postgres to write to.
	 * @param perspective
	 *            - The Tri-Space perspective to write to.
	 * @param dataTypes
	 *			  - An array datatypes for each columns (TEXT, INT, SMALLINT, etc.). Pass in 1-element array if all same dataType
	 * @param classCSV
	 *			  - A CSV of classified input vectors.
	 * @param classLabels
	 *			  - A CSV of class labels (e.g. "1, Vegetation", "2, Urban", etc.), is nullable.
	 * @param normalization
	 *      	  - The normalization to classify. 
	 */
	public static boolean classifyNeurons(PostgreSQLJDBC db, String schema, String perspective, String[] dataTypes, 
								 File classCSV, File classLabels, int normalization) {
		
		ArrayList<String> batchUpdate = new ArrayList<String>();		// ArrayList to hold the insertion statements
		
		if (!perspective.equalsIgnoreCase("l_at") && !perspective.equalsIgnoreCase("a_lt") && !perspective.equalsIgnoreCase("t_la")
			&& !perspective.equalsIgnoreCase("at_l") && !perspective.equalsIgnoreCase("lt_a") && !perspective.equalsIgnoreCase("la_t")	) {
			return false;	
		}	
		
		String writeTable = schema + "." + perspective.toLowerCase() + "_neurons";	
		
		ArrayList<String[]> classTypes = new ArrayList<String[]>();
		
		if (classLabels != null) {
			
			BufferedReader classDictionary;
			try {
				classDictionary = new BufferedReader(new FileReader(classLabels.getAbsolutePath()));
				String classLine = "";
				while ((classLine = classDictionary.readLine()) != null) {
					classTypes.add(classLine.split(","));
				}
				classLine = "";
			} catch (IOException e) {
				e.printStackTrace();
			}			
		}
		
			
		try {
			
			BufferedReader brClass = new BufferedReader(new FileReader(classCSV.getAbsolutePath()));  //read the class file
			String[] headers = brClass.readLine().split(",");  // get the headers from the first line
			
			for (int i = 1; i < headers.length; i++) {
				if (!db.columnExists(schema, perspective.toLowerCase() + "_neurons", headers[i])) {
					String dataType;
					if (dataTypes.length == 1) {
						dataType = dataTypes[0];
					} else if (dataTypes.length == (headers.length - 1)) {
						dataType = dataTypes[i - 1];
					} else {
						System.out.println("Number of headers in CSV does not match the number of dataTypes provided in input parameter.");
						return false;
					}
					if (!db.addColumn(writeTable, headers[i], dataType)) {
						return false;
					}
				}
			}			
			
							
			String classLine = "";													// variable to store the content of a line
			String line = "";														// variable to store the content of a line					
			
			while ((line = brClass.readLine()) != null) {							// iterate thru all lines in file				
									
				String[] classDataLine = line.split(",");							// store line data as an array
				
				ArrayList<String> objectList = new ArrayList<String>();				// list to hold number of objects
		
									
				String[] idArray = classDataLine[0].split("_");						// store the id (index 0)
				String id1 = idArray[0];
				objectList.add(id1);													
				
				for (int i = 1; i < classDataLine.length; i++) {
					String setData = classDataLine[i];
					if (classLabels != null) {
						for (int j = 0; j < classTypes.size(); j++) {
							if (classTypes.get(j)[0].equalsIgnoreCase(classDataLine[i])) {
								String dataType;
								if (classTypes.get(j).length == 1) {
									setData = classTypes.get(j)[1];
								} else if (classTypes.get(j).length == headers.length) {
									setData = classTypes.get(j)[1];
								} else {
									System.out.println("Number of headers in CSV does not match the number of dataTypes provided in input parameter.");
									return false;
								}
								setData = classTypes.get(j)[1];
							}							
						}
					}
					
					String query = "UPDATE " + writeTable + " SET " + headers[i] + " = '" +  setData 
									+ "' WHERE id = " + id1 + " AND normalization = " + normalization; 
					
					batchUpdate.add(query);	
				}				
			}				
			
			// build query statement of array of attributes
			String[] batchInsertionAr = batchUpdate.toArray(new String[batchUpdate.size()]);						// convert ArrayList to an array			
			db.insertIntoTableBatch(schema + "." + perspective.toLowerCase(), batchInsertionAr, BATCH_SIZE);		// try to insert the data into the table
			
			brClass.close();														// close the class BufferedReader
		} catch (IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}						
		
		return true;
		
	}
	
	/**
	 * Project input vector classes onto the SOM neurons.
	 *
	 * @param db
	 *            - An instance of the PostgreSQLJDBC class.
	 * @param schema
	 *            - The schema in Postgres to write to.
	 * @param perspective
	 *            - The Tri-Space perspective to write to.
	 * @param column
	 *			  - The IV column to project onto the neurons.
	 * @param distanceMeasure
	 *			  - The distance measure used to create the SOM.
	 * @param normalizations
	 *      	  - The normalizations to classify.
	 */
	public static boolean projectIVClass(PostgreSQLJDBC db, String schema, String perspective, 
											String column, int distanceMeasure) {
		
		int numLoci 		= db.getTableLength(schema, "locus_key");		// get the number of loci from key		
		int numAttributes 	= db.getTableLength(schema, "attribute_key");	// get the number of attributes from key		
		int numTimes 		= db.getTableLength(schema, "time_key");		// get the number of times from key
		
				
		int numObjects 		= -1;											// the number of objects in the TS perspective
		int numDimensions 	= -1;											// the number of dimensions in the TS perspective
		
		ArrayList<Integer> objectList 		= new ArrayList<Integer>();		// list to hold the objects
		ArrayList<Integer> dimensionList 	= new ArrayList<Integer>();		// list to hold the dimensions
		ArrayList<String> dimensionNames 	= new ArrayList<String>();		// list to hold the dimension names
		ArrayList<String> batchUpdate 		= new ArrayList<String>();		// ArrayList to hold the update statements
		
		ArrayList<Neuron> neuronList		= new ArrayList<Neuron>();		// list to hold the neurons that need complex classification
		ArrayList<Integer> normList			= new ArrayList<Integer>();		// list to hold the neuron normalizations
		ArrayList<String[]> bmIVList		= new ArrayList<String[]>();	// list to hold an array of best matching input vectors
		
		if (perspective.equalsIgnoreCase("l_at")) {
			
			numObjects = numLoci;			
			numDimensions = numAttributes * numTimes;
			
			objectList.add(numLoci);
			dimensionList.add(numAttributes);
			dimensionList.add(numTimes);
			dimensionNames.add("a");
			dimensionNames.add("t");
			
		} else if (perspective.equalsIgnoreCase("a_lt")) {		
			
			numObjects = numAttributes;
			numDimensions = numLoci * numTimes;
			
			objectList.add(numAttributes);
			dimensionList.add(numLoci);
			dimensionList.add(numTimes);
			dimensionNames.add("l");
			dimensionNames.add("t");
			
		} else if (perspective.equalsIgnoreCase("t_la")) {
			
			numObjects = numTimes;
			numDimensions = numLoci * numAttributes;
			
			objectList.add(numTimes);
			dimensionList.add(numLoci);
			dimensionList.add(numAttributes);
			dimensionNames.add("l");
			dimensionNames.add("a");
			
		} else if (perspective.equalsIgnoreCase("la_t")) {
			
			numObjects = numLoci * numAttributes;
			numDimensions = numTimes;
			
			objectList.add(numLoci);
			objectList.add(numAttributes);
			dimensionList.add(numTimes);
			dimensionNames.add("t");
			
		} else if (perspective.equalsIgnoreCase("lt_a")) {			
			numObjects = numLoci * numTimes;
			numDimensions = numAttributes;
			
			objectList.add(numLoci);
			objectList.add(numTimes);
			dimensionList.add(numAttributes);
			dimensionNames.add("a");
			
		} else if (perspective.equalsIgnoreCase("at_l")) {
			
			numObjects = numAttributes * numTimes;
			numDimensions = numLoci;
			
			objectList.add(numAttributes);
			objectList.add(numTimes);
			dimensionList.add(numLoci);
			dimensionNames.add("l");
			
		} else {
			return false;
		}	
		
		String readTable = schema + "." + perspective.toLowerCase();				// input vector table
		String writeTable = schema + "." + perspective.toLowerCase() + "_neurons";	// neuron table
		String writeColumn = "iv_" + column;										// column to write to in neuron table
		
		// make sure the column to read exists
		if (!db.columnExists(schema, perspective.toLowerCase(), column)) {
			System.out.println("column " + column + " does not exist in the IV table " + schema + "." + perspective.toLowerCase());
			return false;
		}
		
		// check if the column to write to exists
		if (!db.columnExists(schema, perspective.toLowerCase() + "_neurons", writeColumn)) {			
			if (!db.addColumn(writeTable, writeColumn, db.getColumnDataType(schema, perspective.toLowerCase(), column))) {
				return false;
			}
		}
		
		String dimensions = "";
		
		if (dimensionList.size() == 1) {
			for (int i = 1; i <= dimensionList.get(0); i++) {
				dimensions += dimensionNames.get(0) + i + ",";
			}
		} else {
			for (int i = 1; i <= dimensionList.get(0); i++) {
				for (int j = 1; j <= dimensionList.get(1); j++) {
					dimensions += dimensionNames.get(0) + i + "_" + dimensionNames.get(1) + j + ",";
				}
			}
		}
		dimensions = dimensions.substring(0, dimensions.length() - 1);
		
		try {
			String query = "SELECT id, normalization, ivs, num_ivs, " + dimensions + " FROM " + writeTable + ";";
			
//			System.out.println(query);

			Statement stmt = db.getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
			
			while (rs.next()) {
				
				if (rs.getInt(4) == 0 || rs.getInt(4) == 1) {  // neurons only have 0 or 1 input vectors
					String bmIV = rs.getString(3);
					
					if (rs.getInt(4) == 0) {  // if no input vectors
						bmIV = bmIV.substring(1);  // remove the _ prefix
					}
					
					if (objectList.size() == 1) {  // if not composite perspective
						query = "UPDATE " + writeTable + " SET " + writeColumn + " = (SELECT " + column + " FROM "
								+ readTable + " WHERE normalization = " + rs.getInt(2) + " AND id = " + bmIV 
								+ ") WHERE id = " + rs.getInt(1) + " AND normalization = " + rs.getInt(2); 
				
						batchUpdate.add(query);	
						
					} else {	// if composite perspective
						
						query = "UPDATE " + writeTable + " SET " + writeColumn + " = (SELECT " + column + " FROM " + readTable 
								+ " WHERE normalization = " + rs.getInt(2) + " AND id1 = " + bmIV.split("_")[0] + " AND id2 = " 
								+ bmIV.split("_")[1] + ") WHERE id = " + rs.getInt(1) + " AND normalization = " + rs.getInt(2); 
				
						batchUpdate.add(query);	
					}
					
				} else {
					
					bmIVList.add(rs.getString(3).split(","));
					normList.add(rs.getInt(2));
					
					float[] neuronAttributes = new float[numDimensions];
					
					for (int i = 0; i < numDimensions; i++) {
						neuronAttributes[i] = rs.getInt(i+5);
					}
					
					Neuron tmpNeuron = new Neuron(rs.getInt(1), neuronAttributes);
					
					neuronList.add(tmpNeuron);
				}
				
			}
			
			rs.close();
			stmt.close();
			
			/// iterate thru neurons that don't have 0 or 1 input vectors matched to them
			for (int i = 0; i < neuronList.size(); i++) {
				
				ArrayList<InputVector> ivs = new ArrayList<InputVector>();
				
				for (int j = 0; j < bmIVList.get(i).length; j++) {

					if (objectList.size() == 1) {
						query = "SELECT id, normalization, " + column + ", " + dimensions + " FROM " + readTable 
								+ " WHERE id = " + bmIVList.get(i)[j] + " AND normalization = " + normList.get(i) + ";";
					} else {
						String id1 = bmIVList.get(i)[j].split("_")[0];
						String id2 = bmIVList.get(i)[j].split("_")[1];
						query = "SELECT id1, id2, normalization, " + column + ", " + dimensions + " FROM " + readTable 
								+ " WHERE id1 = " + id1 + " AND id2 = " + id2 + " AND normalization = " + normList.get(i) + ";";
					}
					
					
//					System.out.println(query);

					stmt = db.getConnection().createStatement();
					rs = stmt.executeQuery(query);
					
					while (rs.next()) {
						
						float[] ivAttributes = new float[numDimensions];
						
						if (objectList.size() == 1) {
							String id = rs.getString(1);
							String classLabel = rs.getString(3);
							
							for (int n = 0; n < numDimensions; n++) {
								ivAttributes[n] = rs.getFloat(n+4);
							}
							
							InputVector tmpIV = new InputVector(0, ivAttributes, id, classLabel);
							ivs.add(tmpIV);
						} else {
							String id = rs.getString(1);
							id += rs.getString(2);
							String classLabel = rs.getString(4);
							
							for (int n = 0; n < numDimensions; n++) {
								ivAttributes[n] = rs.getFloat(n+5);
							}
							
							InputVector tmpIV = new InputVector(0, ivAttributes, id, classLabel);
							ivs.add(tmpIV);
						}						
					}
					
					rs.close();
					stmt.close();
				}
				
				ArrayList<ArrayList<InputVector>> classIVs = new ArrayList<ArrayList<InputVector>>();	// 2D array to hold class-to-IV
				ArrayList<String> classes = new ArrayList<String>();			// list to store the classes
				ArrayList<Integer> classFrequency = new ArrayList<Integer>();	// list to store class frequency
				
				for (int j = 0; j < ivs.size(); j++) {
					if (!classes.contains(ivs.get(j).getGeoID())) {
						classes.add(ivs.get(j).getGeoID());		// store the class name
						classFrequency.add(1);					// initialize frequency to 1
						ArrayList<InputVector> tmpItems = new ArrayList<InputVector>();	// list of IVs for this class
						tmpItems.add(ivs.get(j));				// initialize with this neuron
						classIVs.add(tmpItems);					// add this list of IVs to this class
					} else {						
						int classIdx = classes.indexOf(ivs.get(j).getGeoID());	// get the index that corresponds to a particular class						
						classFrequency.set(classIdx, (classFrequency.get(classIdx) + 1));  // increment the count for that class						
						classIVs.get(classIdx).add(ivs.get(j));					// append the IV to that class
					}
				}

				// determine which class has the most IVs
				// initialize count to -1
				int maxNumber = -1;

				// iterate thru all class counts
				for (int j = 0; j < classFrequency.size(); j++) {
					// store the max
					if (classFrequency.get(j) > maxNumber) {
						maxNumber = classFrequency.get(j);
					}
				}

				// store the index(es) of the LC class(es) that have the max count
				ArrayList<Integer> winners = new ArrayList<Integer>();
				for (int j = 0; j < classFrequency.size(); j++) {
					if (classFrequency.get(j) == maxNumber) {
						winners.add(j);
					}
				}

				if (winners.size() == 1) { // case 1: There is only one winning class
					
					query = "UPDATE " + writeTable + " SET " + writeColumn + " = '" 
							+  classIVs.get(winners.get(0)).get(0).getGeoID() + "' WHERE id = " 
							+ neuronList.get(i).getID() + " AND normalization = " + normList.get(i); 
					
					batchUpdate.add(query);	

				}  else { // case 2: there is a 'tie' between winning classes

					// since no class has the majority, the class that is more similar will be chosen

					// store the AQE for each class
					ArrayList<Float> classAQE = new ArrayList<Float>();

					// get the neuron
					Neuron neuron = neuronList.get(i);

					// parse thru each maximum class
					for (int j = 0; j < winners.size(); j++) {

						// initial AQE set to 0
						float tmpAQE = 0f;

						// iterate thru each IV
						for (int k = 0; k < classIVs.get(winners.get(j)).size(); k++) {

							// get float array to store each of the IV's attributes
							float[] ivAttributes = classIVs.get(winners.get(j)).get(k).getAttributes();

							// append the distance between each IV & the mapped neuron
							tmpAQE += neuron.getDistance(ivAttributes, distanceMeasure);
						}

						// get the Average distance
						tmpAQE /= classIVs.get(winners.get(j)).size();
						// store this distance
						classAQE.add(tmpAQE);
					}

					// find the class with the lowest AQE
					int winningIdx = 0;
					float minAQE = classAQE.get(0);
					for (int j = 1; j < classAQE.size(); j++) {
						if (classAQE.get(j) < minAQE) {
							minAQE = classAQE.get(j);
							winningIdx = j;
						}
					}
					
					query = "UPDATE " + writeTable + " SET " + writeColumn + " = '" 
							+  classIVs.get(winners.get(winningIdx)).get(0).getGeoID() + "' WHERE id = " 
							+ neuronList.get(i).getID() + " AND normalization = " + normList.get(i); 
					
//					if (objectList.size() == 1) {
//						query = "UPDATE " + writeTable + " SET " + writeColumn + " = '" 
//								+  ivs.get(winners.get(winningIdx)).getGeoID() + "' WHERE id = " 
//								+ ivs.get(winners.get(winningIdx)).getName() + " AND normalization = " + normList.get(i); 
//					} else {
//						String id1 = ivs.get(winners.get(winningIdx)).getName().split("_")[0];
//						String id2 = ivs.get(winners.get(winningIdx)).getName().split("_")[1];
//						
//						query = "UPDATE " + writeTable + " SET " + writeColumn + " = '" 
//								+  ivs.get(winners.get(winningIdx)).getGeoID() + "' WHERE id1 = " 
//								+ id1 + " AND id2 = " + id2 + " AND normalization = " + normList.get(i); 
//					}
					
					batchUpdate.add(query);	
				
				}
			}

			// build query statement of array of attributes
			String[] batchUpdateAr = batchUpdate.toArray(new String[batchUpdate.size()]);						// convert ArrayList to an array			
			db.insertIntoTableBatch(schema + "." + perspective.toLowerCase() + "_neurons", batchUpdateAr, BATCH_SIZE);		// try to insert the data into the table
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}						
		
		return true;
		
	}
	
	/**
	 * Project Tri-Space element classes onto the SOM neurons.
	 *
	 * @param db
	 *            - An instance of the PostgreSQLJDBC class.
	 * @param schema
	 *            - The schema in Postgres to write to.
	 * @param perspective
	 *            - The TS perspective to write to.
	 * @param element
	 *            - The TS element write to {l, a, t, locus, attribute, time}
	 * @param column
	 *			  - The TS column to project onto the neurons.
	 * @param distanceMeasure
	 *			  - The distance measure used to create the SOM.
	 * @param normalizations
	 *      	  - The normalizations to classify.
	 */
	public static boolean projectTSClass(PostgreSQLJDBC db, String schema, String perspective, String element, 
											String column, int distanceMeasure) {
		
		int numLoci 		= db.getTableLength(schema, "locus_key");		// get the number of loci from key		
		int numAttributes 	= db.getTableLength(schema, "attribute_key");	// get the number of attributes from key		
		int numTimes 		= db.getTableLength(schema, "time_key");		// get the number of times from key
		
				
		int numObjects 		= -1;											// the number of objects in the TS perspective
		int numDimensions 	= -1;											// the number of dimensions in the TS perspective
		
		ArrayList<Integer> objectList 		= new ArrayList<Integer>();		// list to hold the objects
		ArrayList<Integer> dimensionList 	= new ArrayList<Integer>();		// list to hold the dimensions
		ArrayList<String> dimensionNames 	= new ArrayList<String>();		// list to hold the dimension names
		ArrayList<String> batchUpdate 		= new ArrayList<String>();		// ArrayList to hold the update statements
		
		ArrayList<Neuron> neuronList		= new ArrayList<Neuron>();		// list to hold the neurons that need complex classification
		ArrayList<Integer> normList			= new ArrayList<Integer>();		// list to hold the neuron normalizations
		ArrayList<String[]> bmIVList		= new ArrayList<String[]>();	// list to hold an array of best matching input vectors
		
		String elementTable 				= null;							// identify the ts element table
		int idIdx							= -1;							// first or second id
		
		if (perspective.equalsIgnoreCase("l_at")) {
			
			numObjects = numLoci;			
			numDimensions = numAttributes * numTimes;
			
			objectList.add(numLoci);
			dimensionList.add(numAttributes);
			dimensionList.add(numTimes);
			dimensionNames.add("a");
			dimensionNames.add("t");
			
			if (element.equalsIgnoreCase("l") || element.equalsIgnoreCase("locus") || element.equalsIgnoreCase("loci")) {
				elementTable = "locus_key";				
			}
			
		} else if (perspective.equalsIgnoreCase("a_lt")) {		
			
			numObjects = numAttributes;
			numDimensions = numLoci * numTimes;
			
			objectList.add(numAttributes);
			dimensionList.add(numLoci);
			dimensionList.add(numTimes);
			dimensionNames.add("l");
			dimensionNames.add("t");
			
			if (element.equalsIgnoreCase("a") || element.equalsIgnoreCase("attribute") || element.equalsIgnoreCase("attributes")) {
				elementTable = "attribute_key";
			}
			
		} else if (perspective.equalsIgnoreCase("t_la")) {
			
			numObjects = numTimes;
			numDimensions = numLoci * numAttributes;
			
			objectList.add(numTimes);
			dimensionList.add(numLoci);
			dimensionList.add(numAttributes);
			dimensionNames.add("l");
			dimensionNames.add("a");
			
			if (element.equalsIgnoreCase("t") || element.equalsIgnoreCase("time") || element.equalsIgnoreCase("times")) {
				elementTable = "time_key";
			}
			
		} else if (perspective.equalsIgnoreCase("la_t")) {
			
			numObjects = numLoci * numAttributes;
			numDimensions = numTimes;
			
			objectList.add(numLoci);
			objectList.add(numAttributes);
			dimensionList.add(numTimes);
			dimensionNames.add("t");
			
			if (element.equalsIgnoreCase("l") || element.equalsIgnoreCase("locus") || element.equalsIgnoreCase("loci")) {
				elementTable = "locus_key";
				idIdx = 1;
			} else if (element.equalsIgnoreCase("a") || element.equalsIgnoreCase("attribute") || element.equalsIgnoreCase("attributes")) {
				elementTable = "attribute_key";
				idIdx = 2;
			}
			
		} else if (perspective.equalsIgnoreCase("lt_a")) {			
			numObjects = numLoci * numTimes;
			numDimensions = numAttributes;
			
			objectList.add(numLoci);
			objectList.add(numTimes);
			dimensionList.add(numAttributes);
			dimensionNames.add("a");
			
			if (element.equalsIgnoreCase("l") || element.equalsIgnoreCase("locus") || element.equalsIgnoreCase("loci")) {
				elementTable = "locus_key";
				idIdx = 1;
			} else if (element.equalsIgnoreCase("t") || element.equalsIgnoreCase("time") || element.equalsIgnoreCase("times")) {
				elementTable = "time_key";
				idIdx = 2;
			}
			
		} else if (perspective.equalsIgnoreCase("at_l")) {
			
			numObjects = numAttributes * numTimes;
			numDimensions = numLoci;
			
			objectList.add(numAttributes);
			objectList.add(numTimes);
			dimensionList.add(numLoci);
			dimensionNames.add("l");
			
			if (element.equalsIgnoreCase("a") || element.equalsIgnoreCase("attribute") || element.equalsIgnoreCase("attributes")) {
				elementTable = "attribute_key";
				idIdx = 1;
			} else if (element.equalsIgnoreCase("t") || element.equalsIgnoreCase("time") || element.equalsIgnoreCase("times")) {
				elementTable = "time_key";
				idIdx = 2;
			}
			
		} else {
			return false;
		}
		
		if (elementTable == null) {
			System.out.println("Element provided: " + element + " -> is not valid for perspective: " + perspective);
			return false;
		}
		
		String readTable = schema + "." + perspective.toLowerCase();				// input vector table
		String writeTable = schema + "." + perspective.toLowerCase() + "_neurons";	// neuron table
		String writeColumn = "iv_" + column;										// column to write to in neuron table
		
		// make sure the column to read exists
		if (!db.columnExists(schema, elementTable, column)) {
			System.out.println("column " + column + " does not exist in table " + schema + "." + elementTable);
			return false;
		}
		
		// check if the column to write to exists
		if (!db.columnExists(schema, perspective.toLowerCase() + "_neurons", writeColumn)) {			
			if (!db.addColumn(writeTable, writeColumn, db.getColumnDataType(schema, elementTable, column))) {
				return false;
			}
		}
		
		String dimensions = "";
		
		if (dimensionList.size() == 1) {
			for (int i = 1; i <= dimensionList.get(0); i++) {
				dimensions += dimensionNames.get(0) + i + ",";
			}
		} else {
			for (int i = 1; i <= dimensionList.get(0); i++) {
				for (int j = 1; j <= dimensionList.get(1); j++) {
					dimensions += dimensionNames.get(0) + i + "_" + dimensionNames.get(1) + j + ",";
				}
			}
		}
		dimensions = dimensions.substring(0, dimensions.length() - 1);
		
		try {
			String query = "SELECT id, normalization, ivs, num_ivs, " + dimensions + " FROM " + writeTable + ";";
			
//			System.out.println(query);

			Statement stmt = db.getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
			
			while (rs.next()) {
				
				if (rs.getInt(4) == 0 || rs.getInt(4) == 1) {  // neurons only have 0 or 1 input vectors
					String bmIV = rs.getString(3);
					
					if (rs.getInt(4) == 0) {  // if no input vectors
						bmIV = bmIV.substring(1);  // remove the _ prefix
					}
					
					if (objectList.size() == 1) {  // if not composite perspective
						query = "UPDATE " + writeTable + " SET " + writeColumn + " = (SELECT ts." + column + " FROM " + readTable 
								+ " INNER JOIN " + schema + "." + elementTable + " ts ON id = ts.id WHERE normalization = " + rs.getInt(2) 
								+ " AND id = " + bmIV + ") WHERE id = " + rs.getInt(1) + " AND normalization = " + rs.getInt(2); 
				
						batchUpdate.add(query);	
						
					} else {	// if composite perspective
						
						query = "UPDATE " + writeTable + " SET " + writeColumn + " = (SELECT ts." + column + " FROM " + readTable
								+ " INNER JOIN " + schema + "." + elementTable + " ts on id" + idIdx + " = ts.id WHERE normalization = " 
								+ rs.getInt(2) + " AND id1 = " + bmIV.split("_")[0]	+ " AND id2 = "	+ bmIV.split("_")[1] 
								+ ") WHERE id = " + rs.getInt(1) + " AND normalization = " + rs.getInt(2); 
				
						batchUpdate.add(query);	
					}
					
				} else {
					
					bmIVList.add(rs.getString(3).split(","));
					normList.add(rs.getInt(2));
					
					float[] neuronAttributes = new float[numDimensions];
					
					for (int i = 0; i < numDimensions; i++) {
						neuronAttributes[i] = rs.getInt(i+5);
					}
					
					Neuron tmpNeuron = new Neuron(rs.getInt(1), neuronAttributes);
					
					neuronList.add(tmpNeuron);
				}
				
			}
			
			rs.close();
			stmt.close();
			
			/// iterate thru neurons that don't have 0 or 1 input vectors matched to them
			for (int i = 0; i < neuronList.size(); i++) {
				
				ArrayList<InputVector> ivs = new ArrayList<InputVector>();
				
				for (int j = 0; j < bmIVList.get(i).length; j++) {

					if (objectList.size() == 1) {
						
						query = "SELECT id, normalization, ts." + column + ", " + dimensions + " FROM " + readTable 
								+ " INNER JOIN " + schema + "." + elementTable + " ts ON id = ts.id WHERE id = " + bmIVList.get(i)[j] 
								+ " AND normalization = " + normList.get(i) + ";";
						
					} else {
						
						String id1 = bmIVList.get(i)[j].split("_")[0];
						String id2 = bmIVList.get(i)[j].split("_")[1];
						
						query = "SELECT id1, id2, normalization, ts." + column + ", " + dimensions + " FROM " + readTable 
								+ " INNER JOIN " + schema + "." + elementTable + " ts on id" + idIdx + " = ts.id WHERE id1 = " + id1 
								+ " AND id2 = " + id2 + " AND normalization = " + normList.get(i) + ";";
					
					}
					
//					System.out.println(query);

					stmt = db.getConnection().createStatement();
					rs = stmt.executeQuery(query);
					
					while (rs.next()) {
						
						float[] ivAttributes = new float[numDimensions];
						
						if (objectList.size() == 1) {
							String id = rs.getString(1);
							String classLabel = rs.getString(3);
							
							for (int n = 0; n < numDimensions; n++) {
								ivAttributes[n] = rs.getFloat(n+4);
							}
							
							InputVector tmpIV = new InputVector(0, ivAttributes, id, classLabel);
							ivs.add(tmpIV);
						} else {
							String id = rs.getString(1);
							id += rs.getString(2);
							String classLabel = rs.getString(4);
							
							for (int n = 0; n < numDimensions; n++) {
								ivAttributes[n] = rs.getFloat(n+5);
							}
							
							InputVector tmpIV = new InputVector(0, ivAttributes, id, classLabel);
							ivs.add(tmpIV);
						}						
					}
					
					rs.close();
					stmt.close();
				}
				
				ArrayList<ArrayList<InputVector>> classIVs = new ArrayList<ArrayList<InputVector>>();	// 2D array to hold class-to-IV
				ArrayList<String> classes = new ArrayList<String>();			// list to store the classes
				ArrayList<Integer> classFrequency = new ArrayList<Integer>();	// list to store class frequency
				
				for (int j = 0; j < ivs.size(); j++) {
					if (!classes.contains(ivs.get(j).getGeoID())) {
						classes.add(ivs.get(j).getGeoID());		// store the class name
						classFrequency.add(1);					// initialize frequency to 1
						ArrayList<InputVector> tmpItems = new ArrayList<InputVector>();	// list of IVs for this class
						tmpItems.add(ivs.get(j));				// initialize with this neuron
						classIVs.add(tmpItems);					// add this list of IVs to this class
					} else {						
						int classIdx = classes.indexOf(ivs.get(j).getGeoID());	// get the index that corresponds to a particular class						
						classFrequency.set(classIdx, (classFrequency.get(classIdx) + 1));  // increment the count for that class						
						classIVs.get(classIdx).add(ivs.get(j));					// append the IV to that class
					}
				}

				// determine which class has the most IVs
				// initialize count to -1
				int maxNumber = -1;

				// iterate thru all class counts
				for (int j = 0; j < classFrequency.size(); j++) {
					// store the max
					if (classFrequency.get(j) > maxNumber) {
						maxNumber = classFrequency.get(j);
					}
				}

				// store the index(es) of the LC class(es) that have the max count
				ArrayList<Integer> winners = new ArrayList<Integer>();
				for (int j = 0; j < classFrequency.size(); j++) {
					if (classFrequency.get(j) == maxNumber) {
						winners.add(j);
					}
				}

				if (winners.size() == 1) { // case 1: There is only one winning class
					
					query = "UPDATE " + writeTable + " SET " + writeColumn + " = '" 
							+  classIVs.get(winners.get(0)).get(0).getGeoID() + "' WHERE id = " 
							+ neuronList.get(i).getID() + " AND normalization = " + normList.get(i); 
					
					batchUpdate.add(query);	

				}  else { // case 2: there is a 'tie' between winning classes

					// since no class has the majority, the class that is more similar will be chosen

					// store the AQE for each class
					ArrayList<Float> classAQE = new ArrayList<Float>();

					// get the neuron
					Neuron neuron = neuronList.get(i);

					// parse thru each maximum class
					for (int j = 0; j < winners.size(); j++) {

						// initial AQE set to 0
						float tmpAQE = 0f;

						// iterate thru each IV
						for (int k = 0; k < classIVs.get(winners.get(j)).size(); k++) {

							// get float array to store each of the IV's attributes
							float[] ivAttributes = classIVs.get(winners.get(j)).get(k).getAttributes();

							// append the distance between each IV & the mapped neuron
							tmpAQE += neuron.getDistance(ivAttributes, distanceMeasure);
						}

						// get the Average distance
						tmpAQE /= classIVs.get(winners.get(j)).size();
						// store this distance
						classAQE.add(tmpAQE);
					}

					// find the class with the lowest AQE
					int winningIdx = 0;
					float minAQE = classAQE.get(0);
					for (int j = 1; j < classAQE.size(); j++) {
						if (classAQE.get(j) < minAQE) {
							minAQE = classAQE.get(j);
							winningIdx = j;
						}
					}
					
					query = "UPDATE " + writeTable + " SET " + writeColumn + " = '" 
							+  classIVs.get(winners.get(winningIdx)).get(0).getGeoID() + "' WHERE id = " 
							+ neuronList.get(i).getID() + " AND normalization = " + normList.get(i); 
					
//					if (objectList.size() == 1) {
//						query = "UPDATE " + writeTable + " SET " + writeColumn + " = '" 
//								+  ivs.get(winners.get(winningIdx)).getGeoID() + "' WHERE id = " 
//								+ ivs.get(winners.get(winningIdx)).getName() + " AND normalization = " + normList.get(i); 
//					} else {
//						String id1 = ivs.get(winners.get(winningIdx)).getName().split("_")[0];
//						String id2 = ivs.get(winners.get(winningIdx)).getName().split("_")[1];
//						
//						query = "UPDATE " + writeTable + " SET " + writeColumn + " = '" 
//								+  ivs.get(winners.get(winningIdx)).getGeoID() + "' WHERE id1 = " 
//								+ id1 + " AND id2 = " + id2 + " AND normalization = " + normList.get(i); 
//					}
					
					batchUpdate.add(query);	
				
				}
			}

			// build query statement of array of attributes
			String[] batchUpdateAr = batchUpdate.toArray(new String[batchUpdate.size()]);						// convert ArrayList to an array			
			db.insertIntoTableBatch(schema + "." + perspective.toLowerCase() + "_neurons", batchUpdateAr, BATCH_SIZE);		// try to insert the data into the table
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}						
		
		return true;
		
	}
	
	/**
	 * Write a class to the input vectors. Assumes only 1 header.
	 *
	 * @param db
	 *            - An instance of the PostgreSQLJDBC class.
	 * @param schema
	 *            - The schema in Postgres to write to.
	 * @param perspective
	 *            - The Tri-Space perspective to write to.
	 * @param column
	 *			  - The column to write to.            
	 * @param mdsData
	 *			  - The CSV containing the MDS coordiantes.
	 * @param removeLeadingChar
	 * 			  - If classData uses a prefix set to yet (l1, a1, t1, etc.)
	 * @param normalizations
	 *      	  - The normalizations to classify 
	 */
	public static boolean writeMDS(PostgreSQLJDBC db, String schema, String perspective, String column,
									File mdsData, boolean removeLeadingChar, int normalization) {
		
		ArrayList<String> batchUpdate = new ArrayList<String>();		// ArrayList to hold the insertion statements
		
		boolean compositePerspective;									// test if composite perspective: LA_T, LT_A, or AT_L
		if (perspective.equalsIgnoreCase("l_at") || perspective.equalsIgnoreCase("a_lt") || perspective.equalsIgnoreCase("t_la")) {
			compositePerspective = false;
		} else if (perspective.equalsIgnoreCase("at_l") || perspective.equalsIgnoreCase("lt_a") || perspective.equalsIgnoreCase("la_t")) {
			compositePerspective = true;
		} else {
			return false;												// return false (fail)
		}		
		
		String writeTable = schema + "." + perspective.toLowerCase();					
			
		try {
			
			BufferedReader brClass = new BufferedReader(new FileReader(mdsData.getAbsolutePath()));  //read the class file			
							
			String line = brClass.readLine();									// variable to store the content of a line					
			
			while ((line = brClass.readLine()) != null) {						// iterate thru all lines in file				
									
				String[] lineData = line.split(",");							// store line data as an array
				
				ArrayList<String> objectList = new ArrayList<String>();			// list to hold number of objects
											
				String[] idArray = lineData[0].split("_");						// store the id (index 0)
				String id1 = idArray[0];
				String id2 = "";
				if (removeLeadingChar) id1 = id1.substring(1, id1.length());		// remove prefix (if necessary)
				objectList.add(id1);									
				
				if (compositePerspective) { 										// if composite (2 identifiers)
											
					id2 = idArray[1];												// store the second id (index 1) 						
					if (removeLeadingChar) id2 = id2.substring(1, id2.length());	// remove prefix (if necessary)							
											
					objectList.add(id2);											// place id2 in array of data to be inserted
				}
				
				for (int i = 1; i < lineData.length; i++) {
					
					String setData = "ST_GeomFromText('POINT(" + lineData[1] + " " + lineData[2] + ")')";
					String query;
					
					if (!compositePerspective) {
						query = "UPDATE " + writeTable + " SET " + column
								+ " = " +  setData + " WHERE id = " + id1 + " AND normalization = " + normalization; 
					} else {
						query = "UPDATE " + writeTable + " SET " + column
								+ " = " +  setData + " WHERE id1 = " + id1 + " AND id2 = " + id2 
								+ " AND normalization = " + normalization; 
					}
					
					batchUpdate.add(query);	
				}				
			}				
			
			// build query statement of array of attributes
			String[] batchInsertionAr = batchUpdate.toArray(new String[batchUpdate.size()]);						// convert ArrayList to an array			
			db.insertIntoTableBatch(schema + "." + perspective.toLowerCase(), batchInsertionAr, BATCH_SIZE);		// try to insert the data into the table
			
			brClass.close();														// close the class BufferedReader
		} catch (IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}						
		
		return true;
		
	}
	
	/**
	 * Write a class to the input vectors.
	 *
	 * @param db
	 *            - An instance of the PostgreSQLJDBC class.
	 * @param schema
	 *            - The schema in Postgres to write to.
	 * @param perspective
	 *            - The Tri-Space perspective to write to.
	 * @param columnName
	 *            - The column to write to (Postgres).
	 * @param dataType
	 *			  - The datatype of the column (TEXT, INT, SMALLINT, etc.).
	 * @param classData
	 *			  - A CSV of classified input vectors.
	 * @param classLabels
	 *			  - A CSV of class labels (e.g. "1, Vegetation", "2, Urban", etc.)
	 * @param removeLeadingChar
	 * 			  - If classData uses a prefix set to yet (l1, a1, t1, etc.)
	 * @param headers
	 * 			  - The number of headers in classData CSV
	 * @param normalizations
	 *      	  - The normalizations to classify 
	 */
	public static boolean classifyIV(PostgreSQLJDBC db, String schema, String perspective, String columnName, String dataType, 
								 File classData, File classLabels, boolean removeLeadingChar, int headers, int[] normalizations) {
		
		ArrayList<String> batchUpdate = new ArrayList<String>();		// ArrayList to hold the insertion statements
		
		boolean compositePerspective;									// test if composite perspective: LA_T, LT_A, or AT_L
		if (perspective.equalsIgnoreCase("l_at") || perspective.equalsIgnoreCase("a_lt") || perspective.equalsIgnoreCase("t_la")) {
			compositePerspective = false;
		} else if (perspective.equalsIgnoreCase("at_l") || perspective.equalsIgnoreCase("lt_a") || perspective.equalsIgnoreCase("la_t")) {
			compositePerspective = true;
		} else {
			return false;												// return false (fail)
		}		
		
		String writeTable = schema + "." + perspective.toLowerCase();
		
		if (!db.addColumn(writeTable, columnName, dataType)) {
			return false;
		}
		
		ArrayList<String[]> classTypes = new ArrayList<String[]>();
		
		if (classLabels != null) {
			
			BufferedReader classDictionary;
			try {
				classDictionary = new BufferedReader(new FileReader(classLabels.getAbsolutePath()));
				String classLine = "";
				while ((classLine = classDictionary.readLine()) != null) {
					classTypes.add(classLine.split(","));
				}
				classLine = "";
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		
			
		try {
			
			BufferedReader brClass = new BufferedReader(new FileReader(classData.getAbsolutePath()));
			
			// remove headers
			while (headers > 0) {
				System.out.println("HEADERS");
				System.out.println(brClass.readLine());
				headers--;
			}
							
			String classLine = "";													// variable to store the content of a line
			String line = "";														// variable to store the content of a line					
			
			while ((line = brClass.readLine()) != null) {							// iterate thru all lines in file				
									
				String[] classDataLine = line.split(",");								// store line data as an array
				
				ArrayList<String> objectList = new ArrayList<String>();				// list to hold number of objects
		
									
				String[] idArray = classDataLine[0].split("_");							// store the id (index 0)
				String id1 = idArray[0];
				String id2 = "";
				if (removeLeadingChar) id1 = id1.substring(1, id1.length());		// remove prefix (if necessary)
				objectList.add(id1);									
				
				if (compositePerspective) { 										// if composite (2 identifiers)
											
					id2 = idArray[1];												// store the second id (index 1) 						
					if (removeLeadingChar) id2 = id2.substring(1, id2.length());	// remove prefix (if necessary)							
											
					objectList.add(id2);											// place id2 in array of data to be inserted
				}
				
				String setData = classDataLine[1];
				if (classLabels != null) {
					for (int i = 0; i < classTypes.size(); i++) {
						if (classTypes.get(i)[0].equalsIgnoreCase(classDataLine[1])) {
							setData = classTypes.get(i)[1];
						}							
					}
				}
				
				for (int i = 0; i < normalizations.length; i++) {
					
					String query;
					if (!compositePerspective) {
						query = "UPDATE " + writeTable + " SET " + columnName
								+ " = '" +  setData + "' WHERE id = " + id1 + " AND normalization = " + i; 
					} else {
						query = "UPDATE " + writeTable + " SET " + columnName
								+ " = '" +  setData + "' WHERE id1 = " + id1 + " AND id2 = " + id2 
								+ " AND normalization = " + i; 
					}
					
					batchUpdate.add(query);	
				}																							
								
			}				
			
			// build query statement of array of attributes
			String[] batchInsertionAr = batchUpdate.toArray(new String[batchUpdate.size()]);						// convert ArrayList to an array			
			db.insertIntoTableBatch(schema + "." + perspective.toLowerCase(), batchInsertionAr, BATCH_SIZE);		// try to insert the data into the table
			
			brClass.close();														// close the class BufferedReader
		} catch (IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}						
		
		return true;
		
	}

	public static void insertTSFromCSVBatch(PostgreSQLJDBC db, String schema, String ts, String parentPath, String scene, boolean removeLeadingChar) {
		
		// ArrayList to hold the insertion statements
		ArrayList<String> batchInsertion = new ArrayList<String>();				
		
		if (ts.equals("L_AT")) {
			
			String miniPath = "L_AT";
			
			if (!scene.equals("")) {
				if (!scene.contains("_")) {
					miniPath += "_";
				}
			}
			
			miniPath = miniPath + scene + ".csv";
			
			String[] input = {
					parentPath + "/Non_Normalized/" + miniPath,
					parentPath + "/L_AT_Normalized/" + miniPath,
					parentPath + "/A_LT_Normalized/" + miniPath,
					parentPath + "/T_LA_Normalized/" + miniPath,
					parentPath + "/LA_T_Normalized/" + miniPath,
					parentPath + "/LT_A_Normalized/" + miniPath,
					parentPath + "/AT_L_Normalized/" + miniPath
			};					
			
			for (int i = 0; i < 7; i++) {
				try {
					BufferedReader br = new BufferedReader(new FileReader(input[i]));
					
					// remove headers
					br.readLine();
					br.readLine();
					
					String line = "";
					
					while ((line = br.readLine()) != null) {
						String[] lineData = line.split(",");
						String[] insertData = new String[db.getTableColumnIds(schema + ".l_at").length];	
						
						String uid = lineData[0];
						if (removeLeadingChar) {
							uid = uid.substring(1, uid.length());
						}
						
						insertData[0] = uid;
						insertData[1] = i + "";
						
						for (int j = 1; j < lineData.length; j++) {
							insertData[j+1] = lineData[j];
						}
						
						insertData[insertData.length - 1] = "-1";
						
						String queryStatement = db.insertQueryBuilder(schema + ".l_at",insertData);
						batchInsertion.add(queryStatement);
					}
					
					br.close();
					
				} catch (IOException | SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}												
			}
			
			String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);

			try {
				db.insertIntoTableBatch(schema + ".l_at", batchInsertionAr, 50000);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else if (ts.equals("LA_T")) {
			
			String miniPath = "LA_T";
			
			if (!scene.equals("")) {
				if (!scene.contains("_")) {
					miniPath += "_";
				}
			}
			
			miniPath = miniPath + scene + ".csv";
			
			String[] input = {
					parentPath + "/Non_Normalized/" + miniPath,
					parentPath + "/L_AT_Normalized/" + miniPath,
					parentPath + "/A_LT_Normalized/" + miniPath,
					parentPath + "/T_LA_Normalized/" + miniPath,
					parentPath + "/LA_T_Normalized/" + miniPath,
					parentPath + "/LT_A_Normalized/" + miniPath,
					parentPath + "/AT_L_Normalized/" + miniPath
			};
			
			for (int i = 0; i < 7; i++) {
				
				try {
					
					BufferedReader br = new BufferedReader(new FileReader(input[i]));
					
					// remove header
					br.readLine();
					
					String line = "";
					
					while ((line = br.readLine()) != null) {
						String[] lineData = line.split(",");
						String[] insertData = new String[db.getTableColumnIds(schema + ".la_t").length];	
						
						String uid = lineData[0];
						
						if (removeLeadingChar) {
							String id1 = lineData[0];
							id1 = id1.substring(1, id1.length());
							String id2 = lineData[1];
							id2 = id2.substring(1, id2.length());
							insertData[0] = id1;
							insertData[1] = id2;
						} else {
							insertData[0] = lineData[0];
							insertData[1] = lineData[1];
						}

						insertData[2] = i + "";
						
						for (int j = 2; j < lineData.length; j++) {
							insertData[j+1] = lineData[j];
						}
						
						insertData[insertData.length - 1] = "-1";
						
						String queryStatement = db.insertQueryBuilder(schema + ".la_t",insertData);
						batchInsertion.add(queryStatement);
					}
					
					br.close();
					
				} catch (IOException | SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}												
			}
			
			String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);

			try {
				db.insertIntoTableBatch(schema + ".l_at", batchInsertionAr, 50000);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}						

		} else if (ts.equals("LT_A")) {
			String miniPath = "LT_A";
			if (!scene.equals("")) {
				if (!scene.contains("_")) {
					miniPath += "_";
				}
			}
			miniPath = miniPath + scene + ".csv";
			BufferedReader[] br = new BufferedReader[7];
			try {
				br[0] = new BufferedReader(new FileReader(parentPath + "/Non_Normalized/" + miniPath));
				br[1] = new BufferedReader(new FileReader(parentPath + "/L_AT_Normalized/" + miniPath));
				br[2] = new BufferedReader(new FileReader(parentPath + "/A_LT_Normalized/" + miniPath));
				br[3] = new BufferedReader(new FileReader(parentPath + "/T_LA_Normalized/" + miniPath));
				br[4] = new BufferedReader(new FileReader(parentPath + "/LA_T_Normalized/" + miniPath));
				br[5] = new BufferedReader(new FileReader(parentPath + "/LT_A_Normalized/" + miniPath));
				br[6] = new BufferedReader(new FileReader(parentPath + "/AT_L_Normalized/" + miniPath));

				// remove headers
				for (int i = 0; i < br.length; i++) {
					br[i].readLine();
					//					br[i].readLine();
				}

				String[] line =  new String[7];
				while ((line[0] = br[0].readLine()) != null) {
					//					System.out.println(line[0]);
					for (int i = 1; i < br.length; i++) {
						line[i] = br[i].readLine();
					}
					String[][] data = new String[7][];
					for (int i = 0; i < data.length; i++) {
						data[i] = line[i].split(",");
					}
					//					String[] data = line.split(",");
					String[] insertData = new String[db.getTableColumnIds(schema + ".lt_a").length];
					if (removeLeadingChar) {
						String id1 = data[0][0];
						id1 = id1.substring(1, id1.length());
						String id2 = data[0][1];
						id2 = id2.substring(1, id2.length());
//						+ "_" + data[0][1];
						//					uid = uid.substring(1, uid.length());
//						insertData[0] = "'"+uid+"'";
						insertData[0] = "'"+id1+"'";
						insertData[1] = "'"+id2+"'";
					} else {
//						String uid = data[0][0] + "_" + data[0][1];
						//					uid = uid.substring(1, uid.length());
//						insertData[0] = "'"+uid+"'";
						insertData[0] = "'"+data[0][0]+"'";
						insertData[1] = "'"+data[0][1]+"'";
					}
					
					
					
					int counter1 = 0;
					int counter2 = 2;
					int totalCount = 2;

					for (int i = 2; i < insertData.length - 6; i++) {
						insertData[i] = data[counter1][counter2];
						if (counter2 >= data[counter1].length-1) {
							counter1++;
							counter2 = 2;
						} else {
							counter2++;
						}
						
						totalCount++;
					}
					
					for (int i = 1; i < 7; i++) {
						insertData[totalCount++] = "-1";
					}
					
					//					db.insertIntoTable(schema + ".lt_a",insertData);
					String queryStatement = db.insertQueryBuilder(schema + ".lt_a",insertData);
					batchInsertion.add(queryStatement);
				}
				for (int i = 0; i < br.length; i++) {
					br[i].close();
				}	

				String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);

				//				for (int i = 0; i < batchInsertionAr.length; i++) {
				//					System.out.println(batchInsertionAr[i]);
				//				}

				db.insertIntoTableBatch(schema + ".lt_a", batchInsertionAr);
			} catch (IOException | SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else if (ts.equals("A_LT")) {
			String miniPath = "A_LT";
			if (!scene.equals("")) {
				if (!scene.contains("_")) {
					miniPath += "_";
				}
			}
			miniPath = miniPath + scene + ".csv";
			BufferedReader[] br = new BufferedReader[7];
			try {
				br[0] = new BufferedReader(new FileReader(parentPath + "/Non_Normalized/" + miniPath));
				br[1] = new BufferedReader(new FileReader(parentPath + "/L_AT_Normalized/" + miniPath));
				br[2] = new BufferedReader(new FileReader(parentPath + "/A_LT_Normalized/" + miniPath));
				br[3] = new BufferedReader(new FileReader(parentPath + "/T_LA_Normalized/" + miniPath));
				br[4] = new BufferedReader(new FileReader(parentPath + "/LA_T_Normalized/" + miniPath));
				br[5] = new BufferedReader(new FileReader(parentPath + "/LT_A_Normalized/" + miniPath));
				br[6] = new BufferedReader(new FileReader(parentPath + "/AT_L_Normalized/" + miniPath));

				// remove headers
				for (int i = 0; i < br.length; i++) {
					br[i].readLine();
					br[i].readLine();
				}

				String[] line =  new String[7];
				while ((line[0] = br[0].readLine()) != null) {
					//					System.out.println(line[0]);
					for (int i = 1; i < br.length; i++) {
						line[i] = br[i].readLine();
					}
					String[][] data = new String[7][];
					for (int i = 0; i < data.length; i++) {
						data[i] = line[i].split(",");
					}
					//					String[] data = line.split(",");
					String[] insertData = new String[db.getTableColumnIds(schema + ".a_lt").length];
					String uid = data[0][0];
					if (removeLeadingChar) {
						uid = uid.substring(1, uid.length());
					}
					//					
//					System.out.println("UID " + uid);
					insertData[0] = "'"+uid+"'";
					int counter1 = 0;
					int counter2 = 1;
					int totalCount = 1;

					for (int i = 1; i < insertData.length-6; i++) {
						insertData[i] = data[counter1][counter2];
						if (counter2 >= data[counter1].length-1) {
							counter1++;
							counter2 = 1;
						} else {
							counter2++;
						}
						totalCount++;
					}
					
					for (int i = 1; i < 7; i++) {
						insertData[totalCount++] = "-1";
					}
					//					db.insertIntoTable(schema + ".l_at",insertData);
					String queryStatement = db.insertQueryBuilder(schema + ".a_lt",insertData);
					batchInsertion.add(queryStatement);
				}
				for (int i = 0; i < br.length; i++) {
					br[i].close();
				}



				String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);

				//				for (int i = 0; i < batchInsertionAr.length; i++) {
				//					System.out.println(batchInsertionAr[i]);
				//				}

				db.insertIntoTableBatch(schema + ".a_lt", batchInsertionAr);
			} catch (IOException | SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else if (ts.equals("AT_L")) {
			String miniPath = "AT_L";
			if (!scene.equals("")) {
				if (!scene.contains("_")) {
					miniPath += "_";
				}
			}
			miniPath = miniPath + scene + ".csv";
			BufferedReader[] br = new BufferedReader[7];
			try {
				br[0] = new BufferedReader(new FileReader(parentPath + "/Non_Normalized/" + miniPath));
				br[1] = new BufferedReader(new FileReader(parentPath + "/L_AT_Normalized/" + miniPath));
				br[2] = new BufferedReader(new FileReader(parentPath + "/A_LT_Normalized/" + miniPath));
				br[3] = new BufferedReader(new FileReader(parentPath + "/T_LA_Normalized/" + miniPath));
				br[4] = new BufferedReader(new FileReader(parentPath + "/LA_T_Normalized/" + miniPath));
				br[5] = new BufferedReader(new FileReader(parentPath + "/LT_A_Normalized/" + miniPath));
				br[6] = new BufferedReader(new FileReader(parentPath + "/AT_L_Normalized/" + miniPath));

				// remove headers
				for (int i = 0; i < br.length; i++) {
					br[i].readLine();
					//					br[i].readLine();
				}

				String[] line =  new String[7];
				while ((line[0] = br[0].readLine()) != null) {
					//					System.out.println(line[0]);
					for (int i = 1; i < br.length; i++) {
						line[i] = br[i].readLine();
					}
					String[][] data = new String[7][];
					for (int i = 0; i < data.length; i++) {
						data[i] = line[i].split(",");
					}
					//					String[] data = line.split(",");
					String[] insertData = new String[db.getTableColumnIds(schema + ".at_l").length];
					
//					System.out.println("NUMBER OF LA_T COLUMNS: " + insertData.length);
					if (removeLeadingChar) {
						String id1 = data[0][0];
						id1 = id1.substring(1, id1.length());
						String id2 = data[0][1];
						id2 = id2.substring(1, id2.length());
//						+ "_" + data[0][1];
						//					uid = uid.substring(1, uid.length());
//						insertData[0] = "'"+uid+"'";
						insertData[0] = "'"+id1+"'";
						insertData[1] = "'"+id2+"'";
					} else {
//						String uid = data[0][0] + "_" + data[0][1];
						//					uid = uid.substring(1, uid.length());
//						insertData[0] = "'"+uid+"'";
						insertData[0] = "'"+data[0][0]+"'";
						insertData[1] = "'"+data[0][1]+"'";
					}
					
//					System.out.println(insertData[0] + "-" + insertData[1]);
					
					int counter1 = 0;
					int counter2 = 2;
					int totalCount = 2;

					for (int i = 2; i < insertData.length - 6; i++) {
						insertData[i] = data[counter1][counter2];
						if (counter2 >= data[counter1].length-1) {
							counter1++;
							counter2 = 2;
						} else {
							counter2++;
						}
						
						totalCount++;
					}
					
					for (int i = 1; i < 7; i++) {
						insertData[totalCount++] = "-1";
					}
					//					db.insertIntoTable(schema + ".la_t",insertData);
					String queryStatement = db.insertQueryBuilder(schema + ".at_l",insertData);
					System.out.println(queryStatement);
					batchInsertion.add(queryStatement);
				}
				for (int i = 0; i < br.length; i++) {
					br[i].close();
				}	

				String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);

				//				for (int i = 0; i < batchInsertionAr.length; i++) {
				//					System.out.println(batchInsertionAr[i]);
				//				}

				db.insertIntoTableBatch(schema + ".at_l", batchInsertionAr);
			} catch (IOException | SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
	
	public static void insertTSFromCSVBatch(PostgreSQLJDBC db, String schema, String parentPath, String scene, File lt_a_LC, File landCoverDict, boolean removeLeadingChar) throws IOException {
		ArrayList<String> batchInsertion = new ArrayList<String>();

		String miniPath = "LT_A";
		if (!scene.equals("")) {
			if (!scene.contains("_")) {
				miniPath += "_";
			}
		}
		miniPath = miniPath + scene + ".csv";
		BufferedReader[] br = new BufferedReader[7];
		
		ArrayList<String[]> landcoverTypes = new ArrayList<String[]>();
		BufferedReader landcoverDictionary = new BufferedReader(new FileReader(landCoverDict.getAbsolutePath()));
		String lcLine = "";
		while ((lcLine = landcoverDictionary.readLine()) != null) {
			landcoverTypes.add(lcLine.split(","));
		}
		lcLine = "";
		
		
		try {
			BufferedReader brLC = new BufferedReader(new FileReader(lt_a_LC.getAbsolutePath()));
			br[0] = new BufferedReader(new FileReader(parentPath + "/Non_Normalized/" + miniPath));
			br[1] = new BufferedReader(new FileReader(parentPath + "/L_AT_Normalized/" + miniPath));
			br[2] = new BufferedReader(new FileReader(parentPath + "/A_LT_Normalized/" + miniPath));
			br[3] = new BufferedReader(new FileReader(parentPath + "/T_LA_Normalized/" + miniPath));
			br[4] = new BufferedReader(new FileReader(parentPath + "/LA_T_Normalized/" + miniPath));
			br[5] = new BufferedReader(new FileReader(parentPath + "/LT_A_Normalized/" + miniPath));
			br[6] = new BufferedReader(new FileReader(parentPath + "/AT_L_Normalized/" + miniPath));

			// remove headers
			for (int i = 0; i < br.length; i++) {
				br[i].readLine();
				//					br[i].readLine();
			}
			lcLine = brLC.readLine();

			String[] line =  new String[7];
//			int test = 0;
			while ((line[0] = br[0].readLine()) != null) {
				//					System.out.println(line[0]);
				for (int i = 1; i < br.length; i++) {
					line[i] = br[i].readLine();
				}
				lcLine = brLC.readLine();
				
				String[][] data = new String[7][];
				for (int i = 0; i < data.length; i++) {
					data[i] = line[i].split(",");
				}
				//					String[] data = line.split(",");
				String[] insertData = new String[db.getTableColumnIds(schema + ".lt_a").length];
//				String uid = data[0][0] + "_" + data[0][1];
				//					uid = uid.substring(1, uid.length());
//				insertData[0] = "'"+uid+"'";
				if (removeLeadingChar) {
					String id1 = data[0][0];
					id1 = id1.substring(1, id1.length());
					String id2 = data[0][1];
					id2 = id2.substring(1, id2.length());
//					+ "_" + data[0][1];
					//					uid = uid.substring(1, uid.length());
//					insertData[0] = "'"+uid+"'";
					insertData[0] = "'"+id1+"'";
					insertData[1] = "'"+id2+"'";
				} else {
					String uid = data[0][0] + "_" + data[0][1];
					//					uid = uid.substring(1, uid.length());
//					insertData[0] = "'"+uid+"'";
					insertData[0] = "'"+data[0][0]+"'";
					insertData[1] = "'"+data[0][1]+"'";
				}
				int counter1 = 0;
				int counter2 = 2;
				int totalCount = 2;

				for (int i = 2; i < insertData.length-7; i++) {
					insertData[i] = data[counter1][counter2];
					if (counter2 >= data[counter1].length-1) {
						counter1++;
						counter2 = 2;
					} else {
						counter2++;
					}
					totalCount++;
				}
				
				for (int i = 1; i < 7; i++) {
					insertData[totalCount++] = "-1";
				}
				
				String tmpClass = "";
				String[] lcLineSplit = lcLine.split(",");
				for (int i = 0; i < landcoverTypes.size(); i++) {
					if (landcoverTypes.get(i)[0].equals(lcLineSplit[1])) {
						tmpClass = landcoverTypes.get(i)[1];
						break;
					}
				}
				
				insertData[insertData.length-1] = "'" + tmpClass + "'";
				//					db.insertIntoTable(schema + ".lt_a",insertData);
				String queryStatement = db.insertQueryBuilder(schema + ".lt_a",insertData);
//				test++;
//				if (test < 8) System.out.println(queryStatement);
				batchInsertion.add(queryStatement);
			}
			for (int i = 0; i < br.length; i++) {
				br[i].close();
			}	
			brLC.close();

			String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);

			//				for (int i = 0; i < batchInsertionAr.length; i++) {
			//					System.out.println(batchInsertionAr[i]);
			//				}

			db.insertIntoTableBatch(schema + ".lt_a", batchInsertionAr);
		} catch (IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	
	}

	public static void insertTSFromCSV(PostgreSQLJDBC db, String schema, String ts, String parentPath, String scene, int startIdx, int endIdx) {
		int parseCounter = 0;
		if (ts.equals("L_AT")) {
			String miniPath = "L_AT";
			if (!scene.equals("")) {
				if (!scene.contains("_")) {
					miniPath += "_";
				}
			}
			miniPath = miniPath + scene + ".csv";
			BufferedReader[] br = new BufferedReader[7];
			try {
				br[0] = new BufferedReader(new FileReader(parentPath + "/Non_Normalized/" + miniPath));
				br[1] = new BufferedReader(new FileReader(parentPath + "/L_AT_Normalized/" + miniPath));
				br[2] = new BufferedReader(new FileReader(parentPath + "/A_LT_Normalized/" + miniPath));
				br[3] = new BufferedReader(new FileReader(parentPath + "/T_LA_Normalized/" + miniPath));
				br[4] = new BufferedReader(new FileReader(parentPath + "/LA_T_Normalized/" + miniPath));
				br[5] = new BufferedReader(new FileReader(parentPath + "/LT_A_Normalized/" + miniPath));
				br[6] = new BufferedReader(new FileReader(parentPath + "/AT_L_Normalized/" + miniPath));

				// remove headers
				for (int i = 0; i < br.length; i++) {
					br[i].readLine();
					br[i].readLine();
				}



				String[] line =  new String[7];
				while ((line[0] = br[0].readLine()) != null) {
					if (parseCounter >= startIdx && parseCounter < endIdx) {
						//						System.out.println(line[0]);
						for (int i = 1; i < br.length; i++) {
							line[i] = br[i].readLine();
						}
						String[][] data = new String[7][];
						for (int i = 0; i < data.length; i++) {
							data[i] = line[i].split(",");
						}
						String[] insertData = new String[db.getTableColumnIds(schema + ".l_at").length];
						String uid = data[0][0];
						insertData[0] = "'"+uid+"'";
						int counter1 = 0;
						int counter2 = 1;
						int totalCount = 1;

						for (int i = 1; i < insertData.length-6; i++) {
							insertData[i] = data[counter1][counter2];
							if (counter2 >= data[counter1].length-1) {
								counter1++;
								counter2 = 1;
							} else {
								counter2++;
							}
							totalCount++;
						}
						
						for (int i = 1; i < 7; i++) {
							insertData[totalCount++] = "-1";
						}

						db.insertIntoTable(schema + ".l_at",insertData);
					}

					parseCounter++;
				}
				for (int i = 0; i < br.length; i++) {
					br[i].close();
				}
			} catch (IOException | SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else if (ts.equals("LA_T")) {
			String miniPath = "LA_T";
			if (!scene.equals("")) {
				if (!scene.contains("_")) {
					miniPath += "_";
				}
			}
			miniPath = miniPath + scene + ".csv";
			BufferedReader[] br = new BufferedReader[7];
			try {
				br[0] = new BufferedReader(new FileReader(parentPath + "/Non_Normalized/" + miniPath));
				br[1] = new BufferedReader(new FileReader(parentPath + "/L_AT_Normalized/" + miniPath));
				br[2] = new BufferedReader(new FileReader(parentPath + "/A_LT_Normalized/" + miniPath));
				br[3] = new BufferedReader(new FileReader(parentPath + "/T_LA_Normalized/" + miniPath));
				br[4] = new BufferedReader(new FileReader(parentPath + "/LA_T_Normalized/" + miniPath));
				br[5] = new BufferedReader(new FileReader(parentPath + "/LT_A_Normalized/" + miniPath));
				br[6] = new BufferedReader(new FileReader(parentPath + "/AT_L_Normalized/" + miniPath));

				// remove headers
				for (int i = 0; i < br.length; i++) {
					br[i].readLine();
					//					br[i].readLine();
				}

				String[] line =  new String[7];
				while ((line[0] = br[0].readLine()) != null) {
					if (parseCounter >= startIdx && parseCounter < endIdx) {
						//						System.out.println(line[0]);
						for (int i = 1; i < br.length; i++) {
							line[i] = br[i].readLine();
						}
						String[][] data = new String[7][];
						for (int i = 0; i < data.length; i++) {
							data[i] = line[i].split(",");
						}
						//					String[] data = line.split(",");
						String[] insertData = new String[db.getTableColumnIds(schema + ".la_t").length];
						String uid = data[0][0] + "_" + data[0][1];
						//					uid = uid.substring(1, uid.length());
						insertData[0] = "'"+uid+"'";
						int counter1 = 0;
						int counter2 = 2;
						int totalCount = 1;

						for (int i = 1; i < insertData.length-6; i++) {
							insertData[i] = data[counter1][counter2];
							if (counter2 >= data[counter1].length-1) {
								counter1++;
								counter2 = 2;
							} else {
								counter2++;
							}
							totalCount++;
						}
						
						for (int i = 1; i < 7; i++) {
							insertData[totalCount++] = "-1";
						}
						System.out.println("LA_T Object number " + parseCounter);
						db.insertIntoTable(schema + ".la_t",insertData);
					}
					parseCounter++;
				}
				for (int i = 0; i < br.length; i++) {
					br[i].close();
				}	
			} catch (IOException | SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else if (ts.equals("LT_A")) {
			String miniPath = "LT_A";
			if (!scene.equals("")) {
				if (!scene.contains("_")) {
					miniPath += "_";
				}
			}
			miniPath = miniPath + scene + ".csv";
			BufferedReader[] br = new BufferedReader[7];
			try {
				br[0] = new BufferedReader(new FileReader(parentPath + "/Non_Normalized/" + miniPath));
				br[1] = new BufferedReader(new FileReader(parentPath + "/L_AT_Normalized/" + miniPath));
				br[2] = new BufferedReader(new FileReader(parentPath + "/A_LT_Normalized/" + miniPath));
				br[3] = new BufferedReader(new FileReader(parentPath + "/T_LA_Normalized/" + miniPath));
				br[4] = new BufferedReader(new FileReader(parentPath + "/LA_T_Normalized/" + miniPath));
				br[5] = new BufferedReader(new FileReader(parentPath + "/LT_A_Normalized/" + miniPath));
				br[6] = new BufferedReader(new FileReader(parentPath + "/AT_L_Normalized/" + miniPath));

				// remove headers
				for (int i = 0; i < br.length; i++) {
					br[i].readLine();
					//					br[i].readLine();
				}

				String[] line =  new String[7];
				while ((line[0] = br[0].readLine()) != null) {
					if (parseCounter >= startIdx && parseCounter < endIdx) {
						//						System.out.println(line[0]);
						for (int i = 1; i < br.length; i++) {
							line[i] = br[i].readLine();
						}
						String[][] data = new String[7][];
						for (int i = 0; i < data.length; i++) {
							data[i] = line[i].split(",");
						}
						//					String[] data = line.split(",");
						String[] insertData = new String[db.getTableColumnIds(schema + ".lt_a").length];
						String uid = data[0][0] + "_" + data[0][1];
						//					uid = uid.substring(1, uid.length());
						insertData[0] = "'"+uid+"'";
						int counter1 = 0;
						int counter2 = 2;
						int totalCount = 1;

						for (int i = 1; i < insertData.length-6; i++) {
							insertData[i] = data[counter1][counter2];
							if (counter2 >= data[counter1].length-1) {
								counter1++;
								counter2 = 2;
							} else {
								counter2++;
							}
							totalCount++;
						}
						
						for (int i = 1; i < 7; i++) {
							insertData[totalCount++] = "-1";
						}
						System.out.println("LT_A Object number " + parseCounter);
						db.insertIntoTable(schema + ".lt_a",insertData);
					}
					parseCounter++;
				}
				for (int i = 0; i < br.length; i++) {
					br[i].close();
				}	
			} catch (IOException | SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	public static void insertTSFromCSV(PostgreSQLJDBC db, String schema, String ts, String parentPath) throws IOException, SQLException {
		if (ts.equals("L_AT")) {
			String miniPath = "L_AT.csv";
			BufferedReader[] br = new BufferedReader[7];
			br[0] = new BufferedReader(new FileReader(parentPath + "/Non_Normalized/" + miniPath));
			br[1] = new BufferedReader(new FileReader(parentPath + "/L_AT_Normalized/" + miniPath));
			br[2] = new BufferedReader(new FileReader(parentPath + "/A_LT_Normalized/" + miniPath));
			br[3] = new BufferedReader(new FileReader(parentPath + "/T_LA_Normalized/" + miniPath));
			br[4] = new BufferedReader(new FileReader(parentPath + "/LA_T_Normalized/" + miniPath));
			br[5] = new BufferedReader(new FileReader(parentPath + "/LT_A_Normalized/" + miniPath));
			br[6] = new BufferedReader(new FileReader(parentPath + "/AT_L_Normalized/" + miniPath));

			// remove headers
			for (int i = 0; i < br.length; i++) {
				br[i].readLine();
				br[i].readLine();
			}

			String[] line =  new String[7];
			while ((line[0] = br[0].readLine()) != null) {
				for (int i = 1; i < br.length; i++) {
					line[i] = br[i].readLine();
				}
				String[][] data = new String[7][];
				for (int i = 0; i < data.length; i++) {
					data[i] = line[i].split(",");
				}
				//				String[] data = line.split(",");
				String[] insertData = new String[db.getTableColumnIds(schema + ".l_at").length];
				String uid = data[0][0];
				//				uid = uid.substring(1, uid.length());
				insertData[0] = "'"+uid+"'";
				int counter1 = 0;
				int counter2 = 1;
				int totalCount = 1;

				for (int i = 1; i < insertData.length-6; i++) {
					insertData[i] = data[counter1][counter2];
					if (counter2 >= data[counter1].length-1) {
						counter1++;
						counter2 = 1;
					} else {
						counter2++;
					}
					totalCount++;
				}
				
				for (int i = 1; i < 7; i++) {
					insertData[totalCount++] = "-1";
				}
				db.insertIntoTable(schema + "l_at",insertData);
			}
			for (int i = 0; i < br.length; i++) {
				br[i].close();
			}			
		}
	}

	public static boolean createTable_AT_L(PostgreSQLJDBC db, String schema, boolean pca, boolean som) {
		
		boolean mode = true;
		
		int numLoci = db.getTableLength(schema, "locus_key");

		int numAttributes = db.getTableLength(schema, "attribute_key");

		int numNormalizations = db.getTableLength(schema, "normalization_key");

		int numTimes = db.getTableLength(schema, "time_key");
		
		int numCols = 3;
		int numPCA = -1;
		
		if (som) numCols += 1;

		if (!pca) { // if not using PCA
			
			// set the number of columns to:
			// number of Loci * number of normalizations
			numCols += numLoci;
			
		} else { // if using PCA
			
			if (numAttributes * numTimes < numLoci) { // if number of objects is less than number of dimensions
				
				mode = false;
				
				// then PCA will use the number of objects as its maximum dimensionality 
				numCols += numAttributes * numTimes;
				numPCA = numAttributes * numTimes;
			} else { // if the number of dimensions are less than the number of objects
				
				// then PCA will use the number of dimensions as its maximum dimensionality
				numCols += numLoci;
				numPCA = numLoci;
			}
		}

		int numTest = numTimes * numAttributes;
		
		// Set default type to SERIAL
		String idType1 = "INTEGER";
		String idType2 = "INTEGER";
		
		// If number of objects is less than the range for SMALLSERIAL
		if (numAttributes > 0 && numAttributes <= 32767) {			
			// Use SMALLSERIAL instead of SERIAL
			idType1 = "SMALLINT";
		} else if (numAttributes > 2147483647) {
			// Use BIGSERIAL instead of SERIAL
			idType1 = "BIGINT";
		}	
		
		// If number of objects is less than the range for SMALLSERIAL
		if (numTimes > 0 && numTimes <= 32767) {			
			// Use SMALLSERIAL instead of SERIAL
			idType2 = "SMALLINT";
		} else if (numTimes > 2147483647) {
			// Use BIGSERIAL instead of SERIAL
			idType2 = "BIGINT";
		}	
		

		String[] tableCols = new String[numCols + 1];
		tableCols[0] = "(id1 " + idType1;
		tableCols[1] = "id2 " + idType2;
		tableCols[2] = "normalization SMALLINT";	
		

//		String[] tableCols = new String[numCols + 1];
//		tableCols[0] = "(id " + idType;
//		String[] tableCols = new String[numCols];
//		tableCols[0] = "(ID TEXT PRIMARY KEY NOT NULL";

		int idxCounter = 3;
		
		if (mode) {
			for (int i = 1; i <= numLoci; i++) {
				String colName = "l" + i;
				tableCols[idxCounter++] = colName + " NUMERIC";
			}

			
		} else {
			for (int i = 1; i <= numPCA; i++) {
				String colName = "pc" + i;
				tableCols[idxCounter++] = colName + " NUMERIC";
			}
		}
		
		if (som) { // if SOM need to add BMU column
			
			int somSize = SOMaticManager.findSOMDimension(numAttributes * numTimes);
			somSize *= somSize;
			
			// Set default type to INTEGER
			String bmuDataType = "INTEGER";
			
			// If number of objects is less than the range for SMALLINT
			if (somSize > 0 && somSize <= 32767) {			
				// Use SMALLINT instead of INTEGER
				bmuDataType = "SMALLINT";
			} else if (somSize > 2147483647) {
				// Use BIGINT instead of INTEGER
				bmuDataType = "BIGINT";
			}	

			tableCols[idxCounter++] = "bmu " + bmuDataType;
		}

		
		tableCols[tableCols.length-1] = "PRIMARY KEY(id1,id2,normalization))";

		try {
			return db.createTable(schema + ".at_l", tableCols);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public static boolean createTable_LT_A(PostgreSQLJDBC db, String schema, boolean landcover) {
		
		int numLoci = db.getTableLength(schema, "locus_key");

		int numAttributes = db.getTableLength(schema, "attribute_key");

		int numNormalizations = db.getTableLength(schema, "normalization_key");

		int numTimes = db.getTableLength(schema, "time_key");

		int numCols = numAttributes;

		numCols += 3;
		
		if (landcover) numCols++;
		
//		int numTest = numLoci * numTimes;
		
		// Set default type to SERIAL
		String idType1 = "INTEGER";
		String idType2 = "INTEGER";
		
		// If number of objects is less than the range for SMALLSERIAL
		if (numLoci > 0 && numLoci <= 32767) {			
			// Use SMALLSERIAL instead of SERIAL
			idType1 = "SMALLINT";
		} else if (numLoci > 2147483647) {
			// Use BIGSERIAL instead of SERIAL
			idType1 = "BIGINT";
		}	
		
		// If number of objects is less than the range for SMALLSERIAL
		if (numTimes > 0 && numTimes <= 32767) {			
			// Use SMALLSERIAL instead of SERIAL
			idType2 = "SMALLINT";
		} else if (numTimes > 2147483647) {
			// Use BIGSERIAL instead of SERIAL
			idType2 = "BIGINT";
		}	
		

		String[] tableCols = new String[numCols + 2];
		tableCols[0] = "(id1 " + idType1;
		tableCols[1] = "id2 " + idType2;
		tableCols[2] = "normalization SMALLINT";

		int idxCounter = 3;

		for (int i = 1; i <= numAttributes; i++) {
			String colName = "a" + i;
			tableCols[idxCounter++] = colName + " NUMERIC";
		}
		
		int somSize = SOMaticManager.findSOMDimension(numLoci * numTimes);
		somSize *= somSize;
		
		// Set default type to INTEGER
		String bmuDataType = "INTEGER";
		
		// If number of objects is less than the range for SMALLINT
		if (somSize > 0 && somSize <= 32767) {			
			// Use SMALLINT instead of INTEGER
			bmuDataType = "SMALLINT";
		} else if (somSize > 2147483647) {
			// Use BIGINT instead of INTEGER
			bmuDataType = "BIGINT";
		}	

		tableCols[idxCounter++] = "bmu " + bmuDataType;
		
		if (landcover) tableCols[idxCounter++] = "lc TEXT";

		tableCols[tableCols.length-1] = "PRIMARY KEY(id1,id2,normalization))";

		try {
			return db.createTable(schema + ".lt_a", tableCols);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public static boolean createTable_LA_T(PostgreSQLJDBC db, String schema) {
		
		int numLoci = db.getTableLength(schema, "locus_key");

		int numAttributes = db.getTableLength(schema, "attribute_key");

		int numNormalizations = db.getTableLength(schema, "normalization_key");

		int numTimes = db.getTableLength(schema, "time_key");

		int numCols = numTimes;

		numCols += 3;
		
		int numTest = numLoci * numAttributes;
		
		// Set default type to SERIAL
		String idType1 = "INTEGER";
		String idType2 = "INTEGER";
		
		// If number of objects is less than the range for SMALLSERIAL
		if (numLoci > 0 && numLoci <= 32767) {			
			// Use SMALLSERIAL instead of SERIAL
			idType1 = "SMALLINT";
		} else if (numLoci > 2147483647) {
			// Use BIGSERIAL instead of SERIAL
			idType1 = "BIGINT";
		}	
		
		// If number of objects is less than the range for SMALLSERIAL
		if (numAttributes > 0 && numAttributes <= 32767) {			
			// Use SMALLSERIAL instead of SERIAL
			idType2 = "SMALLINT";
		} else if (numAttributes > 2147483647) {
			// Use BIGSERIAL instead of SERIAL
			idType2 = "BIGINT";
		}			

		String[] tableCols = new String[numCols + 2];
		tableCols[0] = "(id1 " + idType1;
		tableCols[1] = "id2 " + idType2;
		tableCols[2] = "normalization SMALLINT";

		int idxCounter = 3;

		for (int i = 1; i <= numTimes; i++) {
			String colName = "t" + i;
			tableCols[idxCounter++] = colName + " NUMERIC";
		}
		
		int somSize = SOMaticManager.findSOMDimension(numLoci * numAttributes);
		somSize *= somSize;
		
		// Set default type to INTEGER
		String bmuDataType = "INTEGER";
		
		// If number of objects is less than the range for SMALLINT
		if (somSize > 0 && somSize <= 32767) {			
			// Use SMALLINT instead of INTEGER
			bmuDataType = "SMALLINT";
		} else if (somSize > 2147483647) {
			// Use BIGINT instead of INTEGER
			bmuDataType = "BIGINT";
		}	

		tableCols[idxCounter++] = "bmu " + bmuDataType;

		tableCols[tableCols.length-1] = "PRIMARY KEY(id1,id2,normalization))";
		
		for (int i = 0; i < tableCols.length; i++) {
			System.out.println(tableCols[i]);
		}

		try {
			return db.createTable(schema + ".la_t", tableCols);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public static boolean createTable_A_LT(PostgreSQLJDBC db, String schema, boolean pca, boolean som) {
		
		// mode to determine if the A_LT table will utilize PCA instead of regular attributes
		boolean mode = true;
		
		// get the number of loci from key
		int numLoci = db.getTableLength(schema, "locus_key");
		// get the number of attributes from key		
		int numAttributes = db.getTableLength(schema, "attribute_key");
		// get the number of times from key		
		int numTimes = db.getTableLength(schema, "time_key");
		
		// initialize with 2 columns (for id & normalization)
		int numCols = 2;
		
		// initialize number of PCA attributes to -1
		int numPCA = -1;				
		
		if (som) numCols += 1;

		if (!pca) { // if not using PCA
			
			// set the number of columns to:
			// number of Loci * number of Times * number of normalizations
			numCols += numLoci * numTimes;
			
		} else { // if using PCA
									
			if (numAttributes < numLoci * numTimes) { // if number of objects is less than number of dimensions
				mode = false;
				
				// then PCA will use the number of objects as its maximum dimensionality 
				numCols += numAttributes;
				numPCA = numAttributes;
			} else { // if the number of dimensions are less than the number of objects
				
				// then PCA will use the number of dimensions as its maximum dimensionality
				numCols += numLoci * numTimes;
				numPCA = numLoci * numTimes;
			}
		}
		
		// Set default type to SERIAL
		String idType = "SERIAL";
		
		// If number of objects is less than the range for SMALLSERIAL
		if (numAttributes > 0 && numAttributes <= 32767) {			
			// Use SMALLSERIAL instead of SERIAL
			idType = "SMALLSERIAL";
		} else if (numAttributes > 2147483647) {
			// Use BIGSERIAL instead of SERIAL
			idType = "BIGSERIAL";
		}	
		

		String[] tableCols = new String[numCols + 1];
		
		// column for the attribute id
		tableCols[0] = "(id " + idType;
		// column for the normalization
		tableCols[1] = "normalization SMALLINT";

		int idxCounter = 2;
		
		if (mode) {
			for (int i = 1; i <= numLoci; i++) {
				for (int j = 1; j <= numTimes; j++) {
					String colName = "l" + i + "_t" + j;
					tableCols[idxCounter++] = colName + " NUMERIC";
				}
			}

			
		} else {
			for (int i = 1; i <= numPCA; i++) {
				String colName = "pc" + i;
				tableCols[idxCounter++] = colName + " NUMERIC";
			}
		}
		
		if (som) { // if SOM need to add BMU column
			
			int somSize = SOMaticManager.findSOMDimension(numAttributes);
			somSize *= somSize;
			
			// Set default type to INTEGER
			String bmuDataType = "INTEGER";
			
			// If number of objects is less than the range for SMALLINT
			if (somSize > 0 && somSize <= 32767) {			
				// Use SMALLINT instead of INTEGER
				bmuDataType = "SMALLINT";
			} else if (somSize > 2147483647) {
				// Use BIGINT instead of INTEGER
				bmuDataType = "BIGINT";
			}	

			tableCols[idxCounter++] = "bmu " + bmuDataType;
		}

//		tableCols[tableCols.length-1] = tableCols[tableCols.length-1] + ")";
		tableCols[tableCols.length-1] = "PRIMARY KEY(id,normalization))";

		try {
			return db.createTable(schema + ".a_lt", tableCols);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public static boolean createTable_T_LA(PostgreSQLJDBC db, String schema, boolean pca, boolean som) {

		boolean mode = true;
		
		int numLoci = db.getTableLength(schema, "locus_key");

		int numAttributes = db.getTableLength(schema, "attribute_key");

		int numNormalizations = db.getTableLength(schema, "normalization_key");

		int numTimes = db.getTableLength(schema, "time_key");
		
		// initialize with 2 columns (for id & normalization)
		int numCols = 2;
		
		// initialize number of PCA attributes to -1
		int numPCA = -1;	
		
		if (som) numCols += 1;
		
		// Set default type to SERIAL
		String idType = "SERIAL";
		
		// If number of objects is less than the range for SMALLSERIAL
		if (numAttributes > 0 && numAttributes <= 32767) {			
			// Use SMALLSERIAL instead of SERIAL
			idType = "SMALLSERIAL";
		} else if (numAttributes > 2147483647) {
			// Use BIGSERIAL instead of SERIAL
			idType = "BIGSERIAL";
		}	
		

		if (!pca) { // if not using PCA
			
			// set the number of columns to:
			// number of Loci * number of Attributes * number of normalizations
			numCols += numLoci * numAttributes;
			
		} else { // if using PCA
			
			if (numTimes < numLoci * numAttributes) { // if number of objects is less than number of dimensions
				mode = false;
				// then PCA will use the number of objects as its maximum dimensionality 
				numCols += numTimes;
				numPCA = numTimes;
			} else { // if the number of dimensions are less than the number of objects
				
				// then PCA will use the number of dimensions as its maximum dimensionality
				numCols += numLoci * numAttributes;
				numPCA = numLoci * numAttributes;
			}
		}


		String[] tableCols = new String[numCols + 1];
		// column for the time id
		tableCols[0] = "(id " + idType;
		// column for the normalization
		tableCols[1] = "normalization SMALLINT";

		int idxCounter = 2;	
		
		if (mode) {
			for (int i = 1; i <= numLoci; i++) {
				for (int j = 1; j <= numAttributes; j++) {
					String colName = "l" + i + "_a" + j;
					tableCols[idxCounter++] = colName + " NUMERIC";
				}
			}
		} else {
			for (int i = 1; i <= numPCA; i++) {
				String colName = "pc" + i;
				tableCols[idxCounter++] = colName + " NUMERIC";
			}
		}
		
		if (som) { // if SOM need to add BMU column
			
			int somSize = SOMaticManager.findSOMDimension(numTimes);
			somSize *= somSize;
			
			// Set default type to INTEGER
			String bmuDataType = "INTEGER";
			
			// If number of objects is less than the range for SMALLINT
			if (somSize > 0 && somSize <= 32767) {			
				// Use SMALLINT instead of INTEGER
				bmuDataType = "SMALLINT";
			} else if (somSize > 2147483647) {
				// Use BIGINT instead of INTEGER
				bmuDataType = "BIGINT";
			}	

			tableCols[idxCounter++] = "bmu " + bmuDataType;
		}
		
		tableCols[tableCols.length-1] = "PRIMARY KEY(id,normalization))";

		try {
			return db.createTable(schema + ".t_la", tableCols);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public static boolean exportL_A_T2L_AT(PostgreSQLJDBC db, String schema, File dir, int[] primaryKey, int headers, boolean batch) {
		// get the csv files in the directory
		File[] files = dir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".csv");
			}
		});

		// return false if there are no files in directory OR loci not common b/w files
		if (files.length == 0 || !CSVManager.commonLociCheck(dir.getPath(), primaryKey)) {
			return false;
		}	

		if (!createAllDictionaries(db,files,schema,primaryKey,headers,batch)) {
			return false;
		}

		return true;

	}

	public static boolean createAllDictionaries(PostgreSQLJDBC db, File[] files, String schema, int[] primaryKey, boolean[] hasGeometry, boolean batch) {

		if (!createDictionary(db, "t", files, schema, primaryKey, batch)) {
			return false;
		}

		if (!createDictionary(db, "a", files, schema, primaryKey, batch)) {
			return false;
		}

		if (!createDictionary(db, "l", files, schema, primaryKey, batch)) {
			return false;
		}

		return true;
	}

	public static boolean createAllDictionaries(PostgreSQLJDBC db, File[] files, String schema, int[] primaryKey, int headers, boolean batch) {

		if (!createTimeDictionary(db, files, schema, batch)) {
			return false;
		}

		if (!createAttributeDictionary(db, files, schema, primaryKey, batch)) {
			return false;
		}

		if (!createLocusDictionary(db, files, schema, primaryKey, headers, batch)) {
			return false;
		}

		if (!createNormalizationDictionary(db, schema, batch)) {
			return false;
		}

		return true;
	}

	public static boolean createAllDictionaries(PostgreSQLJDBC db, File[] files, String schema, int[] primaryKey, int headers, String alias, boolean batch) {

		if (!createTimeDictionary(db, files, schema, batch)) {
			return false;
		}

		if (!createAttributeDictionary(db, files, schema, primaryKey, batch)) {
			return false;
		}

		if (!createLocusDictionary(db, files, schema, alias, headers, batch)) {
			return false;
		}

		if (!createNormalizationDictionary(db, schema, batch)) {
			return false;
		}

		return true;
	}
	
	public static boolean createAllDictionaries(PostgreSQLJDBC db, Map<String, File> tsFiles, String schema, int headers, 
												boolean batch, int[] normalizations, String[] normlabels) throws SQLException {

		if (!createTimeDictionary(db, tsFiles.get("t_la"), schema, headers, batch)) {
			return false;
		}

		if (!createAttributeDictionary(db, tsFiles.get("a_lt"), schema, headers, batch)) {
			return false;
		}

		if (!createLocusDictionary(db, tsFiles.get("l_at"), schema, headers, batch)) {
			return false;
		}

		if (!createNormalizationDictionary(db, schema, batch, normalizations, normlabels)) {
			return false;
		}

		return true;
	}

	public static boolean createNormalizationDictionary(PostgreSQLJDBC db, String schema, boolean batch) {
		try {
			return createAliasTable(db, schema, NORMALIZATIONS, 3, batch);
		} catch (IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	public static boolean createNormalizationDictionary(PostgreSQLJDBC db, String schema, boolean batch, 
														int[] normalizations, String[] normLabels) throws SQLException {
		// Set default type to SMALLINT
		String idType = "SMALLINT";
		
		String[] tableCols = new String[3];
		tableCols[0] = "(id " + idType;
		tableCols[1] = "alias TEXT NOT NULL";
		tableCols[2] = "PRIMARY KEY(id))";
		String table = schema + ".normalization_key";

		db.createTable(table, tableCols);
		
		boolean insert = true;

		if (!batch) {
			for (int i = 0; i < normalizations.length; i++) {
				String[] insertData = new String[2];				
				insertData[0] = "'" + normalizations[i] + "'";				
				insertData[1] = "'" + normLabels[i] + "'";
				if (!db.insertIntoTable(table,insertData)) {
					insert = false;
				}
			}
		} else {
			ArrayList<String> batchInsertion = new ArrayList<String>();
			for (int i = 0; i < normalizations.length; i++) {
				String[] insertData = new String[2];
				insertData[0] = "'" + normalizations[i] + "'";
				insertData[1] = "'" + normLabels[i] + "'";
				batchInsertion.add(db.insertQueryBuilder(table,insertData));
			}
			
			String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
			db.insertIntoTableBatch(table, batchInsertionAr);
		}

		return insert;	
	}

	public static boolean createTimeDictionary(PostgreSQLJDBC db, File[] files, String schema, boolean batch) {
		String[] times = new String[files.length];
		for (int i = 0; i < files.length; i++) {
			String[] nameSplit = files[i].getName().split("_");

			times[i] = nameSplit[0];
		}

		try {
			return createAliasTable(db, schema, times, 2, batch);
		} catch (IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	public static boolean createTimeDictionary(PostgreSQLJDBC db, File file, String schema, int headers, boolean batch) {

		// list to hold the times
		ArrayList<String> timeList = new ArrayList<String>();
		
		try {
			
			// BR to read thru the lines of file
			BufferedReader br = new BufferedReader(new FileReader(file.getPath()));
			
			// remove headers
			while (headers > 0) {
				System.out.println("HEADERS");
				System.out.println(br.readLine());
				headers--;
			}
			
			// variable to hold the line content
			String line = "";
			
			// iterate thru all remaining lines
			while ((line = br.readLine()) != null) {
				
				// split it into an array
				String[] split = line.split(",");
				
				// append the times to the time list
				timeList.add(split[0]);
			}
			
			// close the reader
			br.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		// test if times are found
		if (timeList.size() == 0) return false;
		
		// convert list to array
		String[] times = timeList.toArray(new String[timeList.size()]);
		
		try {
			return createAliasTable(db, schema, times, 2, batch);
		} catch (IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public static boolean createAttributeDictionary(PostgreSQLJDBC db, File[] files, String schema, int[] primaryKey, boolean batch) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(files[0].getPath()));
			String line = br.readLine();
			String[] data = line.split(",");
			ArrayList<String> attributeList = new ArrayList<String>();
			for (int i = 0; i < data.length; i++) {
				boolean isKey = false;
				for (int j = 0; j < primaryKey.length; j++) {
					if (i == primaryKey[j]) {
						isKey = true;
					}
				}

				if (!isKey) {
					attributeList.add(data[i]);
				}

			}
			br.close();

			String[] attributes = attributeList.toArray(new String[attributeList.size()]);

			return createAliasTable(db, schema, attributes, 1, batch);

		} catch (IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	public static boolean createAttributeDictionary(PostgreSQLJDBC db, File file, String schema, int headers, boolean batch) {
		
		// list to hold the times
		ArrayList<String> attributeList = new ArrayList<String>();
		
		try {
			
			// BR to read thru the lines of file
			BufferedReader br = new BufferedReader(new FileReader(file.getPath()));
			
			// remove headers
			while (headers > 0) {
				System.out.println("HEADERS");
				System.out.println(br.readLine());
				headers--;
			}
			
			// variable to hold the line content
			String line = "";
			
			// iterate thru all remaining lines
			while ((line = br.readLine()) != null) {
				
				// split it into an array
				String[] split = line.split(",");
				
				// append the times to the time list
				attributeList.add(split[0]);
			}
			
			// close the reader
			br.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		// test if times are found
		if (attributeList.size() == 0) return false;
		
		// convert list to array
		String[] attributes = attributeList.toArray(new String[attributeList.size()]);
		
		try {
			return createAliasTable(db, schema, attributes, 1, batch);
		} catch (IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public static boolean createLocusDictionary(PostgreSQLJDBC db, File[] files, String schema, int[] primaryKey, int headers, boolean batch) {

		String[] tableCols = new String[2];
		tableCols[0] = "(ID TEXT PRIMARY KEY NOT NULL";
		tableCols[1] = "ALIAS TEXT NOT NULL)";
		String table = schema;
		table = table + ".locus_key";
		try {
			db.createTable(table, tableCols);

			BufferedReader br = new BufferedReader(new FileReader(files[0].getPath()));

			while (headers > 0) {
				System.out.println("HEADERS");
				System.out.println(br.readLine());
				headers--;
			}


			String line = "";
			int lociCount = 1;
			
			if (!batch) {
				while ((line = br.readLine()) != null) {
					String[] split = line.split(",");
					String[] insertData = new String[2];

					insertData[0] = "'l" + lociCount + "'";

					if (primaryKey.length == 1) {
						insertData[1] = "'" + split[primaryKey[0]] + "'";
					} else if (primaryKey.length > 1) {
						insertData[1] = "'" + split[primaryKey[0]] + "'";
						for (int i = 1; i < primaryKey.length; i++) {
							insertData[1] = insertData[1] + "_" + split[primaryKey[i]];
						}
					} else {
						br.close();
						return false;
					}
					if (!db.insertIntoTable(schema + ".locus_key",insertData)) {
						br.close();
						return false;
					}
					lociCount++;
				}
				br.close();
			} else {
				ArrayList<String> batchInsertion = new ArrayList<String>();
				while ((line = br.readLine()) != null) {
					String[] split = line.split(",");
					String[] insertData = new String[2];

					insertData[0] = "'l" + lociCount + "'";

					if (primaryKey.length == 1) {
						insertData[1] = "'" + split[primaryKey[0]] + "'";
					} else if (primaryKey.length > 1) {
						insertData[1] = "'" + split[primaryKey[0]] + "'";
						for (int i = 1; i < primaryKey.length; i++) {
							insertData[1] = insertData[1] + "_" + split[primaryKey[i]];
						}
					} else {
						br.close();
						return false;
					}
					batchInsertion.add(db.insertQueryBuilder(schema + ".locus_key",insertData));
					lociCount++;
				}
				br.close();
				
				String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
				db.insertIntoTableBatch(table, batchInsertionAr);
			}

			return true;
		} catch (IOException | SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return false;
		}
	}
	
	public static boolean createLocusDictionary(PostgreSQLJDBC db, File file, String schema, int headers, boolean batch) {

		// list to hold the times
		ArrayList<String> locusList = new ArrayList<String>();
		
		try {
			
			// BR to read thru the lines of file
			BufferedReader br = new BufferedReader(new FileReader(file.getPath()));
			
			// remove headers
			while (headers > 0) {
				System.out.println("HEADERS");
				System.out.println(br.readLine());
				headers--;
			}
			
			// variable to hold the line content
			String line = "";
			
			// iterate thru all remaining lines
			while ((line = br.readLine()) != null) {
				
				// split it into an array
				String[] split = line.split(",");
				
				// append the times to the time list
				locusList.add(split[0]);
			}
			
			// close the reader
			br.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		// test if times are found
		if (locusList.size() == 0) return false;
		
		// convert list to array
		String[] loci = locusList.toArray(new String[locusList.size()]);
		
		try {
			return createAliasTable(db, schema, loci, 0, batch);
		} catch (IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public static boolean createLocusDictionary(PostgreSQLJDBC db, File[] files, String schema, String alias, int headers, boolean batch) {

		// Set default type to SERIAL
//		String idType = "SERIAL";
//		
//		// If number of objects is less than the range for SMALLSERIAL
//		if (data.length > 0 && data.length <= 32767) {			
//			// Use SMALLSERIAL instead of SERIAL
//			idType = "SMALLSERIAL";
//		} else if (data.length > 0 && data.length > 2147483647) {
//			// Use BIGSERIAL instead of SERIAL
//			idType = "BIGSERIAL";
//		}	
//		
//		String[] tableCols = new String[3];
//		tableCols[0] = "(id " + idType;
//		tableCols[1] = "alias TEXT NOT NULL";
//		tableCols[2] = "PRIMARY KEY(id))";
//		String table = schema;
//
//		table = table + ".locus_key";
		
		

		
		
		try {
			
			

			BufferedReader br = new BufferedReader(new FileReader(files[0].getPath()));
			int numLines = 0;
			while (br.readLine() != null) numLines++;
			br.close();
			
			br = new BufferedReader(new FileReader(files[0].getPath()));
			
			String table = schema;
			table = table + ".locus_key";

			while (headers > 0) {
				System.out.println("HEADERS");
				System.out.println(br.readLine());
				headers--;
				numLines--;
			}
			
			// Set default type to SERIAL
			String idType = "SERIAL";
			
			// If number of objects is less than the range for SMALLSERIAL
			if (numLines > 0 && numLines <= 32767) {			
				// Use SMALLSERIAL instead of SERIAL
				idType = "SMALLSERIAL";
			} else if (numLines > 2147483647) {
				// Use BIGSERIAL instead of SERIAL
				idType = "BIGSERIAL";
			}	
			
			String[] tableCols = new String[3];
			tableCols[0] = "(id " + idType;
			tableCols[1] = "alias TEXT NOT NULL";
			tableCols[2] = "PRIMARY KEY(id))";
			db.createTable(table, tableCols);


			String line = "";
			int lociCount = 1;
			
			if (!batch) {
				
				while ((line = br.readLine()) != null) {
					
					
					//				String[] split = line.split(",");
					String[] insertData = new String[2];

//					insertData[0] = "'l" + lociCount + "'";					
					insertData[0] = "'" + lociCount + "'";
					insertData[1] = "'" + alias + lociCount + "'";

					if (!db.insertIntoTable(schema + ".locus_key",insertData)) {
						br.close();
						return false;
					}
					lociCount++;
				}

				br.close();
			} else {
				ArrayList<String> batchInsertion = new ArrayList<String>();
				while ((line = br.readLine()) != null) {
					//				String[] split = line.split(",");
					String[] insertData = new String[2];

//					insertData[0] = "'l" + lociCount + "'";
					insertData[0] = "'" + lociCount + "'";
					insertData[1] = "'" + alias + lociCount + "'";

					batchInsertion.add(db.insertQueryBuilder(schema + ".locus_key",insertData));
					lociCount++;
				}
				
				br.close();
							
				
				String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
				db.insertIntoTableBatch(table, batchInsertionAr);
			}
			
			return true;
		} catch (IOException | SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return false;
		}
	}

	private static boolean createDictionary(PostgreSQLJDBC db, String type, File[] files, String schema, int[] primaryKey, boolean batch) {

		if (type.equalsIgnoreCase("t")) {
			try {
				String[] times = new String[files.length];
				//				FileWriter writer = new FileWriter(output + "/timeDictionary.csv");
				for (int i = 0; i < files.length; i++) {
					String[] nameSplit = files[i].getName().split("_");

					times[i] = nameSplit[0];
					//					writer.append("t" + (i + 1) + "," + nameSplit[0] + "\n");
				}

				return createAliasTable(db, schema, times, 2, batch);
				//				writer.flush();
				//				writer.close();
			} catch (IOException | SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		} else if (type.equalsIgnoreCase("a")) {
			try {
				//				FileWriter writer = new FileWriter(output + "/attrDictionary.csv");
				BufferedReader br = new BufferedReader(new FileReader(files[0].getPath()));
				//				int attribute = 1;
				String line = br.readLine();
				String[] data = line.split(",");
				ArrayList<String> attributeList = new ArrayList<String>();
				for (int i = 0; i < data.length; i++) {
					boolean isKey = false;
					for (int j = 0; j < primaryKey.length; j++) {
						if (i == primaryKey[j]) {
							isKey = true;
						}
					}

					if (!isKey) {
						attributeList.add(data[i]);
						//						writer.append("a" + attribute + "," + data[i] + "\n");
						//						attribute++;
					}

				}

				//				writer.flush();
				//				writer.close();
				br.close();

				String[] attributes = attributeList.toArray(new String[attributeList.size()]);

				return createAliasTable(db, schema, attributes, 1, batch);
			} catch (IOException | SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (type.equalsIgnoreCase("l")) {
			try {
				//				FileWriter writer = new FileWriter(output + "/lociDictionary.csv");
				BufferedReader br = new BufferedReader(new FileReader(files[0].getPath()));
				int locus = 1;
				String line = br.readLine();

				// iterate thru input file
				while ((line = br.readLine()) != null) {
					String[] data = line.split(",");
					//					writer.append("l" + locus);
					for (int i = 0; i < primaryKey.length; i++) {
						//						writer.append("," + data[primaryKey[i]]);
					}
					//					writer.append("\n");
					locus++;
				}
				//				writer.flush();
				//				writer.close();
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return false;
	}

	public static boolean extractPtGeomFromCSV(PostgreSQLJDBC db, String schema, File f, int[] coordinates, String tsElement, int headers, String epsg, boolean batch) {

		File targetFile;

		if (f.isDirectory()) {
			File[] children = f.listFiles();
			targetFile = children[0];
		} else {
			targetFile = f;
		}

		try {
			if (!createPointTable(db, schema, tsElement, epsg, coordinates)) {
				return false;
			}

			if (!insertPtsFromCSV(db, schema, tsElement, targetFile, coordinates, epsg, headers, batch)) {
				return false;
			}
		} catch (SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}

		return true;
	}

	public static boolean insertPtsFromCSV(PostgreSQLJDBC db, String schema, String tsElement, File f, int[] coordinates, String epsg, int headers, boolean batch) throws IOException, SQLException {

		String element = "";

		if (tsElement.equalsIgnoreCase("t") || tsElement.equalsIgnoreCase("time") || tsElement.equalsIgnoreCase("times")) {
			element = "time_pt";
		} else if (tsElement.equalsIgnoreCase("a") || tsElement.equalsIgnoreCase("attribute") || tsElement.equalsIgnoreCase("attributes")) {
			element = "attribute_pt";
		}  else if (tsElement.equalsIgnoreCase("l") || tsElement.equalsIgnoreCase("locus") || tsElement.equalsIgnoreCase("loci")) {
			element = "locus_pt";
		} else {
			return false;
		}

		BufferedReader br = new BufferedReader(new FileReader(f.getPath()));


		while (headers > 0) {
			System.out.println("HEADERS");
			System.out.println(br.readLine());
			headers--;
		}

		String line = "";
		int elementCount = 1;

		if (!batch) {
			while ((line = br.readLine()) != null) {
				String[] data = line.split(",");
				String[] insertData = new String[2];
//				insertData[0] = "'l" + elementCount + "'";
				insertData[0] = "'" + elementCount + "'";
				if (coordinates.length == 2) {
					insertData[1] = "ST_GeomFromText('POINT(" + data[coordinates[0]];
				} else if (coordinates.length == 3) {
					insertData[1] = "ST_GeomFromText('POINTZ(" + data[coordinates[0]];
				} else {
					br.close();
					return false;
				}


				for (int i = 1; i < coordinates.length; i++) {
					insertData[1] = insertData[1] + " " + data[coordinates[i]];
				}

				insertData[1] = insertData[1] + ")'," + epsg + ")";
				if (!db.insertIntoTable(schema + "." + element,insertData)) {
					br.close();
					return false;
				}
				elementCount++;
			}
			br.close();
		} else {
			ArrayList<String> batchInsertion = new ArrayList<String>();
			while ((line = br.readLine()) != null) {
				String[] data = line.split(",");
				String[] insertData = new String[2];
//				insertData[0] = "'l" + elementCount + "'";
				insertData[0] = "'" + elementCount + "'";
				if (coordinates.length == 2) {
					insertData[1] = "ST_GeomFromText('POINT(" + data[coordinates[0]];
				} else if (coordinates.length == 3) {
					insertData[1] = "ST_GeomFromText('POINTZ(" + data[coordinates[0]];
				} else {
					br.close();
					return false;
				}


				for (int i = 1; i < coordinates.length; i++) {
					insertData[1] = insertData[1] + " " + data[coordinates[i]];
				}

				insertData[1] = insertData[1] + ")'," + epsg + ")";
				batchInsertion.add(db.insertQueryBuilder(schema + "." + element,insertData));
//				if (!db.insertIntoTable(schema + "." + element,insertData)) {
//					br.close();
//					return false;
//				}
				elementCount++;
			}
			br.close();
			
			String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
			db.insertIntoTableBatch(schema + "." + element, batchInsertionAr);
		}
		
		return true;
	}

	public static boolean createPointTable(PostgreSQLJDBC db, String schema, String tsElement, String epsg, int[] coordinates) throws SQLException {
		
		int numLoci = db.getTableLength(schema, "locus_key");
		
		String element = "";

		if (tsElement.equalsIgnoreCase("t") || tsElement.equalsIgnoreCase("time") || tsElement.equalsIgnoreCase("times")) {
			element = "time_pt";
		} else if (tsElement.equalsIgnoreCase("a") || tsElement.equalsIgnoreCase("attribute") || tsElement.equalsIgnoreCase("attributes")) {
			element = "attribute_pt";
		}  else if (tsElement.equalsIgnoreCase("l") || tsElement.equalsIgnoreCase("locus") || tsElement.equalsIgnoreCase("loci")) {
			element = "locus_pt";
		} else {
			return false;
		}

		String pointType = "";
		if (coordinates.length == 2) {
			pointType = "POINT";
		} else if (coordinates.length == 3) {
			pointType = "POINTZ";
		} else {
			return false;
		}
		
		// Set default type to SERIAL
		String idType = "SERIAL";
		
		// If number of objects is less than the range for SMALLSERIAL
		if (numLoci > 0 && numLoci <= 32767) {			
			// Use SMALLSERIAL instead of SERIAL
			idType = "SMALLSERIAL";
		} else if (numLoci > 2147483647) {
			// Use BIGSERIAL instead of SERIAL
			idType = "BIGSERIAL";
		}	
		
		String[] tableCols = new String[3];
		tableCols[0] = "(id " + idType;
		tableCols[1] = "geom geometry(" + pointType + "," + epsg + ")";
		tableCols[2] = "PRIMARY KEY(id))";
		return db.createTable(schema + "." + element, tableCols);

//		String[] tableCols = new String[2];
//		tableCols[0] = "(UID TEXT PRIMARY KEY NOT NULL";
//		tableCols[1] = "geom geometry(" + pointType + "," + epsg + "))";
//		return db.createTable(schema + "." + element, tableCols);
		//		return true;
	}

	// Method to create the Alias Tables (Locus, Attribute, Time, Normalization)
	private static boolean createAliasTable(PostgreSQLJDBC db, String schema, String[] data, int type, boolean batch) throws SQLException, IOException {
		// Set default type to SERIAL
		String idType = "SERIAL";
		
		// If number of objects is less than the range for SMALLSERIAL
		if (data.length > 0 && data.length <= 32767) {			
			// Use SMALLSERIAL instead of SERIAL
			idType = "SMALLSERIAL";
		} else if (data.length > 2147483647) {
			// Use BIGSERIAL instead of SERIAL
			idType = "BIGSERIAL";
		}	
		
		String[] tableCols = new String[3];
		tableCols[0] = "(id " + idType;
		tableCols[1] = "alias TEXT NOT NULL";
		tableCols[2] = "PRIMARY KEY(id))";
		String table = schema;
		if (type == 0) {
			table = table + ".locus_key";
		} else if (type == 1) {
			table = table + ".attribute_key";
		} else if (type == 2) {
			table = table + ".time_key";
		} else if (type == 3) {
			table = table + ".normalization_key";
		}
		db.createTable(table, tableCols);
		return insertAlias(db, schema, data, type, batch);
	}

	private static boolean insertAlias(PostgreSQLJDBC db, String schema, String[] data, int type, boolean batch) throws IOException, SQLException {
		String table = schema;
		if (type == 0) {
			table = table + ".locus_key";
		} else if (type == 1) {
			table = table + ".attribute_key";
		} else if (type == 2) {
			table = table + ".time_key";
		} else if (type == 3) {
			table = table + ".normalization_key";
		}
		
		boolean insert = true;

		if (!batch) {
			for (int i = 0; i < data.length; i++) {
				String[] insertData = new String[2];
				if (type != 3) {
					insertData[0] = "'" + (i+1) + "'";
				} else {
					insertData[0] = "'" + i + "'";
				}
				
				insertData[1] = "'" + data[i] + "'";
				if (!db.insertIntoTable(table,insertData)) {
					insert = false;
				}
			}
		} else {
			ArrayList<String> batchInsertion = new ArrayList<String>();
			for (int i = 0; i < data.length; i++) {
				String[] insertData = new String[2];
				if (type != 3) {
					insertData[0] = "'" + (i+1) + "'";
				} else {
					insertData[0] = "'" + i + "'";
				}
				insertData[1] = "'" + data[i] + "'";
				batchInsertion.add(db.insertQueryBuilder(table,insertData));
			}
			
			String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
			db.insertIntoTableBatch(table, batchInsertionAr);
		}

		return insert;
	}

	private static boolean createAliasTable(PostgreSQLJDBC db, String schema, String[] data, int type, int[] primaryKeys, boolean batch) throws SQLException, IOException {
		String[] tableCols = new String[2];
		tableCols[0] = "(ID TEXT PRIMARY KEY NOT NULL";
		tableCols[1] = "ALIAS TEXT NOT NULL)";
		String table = schema;
		if (type == 0) {
			table = table + ".locus_key";
		} else if (type == 1) {
			table = table + ".attribute_key";
		} else if (type == 2) {
			table = table + ".time_key";
		}
		db.createTable(schema + ".time_key", tableCols);
		return insertAlias(db, schema, data, type, batch);
	}

	private static boolean insertAlias(PostgreSQLJDBC db, String schema, String[] data, int type, int[] primaryKeys, boolean batch) throws IOException, SQLException {
		String table = schema;
		if (type == 0) {
			table = table + ".locus_key";
		} else if (type == 1) {
			table = table + ".attribute_key";
		} else if (type == 2) {
			table = table + ".time_key";
		}

		boolean insert = true;

		if (!batch) {
			for (int i = 0; i < data.length; i++) {
				String[] insertData = new String[2];
				insertData[0] = "'t" + (i+1) + "'";
				insertData[1] = "'" + data[i] + "'";
				if (!db.insertIntoTable(table,insertData)) {
					insert = false;
				}
			}
		} else {
			ArrayList<String> batchInsertion = new ArrayList<String>();
			for (int i = 0; i < data.length; i++) {
				String[] insertData = new String[2];
				insertData[0] = "'t" + (i+1) + "'";
				insertData[1] = "'" + data[i] + "'";
				batchInsertion.add(db.insertQueryBuilder(table,insertData));
			}
			
			String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
			db.insertIntoTableBatch(table, batchInsertionAr);
		}

		return insert;	
	}
	
	public static void updateTable(String filePath, String schema, String table, PostgreSQLJDBC db, boolean batch, int batchSize) throws IOException {
		
		// list to hold the update statements
		ArrayList<String> batchUpdate = new ArrayList<String>();
		
		// BufferedReader to read in file (csv)
		BufferedReader br = new BufferedReader(new FileReader(filePath));

		// line variable to store each line from file
		String line = br.readLine();
		
		// primary key is columnArray[0]
		String[] columnArray = line.split(",");
		// the rest are update columns
		
		// iterate thru the rest of the file
		while ((line = br.readLine()) != null) {
			
			// split based on comma delimiter
			String[] lineSplit = line.split(",");
			
			// iterate thru columns (except for primary key)
			for (int i = 1; i < lineSplit.length; i++) {
				
				if (batch) { // if batch
					
					// append insertion statement to ArrayList
					batchUpdate.add(db.updateQueryBuilder(schema, table, columnArray[0], lineSplit[0], columnArray[i], lineSplit[i]));
				
				} else {
					
					// otherwise update table one at a time
					db.updateTable(schema, table, columnArray[0], lineSplit[0], columnArray[i], lineSplit[i]);
				
				}
			}
		}
		
		// If using batch mode, now we submit insertion statements to database
		if (batch) {
			
			// convert ArrayList to array
			String[] batchUpdateAr = batchUpdate.toArray(new String[batchUpdate.size()]);
			try {
				
				// insert into table
				db.insertIntoTableBatch(schema + "." + table, batchUpdateAr, batchSize);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// close the BufferedReader
		br.close();
	}
}
