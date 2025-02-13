package edu.sdsu.datavis.trispace.tsprep.stats;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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
import edu.sdsu.datavis.trispace.tsprep.utils.JSONUtil;
import edu.sdsu.datavis.trispace.tsprep.xform.PostgreSQLManager;

/**
 * CSVManager is an abstract class for performing basic statistical operations
 * in the PostgreSQL environment.
 * 
 * Created: 1/21/2020
 * 
 * @author      Tim Schempp
 * @version     %I%, %G%
 * @since       1.0
 * 
 */
public abstract class StatsPG {

	final static String[] NORMALIZATIONS = {"Unnormalized", "L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L"};
	final static String[] STATISTICS = {"min", "max", "mean", "median", "std_dev"};
	final static int BATCH_SIZE = 50000; // constant for batch size
	final static int MAX_COLUMNS = 1598; // constant for max columns in DB

	public static boolean createStatsTable(PostgreSQLJDBC db, String schema, String ts) {

		// return false if table already exists
		if (db.tableExists(schema, ts.toLowerCase() + "_stats")) {
			return false;
		}

		int numLoci 		= db.getTableLength(schema, "locus_key");					// number of loci in locus_key table	
		int numAttributes 	= db.getTableLength(schema, "attribute_key");				// number of attributes in attribute_key table				
		int numTimes 		= db.getTableLength(schema, "time_key");					// number of times in time_key table
		
		int numCols 		= 2;														// initialize with 2 columns (id + normalization)				
		int numObjects 		= -1;														// number of objects in the TS perspective
				
		ArrayList<Integer> objectList = new ArrayList<Integer>();						// list to hold number of objects				
		
		if (ts.equalsIgnoreCase("l_at")) {
			
			numObjects = numLoci;						
			objectList.add(numLoci);
			
		} else if (ts.equalsIgnoreCase("a_lt")) {		
			
			numObjects = numAttributes;			
			objectList.add(numAttributes);
			
		} else if (ts.equalsIgnoreCase("t_la")) {
			
			numObjects = numTimes;			
			objectList.add(numTimes);
			
		} else if (ts.equalsIgnoreCase("la_t")) {
			
			numObjects = numLoci * numAttributes;			
			objectList.add(numLoci);
			objectList.add(numAttributes);			
			numCols += 1;
			
		} else if (ts.equalsIgnoreCase("lt_a")) {	
			
			numObjects = numLoci * numTimes;			
			objectList.add(numLoci);
			objectList.add(numTimes);
			numCols += 1;
			
		} else if (ts.equalsIgnoreCase("at_l")) {
			
			numObjects = numAttributes * numTimes;
			objectList.add(numAttributes);
			objectList.add(numTimes);
			numCols += 1;
			
		} else {
			
			return false;
			
		}				
				
		String[] tableCols = new String[numCols+1];									// store the table columns in an array				
		int colIdx = 0;																// keep track of column index
		
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
		
		
		try {
			return db.createTable(schema + "." + ts.toLowerCase() + "_stats", tableCols);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public static boolean populateStatsTable(PostgreSQLJDBC db, String schema, String ts) {
		
		ArrayList<String> batchInsertion = new ArrayList<String>();						// ArrayList to hold the insertion statements
		
		int numLoci 		= db.getTableLength(schema, "locus_key");					// number of loci in locus_key table	
		int numAttributes 	= db.getTableLength(schema, "attribute_key");				// number of attributes in attribute_key table				
		int numTimes 		= db.getTableLength(schema, "time_key");					// number of times in time_key table
		
		int numCols 		= 2;														// initialize with 2 columns (id + normalization)				
				
		ArrayList<Integer> objectList = new ArrayList<Integer>();						// list to hold number of objects				
		
		if (ts.equalsIgnoreCase("l_at")) {
								
			objectList.add(numLoci);
			
		} else if (ts.equalsIgnoreCase("a_lt")) {		
			
			objectList.add(numAttributes);
			
		} else if (ts.equalsIgnoreCase("t_la")) {
			
			objectList.add(numTimes);
			
		} else if (ts.equalsIgnoreCase("la_t")) {
				
			objectList.add(numLoci);
			objectList.add(numAttributes);			
			numCols += 1;
			
		} else if (ts.equalsIgnoreCase("lt_a")) {	
			
			objectList.add(numLoci);
			objectList.add(numTimes);
			numCols += 1;
			
		} else if (ts.equalsIgnoreCase("at_l")) {
			
			objectList.add(numAttributes);
			objectList.add(numTimes);
			numCols += 1;
			
		} else {
			
			return false;
			
		}				
		
		try {								
			if (objectList.size() == 1) {  // if not a composite perspective: l_at, a_lt, t_la
				for (int i = 0; i < NORMALIZATIONS.length; i++) {					
					for (int j = 1; j <= objectList.get(0); j++) {
											
						// array to store each attribute to be inserted
						String[] insertData = new String[numCols];					
						insertData[0] = j + "";
						insertData[1] = i + "";
						
						// build query statement of array of attributes
						String queryStatement = db.insertQueryBuilder(schema + "." + ts.toLowerCase(), insertData);
						batchInsertion.add(queryStatement);
					}							
				}				
			} else {  // composite perspective: lt_a, la_t, at_l				
				for (int i = 0; i < NORMALIZATIONS.length; i++) {					
					for (int j = 1; j <= objectList.get(0); j++) {						
						for (int k = 1; k <= objectList.get(1); k++) {
											
							// array to store each attribute to be inserted
							String[] insertData = new String[numCols];					
							insertData[0] = j + "";
							insertData[1] = k + "";
							insertData[2] = i + "";
							
							// build query statement of array of attributes
							String queryStatement = db.insertQueryBuilder(schema + "." + ts.toLowerCase(), insertData);
							batchInsertion.add(queryStatement);
						}						
					}							
				}				
			}		
					
			String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);		// convert ArrayList to an array			
			db.insertIntoTableBatch(schema + "." + ts.toLowerCase(), batchInsertionAr, BATCH_SIZE);		// try to insert the data into the table
					
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	
	/**
	 * Add columns to store descriptive statistics.
	 *
	 * @param db
	 *            - An instance of the PostgreSQLJDBC class.
	 * @param schema
	 *            - The schema in Postgres to write to.
	 * @param perspective
	 *            - The Tri-Space perspective to write to.
	 */
	public static boolean addDescriptStatsCols (PostgreSQLJDBC db, String schema, String perspective) {
		
		for (int i = 0; i < STATISTICS.length; i++) {
			if (!db.addColumn(schema + "." + perspective.toLowerCase(), STATISTICS[i], "NUMERIC")) {
				return false;
			}
		}
		
		return true;
	}
	
	public static Float getMinQuery(PostgreSQLJDBC db, String schema, String ts, int normalization) {
		
		int numLoci 		= db.getTableLength(schema, "locus_key");					// number of loci in locus_key table	
		int numAttributes 	= db.getTableLength(schema, "attribute_key");				// number of attributes in attribute_key table				
		int numTimes 		= db.getTableLength(schema, "time_key");					// number of times in time_key table	
		
		String[] tsSplit = ts.toLowerCase().split("_");
		String tsInverse = tsSplit[1] + "_" + tsSplit[0];
		String readTable = schema + "." + tsInverse;	
		
		ArrayList<Integer> objectList = new ArrayList<Integer>();						// list to hold number of objects	
		ArrayList<String> columns = new ArrayList<String>();
		
		if (ts.equalsIgnoreCase("l_at")) {
			
			objectList.add(numLoci);
			
		} else if (ts.equalsIgnoreCase("a_lt")) {		
			
			objectList.add(numAttributes);
			
		} else if (ts.equalsIgnoreCase("t_la")) {
			
			objectList.add(numTimes);
			
		} else if (ts.equalsIgnoreCase("la_t")) {
				
			objectList.add(numLoci);
			objectList.add(numAttributes);			
			
		} else if (ts.equalsIgnoreCase("lt_a")) {	
			
			objectList.add(numLoci);
			objectList.add(numTimes);
			
		} else if (ts.equalsIgnoreCase("at_l")) {
			
			objectList.add(numAttributes);
			objectList.add(numTimes);
			
		} else {
			
			return null;
			
		}
		
		// SELECT LEAST(MIN(a1), MIN(a2), MIN(a3), MIN(a4), MIN(a5), MIN(a6)) from sanelijo3.lt_a where normalization = 0;
		
		if (objectList.size() == 1) {  // if not a composite perspective: l_at, a_lt, t_la				
			for (int i = 1; i <= objectList.get(0); i++) {
				
				columns.add(tsSplit[0] + i);
										
			}		
			
		} else {  // composite perspective: lt_a, la_t, at_l		
			
			for (int i = 1; i <= objectList.get(0); i++) {		
				
				for (int j = 1; j <= objectList.get(1); j++) {
					
					columns.add("" + tsSplit[0].charAt(0) + i + "_" + tsSplit[0].charAt(1) + j);
					
				}						
			}							
		}
		
		String query = "SELECT LEAST(min(" + columns.get(0) + ")";
		
		if (columns.size() > 1) {
			for (int i = 1; i < columns.size(); i++) {
				query += ", min(" + columns.get(i) + ")";
			}
		}
		
		query += ") FROM " + readTable + " WHERE normalization = " + normalization + ";";		

		try {
			Statement stmt = db.getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
			
			Float newMin = null;
			while (rs.next()) {
				newMin = rs.getFloat(1);
			}
			rs.close();
			stmt.close();
			return newMin;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1f;
		}

	}
	
	public static Float getMaxQuery(PostgreSQLJDBC db, String schema, String ts, int normalization) {
		
		int numLoci 		= db.getTableLength(schema, "locus_key");					// number of loci in locus_key table	
		int numAttributes 	= db.getTableLength(schema, "attribute_key");				// number of attributes in attribute_key table				
		int numTimes 		= db.getTableLength(schema, "time_key");					// number of times in time_key table	
		
		String[] tsSplit = ts.toLowerCase().split("_");
		String tsInverse = tsSplit[1] + "_" + tsSplit[0];
		String readTable = schema + "." + tsInverse;	
		
		ArrayList<Integer> objectList = new ArrayList<Integer>();						// list to hold number of objects	
		ArrayList<String> columns = new ArrayList<String>();
		
		if (ts.equalsIgnoreCase("l_at")) {
			
			objectList.add(numLoci);
			
		} else if (ts.equalsIgnoreCase("a_lt")) {		
			
			objectList.add(numAttributes);
			
		} else if (ts.equalsIgnoreCase("t_la")) {
			
			objectList.add(numTimes);
			
		} else if (ts.equalsIgnoreCase("la_t")) {
				
			objectList.add(numLoci);
			objectList.add(numAttributes);			
			
		} else if (ts.equalsIgnoreCase("lt_a")) {	
			
			objectList.add(numLoci);
			objectList.add(numTimes);
			
		} else if (ts.equalsIgnoreCase("at_l")) {
			
			objectList.add(numAttributes);
			objectList.add(numTimes);
			
		} else {
			
			return null;
			
		}
		
		// SELECT LEAST(MIN(a1), MIN(a2), MIN(a3), MIN(a4), MIN(a5), MIN(a6)) from sanelijo3.lt_a where normalization = 0;
		
		if (objectList.size() == 1) {  // if not a composite perspective: l_at, a_lt, t_la				
			for (int i = 1; i <= objectList.get(0); i++) {
				
				columns.add(tsSplit[0] + i);
										
			}		
			
		} else {  // composite perspective: lt_a, la_t, at_l		
			
			for (int i = 1; i <= objectList.get(0); i++) {		
				
				for (int j = 1; j <= objectList.get(1); j++) {
					
					columns.add("" + tsSplit[0].charAt(0) + i + "_" + tsSplit[0].charAt(1) + j);
					
				}						
			}							
		}
		
		String query = "SELECT GREATEST(max(" + columns.get(0) + ")";
		
		if (columns.size() > 1) {
			for (int i = 1; i < columns.size(); i++) {
				query += ", max(" + columns.get(i) + ")";
			}
		}
		
		query += ") FROM " + readTable + " WHERE normalization = " + normalization + ";";
		
		try {
			Statement stmt = db.getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
			
			Float newMax = null;
			while (rs.next()) {
				newMax = rs.getFloat(1);
			}
			rs.close();
			stmt.close();
			return newMax;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1f;
		}
	}
	
	
	/**
	 * Compute histograms for a perspective.
	 *
	 * @param db
	 *            - An instance of the PostgreSQLJDBC class.
	 * @param schema
	 *            - The schema in Postgres to write to.
	 * @param perspective
	 *            - The Tri-Space perspective to compute.
	 * @param numBins
	 * 			  - The number of bins in the histogram.
	 * @param min
	 * 			  - The floor of the histogram, set to null to auto-compute.
	 * @param max
	 * 			  - The ceiling of the histogram, set to null to auto-compute.
	 * @param normalization
	 * 			  - The normalization to compute.
	 */
	public static boolean computeHistogram(PostgreSQLJDBC db, String schema, String perspective, int numBins, 
											Float min, Float max, int normalization) {
		
		int numLoci 		= db.getTableLength(schema, "locus_key");					// number of loci in locus_key table	
		int numAttributes 	= db.getTableLength(schema, "attribute_key");				// number of attributes in attribute_key table				
		int numTimes 		= db.getTableLength(schema, "time_key");					// number of times in time_key table		
		
		String[] tsSplit = perspective.toLowerCase().split("_");
		String tsInverse = tsSplit[1] + "_" + tsSplit[0];
		
		ArrayList<String> batchSQL = new ArrayList<String>();							// ArrayList to hold the update statements
		
		ArrayList<Integer> objectList = new ArrayList<Integer>();						// list to hold number of objects				
		
		if (numBins < 3) {
			System.out.println("Must have atleast 3 bins");
			return false;
		}
		
		if (perspective.equalsIgnoreCase("l_at")) {
								
			objectList.add(numLoci);
			
		} else if (perspective.equalsIgnoreCase("a_lt")) {		
			
			objectList.add(numAttributes);
			
		} else if (perspective.equalsIgnoreCase("t_la")) {
			
			objectList.add(numTimes);
			
		} else if (perspective.equalsIgnoreCase("la_t")) {
				
			objectList.add(numLoci);
			objectList.add(numAttributes);			
			
		} else if (perspective.equalsIgnoreCase("lt_a")) {	
			
			objectList.add(numLoci);
			objectList.add(numTimes);
			
		} else if (perspective.equalsIgnoreCase("at_l")) {
			
			objectList.add(numAttributes);
			objectList.add(numTimes);
			
		} else {
			
			return false;
			
		}
		
		String writeTable = schema + "." + perspective.toLowerCase();
		String readTable = schema + "." + tsInverse;		
		
		Float histogramMin = min;
		Float histogramMax = max;
		
			
		if (histogramMin == null) {		
			histogramMin = getMinQuery(db, schema, perspective, normalization);				
		}
		
		if (histogramMax == null) {
			histogramMax = getMaxQuery(db, schema, perspective, normalization);	
		}				

		
		float difference = histogramMax - histogramMin;
		float step = difference / numBins;
		
		try {								
			if (objectList.size() == 1) {  // if not a composite perspective: l_at, a_lt, t_la				
				for (int i = 1; i <= objectList.get(0); i++) {
					
					String query = "UPDATE " + writeTable + " SET bin_min = " + histogramMin  + " WHERE id = " 
							+ i + " AND normalization = " + normalization; 

					batchSQL.add(query);	
					
					query = "UPDATE " + writeTable + " SET bin_max = " + histogramMax  + " WHERE id = " 
							+ i + " AND normalization = " + normalization; 

					batchSQL.add(query);	
					
					float lessThanEqual = histogramMin + step;
					float greaterThan = histogramMin + step;						
					
					query = "UPDATE " + writeTable + " SET bin_1 = (SELECT COUNT(*) FROM "
									+ readTable + " WHERE normalization = " + normalization + " AND " 
									+ tsSplit[0] + i + " <= " + lessThanEqual + ") WHERE id = " 
									+ i + " AND normalization = " + normalization; 
					
					batchSQL.add(query);	
					
					for (int j = 2; j < numBins; j++) {
						lessThanEqual += step;
						
						query = "UPDATE " + writeTable + " SET bin_" + j + " = (SELECT COUNT(*) FROM "
								+ readTable + " WHERE normalization = " + normalization + " AND " 
								+ tsSplit[0] + i + " > " + greaterThan + " AND " + tsSplit[0] + i 
								+ " <= " + lessThanEqual + ") WHERE id = " + i + " AND normalization = " 
								+ normalization; 
				
						batchSQL.add(query);
						
						greaterThan += step;
					}
					
					query = "UPDATE " + writeTable + " SET bin_" + numBins + " = (SELECT COUNT(*) FROM "
							+ readTable + " WHERE normalization = " + normalization + " AND " 
							+ tsSplit[0] + i + " > " + greaterThan + ") WHERE id = " + i 
							+ " AND normalization = " + normalization; 
				
					batchSQL.add(query);	
															
				}									
			} else {  // composite perspective: lt_a, la_t, at_l							
				for (int i = 1; i <= objectList.get(0); i++) {						
					for (int j = 1; j <= objectList.get(1); j++) {
						
						String query = "UPDATE " + writeTable + " SET bin_min = " + histogramMin  
								+ " WHERE id1 = " + i + " AND id2 = " + j	
								+ " AND normalization = " + normalization; 

						batchSQL.add(query);	
						
						query = "UPDATE " + writeTable + " SET bin_max = " + histogramMax  
								+ " WHERE id1 = " + i + " AND id2 = " + j	
								+ " AND normalization = " + normalization; 

						batchSQL.add(query);	
						
						float lessThanEqual = histogramMin + step;
						float greaterThan = histogramMin + step;						
						
						query = "UPDATE " + writeTable + " SET bin_1 = (SELECT COUNT(*) FROM "
										+ readTable + " WHERE normalization = " + normalization + " AND " 
										+ tsSplit[0].charAt(0) + i + "_" + tsSplit[0].charAt(1) + j 
										+ " <= " + lessThanEqual + ") WHERE id1 = " + i + " AND id2 = " 
										+ j	+ " AND normalization = " + normalization; 
						
						batchSQL.add(query);	
						
						for (int k = 2; k < numBins; k++) {
							lessThanEqual += step;
							
							query = "UPDATE " + writeTable + " SET bin_" + k + " = (SELECT COUNT(*) FROM "
									+ readTable + " WHERE normalization = " + normalization + " AND " 
									+ tsSplit[0].charAt(0) + i + "_" + tsSplit[0].charAt(1) + j + " > " 
									+ greaterThan + " AND " + tsSplit[0].charAt(0) + i + "_" + tsSplit[0].charAt(1) 
									+ j + " <= " + lessThanEqual + ") WHERE id1 = " + i + " AND id2 = " 
									+ j	+ " AND normalization = " + normalization; 
					
							batchSQL.add(query);
							
							greaterThan += step;
						}
						
						query = "UPDATE " + writeTable + " SET bin_" + numBins + " = (SELECT COUNT(*) FROM "
								+ readTable + " WHERE normalization = " + normalization + " AND " 
								+ tsSplit[0].charAt(0) + i + "_" + tsSplit[0].charAt(1) + j 
								+ " > " + greaterThan + ") WHERE id1 = " + i + " AND id2 = " 
								+ j	+ " AND normalization = " + normalization; 
				
					
						batchSQL.add(query);	

					}						
				}							
			}
			
			
			String[] batchInsertionAr = batchSQL.toArray(new String[batchSQL.size()]);								// convert ArrayList to an array			
			db.insertIntoTableBatch(schema + "." + perspective.toLowerCase(), batchInsertionAr, BATCH_SIZE);		// try to insert the data into the table
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	
	/**
	 * Add columns to store histogram info.
	 *
	 * @param db
	 *            - An instance of the PostgreSQLJDBC class.
	 * @param schema
	 *            - The schema in Postgres to write to.
	 * @param perspective
	 *            - The Tri-Space perspective to write to.
	 * @param numBins
	 * 			  - The number of bins in the histogram.
	 */
	public static boolean addHistogramCols(PostgreSQLJDBC db, String schema, String perspective, int numBins) {
		
		int numLoci 		= db.getTableLength(schema, "locus_key");					// number of loci in locus_key table	
		int numAttributes 	= db.getTableLength(schema, "attribute_key");				// number of attributes in attribute_key table				
		int numTimes 		= db.getTableLength(schema, "time_key");					// number of times in time_key table			
		int numObjects 		= -1;														// number of objects in the TS perspective		
		
		if (numBins < 3) {
			System.out.println("Must have atleast 3 bins");
			return false;
		}
		
		if (perspective.equalsIgnoreCase("l_at")) {
			
			numObjects = numAttributes * numTimes;					
			
		} else if (perspective.equalsIgnoreCase("a_lt")) {		
			
			numObjects = numLoci * numTimes;
			
		} else if (perspective.equalsIgnoreCase("t_la")) {
			
			numObjects = numLoci * numAttributes;	
			
		} else if (perspective.equalsIgnoreCase("la_t")) {
			
			numObjects = numTimes;			
			
		} else if (perspective.equalsIgnoreCase("lt_a")) {	
			
			numObjects = numAttributes;			
			
		} else if (perspective.equalsIgnoreCase("at_l")) {
			
			numObjects = numLoci;
			
		} else {
			
			return false;
			
		}				
		
		String dataType = "INTEGER";													// set default type to INTEGER
					
		if (numObjects > 0 && numObjects <= 32767) {					// if number of objects is less than the range for SMALLINT				
			dataType = "SMALLINT";													// use SMALLINT instead of INTEGER
		} else if (numObjects > 2147483647) {								// otherwise
			dataType = "BIGINT";														// use BIGINT instead of INTEGER
		}							
		
		if (!db.addColumn(schema + "." + perspective.toLowerCase(), "bin_min", "NUMERIC")) {
			return false;
		}
		if (!db.addColumn(schema + "." + perspective.toLowerCase(), "bin_max", "NUMERIC")) {
			return false;
		}
		
		for (int i = 1; i <= numBins; i++) {
			if (!db.addColumn(schema + "." + perspective.toLowerCase(), "bin_" + i, dataType)) {
				return false;
			}
		}
		
		return true;
	}
	
	
	/**
	 * Compute descriptive statistics for a perspective.
	 *
	 * @param db
	 *            - An instance of the PostgreSQLJDBC class.
	 * @param schema
	 *            - The schema in Postgres to write to.
	 * @param perspective
	 *            - The Tri-Space perspective to compute
	 * @param normalizations
	 * 			  - The normalizations to compute.
	 */
	public static boolean computeAllStatistics(PostgreSQLJDBC db, String schema, String perspective, int[] normalizations) {
		for (int i = 0; i < STATISTICS.length; i++) {
			if (!computeStatistic(db, schema, perspective, normalizations, i)) {
				return false;
			}
		}
		
		return true;
	}
	
	public static boolean computeStatistic(PostgreSQLJDBC db, String schema, String ts, int[] normalizations, int statistic) {
		
		int numLoci 		= db.getTableLength(schema, "locus_key");					// number of loci in locus_key table	
		int numAttributes 	= db.getTableLength(schema, "attribute_key");				// number of attributes in attribute_key table				
		int numTimes 		= db.getTableLength(schema, "time_key");					// number of times in time_key table
		
		String[] tsSplit = ts.toLowerCase().split("_");
		String tsInverse = tsSplit[1] + "_" + tsSplit[0];
		
		ArrayList<String> batchSQL = new ArrayList<String>();							// ArrayList to hold the update statements
		
		ArrayList<Integer> objectList = new ArrayList<Integer>();						// list to hold number of objects				
		
		if (ts.equalsIgnoreCase("l_at")) {
								
			objectList.add(numLoci);
			
		} else if (ts.equalsIgnoreCase("a_lt")) {		
			
			objectList.add(numAttributes);
			
		} else if (ts.equalsIgnoreCase("t_la")) {
			
			objectList.add(numTimes);
			
		} else if (ts.equalsIgnoreCase("la_t")) {
				
			objectList.add(numLoci);
			objectList.add(numAttributes);			
			
		} else if (ts.equalsIgnoreCase("lt_a")) {	
			
			objectList.add(numLoci);
			objectList.add(numTimes);
			
		} else if (ts.equalsIgnoreCase("at_l")) {
			
			objectList.add(numAttributes);
			objectList.add(numTimes);
			
		} else {
			
			return false;
			
		}
		
		String writeTable = schema + "." + ts.toLowerCase();
		String readTable = schema + "." + tsInverse;
		
		try {								
			if (objectList.size() == 1) {  // if not a composite perspective: l_at, a_lt, t_la
				for (int i = 0; i < normalizations.length; i++) {					
					for (int j = 1; j <= objectList.get(0); j++) {
						String query = "UPDATE " + writeTable + " SET " + getStatQuery(tsSplit[0] + j, statistic)
										+ " FROM " + readTable + " WHERE normalization = " + normalizations[i] 
										+ ") WHERE id = " + j + " AND normalization = " + normalizations[i]; 
						
						batchSQL.add(query);							
					}							
				}				
			} else {  // composite perspective: lt_a, la_t, at_l				
				for (int i = 0; i < normalizations.length; i++) {					
					for (int j = 1; j <= objectList.get(0); j++) {						
						for (int k = 1; k <= objectList.get(1); k++) {
							String query = "UPDATE " + writeTable + " SET " 
									+ getStatQuery("" + tsSplit[0].charAt(0) + j + "_" + tsSplit[0].charAt(1) + k, statistic) 
									+ " FROM " + readTable + " WHERE normalization = " + normalizations[i] + ") WHERE id1 = " 
									+ j + " AND id2 = " + k + " AND normalization = " + normalizations[i]; 							
							
							batchSQL.add(query);					

						}						
					}							
				}	
			}
			
			
			String[] batchInsertionAr = batchSQL.toArray(new String[batchSQL.size()]);								// convert ArrayList to an array			
			db.insertIntoTableBatch(schema + "." + ts.toLowerCase(), batchInsertionAr, BATCH_SIZE);		// try to insert the data into the table
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}		
				
		return true;
	}
		
	public static String getStatQuery (String column, int statistic) {
		if (statistic == 0) { // minimum
			return "min = (SELECT min(" + column + ")";
		} else if (statistic == 1) {  // maximum
			return "max = (SELECT max(" + column + ")";
		} else if (statistic == 2) {  // mean
			return "mean = (SELECT avg(" + column + ")";
		} else if (statistic == 3) {
			return "median = (SELECT ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY " + column + ")::numeric, 2)";
		} else if (statistic == 4) {
			return "std_dev = (SELECT stddev_pop(" + column + ")";
		}
		return null;
	}
	
	public static boolean statsTable(PostgreSQLJDBC db, String schema, String ts) {
		if (!createStatsTable(db, schema, ts)) {
			return false;
		}
		
		if (!populateStatsTable(db, schema, ts)) {
			return false;
		}
		
		return true;
	}
	
	public static boolean statsTables(PostgreSQLJDBC db, String schema, int[] perspectives) {
		for (int i = 0; i < perspectives.length; i++) {
			if (!statsTable(db, schema, NORMALIZATIONS[perspectives[i]])) {
				return false;
			}
		}
		
		return true;
	}
}
