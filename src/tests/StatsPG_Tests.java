package tests;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import edu.sdsu.datavis.trispace.tsprep.db.PostgreSQLJDBC;
import edu.sdsu.datavis.trispace.tsprep.dr.SOMaticManager;
import edu.sdsu.datavis.trispace.tsprep.stats.StatsPG;

public class StatsPG_Tests {
//	final static boolean RESET 		= true;
	final static boolean RESET 		= false;

	// Select the study area
	static int STUDY_AREA			= 0;

	// Scene name list
	final static String[] SCENES 	= { "SanElijo", "CocosFire", "Batiquitos" };
	final static String SCENE 		= SCENES[STUDY_AREA];

	// Schema list
	final static String[] SCHEMAS 	= { "sanelijo3", "cocos", "batiquitos" };
	final static String SCHEMA 		= SCHEMAS[STUDY_AREA];

	// specify coordinate system
	final static String EPSG 		= "4326";

	// specify noData values in Raster files
	final static int NO_DATA 		= -9999;
	final static int LC_NO_DATA 	= 0;

	// specify columns that represent unique ID
	final static int[] PRIMARY_KEYS = { 0, 1 };

	// Tri-Space perspectives
	final static String[] PERSPECTIVES = { "L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L" };
	
	final static boolean BATCH 		= true;
	final static boolean HAS_LC 	= true;

	// threshold of input vector count to determine if SOM or MDS
	final static int SOM_THRESHOLD 	= 100;

	// constrain maximum size of SOM
	final static int SOM_NEURON_CAP 			= 250000;
	final static int NUM_TRAIN_ITERATION_CYCLES = 1;
	
	// histogram parameters for MDS
	final static int NUM_HISTOGRAM_BINS			= 8;

	// final static String CREDENTIALS = "./environments/postgresql2.txt";
	// specify PostgreSQL credentials
	final static String CREDENTIALS = "./environments/postgresql3.txt";
	static String url 				= "";
	static String user 				= "";
	static String pw 				= "";

	// specify PostgreSQL DB
	final static String database 	= "schempp17";

	// normalization parameters
	// specify range of normalization (e.g. 0-1; .1-.9, etc.)
	final static float MIN_NORMALIZATION = 0.1f;
	final static float MAX_NORMALIZATION = 0.9f;

	public static void main(String[] args) {
		
		credentialsTest();											// ensure credentials exist and are loaded		
		PostgreSQLJDBC db = new PostgreSQLJDBC(url, user, pw);		// create JDBC postgres connection		
		System.out.println(db.changeDatabase(database));			// change database from default

		// testing
		if (RESET && db.schemaExists(SCHEMA)) {
			db.dropSchemaCascade(SCHEMA);
		}
		
//		StatsPG.computeStatistic(db, SCHEMA, "t_la", 0);
//		StatsPG.computeStatistic(db, SCHEMA, "t_la", 1);
//		StatsPG.computeStatistic(db, SCHEMA, "t_la", 2);
//		StatsPG.computeStatistic(db, SCHEMA, "t_la", 3);
//		StatsPG.addHistogramCols(db, SCHEMA, "t_la", 8);
		
//		StatsPG.computeStatistic(db, SCHEMA, "at_l", 0);
//		StatsPG.computeStatistic(db, SCHEMA, "at_l", 1);
//		StatsPG.computeStatistic(db, SCHEMA, "at_l", 2);
//		StatsPG.computeStatistic(db, SCHEMA, "at_l", 3);
//		StatsPG.computeStatistic(db, SCHEMA, "at_l", 4);
//		StatsPG.addHistogramCols(db, SCHEMA, "at_l", 8);
		
//		StatsPG.computeHistogram(db, SCHEMA, "at_l", 8, 79f, 7145f, 0);
//		StatsPG.computeHistogram(db, SCHEMA, "t_la", 8, MIN_NORMALIZATION, MAX_NORMALIZATION, 1);
//		StatsPG.computeHistogram(db, SCHEMA, "t_la", 8, 0.1f, 0.9f, 2);
//		StatsPG.computeHistogram(db, SCHEMA, "t_la", 8, 0.1f, 0.9f, 3);
//		StatsPG.computeHistogram(db, SCHEMA, "t_la", 8, 0.1f, 0.9f, 4);
//		StatsPG.computeHistogram(db, SCHEMA, "t_la", 8, 0.1f, 0.9f, 5);
//		StatsPG.computeHistogram(db, SCHEMA, "t_la", 8, 0.1f, 0.9f, 6);
		
//		System.out.println("Min: " + StatsPG.getMinQuery(db, SCHEMA, "a_lt", 0));
//		System.out.println("Max: " + StatsPG.getMaxQuery(db, SCHEMA, "a_lt", 0));
		
		SOMaticManager.writeNeuronAttributes(db, SCHEMA, "D:\\Workspace\\OxygenProjects\\tsprep\\data\\sanelijo3\\SOMaticOut\\A_LT_Normalized\\L_AT\\trainedSom.cod", "l_at");
		
		db.disconnect();
		
		System.out.println("Program finished");
		System.exit(0);
//		db.createSchema(SCHEMA);									// create schema is it does not exist
		
//		StatsPG.statsTable(db, schema, "A_LT");
//		StatsPG.addDescriptStatsCols(db, schema, "A_LT");
//		StatsPG.statsTable(db, schema, "T_LA");
//		StatsPG.addDescriptStatsCols(db, schema, "T_LA");
//		StatsPG.statsTable(db, schema, "AT_L");
//		StatsPG.addDescriptStatsCols(db, schema, "AT_L");

	}
	
	private static void credentialsTest() {
		if (CREDENTIALS.equals("") || !loadEnvironments()) {
			System.out.println("Incomplete data for URL/USER/PW");
			System.out.println("System Exiting");
			System.exit(0);
		}
	}
	
	private static boolean loadEnvironments() {
		try {
			BufferedReader br = new BufferedReader(new FileReader(CREDENTIALS));

			url 	= br.readLine();		// get the url/ip that hosts the database
			user 	= br.readLine();		// get the user to access the database
			pw 		= br.readLine();		// get the user password

			br.close();						// close the BR

			if (!url.equals("") && !user.equals("") && !pw.equals("")) return true;
			else return false; 				// return false if empty			

		} catch (IOException e) {
			return false;
		}
	}

}
