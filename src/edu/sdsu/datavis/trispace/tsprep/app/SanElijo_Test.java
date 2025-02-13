package edu.sdsu.datavis.trispace.tsprep.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import edu.sdsu.datavis.trispace.tsprep.db.PostgreSQLJDBC;
import edu.sdsu.datavis.trispace.tsprep.io.img.ImageManager;
import edu.sdsu.datavis.trispace.tsprep.xform.CSVManager;
import edu.sdsu.datavis.trispace.tsprep.xform.PostgreSQLManager;

public class SanElijo_Test {
	final static boolean RESET = false;
	final static int NO_DATA = -9999;
	final static int[] PRIMARY_KEYS = {0,1};
	final static String SCENE = "SanElijo";
	final static String[] PERSPECTIVES = {"L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L"};
	
	// threshold of input vector count to determine if SOM or MDS
	final static int SOM_THRESHOLD = 75;
	
//	final static String CREDENTIALS = "./environments/postgresql2.txt";
	final static String CREDENTIALS = "./environments/postgresql.txt";
	static String url = "";
	static String user = "";
	static String pw = "";
//	static String database = "schempp_thesis";
	static String database = "thesis";
	
	
	final static float MIN_NORMALIZATION = 0.1f;
	final static float MAX_NORMALIZATION = 0.9f;
	
	final static int EUCLIDEAN = 1;
	final static int COSINE = 2;
	final static int MANHATTAN = 3;
	
	final static String SCHEMA = "sanelijo";
	final static String EPSG = "4326";
	
	final static int DIST_MEASURE = COSINE;
	final static int NITERATIONS1 = 10000;
	final static int NITERATIONS2 = 20000;
	final static int NITERATIONS3 = 50000;
	final static int NTHREADS = 1;
	final static boolean ROUNDING = true;
	final static int SCALING_FACTOR = 1;
	
	final static int MAX_K = 12;
	final static int K_ITERATIONS = 1000;

	public static void main(String[] args) {
		// ensure credentials exist and are loaded
		credentialsTest();
		
		// create JDBC postgres connection
		PostgreSQLJDBC db = new PostgreSQLJDBC(url,user,pw);
		
		// change database from default
		System.out.println(db.changeDatabase(database));
		
		// testing
		if (RESET && db.schemaExists(SCHEMA)) {
			db.dropSchemaCascade(SCHEMA);
		}
		
		// create schema is it does not exist
		db.createSchema(SCHEMA);
		
		// set input to folder containing .tif images
		File f = new File("./data/" + SCHEMA + "/input/imagery");	
		
		// output directory 
		String outDir = "./data/" + SCHEMA + "/input/csv";		
		
		// perform gdal2xyz on all files contained in directory 'f'
//		ImageManager.performGDAL2XYZ(f,outDir);
//		System.out.println("GDAL2XYZ COMPLETE!");
		
//		CSVManager.createPixelPolyTable(db, SCHEMA, new File(outDir), EPSG);
//		System.out.println("Pixel Polygon Table Created!");
		
		
		// input directory
		f = new File(outDir);
		
		// new output directory
		outDir = "./data/" + SCHEMA + "/tables/imagery";
				
		// attribute dictionary for header definitions
		String attributeDictionary = "./data/" + SCHEMA + "/input/dictionaries/attributes.txt";
		
		// add headers and remove noData pixels
		ImageManager.cleanData(f, NO_DATA, outDir, true, attributeDictionary);		
		
		// validate loci
		lociTest(outDir);
		
		
		// imagery table directory
		f = new File(outDir);
		
		// new output directory
		outDir = "./data/" + SCHEMA + "/tables/Non_normalized";
		
		// convert a folder (T) containing many files (L_A) to L_AT
//		System.out.println("L_AT?");
//		System.out.println(CSVManager.convertToL_AT("L_A_T", f, PRIMARY_KEYS, outDir));
		
		
//		 input L_AT file
		f = new File(outDir + "/L_AT_" + SCENE + ".csv");
		// convert CSV to all other perspectives
//		CSVManager.fromL_AT2All(f, outDir, SCENE);
		
		// normalize to all perspectives
//		String inDir = outDir + "";
//		for (int i = 0; i < PERSPECTIVES.length; i++) {
//			outDir = "";
//			String name = PERSPECTIVES[i] + "_" + SCENE + ".csv";
//			f = new File(inDir + "/" + name);
//			String[] split = inDir.split("/");
//			for (int j = 0; j < split.length-1; j++) {
//				outDir = outDir + split[j] + "/";
//			}
//			outDir = outDir + PERSPECTIVES[i] + "_Normalized/" + name;
////			CSVManager.normalizeCSVMinMax(f, outDir, PERSPECTIVES[i], 0);
//			CSVManager.normalizeCSVMinMax(f, outDir, PERSPECTIVES[i], MIN_NORMALIZATION, MAX_NORMALIZATION);
////			CSVManager.normalizeCSVTanH(f, outDir, PERSPECTIVES[i]);
////			CSVManager.normalizeCSVUnitVector(f, outDir, PERSPECTIVES[i]);
//		}
		
		// convert each normalized file to L_AT then to the remaining perspectives		
//		for (int i = 0; i < PERSPECTIVES.length; i++) {
//			inDir = "./data/" + SCHEMA + "/tables/" + PERSPECTIVES[i] + "_Normalized";
//			f = new File(inDir + "/" + PERSPECTIVES[i] + "_" + SCENE + ".csv");
//			if (i == 0) {
//				CSVManager.fromL_AT2All(f, inDir, SCENE);
//			} else if (i == 1) {
//				CSVManager.fromA_LT2All(f, inDir, SCENE);
//			} else if (i == 2) {
//				CSVManager.fromT_LA2All(f, inDir, SCENE);
//			} else if (i == 3) {
//				CSVManager.fromLA_T2All(f, inDir, SCENE);
//			} else if (i == 4) {
//				CSVManager.fromLT_A2All(f, inDir, SCENE);
//			} else if (i == 5) {
//				CSVManager.fromAT_L2All(f, inDir, SCENE);
//			}
//		}
		
		// convert each file into a .dat file for SOMatic
//		for (int i = 0; i < PERSPECTIVES.length; i++) {
//			for (int j = 0; j < PERSPECTIVES.length; j++) {
//				File input = new File("./data/" + SCHEMA + "/tables/" + PERSPECTIVES[i] + "_Normalized/" 
//									  + PERSPECTIVES[j] + "_" + SCENE + ".csv");
//				String output = "./data/" + SCHEMA + "/SOMaticIn/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[j] + "_" + SCENE + ".dat";
//				CSVManager.fromCSV2DatFile(PERSPECTIVES[j], input, output);
//			}
//		}
		
		String outDir2 = outDir = "./data/" + SCHEMA + "/tables/imagery";;
		File f2 = new File(outDir2);
		
		// create and populate locus_key table
		System.out.println("EXTRACT Dictionary: ");
		System.out.println(PostgreSQLManager.createTableL_AT(db, "L_A_T", SCHEMA, f2, PRIMARY_KEYS, 1, "p"));
		
		// create and populate locus_pt table
		System.out.println("EXTRACT Points: ");
		System.out.println(PostgreSQLManager.extractPtGeomFromCSV(db, SCHEMA, f2, PRIMARY_KEYS, "l", 1, EPSG));
//		
		// create and populate locus_poly table
		System.out.println("EXTRACT Polygons: ");
		PostgreSQLManager.createFinalPixelPolyTable(db,SCHEMA);
//		
		
		outDir2 = "./data/" + SCHEMA + "/tables";
		
//		CSVManager.insertTSFromCSV(db,SCHEMA,"L_AT", outDir2,SCENE);
		
//		insertTransactionalTS(db, SCHEMA, "L_AT", outDir2, SCENE, 500);
//		insertTransactionalTS(db, SCHEMA, "LT_A", outDir2, SCENE, 500);
//		insertTransactionalTS(db, SCHEMA, "LA_T", outDir2, SCENE, 500);
		
//		CSVManager.insertTSFromCSV(db,SCHEMA,"L_AT", outDir2, SCENE, 3, 4);
		
//		
//		CSVManager.insertTSFromCSV(db,SCHEMA,"LA_T", outDir2,SCENE,1001,1002);
//		
		PostgreSQLManager.insertTSFromCSVBatch(db,SCHEMA,"L_AT", outDir2,SCENE);
		System.out.println("L_AT Table Populated!");
		PostgreSQLManager.insertTSFromCSVBatch(db,SCHEMA,"LT_A", outDir2,SCENE);
		System.out.println("LT_A Table Populated!");
		PostgreSQLManager.insertTSFromCSVBatch(db,SCHEMA,"LA_T", outDir2,SCENE);
		System.out.println("LA_T Table Populated!");
//		
//		CSVManager.createSSETable(db, SCHEMA, MAX_K);
//		CSVManager.insert2SSETable(db, SCHEMA, MAX_K);
		
		
		System.out.println("Exiting Program");
		System.exit(0);
	}
	
	private static void insertTransactionalTS(PostgreSQLJDBC db, String schema, String ts, String parentPath, String scene, int maxTransactionSize) {
		
		int timeCount = db.getTableLength(SCHEMA, "time_key");
//		System.out.println("Time Dimensions: " + timeCount);
		int attributeCount = db.getTableLength(SCHEMA, "attribute_key");
//		System.out.println("Attribute Dimensions: " + attributeCount);
		int lociCount = db.getTableLength(SCHEMA, "locus_key");
//		System.out.println("Loci Dimensions: " + lociCount);
		if (ts.equals("L_AT")) {
			System.out.println("Object Dimensions: " + lociCount);
			System.out.println("divided by " + maxTransactionSize);
			int numFullTransactions = lociCount/maxTransactionSize;
			int numTransactions = numFullTransactions + 0;
			if (lociCount % maxTransactionSize != 0) numTransactions++; 
			
			int[] transactions = new int[numTransactions];
			for (int i = 0; i < numFullTransactions; i++) {
				transactions[i] = maxTransactionSize;
			}

			if (numTransactions > numFullTransactions) {
				transactions[numFullTransactions] = lociCount % maxTransactionSize;
			}
			
			int startIdx = 0;
			for (int i = 0; i < transactions.length; i++) {
//				System.out.println(transactions[i]);
				int endIdx = startIdx + transactions[i];
				PostgreSQLManager.insertTSFromCSV(db,SCHEMA,"L_AT", parentPath, SCENE, startIdx, endIdx);
//				System.out.println("Starting Index: " + startIdx);
//				System.out.println("Endinging Index: " + endIdx);
//				System.out.println("");
				startIdx += transactions[i];
			}
			
			
			
		} else if (ts.equals("LA_T")) {
			System.out.println("Object Dimensions: " + lociCount * attributeCount);
			System.out.println("divided by " + maxTransactionSize);
			int numFullTransactions = (lociCount * attributeCount) / maxTransactionSize;
			int numTransactions = numFullTransactions + 0;
			if ((lociCount * attributeCount) % maxTransactionSize != 0) numTransactions++; 
			
			int[] transactions = new int[numTransactions];
			for (int i = 0; i < numFullTransactions; i++) {
				transactions[i] = maxTransactionSize;
			}

			if (numTransactions > numFullTransactions) {
				transactions[numFullTransactions] = (lociCount * attributeCount) % maxTransactionSize;
			}
			
			int startIdx = 0;
			for (int i = 0; i < transactions.length; i++) {
//				System.out.println(transactions[i]);
				int endIdx = startIdx + transactions[i];
				PostgreSQLManager.insertTSFromCSV(db,SCHEMA,"LA_T", parentPath, SCENE, startIdx, endIdx);
//				System.out.println("Starting Index: " + startIdx);
//				System.out.println("Endinging Index: " + endIdx);
//				System.out.println("");
				startIdx += transactions[i];
			}
		} else if (ts.equals("LT_A")) {
			System.out.println("Object Dimensions: " + lociCount * timeCount);
			System.out.println("divided by " + maxTransactionSize);
			int numFullTransactions = (lociCount * timeCount) / maxTransactionSize;
			int numTransactions = numFullTransactions + 0;
			if ((lociCount * timeCount) % maxTransactionSize != 0) numTransactions++; 
			
			int[] transactions = new int[numTransactions];
			for (int i = 0; i < numFullTransactions; i++) {
				transactions[i] = maxTransactionSize;
			}

			if (numTransactions > numFullTransactions) {
				transactions[numFullTransactions] = (lociCount * timeCount) % maxTransactionSize;
			}
			
			int startIdx = 0;
			for (int i = 0; i < transactions.length; i++) {
//				System.out.println(transactions[i]);
				int endIdx = startIdx + transactions[i];
				PostgreSQLManager.insertTSFromCSV(db,SCHEMA,"LT_A", parentPath, SCENE, startIdx, endIdx);
//				System.out.println("Starting Index: " + startIdx);
//				System.out.println("Endinging Index: " + endIdx);
//				System.out.println("");
				startIdx += transactions[i];
			}
		} else {
			System.out.println("Invalid TriSpace Perspective.");
		}
	}
	
	private static void insertTS(PostgreSQLJDBC db, String schema, String ts, String directory, String scene) {
		if (ts.equals("L_AT")) {
			PostgreSQLManager.insertTSFromCSV(db, SCHEMA,"L_AT", directory, SCENE);
		} else if (ts.equals("LA_T")) {
			PostgreSQLManager.insertTSFromCSV(db, SCHEMA,"LA_T", directory, SCENE);
		} else if (ts.equals("LT_A")) {
			PostgreSQLManager.insertTSFromCSV(db, SCHEMA,"LT_A", directory, SCENE);
		} else {
			System.out.println("Invalid TriSpace Perspective.");
		}
	}
	
	private static void credentialsTest() {
		if (CREDENTIALS.equals("") || !loadEnvironments()) {
			System.out.println("Incomplete data for URL/USER/PW");
			System.out.println("System Exiting");
			System.exit(0);
		}
	}
	
	private static void lociTest(String directory) {
		System.out.println("Verifying loci are stable...");
		if (!CSVManager.commonLociCheck(directory, PRIMARY_KEYS)) {
			System.out.println("Loci are not stable across the time series!");
			System.out.println("Stopping process.");
			System.exit(0);
		}
		System.out.println("Loci are STABLE: Proceeding...");
	}
	
	private static boolean loadEnvironments() {
		try {
			BufferedReader br = new BufferedReader(new FileReader(CREDENTIALS));
			
			url = br.readLine();
			user = br.readLine();
			pw = br.readLine();
			
			br.close();
			
			if (!url.equals("") && !user.equals("") && !pw.equals("")) {
				return true;
			} else {
				return false;
			}
			
		} catch (IOException e) {
			return false;
		}
	}

}
