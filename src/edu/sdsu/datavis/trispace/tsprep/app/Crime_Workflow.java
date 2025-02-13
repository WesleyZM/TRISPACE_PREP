package edu.sdsu.datavis.trispace.tsprep.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;

import edu.sdsu.datavis.trispace.tsprep.db.PostgreSQLJDBC;
import edu.sdsu.datavis.trispace.tsprep.dr.MDSManager;
import edu.sdsu.datavis.trispace.tsprep.dr.SOMaticManager;
import edu.sdsu.datavis.trispace.tsprep.xform.CSVManager;
import edu.sdsu.datavis.trispace.tsprep.xform.PostgreSQLManager;

public class Crime_Workflow {
	
	final static boolean RESET = false;
	
	final static String[] PERSPECTIVES = { "L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L" };
	final static boolean BATCH = true;
	final static boolean HAS_LC = false;
	final static int BATCH_SIZE = 50000;

	// threshold of input vector count to determine if SOM or MDS
	final static int SOM_THRESHOLD = 100;

	// constrain maximum size of SOM
	final static int SOM_NEURON_CAP = 250000;
	
	// specify PostgreSQL credentials
	final static String CREDENTIALS = "./environments/postgresql3.txt";
	static String url = "";
	static String user = "";
	static String pw = "";
	// specify PostgreSQL DB
	static String database = "schempp17";
	
	final static String SCHEMA = "crime2";
	
	// specify columns that represent unique ID
	final static int[] PRIMARY_KEYS = { 0 };
	
	// normalization parameters
	// specify range of normalization (e.g. 0-1; .1-.9, etc.)
	final static float MIN_NORMALIZATION = 0.1f;
	final static float MAX_NORMALIZATION = 0.9f;
	
	// implemented distance measures
	final static int EUCLIDEAN = 1;
	final static int COSINE = 2;
	final static int MANHATTAN = 3;

	// specify distance measure
	final static int DIST_MEASURE = COSINE;
	
	// SOM training parameters
	final static int NITERATIONS1 = 10000;
	final static int NITERATIONS2 = 20000;
	final static int NITERATIONS3 = 50000;
	final static int NTHREADS = 4;
	final static boolean ROUNDING = true;
	final static int SCALING_FACTOR = 1;

	final static int MAX_K = 12;
	final static int K_ITERATIONS = 1000;
	
	
	public static void main(String[] args) {
		// ensure credentials exist and are loaded
		credentialsTest();
	
		// create JDBC postgres connection
		PostgreSQLJDBC db = new PostgreSQLJDBC(url, user, pw);
	
		// change database from default
		System.out.println(db.changeDatabase(database));
	
		// testing
		if (RESET && db.schemaExists(SCHEMA)) {
			db.dropSchemaCascade(SCHEMA);
		}
	
		// create schema is it does not exist
		db.createSchema(SCHEMA);
		
		File input = new File("./data/" + SCHEMA + "/input/csv/crime.csv");
		String output = "./data/" + SCHEMA + "/tables/Non_Normalized";
		
//		CSVManager.processFile(input, PERSPECTIVES[4], output, true);
		
		input = new File(output + "/LT_A.csv");
		
		// convert from LT_A to all other TriSpace perspectives (T_LA, AT_L, etc)		
//		CSVManager.fromLT_A2All(input, output);
		
		
		// normalize all TS perspectives using ROW normalization
		// THEN convert all normalized perspectives into all REMAINING perspectives
//		normalizeAllPerspectives(SCHEMA, 0, MIN_NORMALIZATION, MAX_NORMALIZATION);
		
		
		// convert each file into a .dat file for SOMatic
//		convert2DAT(SCHEMA);
		
		
		// create and populate locus_key table
		System.out.println(PostgreSQLManager.createTableL_AT(db, "TS", SCHEMA,
				new File("./data/" + SCHEMA + "/tables/Non_Normalized"), PRIMARY_KEYS, 2, "l", BATCH, false, SOM_THRESHOLD));
		System.out.println("Created locus_key Table");
		
		
		// add abbreviations to locus_key
//		System.out.println(db.addColumn(SCHEMA + ".locus_key", "abbreviation", "TEXT"));
//		try {
//			BufferedReader br = new BufferedReader(new FileReader("./data/" + SCHEMA + "/input/dictionaries/loci.txt"));
//			
//			String line = "";
//			int lineCount = 0;
//			
//			while ((line = br.readLine()) != null) {
//				lineCount++;
//				String[] split = line.split(",");
//				System.out.println("id = " + lineCount);
//				System.out.println("alias = " + split[0]);
//				System.out.println("abbreviation = " + split[1]);
//				db.updateTable(SCHEMA, "locus_key", "id", lineCount + "", "abbreviation", "'" + split[1] + "'");
//				db.updateTable(SCHEMA, "locus_key", "id", lineCount + "", "alias", "'" + split[0] + "'");
//			}
//			br.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
//		try {
//			BufferedReader br = new BufferedReader(new FileReader("./data/" + SCHEMA + "/input/dictionaries/attributes.txt"));
//			
//			String line = "";
//			int lineCount = 0;
//			
//			while ((line = br.readLine()) != null) {
//				lineCount++;
//				db.updateTable(SCHEMA, "attribute_key", "id", lineCount + "", "alias", "'" + line + "'");
//			}
//			br.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
//		try {
//			BufferedReader br = new BufferedReader(new FileReader("./data/" + SCHEMA + "/input/dictionaries/times.txt"));
//			
//			String line = "";
//			int lineCount = 0;
//			
//			while ((line = br.readLine()) != null) {
//				lineCount++;
//				db.updateTable(SCHEMA, "time_key", "id", lineCount + "", "alias", "'" + line + "'");
//			}
//			br.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
//		File f = new File("./data/crime/input/geom/StateCoords.csv");
//		System.out.println("polyFromTable");
//		System.out.println(CSVManager.polyFromTable(db, SCHEMA, "locus", "", f, BATCH, BATCH_SIZE, true, "abbreviation", true));
		
		// create the geometry for the TS perspectives
		// false since geometry already computed
		createTSGeometry(db, SCHEMA, true);
		
		
		// create SSE table
//		CSVManager.createSSETable(db, SCHEMA, MAX_K);
//		System.out.println("Created SSE Table");

		// populate SSE table with dummy values
//		CSVManager.insert2SSETable(db, SCHEMA, MAX_K, BATCH);
//		System.out.println("Populated SSE Table");
		

//		CSVManager.insertTSFromCSVBatch(db, SCHEMA, "AT_L", "./data/" + SCHEMA + "/tables", "", true);
//		CSVManager.insertTSFromCSVBatch(db, SCHEMA, "LA_T", "./data/" + SCHEMA + "/tables", "", true);
//		CSVManager.insertTSFromCSVBatch(db, SCHEMA, "LT_A", "./data/" + SCHEMA + "/tables", "", true);
		
		// Must perform RStudio PCA analysis prior to this step
		createPopulatePCA(db, SCHEMA);
		
		// populate the SOM & MDS attributes in PGSQL
//		populateTSData(db,SCHEMA, new int[] {3});
		

		
		// create and populate locus_poly table
//		CSVManager.createFinalPixelPolyTable(db, SCHEMA, BATCH);
//		System.out.println("Created locus_poly Table");
		

		System.out.println("Exiting Program");
		System.exit(0);
	}
	
	// function to perform the TS normalizations
	// normType can be { 0 - min/max, 1 - Unit Vector, 2 - TanH}
	// min & max parameters only used when normType == 0
	public static void normalizeAllPerspectives(String schema, int normType, float min, float max) {
		// new output directory
		String inDir = "./data/" + schema + "/tables/Non_normalized";
	
		// iterate thru each TS perspective and perform ROW normalization
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			String outDir = "";
			String name = PERSPECTIVES[i] + ".csv";
			File f = new File(inDir + "/" + name);
			String[] split = inDir.split("/");
			for (int j = 0; j < split.length - 1; j++) {
				outDir = outDir + split[j] + "/";
			}
			outDir = outDir + PERSPECTIVES[i] + "_Normalized/" + name;
	
			if (normType == 0) {
				if (min == 0 && max == 1) {
					CSVManager.normalizeCSVMinMax(f, outDir, PERSPECTIVES[i], 0);
				} else {
					CSVManager.normalizeCSVMinMax(f, outDir, PERSPECTIVES[i], min, max);
				}
			} else if (normType == 1) {
				CSVManager.normalizeCSVUnitVector(f, outDir, PERSPECTIVES[i]);
			} else if (normType == 2) {
				CSVManager.normalizeCSVTanH(f, outDir, PERSPECTIVES[i]);
			} else {
				System.out.println("Normalization Type is not implemented!");
				System.out.println("normType can be: 0 - min/max; 1 - Unit Vector; 2 - TanH");
			}
		}
	
		// convert each normalized file to L_AT then to the remaining perspectives
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			inDir = "./data/" + schema + "/tables/" + PERSPECTIVES[i] + "_Normalized";
			File f = new File(inDir + "/" + PERSPECTIVES[i] + ".csv");
			if (i == 0) {
				CSVManager.fromL_AT2All(f, inDir);
			} else if (i == 1) {
				CSVManager.fromA_LT2All(f, inDir);
			} else if (i == 2) {
				CSVManager.fromT_LA2All(f, inDir);
			} else if (i == 3) {
				CSVManager.fromLA_T2All(f, inDir);
			} else if (i == 4) {
				CSVManager.fromLT_A2All(f, inDir);
			} else if (i == 5) {
				CSVManager.fromAT_L2All(f, inDir);
			}
		}
	}
	
	public static void convert2DAT(String schema) {

		// iterate thru all perspectives
		for (int i = 0; i < PERSPECTIVES.length; i++) {

			// iterate thru all normalizations
			for (int j = 0; j < PERSPECTIVES.length; j++) {

				// input
				File input = new File("./data/" + schema + "/tables/" + PERSPECTIVES[i] + "_Normalized/"
						+ PERSPECTIVES[j] + ".csv");
				// output
				String output = "./data/" + schema + "/SOMaticIn/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[j]
						+ ".dat";

				// convert to a .dat file
				CSVManager.fromCSV2DatFile(PERSPECTIVES[j], input, output);
			}
		}
	}
	
	private static void credentialsTest() {
		if (CREDENTIALS.equals("") || !loadEnvironments()) {
			System.out.println("Incomplete data for URL/USER/PW");
			System.out.println("System Exiting");
			System.exit(0);
		}
	}
	
	public static void createPopulatePCA(PostgreSQLJDBC db, String schema) {
		PostgreSQLManager.createPCATable(db, schema);
		String tmpPath = "./data/" + schema + "/tables/PCA/L_AT";
		PostgreSQLManager.populatePCATable(db, schema, 1, tmpPath, BATCH);
		PostgreSQLManager.populateTSPCATable(db, schema, "L_AT", tmpPath, BATCH);
		tmpPath = "./data/" + schema + "/tables/PCA/A_LT";
		PostgreSQLManager.populatePCATable(db, schema, 2, tmpPath, BATCH);
		PostgreSQLManager.populateTSPCATable(db, schema, "A_LT", tmpPath, BATCH);
		tmpPath = "./data/" + schema + "/tables/PCA/T_LA";
		PostgreSQLManager.populatePCATable(db, schema, 3, tmpPath, BATCH);
		PostgreSQLManager.populateTSPCATable(db, schema, "T_LA", tmpPath, BATCH);
	}
	
	public static void createTSGeometry(PostgreSQLJDBC db, String schema, boolean createGeom) {
		// retrieve number of time objects
		int timeCount = db.getTableLength(schema, "time_key");

		// retrieve number of attribute objects
		int attributeCount = db.getTableLength(schema, "attribute_key");

		// retrieve number of loci objects
		int lociCount = db.getTableLength(schema, "locus_key");

		// compute side of SOM - used for X & Y
//		int[] somSizes = SOMaticManager.getSOMDimension(lociCount, attributeCount, timeCount, SOM_NEURON_CAP);
		int[] somSizes = { 1, 1, 1, 19, 21, 46};

		String normalization = PERSPECTIVES[0];
		ArrayList<Integer> sizeList = new ArrayList<Integer>();

		// create SOM geometry, few iterations since the data values do not matter
		for (int i = 0; i < PERSPECTIVES.length; i++) {
//		int i = 4;
			if ((somSizes[i] * somSizes[i]) > SOM_THRESHOLD) {

				PostgreSQLManager.createSOMTable(db, SCHEMA, PERSPECTIVES[i], MAX_K, somSizes[i] * somSizes[i]);
				String newOutput = "./data/" + schema + "/SOMaticGeom/" + somSizes[i];
				if (!sizeList.contains(somSizes[i])) {
					String newInput = "data/" + schema + "/SOMaticIn/" + normalization + "_Normalized/"
							+ PERSPECTIVES[0] + ".dat";
					try {
						Files.createDirectories(Paths.get(newOutput));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					if (createGeom) {
						SOMaticManager.runSOM(newInput, newOutput, somSizes[i], somSizes[i], (float) 0.04, somSizes[i],
								50, 75, 100, DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR, 1);
					}

				}

				System.out.println("SOM: " + PERSPECTIVES[i]);
				PostgreSQLManager.insert2SOM(db, schema, PERSPECTIVES[i], newOutput + "/trainedSOM.geojson", MAX_K,
						BATCH, 50000);

			} else {
				System.out.println("MDS: " + PERSPECTIVES[i]);
				PostgreSQLManager.createMDSTable(db, SCHEMA, PERSPECTIVES[i], MAX_K);
			}
		}
	}
	
	public static void populateTSData(PostgreSQLJDBC db, String schema, int[] normalizations) {

		// retrieve number of time objects
		int timeCount = db.getTableLength(schema, "time_key");

		// retrieve number of attribute objects
		int attributeCount = db.getTableLength(schema, "attribute_key");

		// retrieve number of loci objects
		int lociCount = db.getTableLength(schema, "locus_key");

		// compute side of SOM - used for X & Y
		int[] somSizes = SOMaticManager.getSOMDimension(lociCount, attributeCount, timeCount, SOM_NEURON_CAP);

		// iterate thru each normalization
		for (int i = 0; i < normalizations.length; i++) {

			String normalization = PERSPECTIVES[normalizations[i]];

			// iterate thru each perspective
//			for (int k = 0; k < PERSPECTIVES.length; k++) {
			int k = 0;

				if (somSizes[k] * somSizes[k] > SOM_THRESHOLD) {
					String newInput = "data/" + schema + "/SOMaticIn/" + normalization + "_Normalized/" + PERSPECTIVES[k]
							+ ".dat";
					String newOutput = "./data/" + schema + "/SOMaticOut/" + normalization + "_Normalized/"
							+ PERSPECTIVES[k];
					try {
						Files.createDirectories(Paths.get(newOutput));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					SOMaticManager.extractSOMData(newInput, newOutput + "/trainedSom.cod", PERSPECTIVES[k], MAX_K, DIST_MEASURE, schema, db, i, true, 50000);
				} else {
					String mdsInput = "./data/" + schema + "/tables/" + normalization + "_Normalized/" + PERSPECTIVES[k]
							+ ".csv";
					File mdsFile = new File(mdsInput);

					String newOutput = "./data/" + schema + "/MDSOut/" + normalization + "_Normalized";
					try {
						Files.createDirectories(Paths.get(newOutput));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					newOutput = newOutput + "/" + PERSPECTIVES[k] + ".csv";

					MDSManager.createMDSFile(mdsFile, PERSPECTIVES[k], DIST_MEASURE, newOutput);
					// MDSManager.performMDS(mdsFile, PERSPECTIVES[k], normalization, DIST_MEASURE, db, schema, MAX_K,
							K_ITERATIONS, BATCH);
				}
//			}
		}
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
