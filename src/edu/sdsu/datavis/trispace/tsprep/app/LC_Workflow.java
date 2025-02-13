package edu.sdsu.datavis.trispace.tsprep.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

import edu.sdsu.datavis.trispace.tsprep.db.PostgreSQLJDBC;
import edu.sdsu.datavis.trispace.tsprep.dr.MDSManager;
import edu.sdsu.datavis.trispace.tsprep.dr.SOMaticManager;
import edu.sdsu.datavis.trispace.tsprep.io.img.ImageManager;
import edu.sdsu.datavis.trispace.tsprep.som.Cod;
import edu.sdsu.datavis.trispace.tsprep.som.Dat;
import edu.sdsu.datavis.trispace.tsprep.som.FileHandler;
import edu.sdsu.datavis.trispace.tsprep.xform.CSVManager;
import edu.sdsu.datavis.trispace.tsprep.xform.PostgreSQLManager;
import edu.sdsu.datavis.trispace.tsprep.stats.StatsPG;

import org.apache.log4j.BasicConfigurator;

import smile.projection.PCA;

public class LC_Workflow {
	final static boolean RESET 		= true;
//	final static boolean RESET 		= false;

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
	final static String[] PERSPECTIVES 		= { "L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L" };
	final static int[] STUDY_PERSPECTIVES 	= { 0, 1, 2, 3, 4, 5 };
//	final static int[] STUDY_PERSPECTIVES 	= { 0, 4 };
	
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
	final static String database 			= "schempp17";

	// normalization parameters
	// specify range of normalization (e.g. 0-1; .1-.9, etc.)
	final static float MIN_NORMALIZATION = 0.1f;
	final static float MAX_NORMALIZATION = 0.9f;

	// implemented distance measures
	final static int EUCLIDEAN 		= 1;
	final static int COSINE 		= 2;
	final static int MANHATTAN 		= 3;

	// specify distance measure
	final static int DIST_MEASURE 	= COSINE;

	// SOM training parameters
	final static int NITERATIONS1 	= 10000;
	final static int NITERATIONS2 	= 20000;
	final static int NITERATIONS3 	= 50000;
	final static int NTHREADS 		= 1;
	final static boolean ROUNDING 	= true;
	final static int SCALING_FACTOR = 1;

	final static int MIN_K			= 2;
	final static int MAX_K 			= 12;
	final static int K_ITERATIONS 	= 1000;
	
	final static int HISTOGRAM_BINS = 8;

	public static void main(String[] args) throws IOException {
		
		credentialsTest();											// ensure credentials exist and are loaded		
		PostgreSQLJDBC db = new PostgreSQLJDBC(url, user, pw);		// create JDBC postgres connection		
		System.out.println(db.changeDatabase(database));			// change database from default

		// testing
		if (RESET && db.schemaExists(SCHEMA)) {
			db.dropSchemaCascade(SCHEMA);
		}
		
		db.createSchema(SCHEMA);									// create schema is it does not exist
		
		createTileLayerDictionary(db, SCHEMA);						// dictionary of leaflet tilelayers
		
//		gdal2XYZ(SCHEMA, HAS_LC);									// convert rasters to tables

		PostgreSQLManager.createPixelPolyTableBatch2(db, SCHEMA, new File("./data/" + SCHEMA + "/input/csv"), EPSG);
//		System.out.println("Pixel Fishnet Table Created!");
		
//		cleanData(SCHEMA, HAS_LC);									// clean the data (remove noData loci)
		
//		lociTest("./data/" + SCHEMA + "/tables/imagery");			// validate remaining loci
		
//		convert2L_AT(SCHEMA, HAS_LC, PRIMARY_KEYS);					// convert from folder/file structure to a single L_AT file
		
//		fromL_AT2All(SCHEMA, SCENE);								// convert from L_AT to all other perspectives (T_LA, AT_L, etc)

		// normalize all TS perspectives using ROW normalization
		// THEN convert all normalized perspectives into all REMAINING perspectives
//		normalizeAllPerspectives(SCHEMA, SCENE, 0, MIN_NORMALIZATION, MAX_NORMALIZATION);

		// convert each file into a .dat file for SOMatic
//		convert2DAT(SCHEMA, SCENE);

		// create and populate locus_key table		
		System.out.println(PostgreSQLManager.createTables(db, SCHEMA, new File("./data/" + SCHEMA + "/tables/Non_Normalized"), 
														new File("./data/" + SCHEMA + "/input/dictionaries"), 2, BATCH, 
														SOM_THRESHOLD, STUDY_PERSPECTIVES));

		// create and populate locus_pt table
		System.out.println(PostgreSQLManager.extractPtGeomFromCSV(db, SCHEMA,
				new File("./data/" + SCHEMA + "/tables/imagery"), PRIMARY_KEYS, "l", 1, EPSG, BATCH));
		System.out.println("Created locus_pt Table");

//		// create and populate locus_poly table
		PostgreSQLManager.createFinalPixelPolyTable(db, SCHEMA, BATCH);
		System.out.println("Created locus_poly Table");
//
//		// insert the TS elements (l_at, lt_a, la_t, a_lt, t_la, at_l)
		insertTSElements(db, SCHEMA, SCENE, STUDY_PERSPECTIVES);
		
//
//		// create SSE table
//		PostgreSQLManager.createSSETable(db, SCHEMA, MAX_K);
////		System.out.println("Created SSE Table");
//
//		// populate SSE table with dummy values
//		PostgreSQLManager.insert2SSETable(db, SCHEMA, MAX_K, BATCH);
////		System.out.println("Populated SSE Table");
//
//		// create the geometry for the TS perspectives
//		// false since geometry already computed
		createTSGeometry(db, SCHEMA, SCENE, false);
		
		classify(db, SCHEMA);
		
//		// populate the SOM & MDS attributes in PGSQL
//		populateTSData(db,SCHEMA,SCENE);

//
//		// String datPath = "data/" + SCHEMA + "/SOMaticIn/L_AT_Normalized/L_AT_" +
//		// SCENE + ".dat";
//		// String codPath = "data/" + SCHEMA +
//		// "/SOMaticOut/L_AT_Normalized/L_AT/trainedSom.cod";
//		// String path1 = "./data/" + SCHEMA +
//		// "/SOMaticOut/L_AT_Normalized/L_AT/iv2BMU.csv";
//		// String path2 = "./data/" + SCHEMA +
//		// "/SOMaticOut/L_AT_Normalized/L_AT/neuron2IV.csv";
//		// String path3 = "./data/" + SCHEMA +
//		// "/SOMaticOut/L_AT_Normalized/L_AT/kmeans.csv";
//
//		// SOMaticManager.extractBMU2CSV(datPath, codPath, path1, path2, DIST_MEASURE);
//
//		// SOMaticManager.extractKmeans2CSV(datPath, codPath, "L_AT", 2, 3, path3,
//		// DIST_MEASURE);
		
		db.disconnect();

		System.out.println("Exiting Program");
		System.exit(0);
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

	public static boolean createTileLayerDictionary(PostgreSQLJDBC db, String schema) {
		try {
			String tileLayerPath 	= "./data/" + schema + "/input/dictionaries/tilelayers.txt";
			String[] dataTypes 		= { "TEXT", "TEXT" };
			System.out.println("Created TileLayers Table in DB!");
			return PostgreSQLManager.createTable(db, schema, "tilelayers", tileLayerPath, dataTypes);
		} catch (SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	private static String[][] createClasses(int sceneIdx) {
		
		String[][] classes;
		
		if (sceneIdx == 0 || sceneIdx == 2) {				
			
			classes = new String[6][];
		
			String[] l_at = {};
			String[] a_lt = {"k2","k3","k4","k5","ms_class"};
			String[] t_la = {"k2"};
			String[] la_t = {"ms_class"};
			String[] lt_a = {"landcover"};
			String[] at_l = {"k2","k3","k4","k5","k6","k7","k8","k9","k10","k11","k12","ms_class"};
			
			classes[0] = l_at;
			classes[1] = a_lt;
			classes[2] = t_la;
			classes[3] = la_t;
			classes[4] = lt_a;
			classes[5] = at_l;
			
		} else if (sceneIdx == 1) {
			
			classes = new String[6][];
			
			String[] l_at = {};
			String[] a_lt = {"k2","k3","k4","k5","k6","ms_class"};
			String[] t_la = {"k2","k3","k4","k5","k6","k7","k8","k9","k10","quarter","season","year"};
			String[] la_t = {"ms_class"};
			String[] lt_a = {"landcover","quarter","season","year"};
			String[] at_l = {"k2","k3","k4","k5","k6","k7","k8","k9","k10","k11","k12","ms_class","quarter","season","year"};
			
			classes[0] = l_at;
			classes[1] = a_lt;
			classes[2] = t_la;
			classes[3] = la_t;
			classes[4] = lt_a;
			classes[5] = at_l;			
			
		} else {
			classes = null;
		}
		
		return classes;		
	}
	
	private static String[][] createNeuronClasses(int sceneIdx) {		
		
		String[][] classes;
		
		if (sceneIdx == 0 || sceneIdx == 2) {				
			
			classes = new String[6][];
		
			String[] l_at = {"k2","k3","k4","k5","k6","k7","k8","k9","k10","k11","k12"};
			String[] a_lt = {};
			String[] t_la = {};
			String[] la_t = {"k2","k3","k4","k5","k6","k7","k8","k9","k10","k11","k12","ms_channel","ms_class",};
			String[] lt_a = {"k2","k3","k4","k5","k6","k7","k8","k9","k10","k11","k12","landcover","year"};
			String[] at_l = {};
			
			classes[0] = l_at;
			classes[1] = a_lt;
			classes[2] = t_la;
			classes[3] = la_t;
			classes[4] = lt_a;
			classes[5] = at_l;
			
		} else if (sceneIdx == 1) {
			
			classes = new String[6][];
			
			String[] l_at = {"k2","k3","k4","k5","k6","k7","k8","k9","k10","k11","k12"};
			String[] a_lt = {};
			String[] t_la = {};
			String[] la_t = {"k2","k3","k4","k5","k6","k7","k8","k9","k10","k11","k12","ms_channel","ms_class",};
			String[] lt_a = {"k2","k3","k4","k5","k6","k7","k8","k9","k10","k11","k12","landcover","quarter","season","year"};
			String[] at_l = {};
			
			classes[0] = l_at;
			classes[1] = a_lt;
			classes[2] = t_la;
			classes[3] = la_t;
			classes[4] = lt_a;
			classes[5] = at_l;		
			
		} else {
			
			classes = null;
			
		}					
		
		return classes;		
	}

	public static void gdal2XYZ(String schema, boolean hasLC) {
		
		File f 			= new File("./data/" + schema + "/input/imagery");		// set input to folder containing .tif images		
		String outDir 	= "./data/" + schema + "/input/csv";					// output directory for images in tabular form
		
		ImageManager.performGDAL2XYZ(f, outDir);								// perform gdal2xyz on all files contained in directory 'f'

		if (hasLC) {																// if also extracting a LC raster
			
			File f_LC 		= new File("./data/" + schema + "/input/landcover");	// set input to folder containing .tif LC classifications			
			String outDirLC = "./data/" + schema + "/input/lc_csv";					// output directory for LC in tabular form
			
			ImageManager.performGDAL2XYZ(f_LC, outDirLC);							// perform gdal2xyz on all files contained in directory 'f_LC'
		}

		System.out.println("GDAL2XYZ COMPLETE!");
	}

	public static void cleanData(String schema, boolean hasLC) {
		
		File f 			= new File("./data/" + schema + "/input/csv");								// set input to folder containing .csv image tables		
		String outDir 	= "./data/" + schema + "/tables/imagery";									// new output directory for cleaned imagery tables
		
		String attributeDictionary 	= "./data/" + SCHEMA + "/input/dictionaries/attributes.txt";	// attribute dictionary for header definitions
		String lcDictionary 		= "./data/" + SCHEMA + "/input/dictionaries/landcover.txt";		// landcover dictionary for header definitions
		
		ImageManager.cleanData(f, NO_DATA, outDir, true, attributeDictionary);						// add headers and remove noData pixels

		if (hasLC) {
			
			File f_LC 		= new File("./data/" + schema + "/input/lc_csv");		// set input to folder containing .csv LC classifications tables		
			String outDirLC = "./data/" + SCHEMA + "/tables/landcover";				// new output directory for cleaned LC tables
			
			ImageManager.cleanData(f_LC, LC_NO_DATA, outDirLC, true, lcDictionary);	// add headers and remove noData pixels
		}
	}

	public static void convert2L_AT(String schema, boolean hasLC, int[] pKeys) {
		
		File f 			= new File("./data/" + schema + "/tables/imagery");			// imagery table directory		
		String outDir 	= "./data/" + SCHEMA + "/tables/Non_normalized";			// new output directory
		
		System.out.println("L_AT?");
		System.out.println(CSVManager.convertToL_AT("L_A_T", f, pKeys, outDir));	// convert a folder (T) containing many files (L_A) to L_AT

		if (hasLC) {
			
			File f_LC 	= new File("./data/" + SCHEMA + "/tables/landcover");					// LC table directory			
			f 			= new File(outDir + "/dictionaries/lociDictionary.csv");				// loci dictionary
			
			System.out.println("L_AT?");
			try {
				System.out.println(CSVManager.convertLC2LT_A(f_LC, f, f_LC.getAbsolutePath()));	// convert a folder (T) containing many files (L_A) to L_AT
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	// function to convert from L_AT to all of the other 5 TS perspectives
	public static void fromL_AT2All(String schema, String scene) {
		
		String outDir 	= "./data/" + schema + "/tables/Non_normalized";	// new output directory		
		File f 			= new File(outDir + "/L_AT_" + scene + ".csv");		// input L_AT file
		
		CSVManager.fromL_AT2All(f, outDir, scene);							// convert CSV to all other perspectives
	}

	// function to perform the TS normalizations
	// normType can be { 0 - min/max, 1 - Unit Vector, 2 - TanH}
	// min & max parameters only used when normType == 0
	public static void normalizeAllPerspectives(String schema, String scene, int normType, float min, float max) {
		// new output directory
		String inDir = "./data/" + schema + "/tables/Non_normalized";

		// iterate thru each TS perspective and perform ROW normalization
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			String outDir = "";
			String name = PERSPECTIVES[i] + "_" + scene + ".csv";
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
			File f = new File(inDir + "/" + PERSPECTIVES[i] + "_" + scene + ".csv");
			if (i == 0) {
				CSVManager.fromL_AT2All(f, inDir, scene);
			} else if (i == 1) {
				CSVManager.fromA_LT2All(f, inDir, scene);
			} else if (i == 2) {
				CSVManager.fromT_LA2All(f, inDir, scene);
			} else if (i == 3) {
				CSVManager.fromLA_T2All(f, inDir, scene);
			} else if (i == 4) {
				CSVManager.fromLT_A2All(f, inDir, scene);
			} else if (i == 5) {
				CSVManager.fromAT_L2All(f, inDir, scene);
			}
		}
	}

	public static void convert2DAT(String schema, String scene) {
		
		for (int i = 0; i < PERSPECTIVES.length; i++) {				// iterate thru all perspectives
			
			for (int j = 0; j < PERSPECTIVES.length; j++) {			// iterate thru all normalizations

				// input
				File input = new File("./data/" + schema + "/tables/" + PERSPECTIVES[i] + "_Normalized/"
						+ PERSPECTIVES[j] + "_" + scene + ".csv");
				// output
				String output = "./data/" + schema + "/SOMaticIn/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[j]
						+ "_" + scene + ".dat";
				
				CSVManager.fromCSV2DatFile(PERSPECTIVES[j], input, output);	// convert to a .dat file
			}
		}
	}
	
	public static void classify(PostgreSQLJDBC db, String schema) {
		File lt_a_LC = new File("./data/" + schema + "/tables/landcover/LT_A_LC.csv");
		File landcoverDictionary = new File("./data/" + schema + "/input/dictionaries/landcover.txt");		
		int[] normalizations = {0,1,2,3,4,5,6};		
		PostgreSQLManager.classifyIV(db, schema, "LT_A", "Landcover", "TEXT", lt_a_LC, landcoverDictionary, true, 1, normalizations);		
	}

	public static void insertTSElements(PostgreSQLJDBC db, String schema, String scene, int[] perspectives) {
				
		// convert int[] to ArrayList<Integer>()
		ArrayList<Integer> perspectiveList  = new ArrayList<Integer>(Arrays.stream( perspectives ).boxed().collect( Collectors.toList() ));
		
		if (perspectiveList.contains(0)) {
			PostgreSQLManager.insertTSPerspectives(db, schema, "L_AT", "./data/" + schema + "/tables", scene, true, true, true);
		}
		
		if (perspectiveList.contains(1)) {
			PostgreSQLManager.insertTSPerspectives(db, schema, "A_LT", "./data/" + schema + "/tables", scene, true, false, false);
		}
		
		if (perspectiveList.contains(2)) {
			PostgreSQLManager.insertTSPerspectives(db, schema, "T_LA", "./data/" + schema + "/tables", scene, true, false, false);
		}
				
		if (perspectiveList.contains(3)) {
			PostgreSQLManager.insertTSPerspectives(db, schema, "LA_T", "./data/" + schema + "/tables", scene, true, true, true);
		}
		
		if (perspectiveList.contains(4)) {
			PostgreSQLManager.insertTSPerspectives(db, schema, "LT_A", "./data/" + schema + "/tables", scene, true, true, true);
		}
		
		if (perspectiveList.contains(5)) {
			PostgreSQLManager.insertTSPerspectives(db, schema, "AT_L", "./data/" + schema + "/tables", scene, true, false, false);
		}					
				
	}
	
	public static void statsColumns(PostgreSQLJDBC db, String schema, String ts) {
		
		StatsPG.addDescriptStatsCols(db, schema, ts);
		StatsPG.computeAllStatistics(db, schema, ts);
		createHistogram(db, schema, ts);
		
	}
	
	public static void createHistogram(PostgreSQLJDBC db, String schema, String ts) {
		StatsPG.addHistogramCols(db, schema, ts, HISTOGRAM_BINS);
		StatsPG.computeHistogram(db, schema, ts, HISTOGRAM_BINS, null, null, 0);
		for (int i = 1; i <= PERSPECTIVES.length; i++) {
			StatsPG.computeHistogram(db, schema, ts, HISTOGRAM_BINS, MIN_NORMALIZATION, MAX_NORMALIZATION, i);
		}
	}

	public static void createTSGeometry(PostgreSQLJDBC db, String schema, String scene, boolean createGeom) {
		
		int timeCount 		= db.getTableLength(schema, "time_key");		// number of time objects		
		int attributeCount 	= db.getTableLength(schema, "attribute_key");	// number of attribute objects		
		int lociCount 		= db.getTableLength(schema, "locus_key");		// number of loci objects

		// compute side of SOM - used for X & Y
		int[] somSizes = SOMaticManager.getSOMDimension(lociCount, attributeCount, timeCount, SOM_NEURON_CAP);
		int[] objSizes = SOMaticManager.getNumObjects(lociCount, attributeCount, timeCount);

		String normalization = PERSPECTIVES[0];								// initialize normalization to l_at
		ArrayList<Integer> sizeList = new ArrayList<Integer>();				// keep track of sizes encountered, to not create a redundant-sized SOM
//		String[][] classes = createNeuronClasses(STUDY_AREA);				// get classes for Neurons

		// create SOM geometry, few iterations since the data values do not matter
		for (int i = 0; i < PERSPECTIVES.length; i++) {
//		int i = 4;
			if (objSizes[i] > SOM_THRESHOLD) {

				PostgreSQLManager.createSOMTable(db, SCHEMA, PERSPECTIVES[i], somSizes[i] * somSizes[i]);
				PostgreSQLManager.createNeuronTable(db, schema, PERSPECTIVES[i], somSizes[i] * somSizes[i]);
				String newOutput = "./data/" + schema + "/SOMaticGeom/" + somSizes[i];
				if (!sizeList.contains(somSizes[i])) {
					String newInput = "data/" + schema + "/SOMaticIn/" + normalization + "_Normalized/"
							+ PERSPECTIVES[0] + "_" + SCENE + ".dat";
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
					
					sizeList.add(somSizes[i]);
				}
				
				PostgreSQLManager.insertSOM(db, schema, PERSPECTIVES[i], newOutput + "/trainedSOM.geojson", BATCH, 50000);
				PostgreSQLManager.insertNeurons(db, schema, PERSPECTIVES[i]);

			} else {				
				
				statsColumns(db, schema, PERSPECTIVES[i]);
				PostgreSQLManager.addGeometryColumn(db, schema, PERSPECTIVES[i], "POINT");
				
			}
		}
	}

	public static void populateTSData(PostgreSQLJDBC db, String schema, String scene) {
		
		int timeCount 		= db.getTableLength(schema, "time_key");		// number of time objects		
		int attributeCount 	= db.getTableLength(schema, "attribute_key");	// number of attribute objects		
		int lociCount 		= db.getTableLength(schema, "locus_key");		// number of loci objects

		// compute side of SOM - used for X & Y
		int[] somSizes = SOMaticManager.getSOMDimension(lociCount, attributeCount, timeCount, SOM_NEURON_CAP);

		// iterate thru each normalization
		for (int i = 0; i < PERSPECTIVES.length; i++) {

			String normalization = PERSPECTIVES[i];

			// iterate thru each perspective
			for (int k = 0; k < PERSPECTIVES.length; k++) {
//							int k = 4;

				if (somSizes[k] * somSizes[k] > SOM_THRESHOLD) {
					String newInput = "data/" + schema + "/SOMaticIn/" + normalization + "_Normalized/" + PERSPECTIVES[k]
							+ "_" + scene + ".dat";
					String newOutput = "./data/" + schema + "/SOMaticOut/" + normalization + "_Normalized/"
							+ PERSPECTIVES[k];
					try {
						Files.createDirectories(Paths.get(newOutput));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}					

					ArrayList<String> ivData = SOMaticManager.getIV2BMU(newInput, newOutput + "/trainedSom.cod", PERSPECTIVES[k], DIST_MEASURE);
					
					SOMaticManager.writeNeuronAttributes(db, schema, PERSPECTIVES[k], newOutput + "/trainedSom.cod", (i + 1));
					SOMaticManager.writeIV2BMU(db, schema, PERSPECTIVES[k], ivData, i);
					SOMaticManager.writeNeuronIVs(db, schema, PERSPECTIVES[k], newOutput + "/trainedSom.cod", newInput, DIST_MEASURE, i);
					// 	public static void writeNeuronIVs(PostgreSQLJDBC db, String schema, String ts, String codFilePath, String datFilePath, int distMeasure, int normalization) {
//					SOMaticManager.extractSOMData(newInput, newOutput + "/trainedSom.cod", PERSPECTIVES[k], MAX_K, DIST_MEASURE, schema, db, i, true, 50000);
				} else {
					String mdsInput = "./data/" + schema + "/tables/" + normalization + "_Normalized/" + PERSPECTIVES[k]
							+ "_" + scene + ".csv";
					File mdsFile = new File(mdsInput);

					String newOutput = "./data/" + schema + "/MDSOut/" + normalization + "_Normalized";
					try {
						Files.createDirectories(Paths.get(newOutput));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					newOutput = newOutput + "/" + PERSPECTIVES[k] + ".csv";

					// MDSManager.createMDSFile(mdsFile, PERSPECTIVES[k], DIST_MEASURE, newOutput);
//					MDSManager.performMDS(mdsFile, PERSPECTIVES[k], PERSPECTIVES[i], DIST_MEASURE, db, schema, MAX_K,
//							K_ITERATIONS, BATCH);
				}
			}
		}
	}
	
//	public static void performKMeans(PostgreSQLJDBC db, String schema, String scene) throws IOException {
//
//
//		int timeCount 		= db.getTableLength(schema, "time_key");		// number of time objects		
//		int attributeCount 	= db.getTableLength(schema, "attribute_key");	// number of attribute objects		
//		int lociCount 		= db.getTableLength(schema, "locus_key");		// number of loci objects
//		
//		// compute side of SOM - used for X & Y
//		int[] somSizes = SOMaticManager.getSOMDimension(lociCount, attributeCount, timeCount, SOM_NEURON_CAP);
//
//		// iterate thru each normalization
//		for (int i = 0; i < PERSPECTIVES.length; i++) {
//
//			String normalization = PERSPECTIVES[i];
//
//			// iterate thru each perspective
//			for (int k = 0; k < PERSPECTIVES.length; k++) {
////									int k = 4;
//
//				if (somSizes[k] * somSizes[k] > SOM_THRESHOLD) {
//					String newInput = "data/" + schema + "/SOMaticIn/" + normalization + "_Normalized/" + PERSPECTIVES[k]
//							+ "_" + scene + ".dat";
//					String newOutput = "./data/" + schema + "/SOMaticOut/" + normalization + "_Normalized/"
//							+ PERSPECTIVES[k];
//					try {
//						Files.createDirectories(Paths.get(newOutput));
//					} catch (IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}					
//
//					ArrayList<String> ivData = SOMaticManager.getIV2BMU(newInput, newOutput + "/trainedSom.cod", PERSPECTIVES[k], DIST_MEASURE);
//					
//					SOMaticManager.writeNeuronAttributes(db, schema, PERSPECTIVES[k], newOutput + "/trainedSom.cod", (i + 1));
//					SOMaticManager.writeIV2BMU(db, schema, PERSPECTIVES[k], ivData, i);
//					SOMaticManager.writeNeuronIVs(db, schema, PERSPECTIVES[k], newOutput + "/trainedSom.cod", newInput, DIST_MEASURE, i);
//					// 	public static void writeNeuronIVs(PostgreSQLJDBC db, String schema, String ts, String codFilePath, String datFilePath, int distMeasure, int normalization) {
////							SOMaticManager.extractSOMData(newInput, newOutput + "/trainedSom.cod", PERSPECTIVES[k], MAX_K, DIST_MEASURE, schema, db, i, true, 50000);
//				} else {
//					String mdsInput = "./data/" + schema + "/tables/" + normalization + "_Normalized/" + PERSPECTIVES[k]
//							+ "_" + scene + ".csv";
//					File mdsFile = new File(mdsInput);
//
//					String newOutput = "./data/" + schema + "/MDSOut/" + normalization + "_Normalized";
//					try {
//						Files.createDirectories(Paths.get(newOutput));
//					} catch (IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//
//					newOutput = newOutput + "/" + PERSPECTIVES[k] + ".csv";
//
//					// MDSManager.createMDSFile(mdsFile, PERSPECTIVES[k], DIST_MEASURE, newOutput);
////							MDSManager.performMDS(mdsFile, PERSPECTIVES[k], PERSPECTIVES[i], DIST_MEASURE, db, schema, MAX_K,
////									K_ITERATIONS, BATCH);
//				}
//			}
//		}
//		
//		String normalization = PERSPECTIVES[norm];
//		String perspective = PERSPECTIVES[norm];
//		
//		String datInput = "data/" + SCHEMA + "/SOMaticIn/" + normalization + "_Normalized/" + perspective + "_" + SCENE + ".dat";
//		String codInput = "./data/" + SCHEMA + "/SOMaticOut/" + normalization + "_Normalized/" + perspective + "/trainedSom.cod";
//		String outputFolder = "./data/" + SCHEMA + "/kmeans/" + normalization + "_Normalized";
//		
//		try {
//			Files.createDirectories(Paths.get(outputFolder));
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		String output = outputFolder + "/" + perspective + "_" + normalization + "_Normalized";
////		CSVManager.performKMeans(testDAT, testCOD, "L_AT", 3, DIST_MEASURE, 0, "./data/cocos/kmeans/L_AT");
//		CSVManager.performKMeans(datInput, codInput, perspective, MIN_K, MAX_K, DIST_MEASURE, norm, output);
//			
//	}

}
