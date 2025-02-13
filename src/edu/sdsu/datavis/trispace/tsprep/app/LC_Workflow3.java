package edu.sdsu.datavis.trispace.tsprep.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
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
import edu.sdsu.datavis.trispace.tsprep.som.BestMatchingUnit;
import edu.sdsu.datavis.trispace.tsprep.som.Cod;
import edu.sdsu.datavis.trispace.tsprep.som.Dat;
import edu.sdsu.datavis.trispace.tsprep.som.FileHandler;
import edu.sdsu.datavis.trispace.tsprep.som.InputVector;
import edu.sdsu.datavis.trispace.tsprep.som.Neuron;
import edu.sdsu.datavis.trispace.tsprep.xform.CSVManager;
import edu.sdsu.datavis.trispace.tsprep.xform.PostgreSQLManager;
import edu.sdsu.datavis.trispace.tsprep.stats.StatsPG;

import org.apache.log4j.BasicConfigurator;

import smile.projection.PCA;

public class LC_Workflow3 {
	final static boolean RESET 		= true;
//	final static boolean RESET 		= false;

	// Select the study area
	static int STUDY_AREA			= 0;

	// Scene name list
	final static String[] SCENES 	= { "SanElijo", "CocosFire", "Batiquitos" };
	final static String SCENE 		= SCENES[STUDY_AREA];

	// Schema list
	final static String[] SCHEMAS 	= { "sanelijo4", "cocos", "batiquitos" };
	final static String SCHEMA 		= SCHEMAS[STUDY_AREA];

	// specify coordinate system
	final static String EPSG 		= "4326";

	// specify noData values in Raster files
	final static int NO_DATA 		= -9999;
	final static int LC_NO_DATA 	= 0;

	// specify columns that represent unique ID
	final static int[] PRIMARY_KEYS = { 0, 1 };
	final static boolean HAS_LC 	= true;	

	// Tri-Space perspectives
	final static String[] PERSPECTIVES 		= { "L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L" };
//	final static int[] STUDY_PERSPECTIVES 	= { 0, 4 };
	final static int[] STUDY_PERSPECTIVES 	= { 0, 1, 2, 3, 4, 5 };
	
	// Tri-Space normalizations
	final static String[] NORMALIZATIONS 	= { "Unnormalized", "L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L" };
//	final static int[] STUDY_NORMALIZATIONS = { 1, 2, 3, 4, 5, 6 };
	final static int[] STUDY_NORMALIZATIONS = { 0, 1 };
//	final static int[] STUDY_NORMALIZATIONS = { 0, 1, 2, 3, 4, 5, 6 };
//	final static int[] MODEL_NORMALIZATIONS = { 1, 2, 3, 4, 5, 6 };
	final static int[] MODEL_NORMALIZATIONS = { 1 };
	
	// if these are set to null they will be automatically computed
	final static int[] SOM_PERSPECTIVES = null;
	final static int[] MDS_PERSPECTIVES = null;
//	final static int[] SOM_PERSPECTIVES = { 0, 3, 4 };	
//	final static int[] MDS_PERSPECTIVES = { 1, 2, 5 };
	// threshold of input vector count to automatically decide if SOM or MDS
	final static int SOM_THRESHOLD 	= 100;	
	
	final static boolean BATCH 		= true;

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
	final static String DATABASE 	= "schempp17";

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
//	final static int[] TRAINING_COUNT 		= { 1, 2, 5 };
	final static int[] TRAINING_COUNT 		= { 100, 100, 100 };
	final static String[] TRAINING_UNITS 	= { "Iterations", "Cycles" };
	final static int TRAINING_UNIT 			= 0;
	
	final static String[] SOM_OUTPUTs		= { "COD", "GeoJSON" };
	final static int SOM_OUTPUT				= 0;
	
	// additional SOM training parameters
	final static int NTHREADS 		= 1;
	final static boolean ROUNDING 	= true;
	final static int SCALING_FACTOR = 1;

	final static int MIN_K			= 2;
	final static int MAX_K 			= 4;
//	final static int K_ITERATIONS 	= 90000;
	final static int K_ITERATIONS 	= 100;
	
	final static int HISTOGRAM_BINS = 8;

	public static void main(String[] args) throws IOException {
		
//		reformatImagery(SCHEMA, HAS_LC, PRIMARY_KEYS);
//		
//		trispaceConversion(SCHEMA, MIN_NORMALIZATION, MAX_NORMALIZATION);
//		
//		trainSOMs(SOM_PERSPECTIVES, TRAINING_COUNT, TRAINING_UNIT);
//		
//		computeMDS(MDS_PERSPECTIVES);
//		
//		performKMeans(SOM_PERSPECTIVES, MDS_PERSPECTIVES, false);		
//		
		writeToDB(DATABASE, SCHEMA, RESET, EPSG, BATCH, 
				  SOM_PERSPECTIVES, MDS_PERSPECTIVES, PRIMARY_KEYS);
		

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
	
	private static void reformatImagery(String schema, boolean hasLC, int[] primaryKeys) {
		
		gdal2XYZ(schema, hasLC);									// convert rasters to tables
		cleanData(schema, hasLC);									// clean the data (remove noData loci)
		lociTest("./data/" + schema + "/tables/imagery",			// validate remaining loci
					primaryKeys);			
		convert2L_AT(schema, hasLC, primaryKeys);					// convert from folder/file structure to a single L_AT file
	}
	
	private static void trispaceConversion(String schema, float min, float max) {
		fromL_AT2All(schema);								// convert from L_AT to all other perspectives (T_LA, AT_L, etc)
		
		// normalize all TS perspectives using ROW normalization
		// THEN convert all normalized perspectives into all REMAINING perspectives
		normalizeAllPerspectives(schema, 0, min, max);

		// convert each file into a .dat file for SOMatic
		convert2DAT(schema);
	}
	
	private static void writeToDB(String dbName, String schema, boolean reset, String epsg, 
									boolean batch, int[] somPerspectives, int[] mdsPerspectives, int[] primaryKeys) {
		
		// store the list of MDS perspectives
		int[] perspectivesMDS = mdsPerspectives;
		
		if (mdsPerspectives == null) {
			perspectivesMDS = getMDS(SCHEMA, SOM_THRESHOLD);
		}
		
		// store the list of SOM perspectives
		int[] perspectivesSOM = somPerspectives;
		
		if (somPerspectives == null) {
			perspectivesSOM = getSOMs(SCHEMA, SOM_THRESHOLD);
		}
		
		credentialsTest();											// ensure credentials exist and are loaded				
		PostgreSQLJDBC db = new PostgreSQLJDBC(url, user, pw);		// create JDBC postgres connection				
		db.changeDatabase(dbName);									// change database from default

		// testing
		if (reset && db.schemaExists(schema)) {
			db.dropSchemaCascade(schema);
		}
		
		db.createSchema(schema);									// create schema is it does not exist				
		
		createTileLayerDictionary(db, schema);						// dictionary of leaflet tilelayers
		
		// this method should probably become the default, deprecate the other related methods
		PostgreSQLManager.createPixelPolyTableBatch2(db, schema, new File("./data/" + schema + "/input/csv"), epsg);
		
		String[] normalizationLabels = new String[STUDY_NORMALIZATIONS.length];
		
		for (int i = 0; i < STUDY_NORMALIZATIONS.length; i++) {
			normalizationLabels[i] = NORMALIZATIONS[STUDY_NORMALIZATIONS[i]];
		}
		
		// create and populate locus_key table		
		// creates the input vector tables
		System.out.println(PostgreSQLManager.createTables(db, schema, new File("./data/" + schema + "/tables/" + NORMALIZATIONS[0]), 
														new File("./data/" + schema + "/input/dictionaries"), 2, batch, 
														STUDY_PERSPECTIVES, perspectivesSOM, STUDY_NORMALIZATIONS, normalizationLabels));
		
		// create and populate locus_pt table
		System.out.println(PostgreSQLManager.extractPtGeomFromCSV(db, schema,
				new File("./data/" + schema + "/tables/imagery"), primaryKeys, "l", 1, epsg, batch));
		System.out.println("Created locus_pt Table");

		// create and populate locus_poly table
		PostgreSQLManager.createFinalPixelPolyTable(db, schema, batch);
		System.out.println("Created locus_poly Table");

		// insert the TS input vectors (l_at, lt_a, la_t, a_lt, t_la, at_l)
		insertInputVectors(db, schema, STUDY_PERSPECTIVES, perspectivesSOM);
		
		// kmeans and land cover classification
		classifyInputVectors(db, schema, perspectivesMDS);
		
		// create the geometry and data for the TS models
		// false if geometry already computed
		createModelTables(db, schema, perspectivesSOM, perspectivesMDS, false);
		
		classifyNeurons(db, schema, perspectivesSOM);
		
		db.disconnect();
	}

	private static void lociTest(String directory, int[] primaryKeys) {
		System.out.println("Verifying loci are stable...");
		if (!CSVManager.commonLociCheck(directory, primaryKeys)) {
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
	
	private static String[][] neuronClasses(int sceneIdx) {		
		
		String[][] classes;
		
		if (sceneIdx == 0 || sceneIdx == 2) {				
			
			classes = new String[6][];
		
			String[] l_at = {};
			String[] a_lt = {};
			String[] t_la = {};
			String[] la_t = {"a_channel","a_class"};
			String[] lt_a = {"_landcover", "t_year"};
			String[] at_l = {};
			
			classes[0] = l_at;
			classes[1] = a_lt;
			classes[2] = t_la;
			classes[3] = la_t;
			classes[4] = lt_a;
			classes[5] = at_l;
			
		} else if (sceneIdx == 1) {
			
			classes = new String[6][];
			
			String[] l_at = {};
			String[] a_lt = {};
			String[] t_la = {};
			String[] la_t = {"a_channel","a_class"};
			String[] lt_a = {"_landcover", "t_date", "t_year", "t_season"};
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
		
		String attributeDictionary 	= "./data/" + schema + "/input/dictionaries/attributes.txt";	// attribute dictionary for header definitions
		String lcDictionary 		= "./data/" + schema + "/input/dictionaries/landcover.txt";		// landcover dictionary for header definitions
		
		ImageManager.cleanData(f, NO_DATA, outDir, true, attributeDictionary);						// add headers and remove noData pixels

		if (hasLC) {
			
			File f_LC 		= new File("./data/" + schema + "/input/lc_csv");		// set input to folder containing .csv LC classifications tables		
			String outDirLC = "./data/" + schema + "/tables/landcover";				// new output directory for cleaned LC tables
			
			ImageManager.cleanData(f_LC, LC_NO_DATA, outDirLC, true, lcDictionary);	// add headers and remove noData pixels
		}
	}

	public static void convert2L_AT(String schema, boolean hasLC, int[] pKeys) {
		
		File input 			= new File("./data/" + schema + "/tables/imagery");			// imagery table directory		
		String outDir 		= "./data/" + schema + "/tables/" + NORMALIZATIONS[0];		// new output directory
		
		System.out.println("L_AT?");
		System.out.println(CSVManager.convertToL_AT("L_A_T", input, pKeys, outDir));	// convert a folder (T) containing many files (L_A) to L_AT

		if (hasLC) {
			
			File f_LC 	= new File("./data/" + schema + "/tables/landcover");						// LC table directory			
			input 		= new File("./data/" + schema + "/tables/dictionaries/lociDictionary.csv");	// loci dictionary
			
			System.out.println("L_AT?");
			try {
				System.out.println(CSVManager.convertLC2LT_A(f_LC, input, f_LC.getAbsolutePath()));	// convert a folder (T) containing many files (L_A) to L_AT
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	// function to convert from L_AT to all of the other 5 TS perspectives
	public static void fromL_AT2All(String schema) {
		
		String outDir 	= "./data/" + schema + "/tables/" + NORMALIZATIONS[0];	// new output directory		
		File input 		= new File(outDir + "/L_AT.csv");						// input L_AT file
		
		CSVManager.fromL_AT2All(input, outDir);						// convert CSV to all other perspectives
	}

	// function to perform the TS normalizations
	// normType can be { 0 - min/max, 1 - Unit Vector, 2 - TanH}
	// min & max parameters only used when normType == 0
	public static void normalizeAllPerspectives(String schema, int normType, float min, float max) {
		// new output directory
		String inDir = "./data/" + schema + "/tables/" + NORMALIZATIONS[0];

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
		
		for (int i = 0; i < STUDY_PERSPECTIVES.length; i++) {				// iterate thru all perspectives
			
			String perspective = PERSPECTIVES[STUDY_PERSPECTIVES[i]];
			
			for (int j = 0; j < STUDY_NORMALIZATIONS.length; j++) {			// iterate thru all normalizations
				
				String normalization = NORMALIZATIONS[STUDY_NORMALIZATIONS[j]];
				
				if (STUDY_NORMALIZATIONS[j] != 0) {
					normalization += "_Normalized";
				}

				// input
				File input = new File("./data/" + schema + "/tables/" + normalization + 
										"/" + perspective + ".csv");
				// output
				String output = "./data/" + schema + "/SOMaticIn/" + normalization + 
								"/" + perspective + ".dat";
				
				CSVManager.fromCSV2DatFile(perspective, input, output);	// convert to a .dat file
			}
		}
	}
	
	public static void kmeansIVs(PostgreSQLJDBC db, String schema, int[] perspectives) {
		// iterate thru perspectives
		for (int i = 0; i < perspectives.length; i++) {
			// get the perspective as a String
			String perspective = PERSPECTIVES[perspectives[i]];
			// iterate thru normalizations
			for (int j = 0; j < MODEL_NORMALIZATIONS.length; j++) {
				String normalizationPath = NORMALIZATIONS[MODEL_NORMALIZATIONS[j]];
				if (MODEL_NORMALIZATIONS[j] != 0) {
					normalizationPath += "_Normalized";
				}
				File kmeans = new File("./data/" + schema + "/kmeans/ivs/" + normalizationPath + "/" 
										+ perspective + "_kmeans.csv");
				String[] dataTypes = { "SMALLINT" };
				PostgreSQLManager.classifyIVs(db, schema, perspective, dataTypes, kmeans, null, true, MODEL_NORMALIZATIONS[j]);
			}
		}
	}
	
	public static void kmeansNeurons(PostgreSQLJDBC db, String schema, int[] perspectives) {
		// iterate thru perspectives
		for (int i = 0; i < perspectives.length; i++) {
			// get the perspective as a String
			String perspective = PERSPECTIVES[perspectives[i]];
			// iterate thru normalizations
			for (int j = 0; j < MODEL_NORMALIZATIONS.length; j++) {
				String normalizationPath = NORMALIZATIONS[MODEL_NORMALIZATIONS[j]];
				if (MODEL_NORMALIZATIONS[j] != 0) {
					normalizationPath += "_Normalized";
				}
				File kmeans = new File("./data/" + schema + "/kmeans/neurons/" + normalizationPath + "/" 
										+ perspective + "_kmeans.csv");
				String[] dataTypes = { "SMALLINT" };
				PostgreSQLManager.classifyNeurons(db, schema, perspective, dataTypes, kmeans, null, MODEL_NORMALIZATIONS[j]);
			}
		}
	}
	
	public static void classifyNeurons(PostgreSQLJDBC db, String schema, int[] perspectives) {

		// classify kmeans
		kmeansNeurons(db, schema, perspectives);
		
		String[][] classes = neuronClasses(STUDY_AREA);
		
		for (int i = 0; i < classes.length; i++) {
			for (int j = 0; j < classes[i].length; j++) {
				if (classes[i][j].startsWith("_")) {
					PostgreSQLManager.projectIVClass(db, schema, PERSPECTIVES[i].toLowerCase(), 
													classes[i][j].substring(1), DIST_MEASURE);
				} else {
					String[] elementClass = classes[i][j].split("_");
					PostgreSQLManager.projectTSClass(db, schema, PERSPECTIVES[i].toLowerCase(), 
													elementClass[0], elementClass[1], DIST_MEASURE);
				}
			}
		}
	}
	
	public static void classifyInputVectors(PostgreSQLJDBC db, String schema, int[] perspectives) {

		// classify kmeans
		kmeansIVs(db, schema, perspectives);

		// classify landcover
		File lt_a_LC = new File("./data/" + schema + "/tables/landcover/LT_A_LC.csv");
		File landcoverDictionary = new File("./data/" + schema + "/input/dictionaries/landcover.txt");			
		PostgreSQLManager.classifyIV(db, schema, "LT_A", "Landcover", "TEXT", lt_a_LC, landcoverDictionary, 
									true, 1, STUDY_NORMALIZATIONS);		
	}

	public static void insertInputVectors(PostgreSQLJDBC db, String schema, int[] perspectives, int[] somPerspectives) {
				
		// convert int[] to ArrayList<Integer>()
		ArrayList<Integer> perspectiveList  = new ArrayList<Integer>(Arrays.stream( perspectives ).boxed().collect( Collectors.toList() ));		
		
		String path = "./data/" + schema + "/tables";
		
		String[] inputFolders = new String[STUDY_NORMALIZATIONS.length];
		
		for (int i = 0; i < inputFolders.length; i++) {
			String normalization = NORMALIZATIONS[STUDY_NORMALIZATIONS[i]];
			
			if (STUDY_NORMALIZATIONS[i] != 0) {
				normalization += "_Normalized";
			}
			
			inputFolders[i] = path + "/" + normalization + "/";
		}		
		
		for (int i = 0; i < perspectives.length; i++) {
			boolean somPerspective = false;
			boolean writeData = false;
			for (int j = 0; j < somPerspectives.length; j++) {
				if (perspectives[i] == somPerspectives[j]) {
					somPerspective = true;
					writeData = true;
					break;
				}
			}
			
			String[] inputFiles = new String[inputFolders.length];
			for (int j = 0; j < inputFolders.length; j++) {
				inputFiles[j] = inputFolders[j] + PERSPECTIVES[perspectives[i]] + ".csv";
			}

			PostgreSQLManager.insertTSPerspectives(db, schema, PERSPECTIVES[perspectives[i]], STUDY_NORMALIZATIONS, 
													inputFiles, true, somPerspective, writeData);		
		}								
	}
	
	public static void statsColumns(PostgreSQLJDBC db, String schema, String perspective) {
		
		StatsPG.addDescriptStatsCols(db, schema, perspective);
		StatsPG.computeAllStatistics(db, schema, perspective, STUDY_NORMALIZATIONS);
		createHistogram(db, schema, perspective);
		
	}
	
	public static void createHistogram(PostgreSQLJDBC db, String schema, String perspective) {
		StatsPG.addHistogramCols(db, schema, perspective, HISTOGRAM_BINS);
		StatsPG.computeHistogram(db, schema, perspective, HISTOGRAM_BINS, null, null, STUDY_NORMALIZATIONS[0]);
		for (int i = 1; i < STUDY_NORMALIZATIONS.length; i++) {
			StatsPG.computeHistogram(db, schema, perspective, HISTOGRAM_BINS, MIN_NORMALIZATION, MAX_NORMALIZATION, STUDY_NORMALIZATIONS[i]);
		}
	}

	public static void createModelTables(PostgreSQLJDBC db, String schema, int[] somPerspectives, int[] mdsPerspectives, 
										boolean createGeom) {
		
		int timeCount 		= db.getTableLength(schema, "time_key");		// number of time objects		
		int attributeCount 	= db.getTableLength(schema, "attribute_key");	// number of attribute objects		
		int lociCount 		= db.getTableLength(schema, "locus_key");		// number of loci objects

		// compute side of SOM - used for X & Y
		int[] somSizes = SOMaticManager.getSOMDimension(lociCount, attributeCount, timeCount, SOM_NEURON_CAP);
		
		// create SOM geometry table, few iterations since the data values do not matter
		for (int i = 0; i < somPerspectives.length; i++) {

			PostgreSQLManager.createSOMTable(db, SCHEMA, PERSPECTIVES[somPerspectives[i]], 
											somSizes[somPerspectives[i]] * somSizes[somPerspectives[i]]);
			
			PostgreSQLManager.createNeuronTable(db, schema, PERSPECTIVES[somPerspectives[i]], 
											somSizes[somPerspectives[i]] * somSizes[somPerspectives[i]]);			
				
			// keep track of sizes encountered, to not create a redundant-sized SOM
			ArrayList<Integer> sizeList = new ArrayList<Integer>();
			
			String geometryOutput = "./data/" + schema + "/SOMaticGeom/" + somSizes[somPerspectives[i]];
			
			if (!sizeList.contains(somSizes[somPerspectives[i]])) {
				String inputDAT = "data/" + schema + "/SOMaticIn/" + NORMALIZATIONS[MODEL_NORMALIZATIONS[0]] 
									+ "_Normalized/" + PERSPECTIVES[somPerspectives[i]] + ".dat";
				try {
					Files.createDirectories(Paths.get(geometryOutput));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				if (createGeom) {
					SOMaticManager.runSOM(inputDAT, geometryOutput, somSizes[somPerspectives[i]], 
							somSizes[somPerspectives[i]], (float) 0.04, somSizes[somPerspectives[i]],
							50, 75, 100, DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR, 1);
				}
				
				sizeList.add(somSizes[somPerspectives[i]]);
			}
									
			PostgreSQLManager.insertSOM(db, schema, PERSPECTIVES[somPerspectives[i]], 
										geometryOutput + "/trainedSOM.geojson", BATCH, 50000);
			
			PostgreSQLManager.initializeNeurons(db, schema, PERSPECTIVES[somPerspectives[i]], MODEL_NORMALIZATIONS);
			
			for (int j = 0; j < MODEL_NORMALIZATIONS.length; j++) {
				
				String normalization = NORMALIZATIONS[MODEL_NORMALIZATIONS[j]];
				
				if (MODEL_NORMALIZATIONS[j] != 0) {
					normalization += "_Normalized";
				}
				
				String inputDAT = "data/" + schema + "/SOMaticIn/" + normalization
									+ "/" + PERSPECTIVES[somPerspectives[i]] + ".dat";
				
				String inputCOD = "./data/" + schema + "/SOMaticOut/" + normalization + "/" + 
								  PERSPECTIVES[somPerspectives[i]]+ "/trainedSom.cod";
				
				ArrayList<String> ivData = SOMaticManager.getIV2BMU(inputDAT, inputCOD, PERSPECTIVES[somPerspectives[i]], 
																	DIST_MEASURE);
				
				SOMaticManager.writeNeuronAttributes(db, schema, PERSPECTIVES[somPerspectives[i]], 
													 inputCOD, MODEL_NORMALIZATIONS[j]);
				
				SOMaticManager.writeIV2BMU(db, schema, PERSPECTIVES[somPerspectives[i]], ivData, MODEL_NORMALIZATIONS[j]);
				
				SOMaticManager.writeNeuronIVs(db, schema, PERSPECTIVES[somPerspectives[i]], inputCOD, inputDAT, DIST_MEASURE, 
											  MODEL_NORMALIZATIONS[j]);
				
//				SOMaticManager.extractSOMData(newInput, newOutput + "/trainedSom.cod", PERSPECTIVES[k], MAX_K, DIST_MEASURE, schema, db, i, true, 50000);
			}						
		}
		
		// going to compute the statistics and add a geometry column for MDS perspectives
		for (int i = 0; i < mdsPerspectives.length; i++) {
			
			// too many dimensions to store in table, need to characterize it somehow
			statsColumns(db, schema, PERSPECTIVES[mdsPerspectives[i]]);
			
			// for mds
			PostgreSQLManager.addGeometryColumn(db, schema, PERSPECTIVES[mdsPerspectives[i]], "geom", "POINT");
			
			for (int j = 0; j < MODEL_NORMALIZATIONS.length; j++) {
				
				String normalization = NORMALIZATIONS[MODEL_NORMALIZATIONS[j]];
				
				if (MODEL_NORMALIZATIONS[j] != 0) {
					normalization += "_Normalized";
				}
				
				String mdsInput = "./data/" + schema + "/MDS/" + normalization + "/" + PERSPECTIVES[mdsPerspectives[i]] + ".csv";
				File mdsFile = new File(mdsInput);
				
				PostgreSQLManager.writeMDS(db, schema, PERSPECTIVES[mdsPerspectives[i]], "geom", mdsFile, true, MODEL_NORMALIZATIONS[j]);

			}		
		}		
	}

	public static void trainSOMs(int[] somPerspectives, int[] trainingCount, int trainingUnit) throws IOException {
		
		if (trainingCount.length != 3) {
			System.out.println("Three training cycles or number of iterations must be provided. Use designated training cycle project.");
			System.exit(1);
		}
		
		// store the list of SOM perspectives
		int[] trainingPerspectives;
		
		if (somPerspectives != null) {
			trainingPerspectives = somPerspectives;
		} else {
			trainingPerspectives = getSOMs(SCHEMA, SOM_THRESHOLD);
		}
		
		int lociCount = CSVManager.rowCount("./data/" + SCHEMA + "/tables/dictionaries/lociDictionary.csv");		
		int attributeCount = CSVManager.rowCount("./data/" + SCHEMA + "/tables/dictionaries/attrDictionary.csv");
		int timeCount = CSVManager.rowCount("./data/" + SCHEMA + "/tables/dictionaries/timeDictionary.csv");

		int[] somSize = SOMaticManager.getSOMDimension(lociCount, attributeCount, timeCount, SOM_NEURON_CAP);
		// create the path if it does not already exist
		Files.createDirectories(Paths.get("./data/" + SCHEMA + "/SOMaticOut/"));

		// iterate thru all training perspectives
		for (int i = 0; i < trainingPerspectives.length; i++) {

			// get the training perspective
			String perspective = PERSPECTIVES[trainingPerspectives[i]];

			// list to hold the iterations for the 3 training phases
			ArrayList<Integer> somIterations = new ArrayList<Integer>();					
			
			if (trainingUnit == 1) {  // using training cycles
				
				for (int j = 0; j < trainingCount.length; j++) {  // iterate thru each training cycle
					
					// append the computed number of iterations (based off cycles)
					somIterations.add(SOMaticManager.getSOMIterations(trainingCount[j], lociCount, attributeCount,
							timeCount, trainingPerspectives[i]));
				}
				
			} else {  // using training iterations
				
				// use the input as-is without computation
				somIterations  = new ArrayList<Integer>(Arrays.stream( trainingCount ).boxed().collect( Collectors.toList() ));
			
			}			
			
			// iterate thru all normalizations
			for (int j = 0; j < MODEL_NORMALIZATIONS.length; j++) {
				
				// store the current normalization
				String normalization = NORMALIZATIONS[MODEL_NORMALIZATIONS[j]];
				if (MODEL_NORMALIZATIONS[j] != 0) {
					normalization += "_Normalized";
				}				
				
				Files.createDirectories(Paths.get("./data/" + SCHEMA + "/SOMaticOut/" + normalization + "/"	+ perspective + "/"));
									
				// variable to store the input location
				String input = "./data/" + SCHEMA + "/SOMaticIn/" + normalization + "/"	+ perspective + ".dat";

				// variable to store the output location
				String output = "data/" + SCHEMA + "/SOMaticOut/" + normalization + "/" + perspective;
				
				SOMaticManager.runSOM(input, output, somSize[trainingPerspectives[i]], somSize[trainingPerspectives[i]], 
										(float) 0.04, somSize[trainingPerspectives[i]],	somIterations.get(0), somIterations.get(1), 
										somIterations.get(2), DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR, SOM_OUTPUT);
				
			}
		}
	}
	
	public static void computeMDS(int[] mdsPerspectives) throws IOException {
		
		// store the list of MDS perspectives
		int[] perspectivesMDS;
		
		if (mdsPerspectives != null) {
			perspectivesMDS = mdsPerspectives;
		} else {
			perspectivesMDS = getMDS(SCHEMA, SOM_THRESHOLD);
		}		
		
		// create the path if it does not already exist
		Files.createDirectories(Paths.get("./data/" + SCHEMA + "/MDS/"));
		
		// iterate thru all MDS perspectives
		for (int i = 0; i < perspectivesMDS.length; i++) {

			// get the training perspective
			String perspective = PERSPECTIVES[perspectivesMDS[i]];		
			
			// iterate thru all normalizations
			for (int j = 0; j < MODEL_NORMALIZATIONS.length; j++) {
				
				// store the current normalization
				String normalization = NORMALIZATIONS[MODEL_NORMALIZATIONS[j]];
				if (MODEL_NORMALIZATIONS[j] != 0) {
					normalization += "_Normalized";
				}				
				
				Files.createDirectories(Paths.get("./data/" + SCHEMA + "/MDS/" + normalization + "/"));
									
				// variable to store the input location
				String input = "./data/" + SCHEMA + "/tables/" + normalization + "/" + perspective + ".csv";

				// variable to store the output location
				String output = "data/" + SCHEMA + "/MDS/" + normalization + "/" + perspective + ".csv";
				
				MDSManager.createMDSFile(new File(input), perspective, DIST_MEASURE, output);
			}
		}		
	}
	
	public static int[] getSOMs(String schema, int somThreshold) {
		
		ArrayList<Integer> somPerspectives = new ArrayList<Integer>();
		
		for (int i = 0; i < STUDY_PERSPECTIVES.length; i++) {
			if (getObjectCount(STUDY_PERSPECTIVES[i]) > somThreshold) {
				somPerspectives.add(STUDY_PERSPECTIVES[i]);
			}
		}
		
		int[] perspectivesSOM = somPerspectives.stream().mapToInt(i->i).toArray();
		
		return perspectivesSOM;

	}
	
	public static int[] getMDS(String schema, int somThreshold) {

		ArrayList<Integer> mdsPerspectives = new ArrayList<Integer>();
		
		for (int i = 0; i < STUDY_PERSPECTIVES.length; i++) {
			if (getObjectCount(STUDY_PERSPECTIVES[i]) <= somThreshold) {
				mdsPerspectives.add(STUDY_PERSPECTIVES[i]);
			}
		}
		
		int[] perspectivesMDS = mdsPerspectives.stream().mapToInt(i->i).toArray();
		
		return perspectivesMDS;
	}
	
	public static void performKMeans(int[] somPerspectives, int[] mdsPerspectives, boolean classifySOM_IVs) throws IOException {		
		
		// store the list of MDS perspectives
		int[] perspectivesMDS;
		
		if (mdsPerspectives != null) {
			perspectivesMDS = mdsPerspectives;
		} else {
			perspectivesMDS = getMDS(SCHEMA, SOM_THRESHOLD);
		}
		
		// store the list of SOM perspectives
		int[] perspectivesSOM;
		
		if (somPerspectives != null) {
			perspectivesSOM = somPerspectives;
		} else {
			perspectivesSOM = getSOMs(SCHEMA, SOM_THRESHOLD);
		}
		
		// list of perspectives to perform kmeans on their Input Vectors (IVs)
		ArrayList<Integer> list  = new ArrayList<Integer>(Arrays.stream( perspectivesMDS ).boxed().collect( Collectors.toList() ));
		if (classifySOM_IVs) {
			list.addAll(Arrays.stream( perspectivesSOM ).boxed().collect( Collectors.toList() ));
		}
		
		// create the path if it does not already exist
		Files.createDirectories(Paths.get("./data/" + SCHEMA + "/kmeans/"));
		
		if (perspectivesSOM.length > 0) {
			// create the path if it does not already exist
			Files.createDirectories(Paths.get("./data/" + SCHEMA + "/kmeans/neurons/"));
			
			// iterate thru SOMs and classify neurons
			for (int i = 0; i < perspectivesSOM.length; i++) {
				
				String perspective = PERSPECTIVES[perspectivesSOM[i]];
				
				int perspectiveMaxK = getObjectCount(perspectivesSOM[i]) - 1;
				if (MAX_K < perspectiveMaxK) {
					perspectiveMaxK = MAX_K;
				}
				
				for (int j = 0; j < MODEL_NORMALIZATIONS.length; j++) {
					
					String normalization = NORMALIZATIONS[MODEL_NORMALIZATIONS[j]];
					
					if (MODEL_NORMALIZATIONS[j] != 0) {
						normalization += "_Normalized";
					}
					
					Files.createDirectories(Paths.get("./data/" + SCHEMA + "/kmeans/neurons/" + normalization + "/"));
					
					String codInput = "./data/" + SCHEMA + "/SOMaticOut/" + normalization + "/" + perspective + "/trainedSom.cod";
					String outputFolder = "./data/" + SCHEMA + "/kmeans/neurons/" + normalization + "/" + perspective;
					CSVManager.performKMeansNew(perspective, codInput, outputFolder, MIN_K, perspectiveMaxK, K_ITERATIONS, DIST_MEASURE);
				}
			}
		}
		
		if (list.size() > 0) {
			// create the path if it does not already exist
			Files.createDirectories(Paths.get("./data/" + SCHEMA + "/kmeans/ivs/"));
						
			// iterate thru perspectives and classify input vectors
			for (int i = 0; i < list.size(); i++) {
				
				String perspective = PERSPECTIVES[list.get(i)];
				
				int perspectiveMaxK = getObjectCount(list.get(i)) - 1;
				if (MAX_K < perspectiveMaxK) {
					perspectiveMaxK = MAX_K;
				}
				
				for (int j = 0; j < STUDY_NORMALIZATIONS.length; j++) {
					
					String normalization = NORMALIZATIONS[STUDY_NORMALIZATIONS[j]];
					
					if (STUDY_NORMALIZATIONS[j] != 0) {
						normalization += "_Normalized";
					}
					
					Files.createDirectories(Paths.get("./data/" + SCHEMA + "/kmeans/ivs/" + normalization + "/"));
					
					String datFilePath = "data/" + SCHEMA + "/SOMaticIn/" + normalization + "/" + perspective + ".dat";			
					// read the input vectors
					Dat dat = FileHandler.readDatFile(datFilePath, perspective);

					// convert to Neuron class to utilize KMeans.java
					Neuron[] inputVectors = new Neuron[dat.inputVectors.length];

					int tmpCounter = 0;
					for (InputVector inputVector : dat.inputVectors) {
						inputVectors[tmpCounter] = new Neuron(tmpCounter, inputVector.getAttributes());
						tmpCounter++;
					}
					
					File labelFile = new File("data/" + SCHEMA + "/tables/" + normalization + "/" + perspective + ".csv");
					String[] labels = MDSManager.extractLabels(labelFile, perspective);

					String outputFolder = "./data/" + SCHEMA + "/kmeans/ivs/" + normalization + "/" + perspective;
					CSVManager.performKMeansNew2(perspective, inputVectors, labels, outputFolder, MIN_K, perspectiveMaxK, K_ITERATIONS, DIST_MEASURE);
				}
			}
		}		
	}
	
	public static int getObjectCount(int perspective) {
		
		int lociCount = CSVManager.rowCount("./data/" + SCHEMA + "/tables/dictionaries/lociDictionary.csv");		
		int attributeCount = CSVManager.rowCount("./data/" + SCHEMA + "/tables/dictionaries/attrDictionary.csv");
		int timeCount = CSVManager.rowCount("./data/" + SCHEMA + "/tables/dictionaries/timeDictionary.csv");
		
		if (perspective == 0) {
			return lociCount;
		} else if (perspective == 1) {
			return attributeCount;
		} else if (perspective == 2) {
			return timeCount;
		} else if (perspective == 3) {
			return (lociCount * attributeCount);
		} else if (perspective == 4) {
			return (lociCount * timeCount);
		} else if (perspective == 5) {
			return (attributeCount * timeCount);
		} else {
			return 0;
		}
	}

}
