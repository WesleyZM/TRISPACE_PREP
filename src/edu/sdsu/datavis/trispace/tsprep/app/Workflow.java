package edu.sdsu.datavis.trispace.tsprep.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;

import edu.sdsu.datavis.trispace.tsprep.db.PostgreSQLJDBC;
import edu.sdsu.datavis.trispace.tsprep.dr.MDSManager;
import edu.sdsu.datavis.trispace.tsprep.dr.SOMaticManager;
import edu.sdsu.datavis.trispace.tsprep.io.img.ImageManager;
import edu.sdsu.datavis.trispace.tsprep.xform.CSVManager;
import edu.sdsu.datavis.trispace.tsprep.xform.PostgreSQLManager;

import org.apache.log4j.BasicConfigurator;

import smile.projection.PCA;

public class Workflow {
//	final static boolean RESET = true;
	final static boolean RESET = false;
	final static int NO_DATA = -9999;
	final static int LC_NO_DATA = 0;
	final static int[] PRIMARY_KEYS = {0,1};
	final static String SCENE = "SanElijo";
	final static String[] PERSPECTIVES = {"L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L"};
	final static boolean BATCH = true;
	
	
	// threshold of input vector count to determine if SOM or MDS
	final static int SOM_THRESHOLD = 100;
	final static int SOM_NEURON_CAP = 250000;
	final static int NUM_TRAIN_ITERATION_CYCLES = 1;
	
//	final static String CREDENTIALS = "./environments/postgresql2.txt";
	final static String CREDENTIALS = "./environments/postgresql.txt";
	static String url = "";
	static String user = "";
	static String pw = "";
//	static String database = "schempp_thesis";
	static String database = "schempp17";
	
	
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

	public static void main(String[] args) throws IOException {
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
		File f_LC = new File("./data/" + SCHEMA + "/input/landcover");
		// output directory 
		String outDir = "./data/" + SCHEMA + "/input/csv";
		String outDirLC = "./data/" + SCHEMA + "/input/lc_csv";		
		
		// perform gdal2xyz on all files contained in directory 'f'
//		ImageManager.performGDAL2XYZ(f,outDir);
//		ImageManager.performGDAL2XYZ(f_LC, outDirLC);
//		System.out.println("GDAL2XYZ COMPLETE!");
//		long startTime = System.nanoTime();
//		CSVManager.createPixelPolyTableBatch2(db, SCHEMA, new File(outDir), EPSG);
//		long endTime = System.nanoTime();
//		
//		long duration = (endTime - startTime);
//		System.out.println("Pixel Polygon Table took: " + duration);
//		CSVManager.createPixelPolyTable(db, SCHEMA, new File(outDir), EPSG);
//		System.out.println("Pixel Polygon Table Created!");
		
		
		// input imagery directory
		f = new File(outDir);		
		// new output imagery directory
		outDir = "./data/" + SCHEMA + "/tables/imagery";
		
		// input LC directory
		f_LC = new File(outDirLC);
		
		// new output LC directory
		outDirLC = "./data/" + SCHEMA + "/tables/landcover";
				
		// attribute dictionary for header definitions
		String attributeDictionary = "./data/" + SCHEMA + "/input/dictionaries/attributes.txt";
		String lcDictionary = "./data/" + SCHEMA + "/input/dictionaries/landcover.txt";
		
		// add headers and remove noData pixels
//		ImageManager.cleanData(f, NO_DATA, outDir, true, attributeDictionary);		
		
//		ImageManager.cleanData(f_LC, LC_NO_DATA, outDirLC, true, lcDictionary);
		
		// validate loci
		lociTest(outDir);
		
		
		// imagery table directory
		f = new File(outDir);
		// input LC directory
		f_LC = new File(outDirLC);
		
		// new output directory
		outDir = "./data/" + SCHEMA + "/tables/Non_normalized";
		
		// convert a folder (T) containing many files (L_A) to L_AT
//		System.out.println("L_AT?");
//		System.out.println(CSVManager.convertToL_AT("L_A_T", f, PRIMARY_KEYS, outDir));
		// imagery table directory
		f = new File(outDir + "/dictionaries/lociDictionary.csv");
//		System.out.println(CSVManager.convertLC2LT_A(f_LC, f, f_LC.getAbsolutePath()));
		
		
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
//		System.out.println(CSVManager.createTableL_AT(db, "L_A_T", SCHEMA, f2, PRIMARY_KEYS, 1, "p", BATCH, true));
//		System.out.println("Created locus_key Table");
		
		// create and populate locus_pt table
//		System.out.println(CSVManager.extractPtGeomFromCSV(db, SCHEMA, f2, PRIMARY_KEYS, "l", 1, EPSG, BATCH));
//		System.out.println("Created locus_pt Table");

		// create and populate locus_poly table		
//		CSVManager.createFinalPixelPolyTable(db,SCHEMA,BATCH);
//		System.out.println("Created locus_poly Table");
		
		
		outDir2 = "./data/" + SCHEMA + "/tables";
		
		File lt_a_LC = new File("./data/" + SCHEMA + "/tables/landcover/LT_A_LC.csv");
		File landcoverDictionary = new File("./data/" + SCHEMA + "/input/dictionaries/landcover.txt");
		
//		CSVManager.insertTSFromCSVBatch(db, SCHEMA, "L_AT", outDir2, SCENE);
//		CSVManager.insertTSFromCSVBatch(db, SCHEMA, "LA_T", outDir2, SCENE);
//		CSVManager.insertTSFromCSVBatch(db, SCHEMA, outDir2, SCENE, lt_a_LC, landcoverDictionary);

		
		// create SSE table
//		CSVManager.createSSETable(db, SCHEMA, MAX_K);
		System.out.println("Created SSE Table");
		
		// populate SSE table with dummy values
//		CSVManager.insert2SSETable(db, SCHEMA, MAX_K, BATCH);
		System.out.println("Populated SSE Table");
		
		// retrieve number of time objects
		int timeCount = db.getTableLength(SCHEMA, "time_key");
		
		// retrieve number of attribute objects
		int attributeCount = db.getTableLength(SCHEMA, "attribute_key");
		
		// retrieve number of loci objects		
		int lociCount = db.getTableLength(SCHEMA, "locus_key");
		
		// compute side of SOM - used for X & Y
		int[] somSizes = SOMaticManager.getSOMDimension(lociCount, attributeCount, timeCount, SOM_NEURON_CAP);
		
		String normalization = PERSPECTIVES[0];	
		ArrayList<Integer> sizeList = new ArrayList<Integer>();
		
		
		// create SOM geometry, few iterations since the data values do not matter
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			if ((somSizes[i] * somSizes[i]) > SOM_THRESHOLD) {

				PostgreSQLManager.createSOMTable(db, SCHEMA, PERSPECTIVES[i], MAX_K);
				String newOutput = "./data/" + SCHEMA + "/SOMaticGeom/" + somSizes[i];
				if (!sizeList.contains(somSizes[i])) {
					String newInput = "data/" + SCHEMA + "/SOMaticIn/" + normalization + "_Normalized/" + PERSPECTIVES[0] + "_" + SCENE + ".dat";
					try {
						Files.createDirectories(Paths.get(newOutput));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
//					SOMaticManager.runSOM(newInput, newOutput, somSizes[i], somSizes[i], (float) 0.04, somSizes[i], 
//							50, 75, 100, DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR,1);
					
				}
				
				PostgreSQLManager.insert2SOM(db, SCHEMA, PERSPECTIVES[i], newOutput + "/trainedSOM.geojson", MAX_K, BATCH);
			} else {
//				CSVManager.createMDSTable(db, SCHEMA, PERSPECTIVES[i], MAX_K);
			}
		}
		
		
		// collapse L_AT double headers into a single header for RStudio PCA processing
//		for (int i = 0; i < PERSPECTIVES.length; i++) {
//			String pcaConvert = "./data/" + SCHEMA + "/tables/" + PERSPECTIVES[i] + "_Normalized/L_AT_" + SCENE + ".csv";
//			File tmpFile = new File(pcaConvert);
//			
//			CSVManager.collapseHeader(tmpFile, "./data/" + SCHEMA + "/tables/" + PERSPECTIVES[i] + "_Normalized/L_AT_" + SCENE + "2.csv");
//		}
		
//		CSVManager.createPCATable(db, SCHEMA);
//		String tmpPath = "./data/" + SCHEMA + "/tables/PCA/AT_L";
//		CSVManager.populatePCATable(db, SCHEMA, "AT_L", tmpPath, BATCH);
//		CSVManager.populateTSPCATable(db, SCHEMA, "AT_L", tmpPath, BATCH);
//		tmpPath = "./data/" + SCHEMA + "/tables/PCA/A_LT";
//		CSVManager.populatePCATable(db, SCHEMA, "A_LT", tmpPath, BATCH);		
//		CSVManager.populateTSPCATable(db, SCHEMA, "A_LT", tmpPath, BATCH);
//		tmpPath = "./data/" + SCHEMA + "/tables/PCA/T_LA";
//		CSVManager.populatePCATable(db, SCHEMA, "T_LA", tmpPath, BATCH);
//		CSVManager.populateTSPCATable(db, SCHEMA, "T_LA", tmpPath, BATCH);
		
		
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			
			
			normalization = PERSPECTIVES[i];				
			for (int k = 0; k < PERSPECTIVES.length; k++) {
				
//				if (k == 3) {
				if (somSizes[k] * somSizes[k] > SOM_THRESHOLD) {
					String newInput = "data/" + SCHEMA + "/SOMaticIn/" + normalization + "_Normalized/" + PERSPECTIVES[k] + "_" + SCENE + ".dat";
					String newOutput = "./data/" + SCHEMA + "/SOMaticOut/" + normalization + "_Normalized/" + PERSPECTIVES[k];
					try {
						Files.createDirectories(Paths.get(newOutput));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
//					System.out.println(newInput);
//					System.out.println(newOutput);
//					SOMaticManager.runSOM(newInput, newOutput, somSizes[k], somSizes[k], (float) 0.04, somSizes[k], 
//							NITERATIONS1, NITERATIONS2, NITERATIONS3, DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR, 0);
//					SOMaticManager.extractSOMData(newInput, newOutput + "/trainedSom.cod", PERSPECTIVES[k], 3, DIST_MEASURE, SCHEMA, db, i, BATCH);
//					SOMaticManager.extractSOMData(newInput, newOutput + "/trainedSom.cod", PERSPECTIVES[k], MAX_K, DIST_MEASURE, SCHEMA, db, i, false);
					SOMaticManager.extractSOMData(newInput, newOutput + "/trainedSom.cod", PERSPECTIVES[k], MAX_K, DIST_MEASURE, SCHEMA, db, i, BATCH);
				} else {
//					CSVManager.createMDSTable(db, SCHEMA, PERSPECTIVES[i], MAX_K);
					String mdsInput = "./data/" + SCHEMA + "/tables/" + normalization + "_Normalized/" + PERSPECTIVES[k] + "_" + SCENE + ".csv";
//					String mdsInput = "./data/CSV/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[k] + "_" + SCENE + ".csv";
					File mdsFile = new File(mdsInput);
					
					String newOutput = "./data/" + SCHEMA + "/MDSOut/" + normalization + "_Normalized";
					try {
						Files.createDirectories(Paths.get(newOutput));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					newOutput = newOutput + "/" + PERSPECTIVES[k] + ".csv";
					
//					MDSManager.createMDSFile(mdsFile, PERSPECTIVES[k], DIST_MEASURE, newOutput);
//					MDSManager.performMDS(mdsFile, PERSPECTIVES[k], PERSPECTIVES[i], DIST_MEASURE, db, SCHEMA, MAX_K, K_ITERATIONS, BATCH);
				}
			}
//			}
			
		}
		

		
//		String pcaTest = "./data/" + SCHEMA + "/tables/L_AT_Normalized/AT_L_SanElijo.csv";
//		File pcaFile = new File(pcaTest);
//		
//		double[][] dat = CSVManager.retrieve2DArray("AT_L", pcaFile);
//		
//		runPCA(dat);
		
//		String datPath = "data/" + SCHEMA + "/SOMaticIn/L_AT_Normalized/L_AT_" + SCENE + ".dat";
//		String codPath = "data/" + SCHEMA + "/SOMaticOut/L_AT_Normalized/L_AT/trainedSom.cod";
//		String path1 = "./data/" + SCHEMA + "/SOMaticOut/L_AT_Normalized/L_AT/iv2BMU.csv";
//		String path2 = "./data/" + SCHEMA + "/SOMaticOut/L_AT_Normalized/L_AT/neuron2IV.csv";
//		String path3 = "./data/" + SCHEMA + "/SOMaticOut/L_AT_Normalized/L_AT/kmeans.csv";
		
//		SOMaticManager.extractBMU2CSV(datPath, codPath, path1, path2, DIST_MEASURE);
		
//		SOMaticManager.extractKmeans2CSV(datPath, codPath, "L_AT", 2, 3, path3, DIST_MEASURE);
		
		
		
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
	
	public static void runPCA(double[][] data) {
//		for (int i = 0; i < data.length; i++) {
//			String line = "";
//			for (int j = 0; j < data[i].length; j++) {
//				line = line + " " + data[i][j];
//			}
//			System.out.println(line);
//		}
		BasicConfigurator.configure();
//		double[][] data = new double[5][3];
		
//		for (int i = 0; i < data.length; i++) {
//			data[i][0] = 1 + i;
//			data[i][1] = 1 - i*i;
//			data[i][2] = i * i * i;
//		}
//		
		int max_dim = data.length;
		if (data[0].length < max_dim) max_dim = data[0].length;
		
		PCA pca = new PCA(data);
		
		pca.setProjection(max_dim);
		
		double[][] newy = pca.project(data);
		
		double[] varProportions = pca.getCumulativeVarianceProportion();
		
//		for (int i = 0; i < max_; i++) {
		for (int i = 0; i < varProportions.length; i++) {
			System.out.println(varProportions[i]);
		}
//		
//		for (int i = 0; i < newy.length; i++) {
//			String newDat = "";
//			for (int j = 0; j < newy[i].length; j++) {
//				newDat = newDat + " " + newy[i][j]; 
//			}
//			System.out.println(newDat);
//		}
	}

}
