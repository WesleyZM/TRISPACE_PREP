package edu.sdsu.datavis.trispace.tsprep.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;

import org.somatic.som.SOMatic;
import org.somatic.trainer.Global;

import edu.sdsu.datavis.trispace.tsprep.db.PostgreSQLJDBC;
import edu.sdsu.datavis.trispace.tsprep.dr.MDSManager;
import edu.sdsu.datavis.trispace.tsprep.dr.SOMaticManager;
import edu.sdsu.datavis.trispace.tsprep.io.img.ImageManager;
import edu.sdsu.datavis.trispace.tsprep.xform.CSVManager;
import edu.sdsu.datavis.trispace.tsprep.xform.PostgreSQLManager;

public class PostgreSQLWorkflow2 {
	final static boolean RESET = true;
	final static int NO_DATA = 32767;
	final static int[] PRIMARY_KEYS = {0,1};
	final static String SCENE = "finalthesis";
	final static String[] PERSPECTIVES = {"L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L"};
	
	// threshold of input vector count to determine if SOM or MDS
	final static int SOM_THRESHOLD = 75;
	
	final static String CREDENTIALS = "./environments/postgresql.txt";
	static String url = "";
	static String user = "";
	static String pw = "";
	
	
	final static float MIN_NORMALIZATION = 0.1f;
	final static float MAX_NORMALIZATION = 0.9f;
	
	final static int EUCLIDEAN = 1;
	final static int COSINE = 2;
	final static int MANHATTAN = 3;
	
	final static String SCHEMA = "finalthesis";
	final static String EPSG = "4326";
//	final static String SCHEMA = "thesis";
	
	final static int DIST_MEASURE = COSINE;
	final static int NITERATIONS1 = 10000;
	final static int NITERATIONS2 = 20000;
	final static int NITERATIONS3 = 50000;
	final static int NTHREADS = 1;
	final static boolean ROUNDING = true;
	final static int SCALING_FACTOR = 1;
	
	final static int MAX_K = 12;
	final static int K_ITERATIONS = 1000;
	
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
	

	public static void main(String[] args) {
		
		if (!CREDENTIALS.equals("")) {
			if (!loadEnvironments()) {
				System.out.println("Incomplete data for URL/USER/PW");
				System.out.println("System Exiting");
				System.exit(0);
			}
		}
		
		PostgreSQLJDBC db = new PostgreSQLJDBC(url,user,pw);
		System.out.println(db.changeDatabase("schempp17"));
		
//		if (RESET && db.schemaExists(SCHEMA)) {
//			db.dropSchemaCascade(SCHEMA);
//		}
		
		if (!db.schemaExists(SCHEMA)) {
			db.createSchema(SCHEMA);
		} 
		
		
//		// set input to folder containing .tif images
		for (int i = 1; i <= 3721; i++) {
			String tmpI = i + "";
//			db.updateTable("finalthesis", "lt_a_geom", "id", tmpI, "k3_n1", "-1");
		}
		File f = new File("./data/final_pts.csv");		
		db.updateLT_A_LC(db, f, "finalthesis");
//		// output directory 
//		String outDir = "./data/inputCSV";		
////		// perform gdal2xyz on all files contained in directory 'f'
////		ImageManager.performGDAL2XYZ(f,outDir);
//		System.out.println("GDAL2XYZ COMPLETE!");
////		
//		CSVManager.createPixelPolyTable(db, SCHEMA, new File(outDir), EPSG);
////				
////		// input directory
//		f = new File(outDir);
////		// output directory
//		outDir = "./data/fixedCSV";
////		// add headers and remove noData pixels
////		ImageManager.cleanData(f, NO_DATA, outDir, true);
//		
//		System.out.println("COMMON?");
//		System.out.println(CSVManager.commonLociCheck(outDir, PRIMARY_KEYS));
////				
////		// input directory
//		f = new File(outDir);
////		// output directory
//		outDir = "./data/CSV/Non_Normalized";
////		// convert a folder (T) containing many files (L_A) to L_AT
//		
//		System.out.println("L_AT?");
//		System.out.println(CSVManager.convertToL_AT("L_A_T", f, PRIMARY_KEYS, outDir));
////		
////		// input L_AT file
//		f = new File(outDir + "/L_AT_" + SCENE + ".csv");
////		// convert CSV to all other perspectives
//		CSVManager.fromL_AT2All(f, outDir, SCENE);
////		
////		// normalize to all perspectives
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
////		
////		// convert each normalized file to L_AT then to the remaining perspectives		
//		for (int i = 0; i < PERSPECTIVES.length; i++) {
//			inDir = "./data/CSV/" + PERSPECTIVES[i] + "_Normalized";
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
////		
////		// convert each file into a .dat file for SOMatic
//		for (int i = 0; i < PERSPECTIVES.length; i++) {
//			for (int j = 0; j < PERSPECTIVES.length; j++) {
//				File input = new File("./data/CSV/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[j] + "_" + SCENE + ".csv");
//				String output = "./data/SOMaticIn/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[j] + "_" + SCENE + ".dat";
//				CSVManager.fromCSV2DatFile(PERSPECTIVES[j], input, output);
//			}
//		}
////		
//		String outDir2 = "./data/fixedCSV";
//		File f2 = new File(outDir2);
////		
////		System.out.println("EXTRACT Dictionary: ");
//		System.out.println(CSVManager.createTableL_AT(db, "L_A_T", SCHEMA, f2, PRIMARY_KEYS, 1, "p"));
////		
//////		File f22 = new File("./data/fixedCSV/1993_SanElijo.csv");
////		
////		System.out.println("EXTRACT PTS: ");
//		System.out.println(CSVManager.extractPtGeomFromCSV(db, SCHEMA, f2, PRIMARY_KEYS, "l", 1, EPSG));
////		
//		CSVManager.createFinalPixelPolyTable(db,SCHEMA);
////		
//		outDir2 = "./data/CSV";
//		
//		CSVManager.insertTSFromCSV(db,SCHEMA,"L_AT", outDir2,SCENE);
//		CSVManager.insertTSFromCSV(db,SCHEMA,"LA_T", outDir2,SCENE);
//		CSVManager.insertTSFromCSV(db,SCHEMA,"LT_A", outDir2,SCENE);
//		
//		CSVManager.createSSETable(db, SCHEMA, MAX_K);
//		CSVManager.insert2SSETable(db, SCHEMA, MAX_K);
//		
//		int timeCount = db.getTableLength(SCHEMA, "time_key");
//		int attributeCount = db.getTableLength(SCHEMA, "attribute_key");
//		int lociCount = db.getTableLength(SCHEMA, "locus_key");
//		
//		System.out.println("timeCount: " + timeCount);
//		System.out.println("attributeCount: " + attributeCount);
//		System.out.println("lociCount: " + lociCount);
//		
//		//final static String[] PERSPECTIVES = {"L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L"};
//		
//		
//		
//		int[] somSizes = SOMaticManager.getSOMDimension(lociCount, attributeCount, timeCount);
//		
//			
//		String normalization = PERSPECTIVES[0];				
//		ArrayList<Integer> sizeList = new ArrayList<Integer>();
//		for (int i = 0; i < PERSPECTIVES.length; i++) {
//			if ((somSizes[i] * somSizes[i]) > SOM_THRESHOLD) {
////				System.out.println(PERSPECTIVES[i] + " IS GOOD WITH " + somSizes[i]);
////				System.out.println(x);
//				CSVManager.createSOMTable(db, SCHEMA, PERSPECTIVES[i], MAX_K);
//				String newOutput = "./data/SOMaticGeom/" + somSizes[i];
//				if (!sizeList.contains(somSizes[i])) {
//					String newInput = "./data/SOMaticIn/" + normalization + "_Normalized/" + PERSPECTIVES[0] + "_" + SCENE + ".dat";
//					
//					try {
//						Files.createDirectories(Paths.get(newOutput));
//					} catch (IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//					SOMaticManager.runSOM(newInput, newOutput, somSizes[i], somSizes[i], (float) 0.04, somSizes[i], 
//							50, 75, 100, DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR,1);
//					
//				}
//				
//				CSVManager.insert2SOM(db, SCHEMA, PERSPECTIVES[i], newOutput + "/trainedSOM.geojson", MAX_K);
//			} else {
//				CSVManager.createMDSTable(db, SCHEMA, PERSPECTIVES[i], MAX_K);
////				File mdsInput = "./data/CSV/" + PERSPECTIVES[i]
////				MDSManager.createMDSFile(inputFile, tsType, simMeasure, output)
//			}
//		}
//		
//
//			
//		
//		for (int i = 0; i < PERSPECTIVES.length; i++) {
//			
//			normalization = PERSPECTIVES[i];				
//			for (int k = 0; k < PERSPECTIVES.length; k++) {
//				if (somSizes[k] * somSizes[k] > SOM_THRESHOLD) {
//					String newInput = "./data/SOMaticIn/" + normalization + "_Normalized/" + PERSPECTIVES[k] + "_" + SCENE + ".dat";
//					String newOutput = "./data/SOMaticOut/" + normalization + "_Normalized/" + PERSPECTIVES[k];
//					try {
//						Files.createDirectories(Paths.get(newOutput));
//					} catch (IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//					SOMaticManager.runSOM(newInput, newOutput, somSizes[k], somSizes[k], (float) 0.04, somSizes[k], 
//							NITERATIONS1, NITERATIONS2, NITERATIONS3, DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR, 0);
//					SOMaticManager.extractSOMData(newInput, newOutput + "/trainedSom.cod", PERSPECTIVES[k], MAX_K, DIST_MEASURE, SCHEMA, db, i);
//				} else {
////					CSVManager.createMDSTable(db, SCHEMA, PERSPECTIVES[i], MAX_K);
//					String mdsInput = "./data/CSV/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[k] + "_" + SCENE + ".csv";
//					File mdsFile = new File(mdsInput);
//					
//					String newOutput = "./data/MDSOut/" + normalization + "_Normalized";
//					try {
//						Files.createDirectories(Paths.get(newOutput));
//					} catch (IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//					
//					newOutput = newOutput + "/" + PERSPECTIVES[k] + ".csv";
//					
////					MDSManager.createMDSFile(mdsFile, PERSPECTIVES[k], DIST_MEASURE, newOutput);
//					MDSManager.performMDS(mdsFile, PERSPECTIVES[k], PERSPECTIVES[i], DIST_MEASURE, db, SCHEMA, MAX_K, K_ITERATIONS);
//				}
//			}
//			
//		}
		
//		String datPath = "./data/SOMaticIn/A_LT_Normalized/L_AT_SanElijo.dat";
//		String codPath = "./data/SOMaticOut/A_LT_Normalized/L_AT/trainedSom.cod";
//		
//		SOMaticManager.extractSOMData(datPath, codPath, "L_AT", MAX_K, DIST_MEASURE, SCHEMA, db, 0);
		
//		String[] test = SOMaticManager.getIV2BMU2(datPath, codPath, "L_AT");
		
//		for (int q = 0; q < test.length; q++) {
//			System.out.println(test[q]);
//		}
		
//		String[] test2 = SOMaticManager.getNeuronKMeans(datPath, codPath, "L_AT", 4, DIST_MEASURE);
		
//		String output = "./data/SOMaticOut/" + PERSPECTIVES[0] + "_Normalized/" + PERSPECTIVES[0];
		
//		try {
//			Files.createDirectories(Paths.get(output));
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
//		runSOM("./data/SOMaticIn/" + PERSPECTIVES[0] + "_Normalized/" + PERSPECTIVES[0] +"_" + SCENE + ".dat",
//				output, 36, 36,
//				(float) 0.04, 36, 10000, 1, 1, true, 2);
		
//		for (int i = 0; i < PERSPECTIVES.length; i++) {
			 // Euclidean Distance Similarity
//			for (int k = 0; k < PERSPECTIVES.length; k++) {
//				runSOM("./data/SOMaticIn/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[k] + ".dat",
//						"./data/SOMaticOut/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[k], somSizes[k], somSizes[k],
//						(float) 0.04, somSizes[k], 10000, 1, 1, true, 2);
//			}
//		}
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
		
 
		System.out.println("End of Session");
		System.exit(0);
	}
	
	public static void credentialsTest() {
		if (CREDENTIALS.equals("") || !loadEnvironments()) {
			System.out.println("Incomplete data for URL/USER/PW");
			System.out.println("System Exiting");
			System.exit(0);
		}
	}
}
