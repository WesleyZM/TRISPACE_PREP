package edu.sdsu.datavis.trispace.tsprep.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;

import org.somatic.som.SOMatic;
import org.somatic.trainer.Global;

import edu.sdsu.datavis.trispace.tsprep.db.PostgreSQLJDBC;
import edu.sdsu.datavis.trispace.tsprep.io.img.ImageManager;
import edu.sdsu.datavis.trispace.tsprep.xform.CSVManager;
import edu.sdsu.datavis.trispace.tsprep.xform.PostgreSQLManager;

public class PostgreSQLWorkflow {
	
	final static String CREDENTIALS = "./environments/postgresql.txt";
	static String url = "";
	static String user = "";
	static String pw = "";
	
	final static String SCHEMA = "thesisstuff";
	final static String EPSG = "4326";
//	final static String SCHEMA = "thesis";
	
	final static int NO_DATA = -9999;
	final static int[] PRIMARY_KEYS = {0,1};
	final static String SCENE = "SanElijo";
	final static String[] PERSPECTIVES = {"L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L"};
	
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
		// TODO Auto-generated method stub
		
		
		
		
		if (!CREDENTIALS.equals("")) {
			if (!loadEnvironments()) {
				System.out.println("Incomplete data for URL/USER/PW");
				System.out.println("System Exiting");
				System.exit(0);
			}
		}
		
		PostgreSQLJDBC db = null;
		try {
			db = new PostgreSQLJDBC(url,user,pw);
			System.out.println(db.changeDatabase("schempp17"));
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (!db.schemaExists(SCHEMA)) {
			db.createSchema(SCHEMA);
		}
		
		
		
		// set input to folder containing .tif images
		File f = new File("./data/inputImagery");		
		// output directory 
		String outDir = "./data/inputCSV";		
		// perform gdal2xyz on all files contained in directory 'f'
//		ImageManager.performGDAL2XYZ(f,outDir);
		
		PostgreSQLManager.createPixelPolyTable(db, SCHEMA, new File(outDir), EPSG);
		
		
		
//		System.out.println("COLUMNS: " + ImageManager.getNumColumns("./data/inputImagery/1993_SanElijo.tif"));
//		System.out.println("ROWS: " + ImageManager.getNumRows("./data/inputImagery/1993_SanElijo.tif"));
		
				
		// input directory
		f = new File(outDir);
		// output directory
		String outDir2 = "./data/fixedCSV";
		// add headers and remove noData pixels
//		ImageManager.cleanData(f, NO_DATA, outDir2, true);
		
//		System.out.println(f.getPath());
//		CSVManager.deleteDirectory2(f);
//		CSVManager.deleteDir(f.getPath());
				
		// input directory
		File f2 = new File(outDir2);
		// output directory
		outDir = "./data/CSV/Non_Normalized";
		// convert a folder (T) containing many files (L_A) to L_AT
//		System.out.println("COMMON LOCI TEST: ");
//		System.out.println(CSVManager.commonLociCheck(f2, PRIMARY_KEYS));
//		CSVManager.convertToL_AT("L_A_T", f2, PRIMARY_KEYS, outDir);
		
//		System.out.println(f2.getPath());
//		CSVManager.deleteDirectory(f);
//		CSVManager.deleteDirectory2(f2);
//		CSVManager.deleteDir(f2.getPath());
		
		
		
		
		
		
		
		
//		String schema
//		System.out.println("Schema exists:");
//		System.out.println(db.schemaExists(SCHEMA));
//		
//		System.out.println("Table exists: ");
//		System.out.println(db.tableExists(SCHEMA, "time_keyzyeahyeah"));
//		System.out.println(db.tableExists(SCHEMA, "time_key"));
		
		System.out.println("EXTRACT Dictionary: ");
		System.out.println(PostgreSQLManager.createTableL_AT(db, "L_A_T", SCHEMA, f2, PRIMARY_KEYS, 1, "p"));
		
//		File f22 = new File("./data/fixedCSV/1993_SanElijo.csv");
		
		System.out.println("EXTRACT PTS: ");
		System.out.println(PostgreSQLManager.extractPtGeomFromCSV(db, SCHEMA, f2, PRIMARY_KEYS, "l", 1, "4326"));
		
		PostgreSQLManager.createFinalPixelPolyTable(db,SCHEMA);
		
		
//		CSVManager.deleteDirectory2(f);
		
//		CSVManager.createTable_L_AT(db,SCHEMA);
		
//		CSVManager.deleteDirectory(f);
		
		// input L_AT file
//		f = new File(outDir + "/L_AT_" + SCENE + ".csv");
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
//			CSVManager.normalizeCSV(f, outDir, PERSPECTIVES[i], 0);
//		}
		
		// convert each normalized file to L_AT then to the remaining perspectives		
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
		
		// convert each file into a .dat file for SOMatic
//		for (int i = 0; i < PERSPECTIVES.length; i++) {
//			for (int j = 0; j < PERSPECTIVES.length; j++) {
//				File input = new File("./data/CSV/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[j] + "_" + SCENE + ".csv");
//				String output = "./data/SOMaticIn/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[j] + "_" + SCENE + ".dat";
//				CSVManager.fromCSV2DatFile(PERSPECTIVES[j], input, output);
//			}
//		}
		
//		String output = "./data/SOMaticOut/" + PERSPECTIVES[0] + "_Normalized/" + PERSPECTIVES[0];
		
//		try {
//			Files.createDirectories(Paths.get(output));
//		} catch (IOException e) {
			// TODO Auto-generated catch block
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
			System.out.println("Error with setTopology(); Topology =" + g.HEXA);
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

		System.out.println("Now writing trained SOM to file");
		s.writeSomToAFile();
		System.out.println("Finished writing SOM file");
//		System.exit(0);
	}
}
