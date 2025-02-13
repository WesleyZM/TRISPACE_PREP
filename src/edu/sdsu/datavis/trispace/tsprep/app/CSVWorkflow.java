package edu.sdsu.datavis.trispace.tsprep.app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.somatic.som.SOMatic;
import org.somatic.trainer.Global;

import edu.sdsu.datavis.trispace.tsprep.dr.SOMaticManager;
import edu.sdsu.datavis.trispace.tsprep.io.img.ImageManager;
import edu.sdsu.datavis.trispace.tsprep.xform.CSVManager;

public class CSVWorkflow {
	final static int NO_DATA = -9999;
	final static int[] PRIMARY_KEYS = {0,1};
	final static String SCENE = "SanElijo";
	final static String[] PERSPECTIVES = {"L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L"};
	final static int SOM_THRESHOLD = 75;
	
	final static float MIN_NORMALIZATION = 0.1f;
	final static float MAX_NORMALIZATION = 0.9f;
	
	final static int EUCLIDEAN = 1;
	final static int COSINE = 2;
	final static int MANHATTAN = 3;
	
	final static int DIST_MEASURE = EUCLIDEAN;
	final static int NITERATIONS = 10000;
	final static int NTHREADS = 1;
	final static boolean ROUNDING = true;
	final static int SCALING_FACTOR = 1;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		// set input to folder containing .tif images
		File f = new File("./data/inputImagery");		
		// output directory 
		String outDir = "./data/inputCSV";		
		// perform gdal2xyz on all files contained in directory 'f'
		ImageManager.performGDAL2XYZ(f,outDir);
				
		// input directory
		f = new File(outDir);
		// output directory
		outDir = "./data/fixed_CSV";
		// add headers and remove noData pixels
		ImageManager.cleanData(f, NO_DATA, outDir, true);
				
		// input directory
		f = new File(outDir);
		// output directory
		outDir = "./data/CSV/Non_Normalized";
		// convert a folder (T) containing many files (L_A) to L_AT
		CSVManager.convertToL_AT("L_A_T", f, PRIMARY_KEYS, outDir);
		
		// input L_AT file
		f = new File(outDir + "/L_AT_" + SCENE + ".csv");
		// convert CSV to all other perspectives
		CSVManager.fromL_AT2All(f, outDir, SCENE);
		
		// normalize to all perspectives
		String inDir = outDir + "";
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			outDir = "";
			String name = PERSPECTIVES[i] + "_" + SCENE + ".csv";
			f = new File(inDir + "/" + name);
			String[] split = inDir.split("/");
			for (int j = 0; j < split.length-1; j++) {
				outDir = outDir + split[j] + "/";
			}
			outDir = outDir + PERSPECTIVES[i] + "_Normalized/" + name;
//			CSVManager.normalizeCSVMinMax(f, outDir, PERSPECTIVES[i], 0);
			CSVManager.normalizeCSVMinMax(f, outDir, PERSPECTIVES[i], MIN_NORMALIZATION, MAX_NORMALIZATION);
//			CSVManager.normalizeCSVUnitVector(f, outDir, PERSPECTIVES[i]);
		}
		
		// convert each normalized file to L_AT then to the remaining perspectives		
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			inDir = "./data/CSV/" + PERSPECTIVES[i] + "_Normalized";
			f = new File(inDir + "/" + PERSPECTIVES[i] + "_" + SCENE + ".csv");
			if (i == 0) {
				CSVManager.fromL_AT2All(f, inDir, SCENE);
			} else if (i == 1) {
				CSVManager.fromA_LT2All(f, inDir, SCENE);
			} else if (i == 2) {
				CSVManager.fromT_LA2All(f, inDir, SCENE);
			} else if (i == 3) {
				CSVManager.fromLA_T2All(f, inDir, SCENE);
			} else if (i == 4) {
				CSVManager.fromLT_A2All(f, inDir, SCENE);
			} else if (i == 5) {
				CSVManager.fromAT_L2All(f, inDir, SCENE);
			}
		}
		
		// convert each file into a .dat file for SOMatic
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			for (int j = 0; j < PERSPECTIVES.length; j++) {
				File input = new File("./data/CSV/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[j] + "_" + SCENE + ".csv");
				String output = "./data/SOMaticIn/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[j] + "_" + SCENE + ".dat";
				CSVManager.fromCSV2DatFile(PERSPECTIVES[j], input, output);
			}
		}
		
		int timeCount = CSVManager.rowCount("./data/CSV/Non_Normalized/dictionaries/timeDictionary.csv");
		int attributeCount = CSVManager.rowCount("./data/CSV/Non_Normalized/dictionaries/attrDictionary.csv");
		int lociCount = CSVManager.rowCount("./data/CSV/Non_Normalized/dictionaries/lociDictionary.csv");
		
		System.out.println("timeCount: " + timeCount);
		System.out.println("attributeCount: " + attributeCount);
		System.out.println("lociCount: " + lociCount);
		
		//final static String[] PERSPECTIVES = {"L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L"};
		
		int[] somSizes = SOMaticManager.getSOMDimension(lociCount, attributeCount, timeCount);
		
//		for (int i = 0; i < somSizes.length; i++) {
//			System.out.println(PERSPECTIVES[i] + " size: " + somSizes[i]);
//		}
		
//		f = new File("./data/SOMaticOut");
//		if (!f.exists()) {
//			f.mkdir();
//		}
		
		
		
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			
			String normalization = PERSPECTIVES[i];				
			for (int k = 0; k < PERSPECTIVES.length; k++) {
				if (somSizes[k] * somSizes[k] > SOM_THRESHOLD) {
					String newInput = "./data/SOMaticIn/" + normalization + "_Normalized/" + PERSPECTIVES[k] + "_" + SCENE + ".dat";
					String newOutput = "./data/SOMaticOut/" + normalization + "_Normalized/" + PERSPECTIVES[k];
					try {
						Files.createDirectories(Paths.get(newOutput));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					runSOM(newInput, newOutput, somSizes[k], somSizes[k], (float) 0.04, somSizes[k], 
							NITERATIONS, DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR);
				}
			}
			
		}
		
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			if (somSizes[i] * somSizes[i] <= SOM_THRESHOLD) {
				System.out.println(PERSPECTIVES[i] + " should be MDS!");
			} else {
				
			}
		}
		
		
		
//		boolean[] useSOM = new boolean[PERSPECTIVES.length];
//		
//		if (lociCount >= SOM_THRESHOLD) {
//			useSOM[0] = true;
//		} else {
//			useSOM[0] = false;			
//		}
//		
//		if (attributeCount >= SOM_THRESHOLD) {
//			useSOM[1] = true;
//		} else {
//			useSOM[1] = false;
//		}
//		
//		if (timeCount >= SOM_THRESHOLD) {
//			useSOM[2] = true;
//		} else {
//			useSOM[2] = false;
//		}
//		
//		if (lociCount * attributeCount >= SOM_THRESHOLD) {
//			useSOM[3] = true;
//		} else {
//			useSOM[3] = false;			
//		}
//		
//		if (lociCount * timeCount >= SOM_THRESHOLD) {
//			useSOM[4] = true;
//		} else {
//			useSOM[4] = false;
//		}
//		
//		if (attributeCount * timeCount >= SOM_THRESHOLD) {
//			useSOM[5] = true;
//		} else {
//			useSOM[5] = false;
//		}
		
//		for (int i = 0; i < PERSPECTIVES.length; i++) {
//			
//		}
		
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
