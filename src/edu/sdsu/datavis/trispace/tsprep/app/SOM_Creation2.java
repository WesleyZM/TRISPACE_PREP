package edu.sdsu.datavis.trispace.tsprep.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import edu.sdsu.datavis.trispace.tsprep.db.PostgreSQLJDBC;
import edu.sdsu.datavis.trispace.tsprep.dr.SOMaticManager;
import edu.sdsu.datavis.trispace.tsprep.io.img.ImageManager;
import edu.sdsu.datavis.trispace.tsprep.som.*;
import edu.sdsu.datavis.trispace.tsprep.xform.CSVManager;
import edu.sdsu.datavis.trispace.tsprep.xform.PostgreSQLManager;

public class SOM_Creation2 {
	
	static int studyArea = 0;
	
	static int computerStation = 999;
	
	static int stage = 2;
	
	static int[] numLoci = {7315,9040,87314};
	static int[] numAttr = {6,7,6};
	static int[] numTime = {3,11,3};
	static int lociCount = numLoci[studyArea];
	static int attributeCount = numAttr[studyArea];
	static int timeCount = numTime[studyArea];
	
	final static int NO_DATA = -9999;
	final static String[] SCENES = {"SanElijo","CocosFire","Batiquitos"};
	final static String SCENE = SCENES[studyArea];
//	final static String SCENE = "Batiquitos";
//	final static String SCENE = "CocosFire";
	
	final static String[] PERSPECTIVES = {"L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L"};
//	final static int[] PERSPECTIVE_ORDER = {0,4,3};
//	final static int[] PERSPECTIVE_ORDER = {4,3};
	final static int PERSPECTIVE_INDEX = 0;

	// threshold of input vector count to determine if SOM or MDS
	final static int SOM_THRESHOLD = 100;
	final static int SOM_NEURON_CAP = 250000;


	final static float MIN_NORMALIZATION = 0.1f;
	final static float MAX_NORMALIZATION = 0.9f;

	final static int EUCLIDEAN = 1;
	final static int COSINE = 2;
	final static int MANHATTAN = 3;

	final static String[] SCHEMAS = {"sanelijo","cocos","batiquitos"};
	final static String SCHEMA = SCHEMAS[studyArea];
//	final static String SCHEMA = "sanelijo";
//	final static String SCHEMA = "batiquitos";
//	final static String SCHEMA = "cocos";
	
	final static String EPSG = "4326";

	final static int DIST_MEASURE = COSINE;
	final static int NUM_TRAIN_ITERATION_CYCLES = 1;
//	final static int[] CYCLES = {1,2,3,4,6,8,10,12,16,20,24};
//	final static int[] CYCLES = {1,2,3,4,6,8,10,12};
//	final static int[] CYCLES = {1,2,3,4};
	final static int[] CYCLES = {1,2,3,4};
	//	final static int NITERATIONS1 = 10000;
	//	final static int NITERATIONS2 = 20000;
	//	final static int NITERATIONS3 = 50000;
	final static int NTHREADS = 1;
	final static boolean ROUNDING = true;
	final static int SCALING_FACTOR = 1;

	final static int MAX_K = 12;
	final static int K_ITERATIONS = 1000;
	
	

	public static void main(String[] args) throws IOException {
		
		int[] somSizes = SOMaticManager.getSOMDimension(lociCount, attributeCount, timeCount, SOM_NEURON_CAP);
		
//		for (int i = 0; i < somSizes.length; i++) {
//			System.out.println(somSizes[i]);
//		}
//
//		
//		System.out.println("------------");
		
		for (int k = 0; k < PERSPECTIVES.length; k++) {
//			if (k == 0 || k == 3 || k == 4) {
			if (k == PERSPECTIVE_INDEX) {
				int perspectiveIdx = k;
				
				try {
					Files.createDirectories(Paths.get("./data/" + SCHEMA + "/test/" + PERSPECTIVES[perspectiveIdx] + "/"));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				File[] codFiles = new File[PERSPECTIVES.length];
				for (int i = 0; i < codFiles.length; i++) {
					codFiles[i] = new File("data/" + SCHEMA + "/SOMaticOut/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[perspectiveIdx] + "/trainedSom.cod");
//					System.out.println(codFiles[i].getAbsolutePath());
				}
				
				// FileWriter to write the output (CSV) file
				FileWriter writer = new FileWriter("./data/" + SCHEMA + "/test/" + PERSPECTIVES[perspectiveIdx] + "/time_tests.csv");
				writer.append("perspective,normalization,iterations,stage,time,total_time\n");
				
				
				
				for (int i = 0; i < CYCLES.length; i++) {
					int[] somIterations = SOMaticManager.getSOMIterations(CYCLES[i], lociCount, attributeCount, timeCount);
					
					trainTestingNew(stage, CYCLES[i], codFiles, somSizes, somIterations, 2, perspectiveIdx, writer);
					if (CYCLES.length > 1) {
						sendEmailAlert(true, CYCLES[i]);
					}					
				}
				writer.close();
				
				// FileWriter to write the output (CSV) file
				writer = new FileWriter("./data/" + SCHEMA + "/test/" + PERSPECTIVES[perspectiveIdx] + "/bmutime_tests.csv");
				writer.append("perspective,normalization,iterations,stage,time,total_time,cycles,aqe,aqe_time,te,te_time\n");
				
				BufferedReader br = new BufferedReader(new FileReader("./data/" + SCHEMA + "/test/" + PERSPECTIVES[perspectiveIdx] + "/time_tests.csv"));
				String line = br.readLine();
				
				for (int i = 0; i < CYCLES.length; i++) {
					int[] somIterations = SOMaticManager.getSOMIterations(CYCLES[i], lociCount, attributeCount, timeCount);
					
					bmuTesting2(CYCLES[i], somSizes, somIterations, 2, perspectiveIdx, writer, br);
//					sendEmailAlert(true);
				}
				writer.close();
				br.close();
			}			
		}
		
		
//		File f = new File("./data/test/1/SOMaticOut/A_LT_Normalized/L_AT/7315/trainedSom.cod");
		
		
//		int[] somIterations = SOMaticManager.getSOMIterations(CYCLES[0], lociCount, attributeCount, timeCount);
		
//		trainTesting(CYCLES[0], f, somSizes, somIterations, 2);
		
		
//		for (int i = 0; i < somIterations.length; i++) {
//			System.out.println(somIterations[i]);
//		}
		
		
		
		
		
//		bmuTesting2(somSizes, somIterations, 2);
		
		sendEmailAlert(false, -1);
		
		System.out.println("Exiting Program");
		System.exit(0);
	}	
	
	public static void trainTesting(int[] somSizes, int[] somIterations, int timeUnits) throws IOException {
		
		try {
			Files.createDirectories(Paths.get("./data/test/" + NUM_TRAIN_ITERATION_CYCLES + "/"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// FileWriter to write the output (CSV) file
		FileWriter writer = new FileWriter("./data/test/" + NUM_TRAIN_ITERATION_CYCLES + "/time_tests.csv");
		writer.append("perspective,normalization,iterations,stage,time,total_time\n");
				

		String normalization = PERSPECTIVES[0];	
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			normalization = PERSPECTIVES[i];
			for (int j = 0; j < PERSPECTIVES.length; j++) {
				if ((somSizes[j] * somSizes[j]) > SOM_THRESHOLD) {
					String newInput = "./data/" + SCHEMA + "/SOMaticIn/" + normalization + "_Normalized/" + PERSPECTIVES[j] + "_" + SCENE + ".dat";
					String newOutput = "data/test/" + NUM_TRAIN_ITERATION_CYCLES + "/SOMaticOut/" + normalization + "_Normalized/" + PERSPECTIVES[j];
					
					int size1 = somIterations[j];
					int size2 = somIterations[j] * 2;
					int size3 = somIterations[j] * 5;
					
					try {
//						Files.createDirectories(Paths.get(newOutput + "/50/"));
//						Files.createDirectories(Paths.get(newOutput + "/75/"));
//						Files.createDirectories(Paths.get(newOutput + "/100/"));
						
						
						
						Files.createDirectories(Paths.get(newOutput + "/" + size1 + "/"));
						Files.createDirectories(Paths.get(newOutput + "/" + size2 + "/"));
						Files.createDirectories(Paths.get(newOutput + "/" + size3 + "/"));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
//					SOMaticManager.runSOM(newInput, newOutput, somSizes[i], somSizes[i], (float) 0.04, somSizes[i], 
//							somIterations[i], somIterations[i]*2, somIterations[i]*5, DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR,1);

					SOMaticManager.runSOM_COD(newInput, newOutput, somSizes[j], somSizes[j], (float) 0.04, somSizes[j], 
							size1, size2, size3, DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR,0, PERSPECTIVES[j], 
							PERSPECTIVES[i], writer, timeUnits);
					
//					SOMaticManager.runSOM_COD(newInput, newOutput, somSizes[j], somSizes[j], (float) 0.04, somSizes[j], 
//							50, 75, 100, DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR,0, PERSPECTIVES[j], 
//							PERSPECTIVES[i], writer, timeUnits);
				}
			}

		}
		
		// flush the writer
		writer.flush();
		
		// close the writer
		writer.close();
	}
	
	public static void trainTesting(int cycles, int[] somSizes, int[] somIterations, int timeUnits, int perspectiveIdx, FileWriter writer) throws IOException {
		
//		try {
//			Files.createDirectories(Paths.get("./data/test/" + cycles + "/"));
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		// FileWriter to write the output (CSV) file
//		FileWriter writer = new FileWriter("./data/test/" + cycles + "/time_tests.csv");
//		writer.append("perspective,normalization,iterations,stage,time,total_time\n");
				

		String normalization = PERSPECTIVES[0];	
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			normalization = PERSPECTIVES[i];
//			for (int j = 0; j < PERSPECTIVES.length; j++) {
				if ((somSizes[perspectiveIdx] * somSizes[perspectiveIdx]) > SOM_THRESHOLD) {
					String newInput = "./data/" + SCHEMA + "/SOMaticIn/" + normalization + "_Normalized/" + PERSPECTIVES[perspectiveIdx] + "_" + SCENE + ".dat";
					String newOutput = "data/test/" + PERSPECTIVES[perspectiveIdx] + "/" + cycles + "/" + normalization + "_Normalized";
					
//					int size1 = somIterations[perspectiveIdx];
//					
					try {
						Files.createDirectories(Paths.get(newOutput + "/"));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					int newRadius = (int) (somSizes[perspectiveIdx] / 2);


					SOMaticManager.runSOM_COD(newInput, newOutput, somSizes[perspectiveIdx], somSizes[perspectiveIdx], (float) 0.04, somSizes[perspectiveIdx], 
							somIterations[perspectiveIdx], DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR,0, PERSPECTIVES[perspectiveIdx], 
							normalization, writer, timeUnits, stage);			
					
					writer.flush();
				}
//			}
		}
		
		// flush the writer
//		writer.flush();
		
		// close the writer
//		writer.close();
	}
	
//	public static void trainTesting(int cycles, File inSOM, int[] somSizes, int[] somIterations, int timeUnits) throws IOException {
//		
//		try {
//			Files.createDirectories(Paths.get("./data/test/" + cycles + "/"));
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		// FileWriter to write the output (CSV) file
//		FileWriter writer = new FileWriter("./data/test/" + cycles + "/time_tests.csv");
//		writer.append("perspective,normalization,iterations,stage,time,total_time\n");
//				
//
//		String normalization = PERSPECTIVES[0];	
//		for (int i = 0; i < PERSPECTIVES.length; i++) {
//			normalization = PERSPECTIVES[i];
//			for (int j = 0; j < PERSPECTIVES.length; j++) {
//				if ((somSizes[j] * somSizes[j]) > SOM_THRESHOLD) {
//					String newInput = "./data/" + SCHEMA + "/SOMaticIn/" + normalization + "_Normalized/" + PERSPECTIVES[j] + "_" + SCENE + ".dat";
//					String newOutput = "data/test/" + cycles + "/SOMaticOut/" + normalization + "_Normalized/" + PERSPECTIVES[j];
//					
//					int size1 = somIterations[j];
//					
//					try {
//						Files.createDirectories(Paths.get(newOutput + "/" + size1 + "/"));
//					} catch (IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//
//
//					SOMaticManager.runSOM_COD(newInput, inSOM, newOutput, somSizes[j], somSizes[j], (float) 0.04, somSizes[j], 
//							size1, DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR,0, PERSPECTIVES[j], 
//							normalization, null, timeUnits);
//					
//					writer.flush();
//				}
//			}
//		}
//		
//		// flush the writer
////		writer.flush();
//		
//		// close the writer
//		writer.close();
//	}
	
public static void trainTestingNew(int stage, int cycles, File[] inSOM, int[] somSizes, int[] somIterations, int timeUnits, int perspectiveIdx, FileWriter writer) throws IOException {
	
	float alphaValue = 0.04f;
	if (stage == 3) alphaValue = 0.03f;
	
	int radius = somSizes[perspectiveIdx] / 2;
	if (stage == 3) radius = somSizes[perspectiveIdx] / 5;
	
		String normalization = PERSPECTIVES[0];	
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			normalization = PERSPECTIVES[i];
//			for (int j = 0; j < PERSPECTIVES.length; j++) {
				if ((somSizes[perspectiveIdx] * somSizes[perspectiveIdx]) > SOM_THRESHOLD) {
					String newInput = "./data/" + SCHEMA + "/SOMaticIn/" + normalization + "_Normalized/" + PERSPECTIVES[perspectiveIdx] + "_" + SCENE + ".dat";
					String newOutput = "data/" + SCHEMA + "/test/" + PERSPECTIVES[perspectiveIdx] + "/" + cycles + "/" + normalization + "_Normalized";
					
//					int size1 = somIterations[perspectiveIdx];
					
					try {
						Files.createDirectories(Paths.get(newOutput + "/"));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}


//					SOMaticManager.runSOM_COD(newInput, inSOM[i], newOutput, somSizes[perspectiveIdx], somSizes[perspectiveIdx], (float) 0.04, somSizes[perspectiveIdx], 
//							somIterations[perspectiveIdx], DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR,0, PERSPECTIVES[perspectiveIdx], 
//							normalization, null, timeUnits);	
					
					SOMaticManager.runSOM_COD(newInput, inSOM[i], newOutput, somSizes[perspectiveIdx], somSizes[perspectiveIdx], alphaValue, radius, 
							somIterations[perspectiveIdx], DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR,0, PERSPECTIVES[perspectiveIdx], 
							normalization, writer, timeUnits, stage);	
					
					writer.flush();
				}
//			}
		}
		
		// flush the writer
//		writer.flush();
		
		// close the writer
//		writer.close();
}

	public static void bmuTesting(int[] somSizes, int[] somIterations, int timeUnits) throws IOException {
		
		DecimalFormat f = new DecimalFormat("##.00");
		
		double timeUnit;
		if (timeUnits == 2) {
			timeUnit = 6E10;
		} else if (timeUnits == 1) {
			timeUnit = 1E9;
		} else {
			timeUnit = 1;
		}
		
		try {
			Files.createDirectories(Paths.get("./data/test/" + NUM_TRAIN_ITERATION_CYCLES + "/"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// initialize BufferedReader for csv File
		BufferedReader br = new BufferedReader(new FileReader("./data/test/" + NUM_TRAIN_ITERATION_CYCLES + "/time_tests.csv"));
		String line = br.readLine();
		
		// FileWriter to write the output (CSV) file
		FileWriter writer = new FileWriter("./data/test/" + NUM_TRAIN_ITERATION_CYCLES + "/bmutime_tests.csv");
		line.replace("\n", "");
		writer.append(line + ",aqe,aqe_time\n");
		
		String normalization = PERSPECTIVES[0];	
		for (int i = 0; i < PERSPECTIVES.length; i++) {
//		for (int i = 0; i < 1; i++) {
			
			normalization = PERSPECTIVES[i];
			
			for (int j = 0; j < PERSPECTIVES.length; j++) {
//			for (int j = 0; j < 1; j++) {
				
				if ((somSizes[j] * somSizes[j]) > SOM_THRESHOLD) {
					
					for (int k = 0; k < 3; k++) {
//					for (int k = 0; k < 1; k++) {
						
						line = br.readLine();
						line.replace("\n", "");
						
						String datPath = "./data/" + SCHEMA + "/SOMaticIn/" + normalization + "_Normalized/" + PERSPECTIVES[j] + "_" + SCENE + ".dat";
						
						int stageFactor = 1;
						
						if (k == 1) stageFactor = 2;
						else if (k == 2) stageFactor = 5;
						
						String codPath = "./data/test/" + NUM_TRAIN_ITERATION_CYCLES + "/SOMaticOut/" + normalization + "_Normalized/" + PERSPECTIVES[j] + "/" + (somIterations[j] * stageFactor) + "/trainedSom.cod";
						
//						int stageFactor = 50;
////						
//						if (k == 1) stageFactor = 75;
//						else if (k == 2) stageFactor = 100;
//						String codPath = "./data/test/" + NUM_TRAIN_ITERATION_CYCLES + "/SOMaticOut/" + normalization + "_Normalized/" + PERSPECTIVES[j] + "/" + stageFactor + "/trainedSom.cod";
//						String codPath = "./data/test/" + NUM_TRAIN_ITERATION_CYCLES + "/SOMaticOut/" + normalization + "_Normalized/" + PERSPECTIVES[j] + "/trainedSom.cod";
						long startTime = System.nanoTime();
						
						float aqe = getAQE(datPath,codPath);
						
						long endTime = System.nanoTime();
						long duration = (endTime - startTime);
						double durationUnits = duration / timeUnit;
						String dur = "" + durationUnits;
						if (timeUnits == 1 || timeUnits == 2) {
							dur = f.format(durationUnits);
//							writer.append(perspective + "," + normalization + "," + iterations + ",1," + f.format(dur1) + "," + f.format(totalDur1) + "\n");
						} else {
//							writer.append(perspective + "," + normalization + "," + iterations + ",1," + dur1 + "," + totalDur1 + "\n");
						}
						
						writer.append(line + "," + aqe + "," + dur + "\n");
//						ArrayList<String> thing = getIV2BMU(datPath,codPath);
						
//						for (int n = 0; n < thing.size(); n++) {
//							
//						}
						
						
						
//						double dur1 = duration1 / timeUnit;
//						double totalDur1 = duration1 / timeUnit;
						
						
//						if (i == 0 && j == 0 && k == 0) {
//							System.out.println("AQE: " + getAQE(datPath,codPath));
//							for (int n = 0; n < thing.size(); n++) {
//								System.out.println(thing.get(n));
//							}
//						}
					}
				}
			}
		}
		
		br.close();
		
		// flush the writer
		writer.flush();
			
		// close the writer
		writer.close();
	}
	
	public static void bmuTesting2(int cycles, int[] somSizes, int[] somIterations, int timeUnits, int perspectiveIdx, FileWriter writer, BufferedReader br) throws IOException {
		
		DecimalFormat f = new DecimalFormat("##.00");
		
		double timeUnit;
		if (timeUnits == 2) {
			timeUnit = 6E10;
		} else if (timeUnits == 1) {
			timeUnit = 1E9;
		} else {
			timeUnit = 1;
		}
		
//		try {
//			Files.createDirectories(Paths.get("./data/test/" + cycles + "/"));
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		// initialize BufferedReader for csv File
//		BufferedReader br = new BufferedReader(new FileReader("./data/test/" + cycles + "/time_tests.csv"));
//		String line = br.readLine();
		String line = "";
		
		// FileWriter to write the output (CSV) file
//		FileWriter writer = new FileWriter("./data/test/" + cycles + "/bmutime_tests.csv");
//		line.replace("\n", "");
//		writer.append(line + ",aqe,aqe_time,te,te_time\n");
		
		String normalization = PERSPECTIVES[0];	
		for (int i = 0; i < PERSPECTIVES.length; i++) {
//		for (int i = 0; i < 1; i++) {
			
			normalization = PERSPECTIVES[i];
			
//			for (int j = 0; j < PERSPECTIVES.length; j++) {
//			for (int j = 0; j < 1; j++) {
				
				if ((somSizes[perspectiveIdx] * somSizes[perspectiveIdx]) > SOM_THRESHOLD) {
						
					line = br.readLine();
					line.replace("\n", "");
					
					String datPath = "./data/" + SCHEMA + "/SOMaticIn/" + normalization + "_Normalized/" + PERSPECTIVES[perspectiveIdx] + "_" + SCENE + ".dat";
										
					String codPath = "./data/" + SCHEMA + "/test/" + PERSPECTIVES[perspectiveIdx] + "/" + cycles + "/" + normalization + "_Normalized/trainedSom.cod";
					

					long startTime = System.nanoTime();
					
					float aqe = getAQE(datPath,codPath);

					long endTime = System.nanoTime();
					long duration = (endTime - startTime);
					double durationUnits = duration / timeUnit;
					String aqeDur = "" + durationUnits;
					if (timeUnits == 1 || timeUnits == 2) {
						aqeDur = f.format(durationUnits);
					}
					
					startTime = System.nanoTime();
					
					float topoError = getTE(datPath,codPath);
					
					endTime = System.nanoTime();
					duration = (endTime - startTime);
					durationUnits = duration / timeUnit;
					String teDur = "" + durationUnits;
					if (timeUnits == 1 || timeUnits == 2) {
						teDur = f.format(durationUnits);
					}
					
					
					writer.append(line + "," + cycles + "," + aqe + "," + aqeDur + "," + topoError + "," + teDur + "\n");
					writer.flush();
				}
//			}
		}

		
//		br.close();
		
		// flush the writer
//		writer.flush();
			
		// close the writer
//		writer.close();
	}

	public static ArrayList<String> getIV2BMU(String datFilePath, String codFilePath) {
		
		ArrayList<String> output = new ArrayList<String>();
		
		Dat dat = FileHandler.readDatFile(datFilePath,"L_AT");
		Cod cod = FileHandler.readCodFile(codFilePath,"L_AT");
		
		for (InputVector inputVector : dat.inputVectors) {
			float minDist = BestMatchingUnit.findBMU(inputVector, cod.neurons, DIST_MEASURE);
			output.add(inputVector.getName() + "," + (inputVector.getMatchingNeuronID()+1) + "," + minDist);
		}
		
		return output;
	}
	
	public static float getAQE(String datFilePath, String codFilePath) {
		
		ArrayList<String> output = new ArrayList<String>();
		
		Dat dat = FileHandler.readDatFile(datFilePath,"L_AT");
		Cod cod = FileHandler.readCodFile(codFilePath,"L_AT");
		
		float aqe = 0f;
		int aqeCount = 0;
		
		for (InputVector inputVector : dat.inputVectors) {
			float next = BestMatchingUnit.findBMU(inputVector, cod.neurons, DIST_MEASURE);
			
			if (!Float.isNaN(next)) {
				aqe += next;
				aqeCount++;
			}
//			aqe += BestMatchingUnit.findBMU(inputVector, cod.neurons, DIST_MEASURE);
//			float minDist = BestMatchingUnit.findBMU(inputVector, cod.neurons, DIST_MEASURE);
//			output.add(inputVector.getName() + "," + (inputVector.getMatchingNeuronID()+1) + "," + minDist);
		}
		
		aqe /= aqeCount;
		
		return aqe;
	}
	
	public static float getTE(String datFilePath, String codFilePath) {
		
		ArrayList<String> output = new ArrayList<String>();
		
		Dat dat = FileHandler.readDatFile(datFilePath,"L_AT");
		Cod cod = FileHandler.readCodFile(codFilePath,"L_AT");
		
		int numIncorrect = 0;
		
		for (InputVector inputVector : dat.inputVectors) {
			if (BestMatchingUnit.topographicErrorBMU(inputVector, cod, DIST_MEASURE) == false) {
				numIncorrect++;
			}
		}
		
		float incorrect = (float) numIncorrect;
		float ivLength = (float) dat.inputVectors.length;
		
		float topoError = incorrect / ivLength;
		
		return topoError;
	}
	
	public static void sendEmailAlert(boolean update, int cycles) {
		final String username = "schemppthesis@gmail.com";
		final String send2User = "tschempp@gmail.com";
        final String password = "G30gthesis";
        
        String updateMsg = "Update";
        String updateMsg2 = "ongoing";
        if (!update) {
        	updateMsg = "Complete";
        	updateMsg2 = "completed";
        }
        
        Properties props = new Properties();
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.host", "smtp.gmail.com");
        props.put("mail.smtp.port", "587");

        Session session = Session.getInstance(props,
          new javax.mail.Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password);
            }
          });

        try {

            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(username));
            message.setRecipients(Message.RecipientType.TO,
                InternetAddress.parse(send2User));
            message.setSubject("Computer Station " + computerStation + " Process " + updateMsg);
            String cycleString = "" + cycles;
            if (cycles == -1) {
            	if (CYCLES.length > 1) {
            		cycleString = "";
            		for (int i = 0; i < CYCLES.length; i++) {
            			cycleString = cycleString + " " + CYCLES[i];
            		}
            	}
            }
            message.setText("FYI,"
                + "\n\n The process at computer station " + computerStation + " is " + updateMsg2 + "!"
                + "\n\n Cycle: " + cycleString);
            
            Transport.send(message);

            System.out.println("Done");

        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }	
}
