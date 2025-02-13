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

public class Motlow_SOM_Creation {

	/* Store an ID for the computer that is performing the training */
	static int computerStation = 1;

	/* Select the appropriate training phase: 1, 2, or 3 */
	static int phase = 1;

	/* Select the number of training cycles */
	// final static int[] CYCLES = { 1, 2, 3, 4, 6, 8, 10, 12, 16, 24 };
	// I am changing this to 1, instead of 2 so that the time is shorter
	final static int[] CYCLES = { 1 };

	/*
	 * Units for time in output CSV: 1 = seconds 2 = minutes anything else =
	 * nanoseconds
	 */
	final static int TIME_UNITS = 2;

	/* Email Updates */
	final static boolean emailUpdate = false;

	/* Select the study area to train */
    // 0 = Port, 1 = Cali, 2 = Afri 3 = All
    // first attempt was on Cali, looked like this: static int studyArea = 1;
	static int studyArea = 3
    ;

	// commment out for now
	// -----------------------------------------------------
	// Scene name list
	final static String[] SCENES = { "Port", "Cali", "Afri", "All_2" };
	final static String SCENE = SCENES[studyArea];

	// commment out for now
	// -----------------------------------------------------
	// Schema list
	final static String[] SCHEMAS = { "Port", "Cali", "Afri", "All_2" };
	final static String SCHEMA = SCHEMAS[studyArea];

	// Tri-Space perspectives
	final static String[] PERSPECTIVES = { "L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L" };
	// final static String[] PERSPECTIVES = { "T_LA", "L_AT", "A_LT", "LA_T", "LT_A", "AT_L" };

	/* Tri-Space perspectives to train SOMs for */
	// final static int[] TRAINING_PERSPECTIVES = { 0, 3, 4 };
	// final static int[] TRAINING_NORMALIZATIONS = { 0, 1, 2, 3, 4, 5 };
    // I think the 3 corresponds to LA_T in the array PERSPECTIVES
    // Testing by changing the 3 in TRAINING_PERSPECTIVES = { 3 } to 4
	final static int[] TRAINING_PERSPECTIVES = { 0 };
    // switch the 5 to 0(0 is L_AT)
	// final static int[] TRAINING_NORMALIZATIONS = { 0 };
	// switch the 0 to 4(4 is LT_A)
	final static int[] TRAINING_NORMALIZATIONS = { 0 };


	// Nodata value
	final static int NO_DATA = -9999;

	// Tri-Space element count initialized - computed in program
	static int lociCount = -1;
	static int attributeCount = -1;
	static int timeCount = -1;

	// threshold of input vector count to determine if SOM or MDS
	final static int SOM_THRESHOLD = 100;
	// upper limit on SOM resolution
	final static int SOM_NEURON_CAP = 250000;

	// Set normalization boundaries: 0-1; 0.1-0.9
	final static float MIN_NORMALIZATION = 0.1f;
	final static float MAX_NORMALIZATION = 0.9f;

	// Distance measures
	final static int EUCLIDEAN = 1;
	final static int COSINE = 2;
	final static int MANHATTAN = 3;
	final static int DIST_MEASURE = COSINE;

	// Geographic Coordinate System OR Projection
	final static String EPSG = "4326";

	// Number of threads for training
	final static int NTHREADS = 1;

	// Use rounding
	final static boolean ROUNDING = true;

	// Scaling for SOM geometry (Kowatsch)
	final static int SCALING_FACTOR = 1;

	// Compute kmeans from 2 to MAX
	final static int MAX_K = 12;

	// Maximum number of iterations per k-class computation
	final static int K_ITERATIONS = 1000;

    public static void main(String[] args) throws IOException {
		Som_info("C:/Users/wesle/Desktop/Thesis_files/Github/TriSpace_Prep/data/motlow/completed_som_trainings/10k/ALL_2/SOMaticIn/L_AT_Normalized/L_AT.dat", "./data/motlow/completed_som_trainings/10k/ALL_2/test/L_AT/L_AT_1/1/L_AT_Normalized/trainedSom.cod", "./data/motlow/completed_som_trainings/10k/ALL_2/test/L_AT/L_AT_1/1/L_AT_Normalized/IVs.csv", "./data/motlow/completed_som_trainings/10k/ALL_2/test/L_AT/L_AT_1/1/L_AT_Normalized/Training_bmus.csv");
		// "data\motlow\ALL\SOMaticIn\A_LT_Normalized\L_AT.dat"
		// "data\motlow\ALL\test\L_AT\L_AT_1\1\L_AT_Normalized\trainedSom.cod"
		// repeatTraining(3);
		System.out.println("Exiting Program");
		System.exit(0);
	}

    public static void regularTraining() throws IOException {
		// if (emailUpdate)
		// 	sendEmailStart();

		lociCount = CSVManager.rowCount("./data/motlow/" + SCHEMA + "/trispace/dictionaries/locusDictionary.csv");
		attributeCount = CSVManager
				.rowCount("./data/motlow/" + SCHEMA + "/trispace/dictionaries/attributeDictionary.csv");

		timeCount = CSVManager.rowCount("./data/motlow/" + SCHEMA + "/trispace/dictionaries/timeDictionary.csv");

		int[] somSizes = SOMaticManager.getSOMDimension(lociCount, attributeCount, timeCount, SOM_NEURON_CAP);

		performTraining(phase, CYCLES, TRAINING_PERSPECTIVES, TRAINING_NORMALIZATIONS, somSizes);

		computeError(CYCLES, TRAINING_PERSPECTIVES, TRAINING_NORMALIZATIONS, somSizes);

		// if (emailUpdate)
		// 	sendEmailAlert(false, -1);
	}

	
	// this is where I can create a som from
	// -----------------------------------------------------
	public static void performTraining(int trainingPhase, int[] trainingCycles, int[] trainingPerspectives,
			int[] trainingNormalizations, int[] somSize) throws IOException {

		// iterate thru all training perspectives
		for (int i = 0; i < trainingPerspectives.length; i++) {

			// set each training perspective to the current perspective index
			int currentPerspective = trainingPerspectives[i];

			// create the path if it does not already exist
			Files.createDirectories(Paths.get("./data/motlow/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/"));

			// initialize array to store previous COD files
			File[] codFiles = null;

			// if training phase > 1 then populate COD file array
			if (trainingPhase == 2 || trainingPhase == 3) {
				codFiles = new File[trainingNormalizations.length];
				for (int j = 0; j < codFiles.length; j++) {
					codFiles[j] = new File("data/motlow/" + SCHEMA + "/SOMaticOut/" + PERSPECTIVES[trainingNormalizations[j]]
							+ "_Normalized/" + PERSPECTIVES[currentPerspective] + "/trainedSom.cod");
				}
			}

			// FileWriter to write the output (CSV) file
			FileWriter writer = new FileWriter(
					"./data/motlow/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/time_tests.csv");
			// write headers
			writer.append("perspective,normalization,iterations,phase,time,total_time\n");

			// iterate thru all training cycles
			for (int j = 0; j < trainingCycles.length; j++) {

				// compute number of iterations from cycles
				int[] somIterations = SOMaticManager.getSOMIterations(trainingCycles[j], lociCount, attributeCount,
						timeCount);

				// compute alpha value
				float alphaValue = 0.04f;
				if (trainingPhase == 2)
					alphaValue = 0.03f;
				else if (trainingPhase == 3)
					alphaValue = 0.02f;

				// compute radius value
				int radius = somSize[currentPerspective];
				if (trainingPhase == 2)
					radius = somSize[currentPerspective] / 2;
				else if (trainingPhase == 3)
					radius = somSize[currentPerspective] / 5;

				// iterate thru all normalizations
				for (int k = 0; k < trainingNormalizations.length; k++) {

					// store the current normalization
					String normalization = PERSPECTIVES[trainingNormalizations[k]];

					// test to ensure perspective should be a SOM
					if ((somSize[currentPerspective] * somSize[currentPerspective]) > SOM_THRESHOLD) {

						// variable to store the input location
						String input = "./data/motlow/" + SCHEMA + "/SOMaticIn/" + normalization + "_Normalized/"
								+ PERSPECTIVES[currentPerspective] + ".dat";

						// variable to store the output location
						String output = "data/motlow/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/"
								+ trainingCycles[j] + "/" + normalization + "_Normalized";

						Files.createDirectories(Paths.get(output + "/"));

						if (trainingPhase == 1) {
							SOMaticManager.runSOM_COD(input, output, somSize[currentPerspective],
									somSize[currentPerspective], alphaValue, radius, somIterations[currentPerspective],
									DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR,
									PERSPECTIVES[currentPerspective], normalization, writer, TIME_UNITS, trainingPhase);
						} else if (trainingPhase == 2 || trainingPhase == 3) {
							SOMaticManager.runSOM_COD(input, codFiles[i], output, somSize[currentPerspective],
									somSize[currentPerspective], alphaValue, radius, somIterations[currentPerspective],
									DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR,
									PERSPECTIVES[currentPerspective], normalization, writer, TIME_UNITS, trainingPhase);
						}

						writer.flush();
					}

				}

				// if (emailUpdate && trainingCycles.length > 1) {
				// 	sendEmailAlert(true, trainingCycles[j]);
				// }
			}
			writer.close();
		}
	}

    public static void computeError(int[] trainingCycles, int[] trainingPerspectives, int[] trainingNormalizations,
			int[] somSize) throws IOException {

		DecimalFormat f = new DecimalFormat("##.00");

		double timeUnit;
		if (TIME_UNITS == 2) {
			timeUnit = 6E10;
		} else if (TIME_UNITS == 1) {
			timeUnit = 1E9;
		} else {
			timeUnit = 1;
		}

		// iterate thru all training perspectives
		for (int i = 0; i < trainingPerspectives.length; i++) {

			// set each training perspective to the current perspective index
			int currentPerspective = trainingPerspectives[i];

			// FileWriter to write the output (CSV) file
			FileWriter writer = new FileWriter(
					"./data/motlow/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/bmutime_tests.csv");
			// write headers
			writer.append(
					"perspective,normalization,iterations,phase,time,total_time,cycles,aqe,aqe_time,te,te_time\n");

			// buffered reader to parse the training output
			BufferedReader br = new BufferedReader(new FileReader(
					"./data/motlow/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/time_tests.csv"));
			String line = br.readLine();

			for (int j = 0; j < trainingCycles.length; j++) {

				// iterate thru all normalizations
				for (int k = 0; k < trainingNormalizations.length; k++) {

					// store the current normalization
					String normalization = PERSPECTIVES[trainingNormalizations[k]];

					// test to ensure perspective should be a SOM
					if ((somSize[currentPerspective] * somSize[currentPerspective]) > SOM_THRESHOLD) {

						// read next line
						line = br.readLine();

						// replace new line char (temporarily) to append new data
						line.replace("\n", "");

						// path to DAT file
						String datPath = "./data/motlow/" + SCHEMA + "/SOMaticIn/" + normalization + "_Normalized/"
								+ PERSPECTIVES[currentPerspective] + ".dat";
						// path to COD file
						String codPath = "./data/motlow/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/"
								+ trainingCycles[j] + "/" + normalization + "_Normalized/trainedSom.cod";

								

						// store starting time for AQE calculation
						long startTime = System.nanoTime();

						// compute AQE
						float aqe = getAQE(datPath, codPath);

						// store ending time for AQE calculation
						long endTime = System.nanoTime();

						// compute duration
						long duration = (endTime - startTime);

						// convert duration into TIME_UNITS format
						double durationUnits = duration / timeUnit;
						String aqeDur = "" + durationUnits;
						if (TIME_UNITS == 1 || TIME_UNITS == 2) {
							aqeDur = f.format(durationUnits);
						}

						// store starting time for Topo Error calculation
						startTime = System.nanoTime();

						// compute Topographic Error
						float topoError = getTE(datPath, codPath);

						// store the ending time for TE calculation
						endTime = System.nanoTime();

						// compute duration
						duration = (endTime - startTime);

						// convert duration into TIME_UNITS format
						durationUnits = duration / timeUnit;
						String teDur = "" + durationUnits;
						if (TIME_UNITS == 1 || TIME_UNITS == 2) {
							teDur = f.format(durationUnits);
						}

						// append to writer
						writer.append(line + "," + trainingCycles[j] + "," + aqe + "," + aqeDur + "," + topoError + ","
								+ teDur + "\n");
						// flush writer
						writer.flush();
					}
				}
			}

			// close writer
			writer.close();

			// close buffered reader
			br.close();
		}
	}

    public static void repeatTraining(int repetitions) throws IOException {
		// if (emailUpdate)
		// 	sendEmailStart();

		lociCount = CSVManager.rowCount("./data/motlow/" + SCHEMA + "/trispace/dictionaries/locusDictionary.csv");
		attributeCount = CSVManager
				.rowCount("./data/motlow/" + SCHEMA + "/trispace/dictionaries/attributeDictionary.csv");

		timeCount = CSVManager.rowCount("./data/motlow/" + SCHEMA + "/trispace/dictionaries/timeDictionary.csv");

		int[] somSizes = SOMaticManager.getSOMDimension(lociCount, attributeCount, timeCount, SOM_NEURON_CAP);

		for (int i = 1; i <= repetitions; i++) {
			performTraining(phase, CYCLES, TRAINING_PERSPECTIVES, TRAINING_NORMALIZATIONS, somSizes, i);
		}

		for (int i = 1; i <= repetitions; i++) {
			computeError(CYCLES, TRAINING_PERSPECTIVES, TRAINING_NORMALIZATIONS, somSizes, i);
		}

		// if (emailUpdate)
		// 	sendEmailAlert(false, -1);
	}


	public static void performTraining(int trainingPhase, int[] trainingCycles, int[] trainingPerspectives,
			int[] trainingNormalizations, int[] somSize, int repeatCycle) throws IOException {

		// iterate thru all training perspectives
		for (int i = 0; i < trainingPerspectives.length; i++) {

			// set each training perspective to the current perspective index
			int currentPerspective = trainingPerspectives[i];

			// create the path if it does not already exist
			Files.createDirectories(Paths.get("./data/motlow/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/"));

			// initialize array to store previous COD files
			File[] codFiles = null;

			// if training phase > 1 then populate COD file array
			if (trainingPhase == 2 || trainingPhase == 3) {
				codFiles = new File[trainingNormalizations.length];

				// store the current normalization
				String normalization = PERSPECTIVES[trainingNormalizations[i]];

				for (int j = 0; j < codFiles.length; j++) {
					// codFiles[j] = new File("data/motlow/" + SCHEMA + "/SOMaticOut/" + PERSPECTIVES[trainingNormalizations[j]]
					// 		+ "_Normalized/" + PERSPECTIVES[currentPerspective] + "/trainedSom.cod");
					codFiles[j] = new File("data/motlow/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/"
								+ PERSPECTIVES[currentPerspective] + "_" + repeatCycle + "/" + trainingCycles[j] + "/"
								+ normalization + "_Normalized" + "/trainedSom.cod");

							
				}
			}

			String timeTests = "./data/motlow/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/time_tests.csv";
			FileWriter writer = null;
			File existanceTest = new File(timeTests);

			if (existanceTest.exists()) {
				writer = new FileWriter(timeTests, true);
			} else {
				writer = new FileWriter(timeTests);
				// write headers
				writer.append("repeat_cycle,perspective,normalization,iterations,phase,time,total_time\n");
			}

			// iterate thru all training cycles
			for (int j = 0; j < trainingCycles.length; j++) {

				// compute number of iterations from cycles
				int[] somIterations = SOMaticManager.getSOMIterations(trainingCycles[j], lociCount, attributeCount,
						timeCount);

				// compute alpha value
				float alphaValue = 0.04f;
				if (trainingPhase == 2)
					alphaValue = 0.03f;
				else if (trainingPhase == 3)
					alphaValue = 0.02f;

				// compute radius value
				int radius = somSize[currentPerspective];
				if (trainingPhase == 2)
					radius = somSize[currentPerspective] / 2;
				else if (trainingPhase == 3)
					radius = somSize[currentPerspective] / 5;

				// iterate thru all normalizations
				for (int k = 0; k < trainingNormalizations.length; k++) {

					// store the current normalization
					String normalization = PERSPECTIVES[trainingNormalizations[k]];

					// test to ensure perspective should be a SOM
					if ((somSize[currentPerspective] * somSize[currentPerspective]) > SOM_THRESHOLD) {

						// variable to store the input location
						String input = "./data/motlow/" + SCHEMA + "/SOMaticIn/" + normalization + "_Normalized/"
								+ PERSPECTIVES[currentPerspective] + ".dat";

						// variable to store the output location
						String output = "data/motlow/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/"
								+ PERSPECTIVES[currentPerspective] + "_" + repeatCycle + "/" + trainingCycles[j] + "/"
								+ normalization + "_Normalized";

						Files.createDirectories(Paths.get(output + "/"));

						writer.append(repeatCycle + ",");

						if (trainingPhase == 1) {
							SOMaticManager.runSOM_COD(input, output, somSize[currentPerspective],
									somSize[currentPerspective], alphaValue, radius, somIterations[currentPerspective],
									DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR,
									PERSPECTIVES[currentPerspective], normalization, writer, TIME_UNITS, trainingPhase);
						} else if (trainingPhase == 2 || trainingPhase == 3) {
							SOMaticManager.runSOM_COD(input, codFiles[i], output, somSize[currentPerspective],
									somSize[currentPerspective], alphaValue, radius, somIterations[currentPerspective],
									DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR,
									PERSPECTIVES[currentPerspective], normalization, writer, TIME_UNITS, trainingPhase);
						}

						writer.flush();
					}

				}

				// if (emailUpdate && trainingCycles.length > 1) {
				// 	sendEmailAlert(true, trainingCycles[j]);
				// }
			}
			writer.close();
		}
	}

    public static void computeError(int[] trainingCycles, int[] trainingPerspectives, int[] trainingNormalizations,
			int[] somSize, int repeatCycle) throws IOException {

		DecimalFormat f = new DecimalFormat("##.00");

		double timeUnit;
		if (TIME_UNITS == 2) {
			timeUnit = 6E10;
		} else if (TIME_UNITS == 1) {
			timeUnit = 1E9;
		} else {
			timeUnit = 1;
		}

		// iterate thru all training perspectives
		for (int i = 0; i < trainingPerspectives.length; i++) {

			// set each training perspective to the current perspective index
			int currentPerspective = trainingPerspectives[i];

			String bmuTests = "./data/motlow/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/bmutime_tests.csv";

			// FileWriter to write the output (CSV) file;
			FileWriter writer = null;
			File existanceTest = new File(bmuTests);

			if (existanceTest.exists()) {
				writer = new FileWriter(bmuTests, true);
			} else {
				writer = new FileWriter(bmuTests);
				// write headers
				writer.append(
						"repeat_cycle,perspective,normalization,iterations,phase,time,total_time,cycles,aqe,aqe_time,te,te_time\n");
			}

			for (int j = 0; j < trainingCycles.length; j++) {

				// iterate thru all normalizations
				for (int k = 0; k < trainingNormalizations.length; k++) {

					// store the current normalization
					String normalization = PERSPECTIVES[trainingNormalizations[k]];

					// test to ensure perspective should be a SOM
					if ((somSize[currentPerspective] * somSize[currentPerspective]) > SOM_THRESHOLD) {

						// buffered reader to parse the training output
						BufferedReader br = new BufferedReader(new FileReader(
								"./data/motlow/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/time_tests.csv"));
						String line = br.readLine();
						while ((line = br.readLine()) != null) {
							String[] lineSplit = line.split(",");

							int rptCycle = Integer.parseInt(lineSplit[0]);

							String persp = lineSplit[1];
							String norm = lineSplit[2];

							if (PERSPECTIVES[currentPerspective].equals(persp) && normalization.equals(norm)
									&& rptCycle == repeatCycle)
								break;
						}

						// read next line
						// line = br.readLine();

						// replace new line char (temporarily) to append new data
						line.replace("\n", "");

						// path to DAT file
						String datPath = "./data/motlow/" + SCHEMA + "/SOMaticIn/" + normalization + "_Normalized/"
								+ PERSPECTIVES[currentPerspective] + ".dat";
						// path to COD file
						String codPath = "./data/motlow/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/"
								+ PERSPECTIVES[currentPerspective] + "_" + repeatCycle + "/" + trainingCycles[j] + "/"
								+ normalization + "_Normalized/trainedSom.cod";

						// store starting time for AQE calculation
						long startTime = System.nanoTime();

						// compute AQE
						// float aqe = getAQE(datPath, codPath);

						// store ending time for AQE calculation
						long endTime = System.nanoTime();

						// compute duration
						long duration = (endTime - startTime);

						// convert duration into TIME_UNITS format
						double durationUnits = duration / timeUnit;
						String aqeDur = "" + durationUnits;
						if (TIME_UNITS == 1 || TIME_UNITS == 2) {
							aqeDur = f.format(durationUnits);
						}

						// store starting time for Topo Error calculation
						startTime = System.nanoTime();

						// compute Topographic Error
						// float topoError = getTE(datPath, codPath);

						// store the ending time for TE calculation
						endTime = System.nanoTime();

						// compute duration
						duration = (endTime - startTime);

						// convert duration into TIME_UNITS format
						durationUnits = duration / timeUnit;
						String teDur = "" + durationUnits;
						if (TIME_UNITS == 1 || TIME_UNITS == 2) {
							teDur = f.format(durationUnits);
						}

						// append to writer
						// writer.append(line + "," + trainingCycles[j] + "," + aqe + "," + aqeDur + "," + topoError + ","
						// 		+ teDur + "\n");
						// flush writer
						writer.flush();

						// close buffered reader
						br.close();
					}
				}
			}

			// close writer
			writer.close();
		}
	}

    // public static ArrayList<String> getIV2BMU(String datFilePath, String codFilePath) {

	// 	ArrayList<String> output = new ArrayList<String>();

	// 	Dat dat = FileHandler.readDatFile(datFilePath, "L_AT");
	// 	Cod cod = FileHandler.readCodFile(codFilePath, "L_AT");

	// 	for (InputVector inputVector : dat.inputVectors) {
	// 		float minDist = BestMatchingUnit.findBMU(inputVector, cod.neurons, DIST_MEASURE);
	// 		output.add(inputVector.getName() + "," + (inputVector.getMatchingNeuronID() + 1) + "," + minDist);
	// 	}

	// 	return output;
	// }

    // public static float getAQE(String datFilePath, String codFilePath) {

	// 	ArrayList<String> output = new ArrayList<String>();

	// 	Dat dat = FileHandler.readDatFile(datFilePath, "L_AT");
	// 	Cod cod = FileHandler.readCodFile(codFilePath, "L_AT");

	// 	float aqe = 0f;
	// 	int aqeCount = 0;

	// 	for (InputVector inputVector : dat.inputVectors) {
	// 		float next = BestMatchingUnit.findBMU(inputVector, cod.neurons, DIST_MEASURE);

	// 		if (!Float.isNaN(next)) {
	// 			aqe += next;
	// 			aqeCount++;
	// 		}
	// 		// aqe += BestMatchingUnit.findBMU(inputVector, cod.neurons, DIST_MEASURE);
	// 		// float minDist = BestMatchingUnit.findBMU(inputVector, cod.neurons,
	// 		// DIST_MEASURE);
	// 		// output.add(inputVector.getName() + "," +
	// 		// (inputVector.getMatchingNeuronID()+1) + "," + minDist);
	// 	}

	// 	aqe /= aqeCount;

	// 	return aqe;
	// }

    // public static float getTE(String datFilePath, String codFilePath) {

	// 	ArrayList<String> output = new ArrayList<String>();

	// 	Dat dat = FileHandler.readDatFile(datFilePath, "L_AT");
	// 	Cod cod = FileHandler.readCodFile(codFilePath, "L_AT");

	// 	int numIncorrect = 0;

	// 	for (InputVector inputVector : dat.inputVectors) {
	// 		if (BestMatchingUnit.topographicErrorBMU(inputVector, cod, DIST_MEASURE) == false) {
	// 			numIncorrect++;
	// 		}
	// 	}

	// 	float incorrect = (float) numIncorrect;
	// 	float ivLength = (float) dat.inputVectors.length;

	// 	float topoError = incorrect / ivLength;

	// 	return topoError;
	// }


	public static void Som_info(String datFilePath, String codFilePath, String iV2BMU, String bMU2IV) {
        Dat dat = FileHandler.readDatFile(datFilePath, "L_AT");
        Cod cod = FileHandler.readCodFile(codFilePath, "L_AT");
        Neuron[] neurons = cod.neurons;
        InputVector[] inputVectors = dat.inputVectors;
        ArrayList<String> iVList = new ArrayList<String>();
        
        try {
			FileWriter writerIV = new FileWriter(iV2BMU);
	        for (InputVector inputVector : dat.inputVectors) {
	//        	System.out.println("IV to BMU");
	        	iVList.add(inputVector.getName().replace(",", "_"));
	            BestMatchingUnit.findBMU(inputVector, cod.neurons);
	            writerIV.append(inputVector.getName());
	            writerIV.append(',');
	            writerIV.append("" + (inputVector.getMatchingNeuronID()+1));
	            writerIV.append('\n');
//	            System.out.println(inputVector.getName().replaceAll(",", "_"));
//	            System.out.println(inputVector.getMatchingNeuronID());
	        }
	        writerIV.flush();
			writerIV.close();
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        int maxHits = 0;
        System.out.println("Number of Neurons: " + neurons.length);
        // ArrayList<KMeans> kMeansTotal = new ArrayList<KMeans>();
        // for (int j = 2; j <= kValue; j++) {
        // 	KMeans kMeansClustering = null;
        //     boolean kMeansComplete = false;
        //     while (!kMeansComplete) {
        //     	try {
        //         	kMeansClustering = new KMeans(j,neurons);
        //         	kMeansComplete = true;
        //         } catch (IndexOutOfBoundsException e) {
        //             System.out.println("Invalid option");
        //             kMeansComplete = false;
        //         }
            // }
            // kMeansTotal.add(kMeansClustering);
        // }
        // ArrayList<ArrayList<ArrayList<Integer>>> allVals = new ArrayList<ArrayList<ArrayList<Integer>>>();
        // for (int j = 0; j < kMeansTotal.size(); j++) {
        // 	ArrayList<ArrayList<Integer>> totalVals = new ArrayList<ArrayList<Integer>>();
        // 	for (int i = 0; i < kMeansTotal.get(j).kMeansClusters.length; i++) {
        //     	totalVals.add(kMeansTotal.get(j).kMeansClusters[i].memberIDs);
        //     }
        // 	allVals.add(totalVals);
        	
        // }
        
        // try {
        // 	FileWriter writerSSE = new FileWriter(sSE_CSV);
        // 	for (int j = 0; j < kMeansTotal.size(); j++) {
        // 		writerSSE.append(""+kMeansTotal.get(j).calcSSE(kMeansTotal.get(j).distMatrix, kMeansTotal.get(j).clusterMatrix));
        // 		if (j < kMeansTotal.size()-1) {
        // 			writerSSE.append('\n');
        // 		}        		
        //     }
        // 	writerSSE.flush();
	    //     writerSSE.close();
        // }  catch (IOException e) {
		// 	// TODO Auto-generated catch block
		// 	e.printStackTrace();
		// }
        
        
        
        
//        ArrayList<Integer> vals = kMeansClustering.kMeansClusters[0].memberIDs;
        try {
			FileWriter writerBMU = new FileWriter(bMU2IV);
		
	        for (Neuron neuron : cod.neurons) {
	            ArrayList<Integer> hits = neuron.getMatchingVectorIDs();
	            if (hits != null && hits.size() > maxHits) {
	                maxHits = hits.size();
	            }
	            String[] mVIDs = new String[neuron.getMatchingVectorIDs().size()];
	           
	            for (int ii = 0; ii < neuron.getMatchingVectorIDs().size(); ii++) {
//	            	if (targetData == "L_AT") {
//	            		mVIDs[ii] = "P" + (neuron.getMatchingVectorIDs().get(ii)+1);
//	            	} else if (targetData == "A_LT") {
//	            		mVIDs[ii] = "Band" + (neuron.getMatchingVectorIDs().get(ii)+1);
//	            	} else {
	            		mVIDs[ii] = "" + iVList.get(neuron.getMatchingVectorIDs().get(ii));
//	            	}
	            	
//	            	mVIDs[ii] = "" + neuron.getMatchingVectorIDs().get(ii);
	            }
	            int neuronID = neuron.getID()+1;
	            writerBMU.append("" + neuronID);
	            writerBMU.append(',');
	            for (int ii = 0; ii < neuron.getMatchingVectorIDs().size(); ii++) {
	            	writerBMU.append(mVIDs[ii]);
	            	if (ii < mVIDs.length-1) {
	            		writerBMU.append(" ");
	            	}
	            }
	            // ArrayList<Integer> kClusters = new ArrayList<Integer>();
	            // for (int ii = 2; ii <= kValue; ii++) {
	            // 	kClusters.add(-1);
	            // }
//	            int kCluster = -1;
// 	            for (int kk = 0; kk < allVals.size(); kk++) {
// 	            	for (int ii = 0; ii < allVals.get(kk).size(); ii++) {
// 	            		for (int jj = 0; jj < allVals.get(kk).get(ii).size(); jj++) {
// 	            			if (allVals.get(kk).get(ii).get(jj) == neuronID) {
// 	            				kClusters.set(kk,  ii);
// 	            				break;
// //	            				set(kk) = ii;
// 	            			}
// 	            		}
// 	            	}
// 	            }
	            // for (int kk = 0; kk < kClusters.size(); kk++) {
	            // 	writerBMU.append(',');
		        //     writerBMU.append("" + (kClusters.get(kk)+1));
	            // }
//	            for (int ii = 0; ii < totalVals.size(); ii++) {
//	            	for (int iii = 0; iii < totalVals.get(ii).size(); iii++) {
//	            		if (totalVals.get(ii).get(iii) == neuronID) {
//	            			kCluster = ii;
//	            			break;
//	            		}
//	            	}
//	            }
//	            writerBMU.append(',');
//	            writerBMU.append("" + (kCluster+1));
//	            writerBMU.append(mVIDs);
	            writerBMU.append('\n');
	//            if (neuron.getID() == 0) {
	//            	System.out.println(neuron.getID());
	//            	System.out.println(neuron.getMatchingVectorIDs());
	//            }
	//            System.out.println(neuron.getID());
	//            System.out.println(neuron.getMatchingVectorIDs());
	        }
	        writerBMU.flush();
	        writerBMU.close();
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
        
//        kMeansClustering.
    }
	

}


class BestMatchingUnit {

    public static void findBMU(InputVector inputVector, Neuron[] neurons) {
        float minDistance = neurons[0].getDistance(inputVector.getAttributes(), 2);
        Neuron bmu = neurons[0];
        for (int i = 1; i < neurons.length; i++) {
            Neuron neuron = neurons[i];
            float dist = neuron.getDistance(inputVector.getAttributes(),2);
            if (dist < minDistance) {
                minDistance = dist;
                bmu = neuron;
            }
        }
        bmu.addMatchingVectorID(inputVector.getID());
        inputVector.setMatchingNeuronID(bmu.getID());
    }
}