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

public class SOM_Creation_Final {

	/* Store an ID for the computer that is performing the training */
	static int computerStation = 1;

	/* Select the appropriate training phase: 1, 2, or 3 */
	static int phase = 1;

	/* Select the number of training cycles */
	// final static int[] CYCLES = { 1, 2, 3, 4, 6, 8, 10, 12, 16, 24 };
	final static int[] CYCLES = { 2 };

	/*
	 * Units for time in output CSV: 1 = seconds 2 = minutes anything else =
	 * nanoseconds
	 */
	final static int TIME_UNITS = 2;

	/* Email Updates */
	final static boolean emailUpdate = false;

	/* Select the study area to train */
	static int studyArea = 1;

	// commment out for now
	// -----------------------------------------------------
	// Scene name list
	final static String[] SCENES = { "SanElijo", "CocosFire", "Batiquitos" };
	final static String SCENE = SCENES[studyArea];

	// commment out for now
	// -----------------------------------------------------
	// Schema list
	final static String[] SCHEMAS = { "sanelijo", "cocos", "batiquitos" };
	final static String SCHEMA = SCHEMAS[studyArea];

	// Tri-Space perspectives
	final static String[] PERSPECTIVES = { "L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L" };

	/* Tri-Space perspectives to train SOMs for */
	// final static int[] TRAINING_PERSPECTIVES = { 0, 3, 4 };
	// final static int[] TRAINING_NORMALIZATIONS = { 0, 1, 2, 3, 4, 5 };
	final static int[] TRAINING_PERSPECTIVES = { 3 };
	final static int[] TRAINING_NORMALIZATIONS = { 5 };

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

		repeatTraining(3);
		System.out.println("Exiting Program");
		System.exit(0);
	}

	public static void regularTraining() throws IOException {
		if (emailUpdate)
			sendEmailStart();

		lociCount = CSVManager.rowCount("./data/" + SCHEMA + "/tables/Non_normalized/dictionaries/lociDictionary.csv");
		attributeCount = CSVManager
				.rowCount("./data/" + SCHEMA + "/tables/Non_normalized/dictionaries/attrDictionary.csv");

		timeCount = CSVManager.rowCount("./data/" + SCHEMA + "/tables/Non_normalized/dictionaries/timeDictionary.csv");

		int[] somSizes = SOMaticManager.getSOMDimension(lociCount, attributeCount, timeCount, SOM_NEURON_CAP);

		performTraining(phase, CYCLES, TRAINING_PERSPECTIVES, TRAINING_NORMALIZATIONS, somSizes);

		computeError(CYCLES, TRAINING_PERSPECTIVES, TRAINING_NORMALIZATIONS, somSizes);

		if (emailUpdate)
			sendEmailAlert(false, -1);
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
			Files.createDirectories(Paths.get("./data/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/"));

			// initialize array to store previous COD files
			File[] codFiles = null;

			// if training phase > 1 then populate COD file array
			if (trainingPhase == 2 || trainingPhase == 3) {
				codFiles = new File[trainingNormalizations.length];
				for (int j = 0; j < codFiles.length; j++) {
					codFiles[j] = new File("data/" + SCHEMA + "/SOMaticOut/" + PERSPECTIVES[trainingNormalizations[j]]
							+ "_Normalized/" + PERSPECTIVES[currentPerspective] + "/trainedSom.cod");
				}
			}

			// FileWriter to write the output (CSV) file
			FileWriter writer = new FileWriter(
					"./data/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/time_tests.csv");
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
						String input = "./data/" + SCHEMA + "/SOMaticIn/" + normalization + "_Normalized/"
								+ PERSPECTIVES[currentPerspective] + "_" + SCENE + ".dat";

						// variable to store the output location
						String output = "data/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/"
								+ trainingCycles[j] + "/" + normalization + "_Normalized";

						Files.createDirectories(Paths.get(output + "/"));

						if (trainingPhase == 1) {
							SOMaticManager.runSOM_COD(input, output, somSize[currentPerspective],
									somSize[currentPerspective], alphaValue, radius, somIterations[currentPerspective],
									DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR, 0,
									PERSPECTIVES[currentPerspective], normalization, writer, TIME_UNITS, trainingPhase);
						} else if (trainingPhase == 2 || trainingPhase == 3) {
							SOMaticManager.runSOM_COD(input, codFiles[i], output, somSize[currentPerspective],
									somSize[currentPerspective], alphaValue, radius, somIterations[currentPerspective],
									DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR, 0,
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
					"./data/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/bmutime_tests.csv");
			// write headers
			writer.append(
					"perspective,normalization,iterations,phase,time,total_time,cycles,aqe,aqe_time,te,te_time\n");

			// buffered reader to parse the training output
			BufferedReader br = new BufferedReader(new FileReader(
					"./data/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/time_tests.csv"));
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
						String datPath = "./data/" + SCHEMA + "/SOMaticIn/" + normalization + "_Normalized/"
								+ PERSPECTIVES[currentPerspective] + "_" + SCENE + ".dat";
						// path to COD file
						String codPath = "./data/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/"
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
		if (emailUpdate)
			sendEmailStart();

		lociCount = CSVManager.rowCount("./data/" + SCHEMA + "/tables/Non_normalized/dictionaries/lociDictionary.csv");
		attributeCount = CSVManager
				.rowCount("./data/" + SCHEMA + "/tables/Non_normalized/dictionaries/attrDictionary.csv");

		timeCount = CSVManager.rowCount("./data/" + SCHEMA + "/tables/Non_normalized/dictionaries/timeDictionary.csv");

		int[] somSizes = SOMaticManager.getSOMDimension(lociCount, attributeCount, timeCount, SOM_NEURON_CAP);

		for (int i = 1; i <= repetitions; i++) {
			performTraining(phase, CYCLES, TRAINING_PERSPECTIVES, TRAINING_NORMALIZATIONS, somSizes, i);
		}

		for (int i = 1; i <= repetitions; i++) {
			computeError(CYCLES, TRAINING_PERSPECTIVES, TRAINING_NORMALIZATIONS, somSizes, i);
		}

		if (emailUpdate)
			sendEmailAlert(false, -1);
	}

	public static void performTraining(int trainingPhase, int[] trainingCycles, int[] trainingPerspectives,
			int[] trainingNormalizations, int[] somSize, int repeatCycle) throws IOException {

		// iterate thru all training perspectives
		for (int i = 0; i < trainingPerspectives.length; i++) {

			// set each training perspective to the current perspective index
			int currentPerspective = trainingPerspectives[i];

			// create the path if it does not already exist
			Files.createDirectories(Paths.get("./data/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/"));

			// initialize array to store previous COD files
			File[] codFiles = null;

			// if training phase > 1 then populate COD file array
			if (trainingPhase == 2 || trainingPhase == 3) {
				codFiles = new File[trainingNormalizations.length];
				for (int j = 0; j < codFiles.length; j++) {
					codFiles[j] = new File("data/" + SCHEMA + "/SOMaticOut/" + PERSPECTIVES[trainingNormalizations[j]]
							+ "_Normalized/" + PERSPECTIVES[currentPerspective] + "/trainedSom.cod");
				}
			}

			String timeTests = "./data/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/time_tests.csv";
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
						String input = "./data/" + SCHEMA + "/SOMaticIn/" + normalization + "_Normalized/"
								+ PERSPECTIVES[currentPerspective] + "_" + SCENE + ".dat";

						// variable to store the output location
						String output = "data/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/"
								+ PERSPECTIVES[currentPerspective] + "_" + repeatCycle + "/" + trainingCycles[j] + "/"
								+ normalization + "_Normalized";

						Files.createDirectories(Paths.get(output + "/"));

						writer.append(repeatCycle + ",");

						if (trainingPhase == 1) {
							SOMaticManager.runSOM_COD(input, output, somSize[currentPerspective],
									somSize[currentPerspective], alphaValue, radius, somIterations[currentPerspective],
									DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR, 0,
									PERSPECTIVES[currentPerspective], normalization, writer, TIME_UNITS, trainingPhase);
						} else if (trainingPhase == 2 || trainingPhase == 3) {
							SOMaticManager.runSOM_COD(input, codFiles[i], output, somSize[currentPerspective],
									somSize[currentPerspective], alphaValue, radius, somIterations[currentPerspective],
									DIST_MEASURE, NTHREADS, ROUNDING, SCALING_FACTOR, 0,
									PERSPECTIVES[currentPerspective], normalization, writer, TIME_UNITS, trainingPhase);
						}

						writer.flush();
					}

				}

				if (emailUpdate && trainingCycles.length > 1) {
					sendEmailAlert(true, trainingCycles[j]);
				}
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

			String bmuTests = "./data/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/bmutime_tests.csv";

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
								"./data/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/time_tests.csv"));
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
						String datPath = "./data/" + SCHEMA + "/SOMaticIn/" + normalization + "_Normalized/"
								+ PERSPECTIVES[currentPerspective] + "_" + SCENE + ".dat";
						// path to COD file
						String codPath = "./data/" + SCHEMA + "/test/" + PERSPECTIVES[currentPerspective] + "/"
								+ PERSPECTIVES[currentPerspective] + "_" + repeatCycle + "/" + trainingCycles[j] + "/"
								+ normalization + "_Normalized/trainedSom.cod";

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

						// close buffered reader
						br.close();
					}
				}
			}

			// close writer
			writer.close();
		}
	}

	public static ArrayList<String> getIV2BMU(String datFilePath, String codFilePath) {

		ArrayList<String> output = new ArrayList<String>();

		Dat dat = FileHandler.readDatFile(datFilePath, "L_AT");
		Cod cod = FileHandler.readCodFile(codFilePath, "L_AT");

		for (InputVector inputVector : dat.inputVectors) {
			float minDist = BestMatchingUnit.findBMU(inputVector, cod.neurons, DIST_MEASURE);
			output.add(inputVector.getName() + "," + (inputVector.getMatchingNeuronID() + 1) + "," + minDist);
		}

		return output;
	}

	public static float getAQE(String datFilePath, String codFilePath) {

		ArrayList<String> output = new ArrayList<String>();

		Dat dat = FileHandler.readDatFile(datFilePath, "L_AT");
		Cod cod = FileHandler.readCodFile(codFilePath, "L_AT");

		float aqe = 0f;
		int aqeCount = 0;

		for (InputVector inputVector : dat.inputVectors) {
			float next = BestMatchingUnit.findBMU(inputVector, cod.neurons, DIST_MEASURE);

			if (!Float.isNaN(next)) {
				aqe += next;
				aqeCount++;
			}
			// aqe += BestMatchingUnit.findBMU(inputVector, cod.neurons, DIST_MEASURE);
			// float minDist = BestMatchingUnit.findBMU(inputVector, cod.neurons,
			// DIST_MEASURE);
			// output.add(inputVector.getName() + "," +
			// (inputVector.getMatchingNeuronID()+1) + "," + minDist);
		}

		aqe /= aqeCount;

		return aqe;
	}

	public static float getTE(String datFilePath, String codFilePath) {

		ArrayList<String> output = new ArrayList<String>();

		Dat dat = FileHandler.readDatFile(datFilePath, "L_AT");
		Cod cod = FileHandler.readCodFile(codFilePath, "L_AT");

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

		Session session = Session.getInstance(props, new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(username, password);
			}
		});

		try {

			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress(username));
			message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(send2User));
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
			message.setText("FYI," + "\n\n The process at computer station " + computerStation + " is " + updateMsg2
					+ "!" + "\n\n Cycle: " + cycleString);

			Transport.send(message);

			System.out.println("Done");

		} catch (MessagingException e) {
			throw new RuntimeException(e);
		}
	}

	public static void sendEmailStart() {
		final String username = "schemppthesis@gmail.com";
		final String send2User = "tschempp@gmail.com";
		final String password = "G30gthesis";

		String updateMsg = "Start";
		String updateMsg2 = "starting";

		Properties props = new Properties();
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");

		Session session = Session.getInstance(props, new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(username, password);
			}
		});

		try {

			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress(username));
			message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(send2User));
			message.setSubject("Computer Station " + computerStation + " Process " + updateMsg);

			message.setText(
					"FYI," + "\n\n The process at computer station " + computerStation + " is " + updateMsg2 + "!");

			Transport.send(message);

			System.out.println("Done");

		} catch (MessagingException e) {
			throw new RuntimeException(e);
		}
	}
}
