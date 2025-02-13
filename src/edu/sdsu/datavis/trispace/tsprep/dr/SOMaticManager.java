package edu.sdsu.datavis.trispace.tsprep.dr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.IntStream;

import org.somatic.entities.SOM;
import org.somatic.entities.TrainingVector;
import org.somatic.exceptions.InvalidNormalizationMethodException;
import org.somatic.som.SOMatic;
import org.somatic.trainer.Global;
import org.somatic.trainer.io.FileReader;
// import org.somatic.trainer.io.SOMReader;
import org.somatic.trainer.io.FileWriter;
import org.somatic.trainer.io.SomLog.PreprocessingParams;

import edu.sdsu.datavis.trispace.tsprep.db.PostgreSQLJDBC;
import edu.sdsu.datavis.trispace.tsprep.som.BestMatchingUnit;
import edu.sdsu.datavis.trispace.tsprep.som.Cod;
import edu.sdsu.datavis.trispace.tsprep.som.Dat;
import edu.sdsu.datavis.trispace.tsprep.som.FileHandler;
import edu.sdsu.datavis.trispace.tsprep.som.InputVector;
import edu.sdsu.datavis.trispace.tsprep.som.Neuron;
import edu.sdsu.datavis.trispace.tsprep.utils.KMeans;

public abstract class SOMaticManager {

	public static int MINIMUM_SIZE = 5;
	final static int BATCH_SIZE = 50000; // constant for batch size
	
	public static int[] getNumObjects(int numLoci, int numAttributes, int numTimes) {
		int[] objSizes = new int[6];

		objSizes[0] = numLoci;
		objSizes[1] = numAttributes;
		objSizes[2] = numTimes;
		objSizes[3] = numLoci * numAttributes;
		objSizes[4] = numLoci * numTimes;
		objSizes[5] = numAttributes * numTimes;

		return objSizes;
	}


	public static int[] getSOMDimension(int numLoci, int numAttributes, int numTimes) {
		int[] somSizes = new int[6];

		somSizes[0] = SOMaticManager.findSOMDimension(numLoci);
		somSizes[1] = SOMaticManager.findSOMDimension(numAttributes);
		somSizes[2] = SOMaticManager.findSOMDimension(numTimes);
		somSizes[3] = SOMaticManager.findSOMDimension(numLoci * numAttributes);
		somSizes[4] = SOMaticManager.findSOMDimension(numLoci * numTimes);
		somSizes[5] = SOMaticManager.findSOMDimension(numAttributes * numTimes);

		return somSizes;
	}


	// ============= this is the original four argument getSOMDimension method
	public static int[] getSOMDimension(int numLoci, int numAttributes, int numTimes, int cap) {
		int[] somSizes = new int[6];

		somSizes[0] = SOMaticManager.findSOMDimension(numLoci);
		somSizes[1] = SOMaticManager.findSOMDimension(numAttributes);
		somSizes[2] = SOMaticManager.findSOMDimension(numTimes);
		somSizes[3] = SOMaticManager.findSOMDimension(numLoci * numAttributes);
		somSizes[4] = SOMaticManager.findSOMDimension(numLoci * numTimes);
		somSizes[5] = SOMaticManager.findSOMDimension(numAttributes * numTimes);

		double capSqrt = Math.sqrt(cap);
		for (int i = 0; i < somSizes.length; i++) {
			if (somSizes[i] > capSqrt) {
				somSizes[i] = (int) Math.floor(capSqrt);
			}
		}

		return somSizes;
	}


	

	public static int[] getSOMIterations(int numCycles, int numLoci, int numAttributes, int numTimes) {
		int[] somIterations = new int[6];

		somIterations[0] = numLoci * numCycles;
		somIterations[1] = numAttributes * numCycles;
		somIterations[2] = numTimes * numCycles;
		somIterations[3] = numLoci * numAttributes * numCycles;
		somIterations[4] = numLoci * numTimes * numCycles;
		somIterations[5] = numAttributes * numTimes * numCycles;

		return somIterations;
	}
	
	public static int getSOMIterations(int numCycles, int numLoci, int numAttributes, int numTimes, int tsIdx) {		
		if (tsIdx == 0) {
			return numLoci * numCycles;
		} else if (tsIdx == 1) {
			return numAttributes * numCycles;
		} else if (tsIdx == 2) {
			return numTimes * numCycles;
		} else if (tsIdx == 3) {
			return numLoci * numAttributes * numCycles;
		} else if (tsIdx == 4) {
			return numLoci * numTimes * numCycles;
		} else if (tsIdx == 5) {
			return numAttributes * numTimes * numCycles;
		} else {
			return -1;
		}
	}

	public static int findSOMDimension(int entityCount) {
		int potentialSize = MINIMUM_SIZE;
		int sizeSquared = MINIMUM_SIZE * MINIMUM_SIZE;

		while (sizeSquared < entityCount) {
			potentialSize++;
			sizeSquared = potentialSize * potentialSize;
		}

		return potentialSize;
	}

	
	/**
	 * Train a SOM.
	 *
	 * @param datInputFile
	 *            - The input DAT file.
	 * @param ouputFolder
	 *            - The output folder since SOMatic exports /path/to/folder/trainedSom.cod
	 * @param neuronNumberX
	 *            - The Tri-Space perspective to write to.
	 * @param neuronNumberY
	 * 			  - The number of neurons in the SOM.
	 * @param alphaValue
	 *            - The alpha value (learning rate).
	 * @param radius
	 *            - The radius, or neighborhood for training.
	 * @param iterations
	 *            - Number of iterations for the first training phase.
	 * @param iterations2
	 * 			  - Number of iterations for the second training phase.
	 * @param iterations3
	 * 			  - Number of iterations for the third training phase.
	 * @param simMeasure
	 *            - Similarity measure.
	 * @param nThreads
	 *            - Number of threads to use when training.
	 * @param rounding
	 *            - To use rounding or not.
	 * @param scalingFactor
	 * 			  - The scaling factor.
	 * @param option
	 * 			  - To output a GeoJSON (1) or a DAT file (0).
	 */
	public static void runSOM(String datInputFile, String ouputFolder, int neuronNumberX, int neuronNumberY,
			float alphaValue, int radius, int iterations, int iterations2, int iterations3, int simMeasure,
			int nThreads, boolean rounding, int scalingFactor, int option) {

		SOMatic s = new SOMatic();
		Global g = Global.getInstance();

		// setting of global values
		g.similarityMeasure = simMeasure;
		g.nrOfThreads = nThreads;
		g.rounding = rounding;
		g.scalingFactor = scalingFactor;

		s.setTrainingDataFilePath(new File(datInputFile));
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
			s.setInitialAlphaValue(0.03);
			// second stage: half the radius
			s.setInitialNeighborhoodRadius((int) (radius / 2));
			s.setNumberOfTrainingRuns(iterations2);
		} catch (Exception e) {
			e.printStackTrace();
		}
		s.doTraining();

		try {
			s.setInitialAlphaValue(0.02);
			// third stage: fifth the radius
			s.setInitialNeighborhoodRadius((int) (radius / 5));
			s.setNumberOfTrainingRuns(iterations3);
		} catch (Exception e) {
			e.printStackTrace();
		}
		s.doTraining();

		s.setSOMFilePath(new File(ouputFolder));

		System.out.println("Now writing trained SOM to file");
		s.writeSomToAFile(option);
		System.out.println("Finished writing SOM file");
		// System.exit(0);
	}

	public static void runSOM_COD(String sominputFilePath, String somoutputFilePath, int neuronNumberX,
			int neuronNumberY, float alphaValue, int radius, int iterations, int iterations2, int iterations3,
			int simMeasure, int nThreads, boolean rounding, int scalingFactor, int option, String perspective,
			String normalization, FileWriter writer, int timeUnits) throws IOException {

		DecimalFormat f = new DecimalFormat("##.00");

		double timeUnit;
		if (timeUnits == 2) {
			timeUnit = 6E10;
		} else if (timeUnits == 1) {
			timeUnit = 1E9;
		} else {
			timeUnit = 1;
		}

		long startTime = System.nanoTime();

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

		long endTime = System.nanoTime();
		long duration1 = (endTime - startTime);
		double dur1 = duration1 / timeUnit;
		double totalDur1 = duration1 / timeUnit;
		// double totalDur11 = duration1 / 6E10;

		if (timeUnits == 1 || timeUnits == 2) {
			writer.append(perspective + "," + normalization + "," + iterations + ",1," + f.format(dur1) + ","
					+ f.format(totalDur1) + "\n");
		} else {
			writer.append(perspective + "," + normalization + "," + iterations + ",1," + dur1 + "," + totalDur1 + "\n");
		}

		s.setSOMFilePath(new File(somoutputFilePath + "/"));
		System.out.println("Now writing trained SOM to file");
		s.writeSomToAFile(option);

		startTime = System.nanoTime();

		try {
			s.setInitialAlphaValue(0.03);
			// second stage: half the radius
			s.setInitialNeighborhoodRadius((int) (radius / 2));
			s.setNumberOfTrainingRuns(iterations2);
		} catch (Exception e) {
			e.printStackTrace();
		}
		s.doTraining();

		endTime = System.nanoTime();
		long duration2 = (endTime - startTime);
		double dur2 = duration2 / timeUnit;
		double totalDur2 = (duration1 + duration2) / timeUnit;

		if (timeUnits == 1 || timeUnits == 2) {
			writer.append(perspective + "," + normalization + "," + iterations2 + ",2," + f.format(dur2) + ","
					+ f.format(totalDur2) + "\n");
		} else {
			writer.append(
					perspective + "," + normalization + "," + iterations2 + ",2," + dur2 + "," + totalDur2 + "\n");
		}

		s.setSOMFilePath(new File(somoutputFilePath + "/"));
		System.out.println("Now writing trained SOM to file");
		s.writeSomToAFile(option);

		startTime = System.nanoTime();

		try {
			s.setInitialAlphaValue(0.02);
			// third stage: fifth the radius
			s.setInitialNeighborhoodRadius((int) (radius / 5));
			s.setNumberOfTrainingRuns(iterations3);
		} catch (Exception e) {
			e.printStackTrace();
		}
		s.doTraining();

		endTime = System.nanoTime();
		long duration3 = (endTime - startTime);
		double dur3 = duration3 / timeUnit;
		double totalDur3 = (duration1 + duration2 + duration3) / timeUnit;

		if (timeUnits == 1 || timeUnits == 2) {
			writer.append(perspective + "," + normalization + "," + iterations3 + ",3," + f.format(dur3) + ","
					+ f.format(totalDur3) + "\n");
		} else {
			writer.append(
					perspective + "," + normalization + "," + iterations3 + ",3," + dur3 + "," + totalDur3 + "\n");
		}

		s.setSOMFilePath(new File(somoutputFilePath + "/"));
		System.out.println("Now writing trained SOM to file");
		s.writeSomToAFile(option);

		// s.setSOMFilePath(new File(somoutputFilePath));
		//
		// System.out.println("Now writing trained SOM to file");
		// s.writeSomToAFile(option);
		System.out.println("Finished writing SOM file");
		// System.exit(0);
	}


	// ================== This is the original FIRST runSOM_COD method ==================

	public static void runSOM_COD(String sominputFilePath, String somoutputFilePath, int neuronNumberX,
			int neuronNumberY, float alphaValue, int radius, int iterations, int simMeasure, int nThreads,
			boolean rounding, int scalingFactor, String perspective, String normalization,
			FileWriter writer, int timeUnits, int stage) throws IOException {

		// int newRadius = radius;
		// if (stage == 2) {
		// newRadius = (int) (radius / 2);
		// } else if (stage == 3) {
		// newRadius = (int) (radius / 5);
		// }

		DecimalFormat f = new DecimalFormat("##.00");

		double timeUnit;
		if (timeUnits == 2) {
			timeUnit = 6E10;
		} else if (timeUnits == 1) {
			timeUnit = 1E9;
		} else {
			timeUnit = 1;
		}

		long startTime = System.nanoTime();

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

		long endTime = System.nanoTime();
		long duration1 = (endTime - startTime);
		double dur1 = duration1 / timeUnit;
		double totalDur1 = duration1 / timeUnit;

		if (timeUnits == 1 || timeUnits == 2) {
			writer.append(perspective + "," + normalization + "," + iterations + "," + stage + "," + f.format(dur1)
					+ "," + f.format(totalDur1) + "\n");
		} else {
			writer.append(perspective + "," + normalization + "," + iterations + "," + stage + "," + dur1 + ","
					+ totalDur1 + "\n");
		}

		s.setSOMFilePath(new File(somoutputFilePath));
		System.out.println("Now writing trained SOM to file");
		s.writeSomToAFile();
		// s.writeSomToAFile(0);
		// s.writeSomToAFile(1);
		// s.writeSomToAFile(2);


		System.out.println("Finished writing SOM file");
	}


	// ================== This is the test to run parallels on the FIRST runSOM_COD method ==================

// 	public static void runSOM_COD(String sominputFilePath, String somoutputFilePath, int neuronNumberX,
//         int neuronNumberY, float alphaValue, int radius, int iterations, int simMeasure, int nThreads,
//         boolean rounding, int scalingFactor, String perspective, String normalization,
//         FileWriter writer, int timeUnits, int stage) throws IOException {

//     DecimalFormat f = new DecimalFormat("##.00");
//     double timeUnit = (timeUnits == 2) ? 6E10 : (timeUnits == 1) ? 1E9 : 1;
//     long startTime = System.nanoTime();

//     // Create SOMatic instance and get Global singleton
//     SOMatic s = new SOMatic();
//     Global g = Global.getInstance();

//     // Configure global settings
//     configureSOMSettings(g, simMeasure, nThreads, rounding, scalingFactor);

//     // Read and prepare training data in parallel
//     s.setTrainingDataFilePath(new File(sominputFilePath));
//     CompletableFuture<Void> readFuture = CompletableFuture.runAsync(() -> {
//         try {
//             s.readFile();
//         } catch (Exception e) {
//             e.printStackTrace();
//         }
//     });

//     // Set normalization methods in parallel
//     CompletableFuture<Void> normFuture = readFuture.thenRunAsync(() -> {
//         IntStream.range(0, s.getAttributes().length)
//                 .parallel()
//                 .forEach(i -> {
//                     try {
//                         s.setNormalizationMethod(s.getAttributes()[i], g.NULL);
//                     } catch (Exception e) {
//                         // Handle exception
//                     }
//                 });
//         s.normalizeTrainingVectors();
//     });

//     // Initialize SOM configuration
//     CompletableFuture<Void> initFuture = normFuture.thenRunAsync(() -> {
//         s.setNumberOfNeuronsX(neuronNumberX);
//         s.setNumberOfNeuronsY(neuronNumberY);
//         s.setRandomSOMInitialization(true);
//         try {
//             s.setTopologyOfTheSOM(g.HEXA);
//         } catch (Exception e) {
//             System.out.println("Error with setTopology(); Topology =" + g.HEXA);
//         }
//     });

//     // Initialize SOM and set training parameters
//     CompletableFuture<Void> trainingSetupFuture = initFuture.thenRunAsync(() -> {
//         s.initializeSOM();
//         try {
//             s.setInitialAlphaValue(alphaValue);
//             s.setInitialNeighborhoodRadius(radius);
//             s.setNumberOfTrainingRuns(iterations);
//         } catch (Exception e) {
//             e.printStackTrace();
//         }
//     });

//     // Execute training
//     trainingSetupFuture.thenRun(() -> {
//         s.doTraining();
//     }).join(); // Wait for training to complete

//     // Calculate and write timing information
//     long endTime = System.nanoTime();
//     long duration1 = (endTime - startTime);
//     double dur1 = duration1 / timeUnit;
//     double totalDur1 = duration1 / timeUnit;

//     // Write results
//     writeResults(writer, perspective, normalization, iterations, stage, f, timeUnits, dur1, totalDur1);

//     // Save trained SOM
//     s.setSOMFilePath(new File(somoutputFilePath));
//     System.out.println("Now writing trained SOM to file");
//     s.writeSomToAFile();
//     System.out.println("Finished writing SOM file");
// }

// // Helper method to configure SOM settings
// private static void configureSOMSettings(Global g, int simMeasure, int nThreads, boolean rounding, int scalingFactor) {
//     g.similarityMeasure = simMeasure;
//     g.nrOfThreads = nThreads;
//     g.rounding = rounding;
//     g.scalingFactor = scalingFactor;
// }

// // Helper method to write results
// private static void writeResults(FileWriter writer, String perspective, String normalization, 
//         int iterations, int stage, DecimalFormat f, int timeUnits, double dur1, double totalDur1) 
//         throws IOException {
//     if (timeUnits == 1 || timeUnits == 2) {
//         writer.append(perspective + "," + normalization + "," + iterations + "," + stage + "," 
//                 + f.format(dur1) + "," + f.format(totalDur1) + "\n");
//     } else {
//         writer.append(perspective + "," + normalization + "," + iterations + "," + stage + "," 
//                 + dur1 + "," + totalDur1 + "\n");
//     }
// }

	// ================== This is the original SECOND runSOM_COD method ==================

	public static void runSOM_COD(String trainingInputFilePath, File codInputFile, String somoutputFilePath,
			int neuronNumberX, int neuronNumberY, float alphaValue, int radius, int iterations, int simMeasure,
			int nThreads, boolean rounding, int scalingFactor, String perspective, String normalization,
			FileWriter writer, int timeUnits, int stage) throws IOException {

		// int newRadius = radius;
		// if (stage == 2) {
		// newRadius = (int) (radius / 2);
		// } else if (stage == 3) {
		// newRadius = (int) (radius / 5);
		// }

		DecimalFormat f = new DecimalFormat("##.00");

		double timeUnit;
		if (timeUnits == 2) {
			timeUnit = 6E10;
		} else if (timeUnits == 1) {
			timeUnit = 1E9;
		} else {
			timeUnit = 1;
		}

		long startTime = System.nanoTime();

		// System.out.println(g.som.neurons1d.length);
		SOMatic s = new SOMatic();
		// Global g = s.getG();
		Global g = Global.getInstance();
		// Global g;
		// System.out.println(g.nNeuronsX);
		// System.out.println(g.nNeuronsY);
		// System.out.println("YEAH G SOM 1");
		// System.out.println(g.som);
		// SOMReader inSOM = new SOMReader(codInputFile);
		// inSOM.readSomFile();
		BufferedReader reader = null;
		int nrOfAtts;
		int X;
		int Y;
		int topo;
		String nhf;

		if (!codInputFile.getAbsolutePath().endsWith("cod")) {
			System.out.println("SOMReader.readSomFile; wrong file extension, cannot read this file.");
			return;
		}
		try {
			reader = new BufferedReader(new java.io.FileReader(codInputFile));
		} catch (FileNotFoundException e) {
			System.out.println("SOMReader.readSomFile; Filepath could not be found");
			e.printStackTrace();
		}
		// read and parse the first line
		String[] firstLine;
		try {

			String fl = reader.readLine();

			firstLine = fl.split(" ");

			nrOfAtts = Integer.parseInt(firstLine[0]);
			X = Integer.parseInt(firstLine[2]);
			Y = Integer.parseInt(firstLine[3]);
			topo = (firstLine[1].equals("hexa") ? g.HEXA : g.RECT);
			if (firstLine[4] != null)
				nhf = firstLine[4]; // this line is optional according to the
									// SOM_PAK specification
			System.out.println(firstLine[1]);
			System.out.println(nrOfAtts);
			System.out.println(X);
			System.out.println(Y);
			System.out.println("topo");
			System.out.println(topo);
		} catch (IOException e) {
			System.out.println("SOMReader.readSomFile; Some IOException ocurred.");
			e.printStackTrace();
			return;
		}
		// prepare an empty SOM according to those values
		g.som = new SOM(Y, X, nrOfAtts, topo, false);

		// String test_read = "";
		// for (int i = 0; i < g.som.neurons1d.length; i++) {

		// test_read += " " + g.som.neurons1d[i];
		// for (int j = 0; j < g.som.neurons1d[0].getAttributeLength(); j++) {
		// test_read += " " + g.som.neurons1d[0].getAttribute(j);
		// }
		// }

		// System.out.println("SOM STUFF TIM");
		// System.out.println(test_read);

		// read the values of the SOMfile and put them in the empty SOM
		int neuronIdx = 0;
		for (int i = 0; i < X * Y; i++) {
			try {
				// System.out.println(firstLine);
				String readLine = reader.readLine();
				firstLine = readLine.split(" ");
				if (!firstLine[0].startsWith("#")) {
					// System.out.println(readLine);
					for (int j = 0; j < nrOfAtts; j++) {
						// System.out.println(readLine);
						g.som.neurons1d[neuronIdx].setAttribute(j, Float.parseFloat(firstLine[j]));
					}
					neuronIdx++;
				}
				// continue;
			} catch (IOException e) {
				System.out.println(
						"SOMReader.readSomFile; Some IOException was thrown while reading the values of the SOM file.");
				e.printStackTrace();
				return;
			}

		}

		// test_read = "";
		// for (int i = 0; i < g.som.neurons1d.length; i++) {
		// test_read += " " + g.som.neurons1d[i];
		// for (int j = 0; j < g.som.neurons1d[0].getAttributeLength(); j++) {
		// test_read += " " + g.som.neurons1d[0].getAttribute(j);
		// }
		// }

		// System.out.println("SOM STUFF TIM2");
		// System.out.println(test_read);

		// System.out.println("YEAH G SOM 2");
		// System.out.println(g.som);

		// System.out.println(g.similarityMeasure);
		// System.out.println(g.nrOfThreads);
		// System.out.println(g.rounding);
		// System.out.println(g.scalingFactor);

		// setting of global values
		g.similarityMeasure = simMeasure;
		g.nrOfThreads = nThreads;
		g.rounding = rounding;
		g.scalingFactor = scalingFactor;

		// System.out.println(g.similarityMeasure);
		// System.out.println(g.nrOfThreads);
		// System.out.println(g.rounding);
		// System.out.println(g.scalingFactor);

		System.out.println(g.nNeuronsX);
		System.out.println(g.nNeuronsY);

		// s.setTrainingDataFilePath(new File(trainingInputFilePath));
		g.dataFilePath = trainingInputFilePath;

		try {
			// s.readFile();
			// create a file reader
			FileReader fr = new FileReader();
			// read the data from the file
			ArrayList<String[]> data = fr.readFile(g.dataFilePath);
			// create IVs from the data
			g.TrainingVectors = fr.createTVs(data);
			// derive and create Attributes from data
			g.Attributes = fr.createAttributes(data);
			g.nrOfAttributes = g.Attributes.length;
			// g.nrOfThreads
			// parse numbers from the original values
			for (int i = 0; i < g.TrainingVectors.length; i++) {
				g.TrainingVectors[i].parseNumericValues();
			}
			// base statistics
			for (int i = 0; i < g.Attributes.length; i++) {
				g.Attributes[i].calculateMean();
				g.Attributes[i].calculateStdDev();
				g.Attributes[i].countMissingValues();
			}

			// ### update the LOG-variables: ###
			// file path
			g.log.originalDataFilePath = g.dataFilePath;
		} catch (Exception e) {
			e.printStackTrace();
		}

		// set the normalization method for each attribute (NULL, LINEAR BOOLEAN
		// or ZSCORE)
		// for (int i = 0; i < s.getAttributes().length; i++) {
		// try {
		// s.setNormalizationMethod(s.getAttributes()[i], g.NULL);
		// } catch (Exception e) {
		// }
		// }

		for (int i = 0; i < g.Attributes.length; i++) {
			try {
				// s.setNormalizationMethod(s.getAttributes()[i], g.NULL);
				if (g.NULL == g.BOOLEAN || g.NULL == g.LINEAR || g.NULL == g.ZSCORE || g.NULL == g.NULL) {
					g.Attributes[i].normalizationMethod = g.NULL;
				} else
					throw new InvalidNormalizationMethodException("Not a valid normalization method.");
			} catch (Exception e) {
			}
		}
		// normalize the training data
		// s.normalizeTrainingVectors();
		// s.setNumberOfNeuronsX(x);

		System.out.println("SOMatic.normalizeTVs; #ofAtts:" + g.Attributes.length + " #ofTVs:"
				+ g.TrainingVectors.length + " with " + g.TrainingVectors[0].attribute.length + " attributes;");
		for (int i = 0; i < g.Attributes.length; i++) {
			// System.out.println("PRE Norm: " + g.Attributes[i]);
			g.Attributes[i].normalize(g.TrainingVectors);
			// System.out.println("Post Norm: " + g.Attributes[i]);
		}

		// ### update the LOG-variables: ###
		// normalization parameters
		g.log.prep = g.log.new PreprocessingParams(g.Attributes.length);
		for (int i = 0; i < g.Attributes.length; i++) {
			switch (g.Attributes[i].normalizationMethod) {
			case 7:
				g.log.prep.normalizationMethod[i] = "linear";
				break;
			case 9:
				g.log.prep.normalizationMethod[i] = "boolean";
				break;
			case 10:
				g.log.prep.normalizationMethod[i] = "z-score";
				break;
			default:
				g.log.prep.normalizationMethod[i] = "unnormalized";
				break;
			}
			g.log.prep.maxValue[i] = (float) g.Attributes[i].maxValue;
			g.log.prep.minValue[i] = (float) g.Attributes[i].minValue;
			g.log.prep.mean[i] = (float) g.Attributes[i].mean;
			g.log.prep.stdDev[i] = (float) g.Attributes[i].standardDeviation;
			g.log.prep.boolThreshold[i] = (float) g.booleanThreshold;
			g.log.prep.weight[i] = (float) g.Attributes[i].weight;
		}

		// s.setNumberOfNeuronsX(neuronNumberX);
		g.nNeuronsX = neuronNumberX;
		// s.setNumberOfNeuronsY(neuronNumberY);
		g.nNeuronsY = neuronNumberY;
		// s.setRandomSOMInitialization(false);

		System.out.println(g.nNeuronsX);
		System.out.println(g.nNeuronsY);

		try {
			// s.setTopologyOfTheSOM(g.HEXA);
			g.topology = g.HEXA;
		} catch (Exception e) {
			System.out.println("Error with setTopology(); Topology =" + g.HEXA);
		}
		//
		// s.initializeSOM();
		//
		try {
			// s.setInitialAlphaValue(alphaValue);
			g.iniAlpha = alphaValue;
			// s.setInitialNeighborhoodRadius(radius);
			g.iniNeighRadius = radius;
			// s.setNumberOfTrainingRuns(iterations);
			g.nRuns = iterations;
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println(g.TrainingVectors[0].attribute[0]);
		g.randomizeTrainingVectors = true;
		// System.out.println(g.);
		g.som.prepareNeurons();
		// System.out.println(g.som.neurons[0][0].getAttribute(0));
		// System.out.println(g.som.neurons1d[1].getAttribute(0));
		g.som.doTraining();

		// System.out.println(g.som.);
		// System.out.println(s.g.status = "");
		// s.doTraining();
		// g.som.doTraining();

		long endTime = System.nanoTime();
		long duration1 = (endTime - startTime);
		double dur1 = duration1 / timeUnit;
		double totalDur1 = duration1 / timeUnit;
		//
		if (timeUnits == 1 || timeUnits == 2) {
			writer.append(perspective + "," + normalization + "," + iterations + "," + stage + "," + f.format(dur1)
					+ "," + f.format(totalDur1) + "\n");
		} else {
			writer.append(perspective + "," + normalization + "," + iterations + "," + stage + "," + dur1 + ","
					+ totalDur1 + "\n");
		}
		//
		// ==== adding to try and solve n threads error ========
		// Store original thread count
		// int originalThreads = g.nrOfThreads;

		// // Switch to single thread for file writing
		// g.nrOfThreads = 1;

		// ================================================

		g.somFilePath = somoutputFilePath;
		System.out.println("Now writing trained SOM to file");
		// s.writeSomToAFile(option);
		// g.som.writeS
		// FileWriter fw = new FileWriter();

		// below is commented out to remove the writing of the SOM files one at a time
		org.somatic.trainer.io.FileWriter fw = new org.somatic.trainer.io.FileWriter();
		// if (option == 0) {
			fw.som2File_LineByLine(g.somFilePath);
		// } else if (option == 1) {
			fw.writeSomToGeoJson(g.somFilePath);
		// } else if (option == 2) {
			fw.writeBmusToGeoJson(g.somFilePath);
		// }

		g.log.somFilePath = g.somFilePath;
		g.log.trainingDataFilePath = g.IVfilePath;
		g.log.writeLogToFile(g.somFilePath);

		// ==== adding to try and solve n threads error ========

		// // Restore original thread count
		// g.nrOfThreads = originalThreads;

		// ================================================

		System.out.println("Finished writing SOM file");
	}

	// ================== This is the test to run parallels on the SECOND runSOM_COD method ==================

// 	public static void runSOM_COD(String trainingInputFilePath, File codInputFile, String somoutputFilePath,
//         int neuronNumberX, int neuronNumberY, float alphaValue, int radius, int iterations, int simMeasure,
//         int nThreads, boolean rounding, int scalingFactor, String perspective, String normalization,
//         FileWriter writer, int timeUnits, int stage) throws IOException {

//     DecimalFormat f = new DecimalFormat("##.00");
//     double timeUnit = (timeUnits == 2) ? 6E10 : (timeUnits == 1) ? 1E9 : 1;
//     long startTime = System.nanoTime();

//     // Create instances and get global settings
//     SOMatic s = new SOMatic();
//     Global g = Global.getInstance();

//     // First parallel task: Read and process the COD input file
//     CompletableFuture<Void> codReadFuture = CompletableFuture.runAsync(() -> {
//         try {
//             // Validate and read COD file
//             if (!codInputFile.getAbsolutePath().endsWith("cod")) {
//                 throw new IllegalArgumentException("Wrong file extension, cannot read this file.");
//             }

//             try (BufferedReader reader = new BufferedReader(new FileReader(codInputFile))) {
//                 // Parse first line to get SOM parameters
//                 String[] firstLine = reader.readLine().split(" ");
//                 int nrOfAtts = Integer.parseInt(firstLine[0]);
//                 int X = Integer.parseInt(firstLine[2]);
//                 int Y = Integer.parseInt(firstLine[3]);
//                 int topo = (firstLine[1].equals("hexa") ? g.HEXA : g.RECT);

//                 // Initialize SOM with parameters from COD file
//                 g.som = new SOM(Y, X, nrOfAtts, topo, false);

//                 // Read neuron weights in parallel
//                 List<String> lines = new ArrayList<>();
//                 String line;
//                 while ((line = reader.readLine()) != null) {
//                     lines.add(line);
//                 }

//                 // Process neurons in parallel
//                 IntStream.range(0, X * Y)
//                     .parallel()
//                     .forEach(i -> {
//                         if (i < lines.size()) {
//                             String[] values = lines.get(i).split(" ");
//                             if (!values[0].startsWith("#")) {
//                                 for (int j = 0; j < nrOfAtts; j++) {
//                                     g.som.neurons1d[i].setAttribute(j, Float.parseFloat(values[j]));
//                                 }
//                             }
//                         }
//                     });
//             }
//         } catch (Exception e) {
//             throw new CompletionException(e);
//         }
//     });

//     // Configure global settings in parallel
//     CompletableFuture<Void> configFuture = codReadFuture.thenRunAsync(() -> {
//         g.similarityMeasure = simMeasure;
//         g.nrOfThreads = nThreads;
//         g.rounding = rounding;
//         g.scalingFactor = scalingFactor;
//         g.nNeuronsX = neuronNumberX;
//         g.nNeuronsY = neuronNumberY;
//         g.topology = g.HEXA;
//     });

//     // Read and process training data in parallel
//     CompletableFuture<Void> trainingDataFuture = configFuture.thenRunAsync(() -> {
//         try {
//             g.dataFilePath = trainingInputFilePath;
//             FileReader fr = new FileReader();
//             ArrayList<String[]> data = fr.readFile(g.dataFilePath);

//             // Parallel processing of training vectors
//             CompletableFuture<TrainingVector[]> vectorsFuture = CompletableFuture.supplyAsync(() -> 
//                 fr.createTVs(data));
//             CompletableFuture<Attribute[]> attributesFuture = CompletableFuture.supplyAsync(() -> 
//                 fr.createAttributes(data));

//             // Wait for both futures to complete
//             g.TrainingVectors = vectorsFuture.get();
//             g.Attributes = attributesFuture.get();
//             g.nrOfAttributes = g.Attributes.length;

//             // Parallel processing of numeric values
//             Arrays.stream(g.TrainingVectors)
//                 .parallel()
//                 .forEach(TrainingVector::parseNumericValues);

//             // Parallel calculation of statistics
//             Arrays.stream(g.Attributes)
//                 .parallel()
//                 .forEach(attribute -> {
//                     attribute.calculateMean();
//                     attribute.calculateStdDev();
//                     attribute.countMissingValues();
//                 });

//             g.log.originalDataFilePath = g.dataFilePath;
//         } catch (Exception e) {
//             throw new CompletionException(e);
//         }
//     });

//     // Normalize attributes in parallel
//     CompletableFuture<Void> normalizationFuture = trainingDataFuture.thenRunAsync(() -> {
//         Arrays.stream(g.Attributes)
//             .parallel()
//             .forEach(attribute -> {
//                 try {
//                     attribute.normalizationMethod = g.NULL;
//                     attribute.normalize(g.TrainingVectors);
//                 } catch (Exception e) {
//                     // Handle exception
//                 }
//             });

//         // Update log variables
//         updateLogVariables(g);
//     });

//     // Configure training parameters and execute training
//     CompletableFuture<Void> trainingFuture = normalizationFuture.thenRunAsync(() -> {
//         try {
//             g.iniAlpha = alphaValue;
//             g.iniNeighRadius = radius;
//             g.nRuns = iterations;
//             g.randomizeTrainingVectors = true;
//             g.som.prepareNeurons();
            
//             // Execute training using parallel processing in SOM class
//             g.som.doTraining();
//         } catch (Exception e) {
//             throw new CompletionException(e);
//         }
//     });

//     // Wait for all operations to complete
//     trainingFuture.join();

//     // Calculate and write timing information
//     long endTime = System.nanoTime();
//     long duration1 = (endTime - startTime);
//     writeTimingInformation(writer, perspective, normalization, iterations, stage, 
//             timeUnits, f, duration1, timeUnit);

//     // Write SOM output files
//     writeSOMOutput(g, somoutputFilePath);


	
// }

// // Helper method to update log variables
// private static void updateLogVariables(Global g) {
//     g.log.prep = g.log.new PreprocessingParams(g.Attributes.length);
//     IntStream.range(0, g.Attributes.length)
//         .parallel()
//         .forEach(i -> {
//             updateAttributeLogInfo(g, i);
//         });
// }

// // Helper method to write timing information
// private static void writeTimingInformation(FileWriter writer, String perspective, 
//         String normalization, int iterations, int stage, int timeUnits, 
//         DecimalFormat f, long duration1, double timeUnit) throws IOException {
//     double dur1 = duration1 / timeUnit;
//     double totalDur1 = duration1 / timeUnit;
    
//     if (timeUnits == 1 || timeUnits == 2) {
//         writer.append(String.format("%s,%s,%d,%d,%s,%s\n", 
//             perspective, normalization, iterations, stage, 
//             f.format(dur1), f.format(totalDur1)));
//     } else {
//         writer.append(String.format("%s,%s,%d,%d,%f,%f\n", 
//             perspective, normalization, iterations, stage, dur1, totalDur1));
//     }
// }

// // Helper method to write SOM output
// private static void writeSOMOutput(Global g, String somoutputFilePath) throws IOException {
//     g.somFilePath = somoutputFilePath;
//     System.out.println("Now writing trained SOM to file");
    
//     CompletableFuture.runAsync(() -> {
//         try {
//             org.somatic.trainer.io.FileWriter fw = new org.somatic.trainer.io.FileWriter();
//             fw.som2File_LineByLine(g.somFilePath);
//             fw.writeSomToGeoJson(g.somFilePath);
//             fw.writeBmusToGeoJson(g.somFilePath);
            
//             g.log.somFilePath = g.somFilePath;
//             g.log.trainingDataFilePath = g.IVfilePath;
//             g.log.writeLogToFile(g.somFilePath);
//         } catch (Exception e) {
//             throw new CompletionException(e);
//         }
//     }).join();

//     System.out.println("Finished writing SOM file");
// }

	public static String[] getIV2BMU2(String datFilePath, String codFilePath, String type, int simMeasure) {

		ArrayList<String> output = new ArrayList<String>();

		Dat dat = FileHandler.readDatFile(datFilePath, "L_AT");
		Cod cod = FileHandler.readCodFile(codFilePath, "L_AT");

		for (InputVector inputVector : dat.inputVectors) {
			BestMatchingUnit.findBMU(inputVector, cod.neurons, simMeasure);
			output.add(inputVector.getName() + "," + (inputVector.getMatchingNeuronID() + 1));
		}

		// System.out.println("SOM LENGTH " + cod.neurons.length);

		return output.toArray(new String[output.size()]);
	}

	public static ArrayList<String> getIV2BMU(String datFilePath, String codFilePath, String type, int simMeasure) {

		ArrayList<String> output = new ArrayList<String>();

		Dat dat = FileHandler.readDatFile(datFilePath, "L_AT");
		Cod cod = FileHandler.readCodFile(codFilePath, "L_AT");

		for (InputVector inputVector : dat.inputVectors) {
			BestMatchingUnit.findBMU(inputVector, cod.neurons, simMeasure);
			output.add(inputVector.getName() + "," + (inputVector.getMatchingNeuronID() + 1));
		}

		return output;
	}

	public static void extractSOMData(String datFilePath, String codFilePath, String type, int kMax, int distMeasure,
			String schema, PostgreSQLJDBC db, int normIdx, boolean batch) {

		// read-in input vectors (DAT file)
		Dat dat = FileHandler.readDatFile(datFilePath, type);

		// read-in neurons (COD file)
		Cod cod = FileHandler.readCodFile(codFilePath, type);

		ArrayList<String> ivData = getIV2BMU(datFilePath, codFilePath, type, distMeasure);
		ArrayList<String> batchInsertion = new ArrayList<String>();

		int numAttributes = db.getTableLength(schema, "attribute_key");

		// int numNormalizations = db.getTableLength(schema, "normalization_key");

		// get the number of time objects
		int numTimes = db.getTableLength(schema, "time_key");

		// write the bmu_n# attribute to SOM Input Vector tables (L_AT, LA_T, LT_A)
		for (int i = 0; i < ivData.size(); i++) {
			String[] split = ivData.get(i).split(",");
			String column = "bmu_n" + (normIdx + 1);
			String update = split[1];
			if (!batch) {
				db.updateTable(schema, type.toLowerCase(), "id", split[0], column, update);
			} else {
				batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase(), "id", split[0], column, update));
			}
		}

		// iterate thru all neurons
		// record neuron to IV assignments
		for (int i = 1; i <= cod.neurons.length; i++) {
			String newData = "";
			String neuronId = i + "";
			ArrayList<String> removeData = new ArrayList<String>();

			// create comma-separated list of IV's
			for (int j = 0; j < ivData.size(); j++) {
				// System.out.println(ivData.get(j));
				String[] split = ivData.get(j).split(",");

				if (split[1].equalsIgnoreCase(neuronId)) {
					removeData.add(ivData.get(j));
					if (newData.equals("")) {
						newData = split[0];
					} else {
						newData = newData + "," + split[0];
					}
				}
			}

			// update table if list of IV's is not empty
			if (!newData.equals("")) {
				for (int j = 0; j < removeData.size(); j++) {
					ivData.remove(removeData.get(j));
				}
				String column = "ivs_n" + (normIdx + 1);
				String update = "'" + newData + "'";

				if (!batch) {
					db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
				} else {
					batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id", neuronId,
							column, update));
				}

				String[] newSplit = newData.split(",");

				column = "num_ivs_n" + (normIdx + 1);
				update = "'" + newSplit.length + "'";

				if (!batch) {
					db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
				} else {
					batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id", neuronId,
							column, update));
				}

				if (type.equalsIgnoreCase("lt_a")) {
					// System.out.println(newData);
					// String[] newSplit = newData.split(",");
					// System.out.println(newSplit.length);

					if (newSplit.length == 1) { // case 1: A single IV maps to the neuron
						String lcType = db.getObjectColumnValue(schema, "lt_a", "id", newData, "lc");

						column = "lc_n" + (normIdx + 1);
						update = "'" + lcType + "'";

						if (!batch) {
							db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
						} else {
							batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id",
									neuronId, column, update));
						}

						String[] newDataSplit = newData.split("_");

						column = "t_class_n" + (normIdx + 1);
						update = "'" + newDataSplit[1] + "'";

						if (!batch) {
							db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
						} else {
							batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id",
									neuronId, column, update));
						}
					} else { // case 2: Multiple IVs map to the neuron

						// String[] lcTypes = new String[newSplit.length];

						// list to store the unique LC classes that map to the neuron
						ArrayList<String> lcNames = new ArrayList<String>();

						// list to store the count for each LC class that maps to the neuron
						ArrayList<Integer> lcCount = new ArrayList<Integer>();

						// 2D list to store the IVs according to their LC class
						ArrayList<ArrayList<String>> lt_aItems = new ArrayList<ArrayList<String>>();

						// iterate thru all IVs
						for (int j = 0; j < newSplit.length; j++) {

							// store the LC class
							String lcType = db.getObjectColumnValue(schema, "lt_a", "id", newSplit[j], "lc");

							if (!lcNames.contains(lcType)) { // if list does not contain the LC class
								// store the LC class name
								lcNames.add(lcType);
								// initialize the count to 1
								lcCount.add(1);
								// Create the list of IVs that correspond to this LC class
								ArrayList<String> tmpItems = new ArrayList<String>();
								// append the first IV for that LC class
								tmpItems.add(newSplit[j]);
								// store the list for that LC class
								lt_aItems.add(tmpItems);
							} else { // if list DOES contain the LC class
								// get the index that corresponds to a particular LC class
								int lcIdx = lcNames.indexOf(lcType);
								// increment the count for that LC class
								lcCount.set(lcIdx, (lcCount.get(lcIdx) + 1));
								// append the IV to that LC class
								lt_aItems.get(lcIdx).add(newSplit[j]);
							}
							// lcNames.add(db.getObjectColumnValue(schema, "lt_a", "id", newSplit[j],
							// "lc"));
						}

						// determine which LC class has the most IVs
						// initialize count to -1
						int maxNumber = -1;

						// iterate thru all LC class counts
						for (int j = 0; j < lcCount.size(); j++) {
							// store the max
							if (lcCount.get(j) > maxNumber) {
								maxNumber = lcCount.get(j);
							}
						}

						// boolean singleWinner = true;

						// store the index(es) of the LC class(es) that have the max count
						ArrayList<Integer> winners = new ArrayList<Integer>();
						int tmpCounter = 0;
						for (int j = 0; j < lcCount.size(); j++) {
							if (lcCount.get(j) == maxNumber) {
								tmpCounter++;
								winners.add(j);
							}
						}

						if (tmpCounter == 1) { // case 1: There is only one winning class
							// String lcType = db.getObjectColumnValue(schema, "lt_a", "id", bestMatchingIV,
							// "lc");
							//
							column = "lc_n" + (normIdx + 1);
							update = "'" + lcNames.get(winners.get(0)) + "'";

							if (!batch) {
								db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
							} else {
								batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id",
										neuronId, column, update));
							}

						} else { // case 2: there is a 'tie' between winning classes

							// since no class has the majority, the class that is more similar will be
							// chosen

							// store the AQE for each class
							ArrayList<Float> classAQE = new ArrayList<Float>();
							// ArrayList<String> className = new ArrayList<String>();

							// get the neuron
							Neuron neuron = cod.neurons[i - 1];

							// parse thru each maximum class
							for (int j = 0; j < winners.size(); j++) {

								// initial AQE set to 0
								float tmpAQE = 0f;

								// iterate thru each IV
								for (int k = 0; k < lt_aItems.get(winners.get(j)).size(); k++) {

									// initialize float array to store each of the IV's attributes
									float[] ivAttributes = new float[neuron.getAttributes().length];

									// iterate thru the IV's attributes
									for (int l = 0; l < ivAttributes.length; l++) {

										// get the target column in postgres
										String targetColumn = "a" + (l + 1) + "_n" + (normIdx + 1);
										// get the value stored in the column/row
										String tmpValue = db.getObjectColumnValue(schema, "lt_a", "id",
												lt_aItems.get(winners.get(j)).get(k), targetColumn);
										// store the retrieved value in the IV attribute array
										ivAttributes[l] = Float.parseFloat(tmpValue);
									}

									// append the distance between each IV & the mapped neuron
									tmpAQE += neuron.getDistance(ivAttributes, distMeasure);
								}

								// get the Average distance
								tmpAQE /= lt_aItems.get(winners.get(j)).size();
								// store this distance
								classAQE.add(tmpAQE);
								// className.add(e)
								// ArrayList<Float>
							}

							// find the class with the lowest AQE
							int winningIdx = 0;
							float minAQE = classAQE.get(0);
							for (int j = 1; j < classAQE.size(); j++) {
								if (classAQE.get(j) < minAQE) {
									minAQE = classAQE.get(j);
									winningIdx = j;
								}
							}

							column = "lc_n" + (normIdx + 1);
							update = "'" + lcNames.get(winners.get(winningIdx)) + "'";

							if (!batch) {
								db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
							} else {
								batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id",
										neuronId, column, update));
							}
						}

						ArrayList<String> attrNames = new ArrayList<String>();
						ArrayList<Integer> attrCount = new ArrayList<Integer>();
						ArrayList<ArrayList<String>> la_tItems = new ArrayList<ArrayList<String>>();

						for (int j = 0; j < newSplit.length; j++) {
							String[] attrSplit = newSplit[j].split("_");
							// String lcType = db.getObjectColumnValue(schema, "lt_a", "id", newSplit[j],
							// "lc");
							String attrType = attrSplit[1];
							if (!attrNames.contains(attrType)) {
								attrNames.add(attrType);
								attrCount.add(1);
								ArrayList<String> tmpItems = new ArrayList<String>();
								tmpItems.add(newSplit[j]);
								la_tItems.add(tmpItems);
							} else {
								int lcIdx = attrNames.indexOf(attrType);
								attrCount.set(lcIdx, (attrCount.get(lcIdx) + 1));
								la_tItems.get(lcIdx).add(newSplit[j]);
							}
							// lcNames.add(db.getObjectColumnValue(schema, "lt_a", "id", newSplit[j],
							// "lc"));
						}

						maxNumber = -1;
						for (int j = 0; j < attrCount.size(); j++) {
							if (attrCount.get(j) > maxNumber) {
								maxNumber = attrCount.get(j);
							}
						}

						// boolean singleWinner = true;
						winners = new ArrayList<Integer>();
						tmpCounter = 0;
						for (int j = 0; j < attrCount.size(); j++) {
							if (attrCount.get(j) == maxNumber) {
								tmpCounter++;
								winners.add(j);
							}
						}

						if (tmpCounter == 1) {
							// String lcType = db.getObjectColumnValue(schema, "lt_a", "id", bestMatchingIV,
							// "lc");
							//
							column = "t_class_n" + (normIdx + 1);
							update = "'" + attrNames.get(winners.get(0)) + "'";

							if (!batch) {
								db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
							} else {
								batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id",
										neuronId, column, update));
							}

						} else {
							ArrayList<Float> classAQE = new ArrayList<Float>();
							// ArrayList<String> className = new ArrayList<String>();
							Neuron neuron = cod.neurons[i - 1];

							// parse thru each group
							for (int j = 0; j < winners.size(); j++) {
								float tmpAQE = 0f;

								for (int k = 0; k < la_tItems.get(winners.get(j)).size(); k++) {
									float[] ivAttributes = new float[neuron.getAttributes().length];

									for (int l = 0; l < ivAttributes.length; l++) {
										// get the target column in postgres
										String targetColumn = "a" + (l + 1) + "_n" + (normIdx + 1);
										String tmpValue = db.getObjectColumnValue(schema, "lt_a", "id",
												la_tItems.get(winners.get(j)).get(k), targetColumn);

										ivAttributes[l] = Float.parseFloat(tmpValue);
									}

									tmpAQE += neuron.getDistance(ivAttributes, distMeasure);
								}

								tmpAQE /= la_tItems.get(winners.get(j)).size();
								classAQE.add(tmpAQE);
								// className.add(e)
								// ArrayList<Float>
							}

							int winningIdx = 0;
							float minAQE = classAQE.get(0);
							for (int j = 1; j < classAQE.size(); j++) {
								if (classAQE.get(j) < minAQE) {
									minAQE = classAQE.get(j);
									winningIdx = j;
								}
							}

							column = "t_class_n" + (normIdx + 1);
							update = "'" + attrNames.get(winners.get(winningIdx)) + "'";

							if (!batch) {
								db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
							} else {
								batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id",
										neuronId, column, update));
							}
						}
					}
				} else if (type.equalsIgnoreCase("la_t")) {

					if (newSplit.length == 1) { // case 1: A single IV maps to the neuron

						String[] newDataSplit = newData.split("_");

						column = "a_class_n" + (normIdx + 1);
						update = "'" + newDataSplit[1] + "'";

						if (!batch) {
							db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
						} else {
							batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id",
									neuronId, column, update));
						}
					} else { // case 2: Multiple IVs map to the neuron

						// String[] lcTypes = new String[newSplit.length];

						ArrayList<String> attrNames = new ArrayList<String>();
						ArrayList<Integer> attrCount = new ArrayList<Integer>();
						ArrayList<ArrayList<String>> la_tItems = new ArrayList<ArrayList<String>>();

						for (int j = 0; j < newSplit.length; j++) {
							String[] attrSplit = newSplit[j].split("_");
							// String lcType = db.getObjectColumnValue(schema, "lt_a", "id", newSplit[j],
							// "lc");
							String attrType = attrSplit[1];
							if (!attrNames.contains(attrType)) {
								attrNames.add(attrType);
								attrCount.add(1);
								ArrayList<String> tmpItems = new ArrayList<String>();
								tmpItems.add(newSplit[j]);
								la_tItems.add(tmpItems);
							} else {
								int lcIdx = attrNames.indexOf(attrType);
								attrCount.set(lcIdx, (attrCount.get(lcIdx) + 1));
								la_tItems.get(lcIdx).add(newSplit[j]);
							}
							// lcNames.add(db.getObjectColumnValue(schema, "lt_a", "id", newSplit[j],
							// "lc"));
						}

						int maxNumber = -1;
						for (int j = 0; j < attrCount.size(); j++) {
							if (attrCount.get(j) > maxNumber) {
								maxNumber = attrCount.get(j);
							}
						}

						// boolean singleWinner = true;
						ArrayList<Integer> winners = new ArrayList<Integer>();
						int tmpCounter = 0;
						for (int j = 0; j < attrCount.size(); j++) {
							if (attrCount.get(j) == maxNumber) {
								tmpCounter++;
								winners.add(j);
							}
						}

						if (tmpCounter == 1) {
							// String lcType = db.getObjectColumnValue(schema, "lt_a", "id", bestMatchingIV,
							// "lc");
							//
							column = "a_class_n" + (normIdx + 1);
							update = "'" + attrNames.get(winners.get(0)) + "'";

							if (!batch) {
								db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
							} else {
								batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id",
										neuronId, column, update));
							}

						} else {
							ArrayList<Float> classAQE = new ArrayList<Float>();
							// ArrayList<String> className = new ArrayList<String>();
							Neuron neuron = cod.neurons[i - 1];

							// parse thru each group
							for (int j = 0; j < winners.size(); j++) {
								float tmpAQE = 0f;

								for (int k = 0; k < la_tItems.get(winners.get(j)).size(); k++) {
									float[] ivAttributes = new float[neuron.getAttributes().length];

									for (int l = 0; l < ivAttributes.length; l++) {
										String targetColumn = "t" + (l + 1) + "_n" + (normIdx + 1);
										String tmpValue = db.getObjectColumnValue(schema, "la_t", "id",
												la_tItems.get(winners.get(j)).get(k), targetColumn);

										ivAttributes[l] = Float.parseFloat(tmpValue);
									}

									tmpAQE += neuron.getDistance(ivAttributes, distMeasure);
								}

								tmpAQE /= la_tItems.get(winners.get(j)).size();
								classAQE.add(tmpAQE);
								// className.add(e)
								// ArrayList<Float>
							}

							int winningIdx = 0;
							float minAQE = classAQE.get(0);
							for (int j = 1; j < classAQE.size(); j++) {
								if (classAQE.get(j) < minAQE) {
									minAQE = classAQE.get(j);
									winningIdx = j;
								}
							}

							column = "a_class_n" + (normIdx + 1);
							update = "'" + attrNames.get(winners.get(winningIdx)) + "'";

							if (!batch) {
								db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
							} else {
								batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id",
										neuronId, column, update));
							}
						}
					}
				}
				//
			} else {
				if (type.equalsIgnoreCase("lt_a")) {
					String bestMatchingIV = "";

					InputVector tmpIV = new InputVector(0, cod.neurons[i - 1].getAttributes(), neuronId, "noID");

					Neuron[] inputVectors = new Neuron[dat.inputVectors.length];

					int tmpCounter = 0;
					for (InputVector inputVector : dat.inputVectors) {
						inputVectors[tmpCounter] = new Neuron(tmpCounter, inputVector.getAttributes());
						tmpCounter++;
					}

					// for (int j = 0; j <
					// inputVectors[inputVectors.length-1].getAttributes().length; j++) {
					// System.out.println(inputVectors[inputVectors.length-1].getAttributes()[j]);
					// }
					//
					// System.out.println("");

					// for (int j = 0; j < tmpIV.getAttributes().length; j++) {
					// System.out.println(tmpIV.getAttributes()[j]);
					// }
					// System.out.println("FIRST");
					// System.out.println(tmpIV.getMatchingNeuronID());
					// System.out.println("");
					// for (InputVector inputVector : dat.inputVectors) {
					BestMatchingUnit.findBMU(tmpIV, inputVectors, distMeasure);
					// dat.inputVectors[0].get
					// bestMatchingIV +=
					// dat.inputVectors[tmpIV.getMatchingNeuronID()].getName().toString();
					String ivName = dat.inputVectors[tmpIV.getMatchingNeuronID()].getName().toString();
					// System.out.println("SECOND");
					// System.out.println(tmpIV.getMatchingNeuronID());
					// System.out.println(dat.inputVectors[tmpIV.getMatchingNeuronID()].getName().toString());
					String[] splitName = ivName.split(",");
					// output.add(inputVector.getName() + "," +
					// (inputVector.getMatchingNeuronID()+1));
					// }

					bestMatchingIV += splitName[0] + "_" + splitName[1];

					String column = "ivs_n" + (normIdx + 1);
					String update = "'_" + bestMatchingIV + "'";

					if (!batch) {
						db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
					} else {
						batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id", neuronId,
								column, update));
					}

					String lcType = db.getObjectColumnValue(schema, "lt_a", "id", bestMatchingIV, "lc");

					column = "lc_n" + (normIdx + 1);
					update = "'" + lcType + "'";

					if (!batch) {
						db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
					} else {
						batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id", neuronId,
								column, update));
					}

					column = "t_class_n" + (normIdx + 1);
					update = "'" + splitName[1] + "'";

					if (!batch) {
						db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
					} else {
						batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id", neuronId,
								column, update));
					}

				} else if (type.equalsIgnoreCase("la_t")) {
					String bestMatchingIV = "";

					InputVector tmpIV = new InputVector(0, cod.neurons[i - 1].getAttributes(), neuronId, "noID");

					Neuron[] inputVectors = new Neuron[dat.inputVectors.length];

					int tmpCounter = 0;
					for (InputVector inputVector : dat.inputVectors) {
						inputVectors[tmpCounter] = new Neuron(tmpCounter, inputVector.getAttributes());
						tmpCounter++;
					}

					BestMatchingUnit.findBMU(tmpIV, inputVectors, distMeasure);

					String ivName = dat.inputVectors[tmpIV.getMatchingNeuronID()].getName().toString();

					String[] splitName = ivName.split(",");

					bestMatchingIV += splitName[0] + "_" + splitName[1];

					String column = "ivs_n" + (normIdx + 1);
					String update = "'_" + bestMatchingIV + "'";

					if (!batch) {
						db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
					} else {
						batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id", neuronId,
								column, update));
					}

					column = "a_class_n" + (normIdx + 1);
					update = "'" + splitName[1] + "'";

					if (!batch) {
						db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
					} else {
						batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id", neuronId,
								column, update));
					}
				}
			}

			for (int j = 0; j < cod.neurons[i - 1].getAttributes().length; j++) {

				String column = "";
				String update = "" + cod.neurons[i - 1].getAttributes()[j];

				// int computeIdx = j+1;

				if (type.equalsIgnoreCase("L_AT")) {

					if (j < numTimes) {
						column = "a1_t" + (j + 1) + "_n" + (normIdx + 1);
					} else {
						int attrIdx = j / numTimes;
						int timeIdx = j % numTimes;

						column = "a" + (attrIdx + 1) + "_t" + (timeIdx + 1) + "_n" + (normIdx + 1);
					}
				} else if (type.equalsIgnoreCase("LA_T")) {
					column = "t" + (j + 1) + "_n" + (normIdx + 1);
				} else if (type.equalsIgnoreCase("LT_A")) {
					column = "a" + (j + 1) + "_n" + (normIdx + 1);
				}
				// System.out.println("attribute: " + cod.neurons[i-1].getAttributes()[j]);

				if (!batch) {
					db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
				} else {
					batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id", neuronId,
							column, update));
				}
			}
		}

		// comment out below here to not perform kmeans
		// for (int j = 2; j <= kMax; j++) {
		// 	String[] tmpKMeans = getNeuronKMeans(datFilePath, codFilePath, type, j, distMeasure);

		// 	for (int k = 0; k < tmpKMeans.length - 1; k++) {
		// 		String neuronId = (k + 1) + "";
		// 		String column = "k" + j + "_n" + (normIdx + 1);
		// 		String update = "'" + tmpKMeans[k] + "'";

		// 		if (!batch) {
		// 			db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
		// 		} else {
		// 			batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id", neuronId,
		// 					column, update));
		// 		}
		// 	}

		// 	String column = "k" + j + "_n" + (normIdx + 1);
		// 	String update = tmpKMeans[tmpKMeans.length - 1];

		// 	if (!batch) {
		// 		db.updateTable(schema, "sse", "id", type.toLowerCase(), column, update);
		// 	} else {
		// 		batchInsertion.add(db.updateQueryBuilder(schema, "sse", "id", type.toLowerCase(), column, update));
		// 	}

		// }

		// until here for kmeans

		if (batch) {
			String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
			try {
				db.insertIntoTableBatch(schema + "." + type.toLowerCase() + "_geom", batchInsertionAr);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void writeNeuronIVs(PostgreSQLJDBC db, String schema, String ts, String codFilePath, String datFilePath, int distMeasure, int normalization) {
		
		// read-in neurons (COD file)
		Cod cod = FileHandler.readCodFile(codFilePath, ts);
		
		// read-in input vectors (DAT file)
		Dat dat = FileHandler.readDatFile(datFilePath, ts);
		
		ArrayList<String> ivData = getIV2BMU(datFilePath, codFilePath, ts, distMeasure);
		
		ArrayList<String> batchUpdate = new ArrayList<String>();
		
		// iterate thru all neurons
		// record neuron to IV assignments
		for (int i = 1; i <= cod.neurons.length; i++) {
			String newData = "";
			String neuronId = i + "";
			String removeData = "";

			// create comma-separated list of IV's
			for (int j = 0; j < ivData.size(); j++) {
				// System.out.println(ivData.get(j));
				String[] split = ivData.get(j).split(",");

				if (split[1].equalsIgnoreCase(neuronId)) {
					removeData = ivData.get(j);
					if (newData.equals("")) {
						if (ts.equalsIgnoreCase("l_at") || ts.equalsIgnoreCase("a_lt") || ts.equalsIgnoreCase("t_la")) {
							String idKey = split[0];
							
							if (split[0].toLowerCase().startsWith("l") || split[0].toLowerCase().startsWith("a") || split[0].toLowerCase().startsWith("t")) {
								idKey = split[0].substring(1);
							}
							
							newData = idKey;
						} else if (ts.equalsIgnoreCase("at_l") || ts.equalsIgnoreCase("lt_a") || ts.equalsIgnoreCase("la_t")) {
							String[] subSplit = split[0].split("_");
							
							String[] idKeys = {"", ""};
							
							if (subSplit[0].toLowerCase().startsWith("l") || subSplit[0].toLowerCase().startsWith("a") || subSplit[0].toLowerCase().startsWith("t")) {
								idKeys[0] = subSplit[0].substring(1);
							}
							
							if (subSplit[1].toLowerCase().startsWith("l") || subSplit[1].toLowerCase().startsWith("a") || subSplit[1].toLowerCase().startsWith("t")) {
								idKeys[1] = subSplit[1].substring(1);
							}
							
							newData = idKeys[0] + "_" + idKeys[1];
						}									
						
					} else {			
						
						if (ts.equalsIgnoreCase("l_at") || ts.equalsIgnoreCase("a_lt") || ts.equalsIgnoreCase("t_la")) {
							String idKey = split[0];
							
							if (split[0].toLowerCase().startsWith("l") || split[0].toLowerCase().startsWith("a") || split[0].toLowerCase().startsWith("t")) {
								idKey = split[0].substring(1);
							}
							
							newData += "," + idKey;
						} else if (ts.equalsIgnoreCase("at_l") || ts.equalsIgnoreCase("lt_a") || ts.equalsIgnoreCase("la_t")) {
							String[] subSplit = split[0].split("_");
							
							String[] idKeys = {"", ""};
							
							if (subSplit[0].toLowerCase().startsWith("l") || subSplit[0].toLowerCase().startsWith("a") || subSplit[0].toLowerCase().startsWith("t")) {
								idKeys[0] = subSplit[0].substring(1);
							}
							
							if (subSplit[1].toLowerCase().startsWith("l") || subSplit[1].toLowerCase().startsWith("a") || subSplit[1].toLowerCase().startsWith("t")) {
								idKeys[1] = subSplit[1].substring(1);
							}
							
							newData += "," + idKeys[0] + "_" + idKeys[1];
						}						
					}
				}
			}
			
			ivData.remove(removeData);
			
			String[] idKeys = {"id","normalization"};
			String[] idVals = new String[2];
			idVals[0] = neuronId;
			idVals[1] = normalization + "";

			// update table if list of IV's is not empty
			if (!newData.equals("")) {

				String column = "ivs";
				String update = "'" + newData + "'";				

				batchUpdate.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_neurons", idKeys, idVals,
						column, update));

				String[] newSplit = newData.split(",");

				column = "num_ivs";
				update = "'" + newSplit.length + "'";

				batchUpdate.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_neurons", idKeys, idVals,
						column, update));
				
			} else {
				String bestMatchingIV = "";

				InputVector tmpIV = new InputVector(0, cod.neurons[i - 1].getAttributes(), neuronId, "noID");

				Neuron[] inputVectors = new Neuron[dat.inputVectors.length];

				int tmpCounter = 0;
				for (InputVector inputVector : dat.inputVectors) {
					inputVectors[tmpCounter] = new Neuron(tmpCounter, inputVector.getAttributes());
					tmpCounter++;
				}

				BestMatchingUnit.findBMU(tmpIV, inputVectors, distMeasure);
				String ivName = dat.inputVectors[tmpIV.getMatchingNeuronID()].getName().toString();
				String[] splitName = ivName.split(",");

				bestMatchingIV += splitName[0].substring(1);
				
				if (ts.equalsIgnoreCase("at_l") || ts.equalsIgnoreCase("la_t") || ts.equalsIgnoreCase("lt_a")) {
					bestMatchingIV +=  "_" + splitName[1].substring(1);
				}

				String column = "ivs";
				String update = "'_" + bestMatchingIV + "'";

				batchUpdate.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_neurons", idKeys, idVals,
						column, update));

			}
		}
		
		String[] batchUpdateAr = batchUpdate.toArray(new String[batchUpdate.size()]);		// convert ArrayList to an array			
		
		try {  // try to insert the data into the table
			db.insertIntoTableBatch(schema + "." + ts.toLowerCase(), batchUpdateAr, BATCH_SIZE);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	public static void writeIV2BMU(PostgreSQLJDBC db, String schema, String ts, ArrayList<String> ivData, int normalization) {
		
		ArrayList<String> batchUpdate = new ArrayList<String>();

		// write the bmu attribute to SOM Input Vector tables (L_AT, LA_T, LT_A)
		for (int i = 0; i < ivData.size(); i++) {
			if (ts.equalsIgnoreCase("l_at") || ts.equalsIgnoreCase("a_lt") || ts.equalsIgnoreCase("t_la")) {
				String[] split = ivData.get(i).split(",");
				String column = "bmu";
				String update = split[1];
				
				String[] idKeys = {"id","normalization"};
				String[] idVals = new String[2];
				
				String idKey = split[0];
				
				if (idKey.toLowerCase().startsWith("l") || idKey.toLowerCase().startsWith("a") || idKey.toLowerCase().startsWith("t")) {
					idVals[0] = idKey.substring(1);
				}
				
				idVals[1] = normalization + "";
				
				batchUpdate.add(db.updateQueryBuilder(schema, ts.toLowerCase(), idKeys, idVals, column, update));

			} else if (ts.equalsIgnoreCase("at_l") || ts.equalsIgnoreCase("lt_a") || ts.equalsIgnoreCase("la_t")) {
				String[] split = ivData.get(i).split(",");
				String column = "bmu";
				String update = split[1];
				
				String[] idKeys = {"id1","id2","normalization"};
				String[] idVals = new String[3];
				String[] splitCompositeObj = split[0].split("_");
				if (splitCompositeObj[0].toLowerCase().startsWith("l") || splitCompositeObj[0].toLowerCase().startsWith("a")) {
					idVals[0] = splitCompositeObj[0].substring(1);
				}
				
				if (splitCompositeObj[1].toLowerCase().startsWith("t") || splitCompositeObj[1].toLowerCase().startsWith("a")) {
					idVals[1] = splitCompositeObj[1].substring(1);
				}
				
				idVals[2] = normalization + "";
				
				batchUpdate.add(db.updateQueryBuilder(schema, ts.toLowerCase(), idKeys, idVals, column, update));
			}
		}
		
		String[] batchUpdateAr = batchUpdate.toArray(new String[batchUpdate.size()]);		// convert ArrayList to an array			
		
		try {  // try to insert the data into the table
			db.insertIntoTableBatch(schema + "." + ts.toLowerCase(), batchUpdateAr, BATCH_SIZE);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public static void writeNeuronAttributes(PostgreSQLJDBC db, String schema, String perspective, 
											 String codFilePath, int normalization) {
		
		ArrayList<String> batchSQL = new ArrayList<String>();
				
		// read-in neurons (COD file)
		Cod cod = FileHandler.readCodFile(codFilePath, perspective);
		String[] attributes;
		try {
			attributes = FileHandler.readCodAttributesToArrayList(codFilePath);
			String[] keys = {"id", "normalization"};
			
			for (int i = 1; i <= cod.neurons.length; i++) {
				
				String[] ids = {i + "", normalization + ""};
			
				for (int j = 0; j < cod.neurons[i - 1].getAttributes().length; j++) {
		
					String column = attributes[j];
					String update = "" + cod.neurons[i - 1].getAttributes()[j];				

					batchSQL.add(db.updateQueryBuilder(schema, perspective.toLowerCase() + "_neurons", keys, ids,
							column, update));
				}
			}
			
			String[] batchExecute = batchSQL.toArray(new String[batchSQL.size()]);		// convert ArrayList to an array
			db.insertIntoTableBatch(schema + "." + perspective.toLowerCase(), batchExecute, BATCH_SIZE);		// try to insert the data into the table
		} catch (IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}				
	}
	
	public static void extractSOMData(String datFilePath, String codFilePath, String ts, int kMax, int distMeasure,
			String schema, PostgreSQLJDBC db, int normIdx, boolean batch, int batchSize) {

		// read-in input vectors (DAT file)
		Dat dat = FileHandler.readDatFile(datFilePath, ts);

		// read-in neurons (COD file)
		Cod cod = FileHandler.readCodFile(codFilePath, ts);

		ArrayList<String> ivData = getIV2BMU(datFilePath, codFilePath, ts, distMeasure);
		ArrayList<String> batchInsertion = new ArrayList<String>();

		int numAttributes = db.getTableLength(schema, "attribute_key");

		// int numNormalizations = db.getTableLength(schema, "normalization_key");

		// get the number of time objects
		int numTimes = db.getTableLength(schema, "time_key");

		// write the bmu_n# attribute to SOM Input Vector tables (L_AT, LA_T, LT_A)
		for (int i = 0; i < ivData.size(); i++) {
			if (ts.equalsIgnoreCase("l_at") || ts.equalsIgnoreCase("a_lt") || ts.equalsIgnoreCase("t_la")) {
				String[] split = ivData.get(i).split(",");
				String column = "bmu_n" + (normIdx + 1);
				String update = split[1];
				
				String idKey = split[0];
				
				if (idKey.toLowerCase().startsWith("l") || idKey.toLowerCase().startsWith("a") || idKey.toLowerCase().startsWith("t")) {
					idKey = idKey.substring(1);
				}
				if (!batch) {
					db.updateTable(schema, ts.toLowerCase(), "id", idKey, column, update);
				} else {
					batchInsertion.add(db.updateQueryBuilder(schema, ts.toLowerCase(), "id", idKey, column, update));
				}
			} else if (ts.equalsIgnoreCase("at_l") || ts.equalsIgnoreCase("lt_a") || ts.equalsIgnoreCase("la_t")) {
				String[] split = ivData.get(i).split(",");
				String column = "bmu_n" + (normIdx + 1);
				String update = split[1];
				
				String[] idKeys = {"id1","id2"};
				String[] idVals = new String[2];
				String[] splitCompositeObj = split[0].split("_");
				if (splitCompositeObj[0].toLowerCase().startsWith("l") || splitCompositeObj[0].toLowerCase().startsWith("a")) {
					idVals[0] = splitCompositeObj[0].substring(1);
				}
				
				if (splitCompositeObj[1].toLowerCase().startsWith("t") || splitCompositeObj[1].toLowerCase().startsWith("a")) {
					idVals[1] = splitCompositeObj[1].substring(1);
				}
				
				
				if (!batch) {
					db.updateTable(schema, ts.toLowerCase(), idKeys, idVals, column, update);
				} else {
					batchInsertion.add(db.updateQueryBuilder(schema, ts.toLowerCase(), idKeys, idVals, column, update));
				}
			}
		}

		// iterate thru all neurons
		// record neuron to IV assignments
		for (int i = 1; i <= cod.neurons.length; i++) {
			String newData = "";
			String neuronId = i + "";
			ArrayList<String> removeData = new ArrayList<String>();

			// create comma-separated list of IV's
			for (int j = 0; j < ivData.size(); j++) {
				// System.out.println(ivData.get(j));
				String[] split = ivData.get(j).split(",");

				if (split[1].equalsIgnoreCase(neuronId)) {
					removeData.add(ivData.get(j));
					if (newData.equals("")) {
						if (ts.equalsIgnoreCase("l_at") || ts.equalsIgnoreCase("a_lt") || ts.equalsIgnoreCase("t_la")) {
							String idKey = split[0];
							
							if (split[0].toLowerCase().startsWith("l") || split[0].toLowerCase().startsWith("a") || split[0].toLowerCase().startsWith("t")) {
								idKey = split[0].substring(1);
							}
							
							newData = idKey;
						} else if (ts.equalsIgnoreCase("at_l") || ts.equalsIgnoreCase("lt_a") || ts.equalsIgnoreCase("la_t")) {
							String[] subSplit = split[0].split("_");
							
							String[] idKeys = {"", ""};
							
							if (subSplit[0].toLowerCase().startsWith("l") || subSplit[0].toLowerCase().startsWith("a") || subSplit[0].toLowerCase().startsWith("t")) {
								idKeys[0] = subSplit[0].substring(1);
							}
							
							if (subSplit[1].toLowerCase().startsWith("l") || subSplit[1].toLowerCase().startsWith("a") || subSplit[1].toLowerCase().startsWith("t")) {
								idKeys[1] = subSplit[1].substring(1);
							}
							
							newData = idKeys[0] + "_" + idKeys[1];
						}									
						
					} else {			
						
						if (ts.equalsIgnoreCase("l_at") || ts.equalsIgnoreCase("a_lt") || ts.equalsIgnoreCase("t_la")) {
							String idKey = split[0];
							
							if (split[0].toLowerCase().startsWith("l") || split[0].toLowerCase().startsWith("a") || split[0].toLowerCase().startsWith("t")) {
								idKey = split[0].substring(1);
							}
							
							newData += "," + idKey;
						} else if (ts.equalsIgnoreCase("at_l") || ts.equalsIgnoreCase("lt_a") || ts.equalsIgnoreCase("la_t")) {
							String[] subSplit = split[0].split("_");
							
							String[] idKeys = {"", ""};
							
							if (subSplit[0].toLowerCase().startsWith("l") || subSplit[0].toLowerCase().startsWith("a") || subSplit[0].toLowerCase().startsWith("t")) {
								idKeys[0] = subSplit[0].substring(1);
							}
							
							if (subSplit[1].toLowerCase().startsWith("l") || subSplit[1].toLowerCase().startsWith("a") || subSplit[1].toLowerCase().startsWith("t")) {
								idKeys[1] = subSplit[1].substring(1);
							}
							
							newData += "," + idKeys[0] + "_" + idKeys[1];
						}						
					}
				}
			}

			// update table if list of IV's is not empty
			if (!newData.equals("")) {
				for (int j = 0; j < removeData.size(); j++) {
					ivData.remove(removeData.get(j));
				}
				String column = "ivs_n" + (normIdx + 1);
				String update = "'" + newData + "'";

				if (!batch) {
					db.updateTable(schema, ts.toLowerCase() + "_geom", "id", neuronId, column, update);
				} else {
					batchInsertion.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_geom", "id", neuronId,
							column, update));
				}

				String[] newSplit = newData.split(",");

				column = "num_ivs_n" + (normIdx + 1);
				update = "'" + newSplit.length + "'";

				if (!batch) {
					db.updateTable(schema, ts.toLowerCase() + "_geom", "id", neuronId, column, update);
				} else {
					batchInsertion.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_geom", "id", neuronId,
							column, update));
				}

				if (ts.equalsIgnoreCase("lt_a")) {
					// System.out.println(newData);
					// String[] newSplit = newData.split(",");
					// System.out.println(newSplit.length);

					if (newSplit.length == 1) { // case 1: A single IV maps to the neuron
//						String lcType = db.getObjectColumnValue(schema, "lt_a", "id", newData, "lc");
						String[] compositeColumns = {"id1","id2"};
						String[] compositeValues = newData.split("_");						
						String lcType = db.getObjectColumnValue(schema, "lt_a", compositeColumns, compositeValues, "lc");

						column = "lc_n" + (normIdx + 1);
						update = "'" + lcType + "'";

						if (!batch) {
							db.updateTable(schema, ts.toLowerCase() + "_geom", "id", neuronId, column, update);
						} else {
							batchInsertion.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_geom", "id",
									neuronId, column, update));
						}

						String[] newDataSplit = newData.split("_");

						column = "t_class_n" + (normIdx + 1);
						update = "'" + newDataSplit[1] + "'";

						if (!batch) {
							db.updateTable(schema, ts.toLowerCase() + "_geom", "id", neuronId, column, update);
						} else {
							batchInsertion.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_geom", "id",
									neuronId, column, update));
						}
					} else { // case 2: Multiple IVs map to the neuron

						// String[] lcTypes = new String[newSplit.length];

						// list to store the unique LC classes that map to the neuron
						ArrayList<String> lcNames = new ArrayList<String>();

						// list to store the count for each LC class that maps to the neuron
						ArrayList<Integer> lcCount = new ArrayList<Integer>();

						// 2D list to store the IVs according to their LC class
						ArrayList<ArrayList<String>> lt_aItems = new ArrayList<ArrayList<String>>();

						// iterate thru all IVs
						for (int j = 0; j < newSplit.length; j++) {

							// store the LC class
//							String lcType = db.getObjectColumnValue(schema, "lt_a", "id", newSplit[j], "lc");
							
							String[] compositeColumns = {"id1","id2"};
							String[] compositeValues = newSplit[j].split("_");						
							String lcType = db.getObjectColumnValue(schema, "lt_a", compositeColumns, compositeValues, "lc");
							

							if (!lcNames.contains(lcType)) { // if list does not contain the LC class
								// store the LC class name
								lcNames.add(lcType);
								// initialize the count to 1
								lcCount.add(1);
								// Create the list of IVs that correspond to this LC class
								ArrayList<String> tmpItems = new ArrayList<String>();
								// append the first IV for that LC class
								tmpItems.add(newSplit[j]);
								// store the list for that LC class
								lt_aItems.add(tmpItems);
							} else { // if list DOES contain the LC class
								// get the index that corresponds to a particular LC class
								int lcIdx = lcNames.indexOf(lcType);
								// increment the count for that LC class
								lcCount.set(lcIdx, (lcCount.get(lcIdx) + 1));
								// append the IV to that LC class
								lt_aItems.get(lcIdx).add(newSplit[j]);
							}
							// lcNames.add(db.getObjectColumnValue(schema, "lt_a", "id", newSplit[j],
							// "lc"));
						}

						// determine which LC class has the most IVs
						// initialize count to -1
						int maxNumber = -1;

						// iterate thru all LC class counts
						for (int j = 0; j < lcCount.size(); j++) {
							// store the max
							if (lcCount.get(j) > maxNumber) {
								maxNumber = lcCount.get(j);
							}
						}

						// boolean singleWinner = true;

						// store the index(es) of the LC class(es) that have the max count
						ArrayList<Integer> winners = new ArrayList<Integer>();
						int tmpCounter = 0;
						for (int j = 0; j < lcCount.size(); j++) {
							if (lcCount.get(j) == maxNumber) {
								tmpCounter++;
								winners.add(j);
							}
						}

						if (tmpCounter == 1) { // case 1: There is only one winning class
							// String lcType = db.getObjectColumnValue(schema, "lt_a", "id", bestMatchingIV,
							// "lc");
							//
							column = "lc_n" + (normIdx + 1);
							update = "'" + lcNames.get(winners.get(0)) + "'";

							if (!batch) {
								db.updateTable(schema, ts.toLowerCase() + "_geom", "id", neuronId, column, update);
							} else {
								batchInsertion.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_geom", "id",
										neuronId, column, update));
							}

						} else { // case 2: there is a 'tie' between winning classes

							// since no class has the majority, the class that is more similar will be
							// chosen

							// store the AQE for each class
							ArrayList<Float> classAQE = new ArrayList<Float>();
							// ArrayList<String> className = new ArrayList<String>();

							// get the neuron
							Neuron neuron = cod.neurons[i - 1];

							// parse thru each maximum class
							for (int j = 0; j < winners.size(); j++) {

								// initial AQE set to 0
								float tmpAQE = 0f;

								// iterate thru each IV
								for (int k = 0; k < lt_aItems.get(winners.get(j)).size(); k++) {

									// initialize float array to store each of the IV's attributes
									float[] ivAttributes = new float[neuron.getAttributes().length];

									// iterate thru the IV's attributes
									for (int l = 0; l < ivAttributes.length; l++) {

										// get the target column in postgres
										String targetColumn = "a" + (l + 1) + "_n" + (normIdx + 1);
										// get the value stored in the column/row
//										String tmpValue = db.getObjectColumnValue(schema, "lt_a", "id",
//												lt_aItems.get(winners.get(j)).get(k), targetColumn);
										String[] compositeColumns = {"id1","id2"};
										String[] compositeVals = lt_aItems.get(winners.get(j)).get(k).split("_");
										String tmpValue = db.getObjectColumnValue(schema, "lt_a", compositeColumns,
												compositeVals, targetColumn);
										// store the retrieved value in the IV attribute array
										ivAttributes[l] = Float.parseFloat(tmpValue);
									}

									// append the distance between each IV & the mapped neuron
									tmpAQE += neuron.getDistance(ivAttributes, distMeasure);
								}

								// get the Average distance
								tmpAQE /= lt_aItems.get(winners.get(j)).size();
								// store this distance
								classAQE.add(tmpAQE);
								// className.add(e)
								// ArrayList<Float>
							}

							// find the class with the lowest AQE
							int winningIdx = 0;
							float minAQE = classAQE.get(0);
							for (int j = 1; j < classAQE.size(); j++) {
								if (classAQE.get(j) < minAQE) {
									minAQE = classAQE.get(j);
									winningIdx = j;
								}
							}

							column = "lc_n" + (normIdx + 1);
							update = "'" + lcNames.get(winners.get(winningIdx)) + "'";

							if (!batch) {
								db.updateTable(schema, ts.toLowerCase() + "_geom", "id", neuronId, column, update);
							} else {
								batchInsertion.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_geom", "id",
										neuronId, column, update));
							}
						}

						ArrayList<String> attrNames = new ArrayList<String>();
						ArrayList<Integer> attrCount = new ArrayList<Integer>();
						ArrayList<ArrayList<String>> la_tItems = new ArrayList<ArrayList<String>>();

						for (int j = 0; j < newSplit.length; j++) {
							String[] attrSplit = newSplit[j].split("_");
							// String lcType = db.getObjectColumnValue(schema, "lt_a", "id", newSplit[j],
							// "lc");
							String attrType = attrSplit[1];
							if (!attrNames.contains(attrType)) {
								attrNames.add(attrType);
								attrCount.add(1);
								ArrayList<String> tmpItems = new ArrayList<String>();
								tmpItems.add(newSplit[j]);
								la_tItems.add(tmpItems);
							} else {
								int lcIdx = attrNames.indexOf(attrType);
								attrCount.set(lcIdx, (attrCount.get(lcIdx) + 1));
								la_tItems.get(lcIdx).add(newSplit[j]);
							}
							// lcNames.add(db.getObjectColumnValue(schema, "lt_a", "id", newSplit[j],
							// "lc"));
						}

						maxNumber = -1;
						for (int j = 0; j < attrCount.size(); j++) {
							if (attrCount.get(j) > maxNumber) {
								maxNumber = attrCount.get(j);
							}
						}

						// boolean singleWinner = true;
						winners = new ArrayList<Integer>();
						tmpCounter = 0;
						for (int j = 0; j < attrCount.size(); j++) {
							if (attrCount.get(j) == maxNumber) {
								tmpCounter++;
								winners.add(j);
							}
						}

						if (tmpCounter == 1) {
							// String lcType = db.getObjectColumnValue(schema, "lt_a", "id", bestMatchingIV,
							// "lc");
							//
							column = "t_class_n" + (normIdx + 1);
							update = "'" + attrNames.get(winners.get(0)) + "'";

							if (!batch) {
								db.updateTable(schema, ts.toLowerCase() + "_geom", "id", neuronId, column, update);
							} else {
								batchInsertion.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_geom", "id",
										neuronId, column, update));
							}

						} else {
							ArrayList<Float> classAQE = new ArrayList<Float>();
							// ArrayList<String> className = new ArrayList<String>();
							Neuron neuron = cod.neurons[i - 1];

							// parse thru each group
							for (int j = 0; j < winners.size(); j++) {
								float tmpAQE = 0f;

								for (int k = 0; k < la_tItems.get(winners.get(j)).size(); k++) {
									float[] ivAttributes = new float[neuron.getAttributes().length];

									for (int l = 0; l < ivAttributes.length; l++) {
										// get the target column in postgres
										String targetColumn = "a" + (l + 1) + "_n" + (normIdx + 1);
										String[] compositeColumns = {"id1","id2"};
										String[] compositeVals = la_tItems.get(winners.get(j)).get(k).split("_");
										String tmpValue = db.getObjectColumnValue(schema, "lt_a", compositeColumns,
												compositeVals, targetColumn);
//										String tmpValue = db.getObjectColumnValue(schema, "lt_a", "id",
//												la_tItems.get(winners.get(j)).get(k), targetColumn);

										ivAttributes[l] = Float.parseFloat(tmpValue);
									}

									tmpAQE += neuron.getDistance(ivAttributes, distMeasure);
								}

								tmpAQE /= la_tItems.get(winners.get(j)).size();
								classAQE.add(tmpAQE);
								// className.add(e)
								// ArrayList<Float>
							}

							int winningIdx = 0;
							float minAQE = classAQE.get(0);
							for (int j = 1; j < classAQE.size(); j++) {
								if (classAQE.get(j) < minAQE) {
									minAQE = classAQE.get(j);
									winningIdx = j;
								}
							}

							column = "t_class_n" + (normIdx + 1);
							update = "'" + attrNames.get(winners.get(winningIdx)) + "'";

							if (!batch) {
								db.updateTable(schema, ts.toLowerCase() + "_geom", "id", neuronId, column, update);
							} else {
								batchInsertion.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_geom", "id",
										neuronId, column, update));
							}
						}
					}
				} else if (ts.equalsIgnoreCase("la_t")) {

					if (newSplit.length == 1) { // case 1: A single IV maps to the neuron

						String[] newDataSplit = newData.split("_");

						column = "a_class_n" + (normIdx + 1);
						update = "'" + newDataSplit[1] + "'";

						if (!batch) {
							db.updateTable(schema, ts.toLowerCase() + "_geom", "id", neuronId, column, update);
						} else {
							batchInsertion.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_geom", "id",
									neuronId, column, update));
						}
					} else { // case 2: Multiple IVs map to the neuron

						// String[] lcTypes = new String[newSplit.length];

						ArrayList<String> attrNames = new ArrayList<String>();
						ArrayList<Integer> attrCount = new ArrayList<Integer>();
						ArrayList<ArrayList<String>> la_tItems = new ArrayList<ArrayList<String>>();

						for (int j = 0; j < newSplit.length; j++) {
							String[] attrSplit = newSplit[j].split("_");
							// String lcType = db.getObjectColumnValue(schema, "lt_a", "id", newSplit[j],
							// "lc");
							String attrType = attrSplit[1];
							if (!attrNames.contains(attrType)) {
								attrNames.add(attrType);
								attrCount.add(1);
								ArrayList<String> tmpItems = new ArrayList<String>();
								tmpItems.add(newSplit[j]);
								la_tItems.add(tmpItems);
							} else {
								int lcIdx = attrNames.indexOf(attrType);
								attrCount.set(lcIdx, (attrCount.get(lcIdx) + 1));
								la_tItems.get(lcIdx).add(newSplit[j]);
							}
							// lcNames.add(db.getObjectColumnValue(schema, "lt_a", "id", newSplit[j],
							// "lc"));
						}

						int maxNumber = -1;
						for (int j = 0; j < attrCount.size(); j++) {
							if (attrCount.get(j) > maxNumber) {
								maxNumber = attrCount.get(j);
							}
						}

						// boolean singleWinner = true;
						ArrayList<Integer> winners = new ArrayList<Integer>();
						int tmpCounter = 0;
						for (int j = 0; j < attrCount.size(); j++) {
							if (attrCount.get(j) == maxNumber) {
								tmpCounter++;
								winners.add(j);
							}
						}

						if (tmpCounter == 1) {
							// String lcType = db.getObjectColumnValue(schema, "lt_a", "id", bestMatchingIV,
							// "lc");
							//
							column = "a_class_n" + (normIdx + 1);
							update = "'" + attrNames.get(winners.get(0)) + "'";

							if (!batch) {
								db.updateTable(schema, ts.toLowerCase() + "_geom", "id", neuronId, column, update);
							} else {
								batchInsertion.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_geom", "id",
										neuronId, column, update));
							}

						} else {
							ArrayList<Float> classAQE = new ArrayList<Float>();
							// ArrayList<String> className = new ArrayList<String>();
							Neuron neuron = cod.neurons[i - 1];

							// parse thru each group
							for (int j = 0; j < winners.size(); j++) {
								float tmpAQE = 0f;

								for (int k = 0; k < la_tItems.get(winners.get(j)).size(); k++) {
									float[] ivAttributes = new float[neuron.getAttributes().length];

									for (int l = 0; l < ivAttributes.length; l++) {
										String targetColumn = "t" + (l + 1) + "_n" + (normIdx + 1);
										
										String[] compositeColumns = {"id1","id2"};
										String[] compositeVals = la_tItems.get(winners.get(j)).get(k).split("_");
										String tmpValue = db.getObjectColumnValue(schema, "la_t", compositeColumns,
												compositeVals, targetColumn);
										
//										String tmpValue = db.getObjectColumnValue(schema, "la_t", "id",
//												la_tItems.get(winners.get(j)).get(k), targetColumn);

										ivAttributes[l] = Float.parseFloat(tmpValue);
									}

									tmpAQE += neuron.getDistance(ivAttributes, distMeasure);
								}

								tmpAQE /= la_tItems.get(winners.get(j)).size();
								classAQE.add(tmpAQE);
								// className.add(e)
								// ArrayList<Float>
							}

							int winningIdx = 0;
							float minAQE = classAQE.get(0);
							for (int j = 1; j < classAQE.size(); j++) {
								if (classAQE.get(j) < minAQE) {
									minAQE = classAQE.get(j);
									winningIdx = j;
								}
							}

							column = "a_class_n" + (normIdx + 1);
							update = "'" + attrNames.get(winners.get(winningIdx)) + "'";

							if (!batch) {
								db.updateTable(schema, ts.toLowerCase() + "_geom", "id", neuronId, column, update);
							} else {
								batchInsertion.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_geom", "id",
										neuronId, column, update));
							}
						}
					}
				}
				//
			} else {
				if (ts.equalsIgnoreCase("lt_a")) {
					String bestMatchingIV = "";

					InputVector tmpIV = new InputVector(0, cod.neurons[i - 1].getAttributes(), neuronId, "noID");

					Neuron[] inputVectors = new Neuron[dat.inputVectors.length];

					int tmpCounter = 0;
					for (InputVector inputVector : dat.inputVectors) {
						inputVectors[tmpCounter] = new Neuron(tmpCounter, inputVector.getAttributes());
						tmpCounter++;
					}

					// for (int j = 0; j <
					// inputVectors[inputVectors.length-1].getAttributes().length; j++) {
					// System.out.println(inputVectors[inputVectors.length-1].getAttributes()[j]);
					// }
					//
					// System.out.println("");

					// for (int j = 0; j < tmpIV.getAttributes().length; j++) {
					// System.out.println(tmpIV.getAttributes()[j]);
					// }
					// System.out.println("FIRST");
					// System.out.println(tmpIV.getMatchingNeuronID());
					// System.out.println("");
					// for (InputVector inputVector : dat.inputVectors) {
					BestMatchingUnit.findBMU(tmpIV, inputVectors, distMeasure);
					// dat.inputVectors[0].get
					// bestMatchingIV +=
					// dat.inputVectors[tmpIV.getMatchingNeuronID()].getName().toString();
					String ivName = dat.inputVectors[tmpIV.getMatchingNeuronID()].getName().toString();
					// System.out.println("SECOND");
					// System.out.println(tmpIV.getMatchingNeuronID());
					// System.out.println(dat.inputVectors[tmpIV.getMatchingNeuronID()].getName().toString());
					String[] splitName = ivName.split(",");
					// output.add(inputVector.getName() + "," +
					// (inputVector.getMatchingNeuronID()+1));
					// }

					bestMatchingIV += splitName[0].substring(1) + "_" + splitName[1].substring(1);

					String column = "ivs_n" + (normIdx + 1);
					String update = "'_" + bestMatchingIV + "'";

					if (!batch) {
						db.updateTable(schema, ts.toLowerCase() + "_geom", "id", neuronId, column, update);
					} else {
						batchInsertion.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_geom", "id", neuronId,
								column, update));
					}

					String[] compositeColumns = {"id1","id2"};
					String[] compositeValues = new String[2];
					compositeValues[0] = splitName[0].substring(1);
					compositeValues[1] = splitName[1].substring(1);
					String lcType = db.getObjectColumnValue(schema, "lt_a", compositeColumns, compositeValues, "lc");
					// String lcType = db.getObjectColumnValue(schema, "lt_a", "id", bestMatchingIV, "lc");

					column = "lc_n" + (normIdx + 1);
					update = "'" + lcType + "'";

					if (!batch) {
						db.updateTable(schema, ts.toLowerCase() + "_geom", "id", neuronId, column, update);
					} else {
						batchInsertion.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_geom", "id", neuronId,
								column, update));
					}

					column = "t_class_n" + (normIdx + 1);
					update = "'" + splitName[1].substring(1) + "'";

					if (!batch) {
						db.updateTable(schema, ts.toLowerCase() + "_geom", "id", neuronId, column, update);
					} else {
						batchInsertion.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_geom", "id", neuronId,
								column, update));
					}

				} else if (ts.equalsIgnoreCase("la_t")) {
					String bestMatchingIV = "";

					InputVector tmpIV = new InputVector(0, cod.neurons[i - 1].getAttributes(), neuronId, "noID");

					Neuron[] inputVectors = new Neuron[dat.inputVectors.length];

					int tmpCounter = 0;
					for (InputVector inputVector : dat.inputVectors) {
						inputVectors[tmpCounter] = new Neuron(tmpCounter, inputVector.getAttributes());
						tmpCounter++;
					}

					BestMatchingUnit.findBMU(tmpIV, inputVectors, distMeasure);

					String ivName = dat.inputVectors[tmpIV.getMatchingNeuronID()].getName().toString();

					String[] splitName = ivName.split(",");

					bestMatchingIV += splitName[0].substring(1) + "_" + splitName[1].substring(1);

					String column = "ivs_n" + (normIdx + 1);
					String update = "'_" + bestMatchingIV + "'";

					if (!batch) {
						db.updateTable(schema, ts.toLowerCase() + "_geom", "id", neuronId, column, update);
					} else {
						batchInsertion.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_geom", "id", neuronId,
								column, update));
					}

					column = "a_class_n" + (normIdx + 1);
					update = "'" + splitName[1].substring(1) + "'";

					if (!batch) {
						db.updateTable(schema, ts.toLowerCase() + "_geom", "id", neuronId, column, update);
					} else {
						batchInsertion.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_geom", "id", neuronId,
								column, update));
					}
				}
			}

			for (int j = 0; j < cod.neurons[i - 1].getAttributes().length; j++) {

				String column = "";
				String update = "" + cod.neurons[i - 1].getAttributes()[j];

				// int computeIdx = j+1;

				if (ts.equalsIgnoreCase("L_AT")) {

					if (j < numTimes) {
						column = "a1_t" + (j + 1) + "_n" + (normIdx + 1);
					} else {
						int attrIdx = j / numTimes;
						int timeIdx = j % numTimes;

						column = "a" + (attrIdx + 1) + "_t" + (timeIdx + 1) + "_n" + (normIdx + 1);
					}
				} else if (ts.equalsIgnoreCase("LA_T")) {
					column = "t" + (j + 1) + "_n" + (normIdx + 1);
				} else if (ts.equalsIgnoreCase("LT_A")) {
					column = "a" + (j + 1) + "_n" + (normIdx + 1);
				}
				// System.out.println("attribute: " + cod.neurons[i-1].getAttributes()[j]);

				if (!batch) {
					db.updateTable(schema, ts.toLowerCase() + "_geom", "id", neuronId, column, update);
				} else {
					batchInsertion.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_geom", "id", neuronId,
							column, update));
				}
			}
		}

	// 	// comment out below here to not perform kmeans
	// 	for (int j = 2; j <= kMax; j++) {
	// 		String[] tmpKMeans = getNeuronKMeans(datFilePath, codFilePath, ts, j, distMeasure);

	// 		for (int k = 0; k < tmpKMeans.length - 1; k++) {
	// 			String neuronId = (k + 1) + "";
	// 			String column = "k" + j + "_n" + (normIdx + 1);
	// 			String update = "'" + tmpKMeans[k] + "'";

	// 			if (!batch) {
	// 				db.updateTable(schema, ts.toLowerCase() + "_geom", "id", neuronId, column, update);
	// 			} else {
	// 				batchInsertion.add(db.updateQueryBuilder(schema, ts.toLowerCase() + "_geom", "id", neuronId,
	// 						column, update));
	// 			}
	// 		}

	// 		String column = "k" + j + "_n" + (normIdx + 1);
	// 		String update = tmpKMeans[tmpKMeans.length - 1];

	// 		if (!batch) {
	// 			db.updateTable(schema, "sse", "id", ts.toLowerCase(), column, update);
	// 		} else {
	// 			batchInsertion.add(db.updateQueryBuilder(schema, "sse", "id", ts.toLowerCase(), column, update));
	// 		}

	// 	}

	// 	// until here for kmeans

	// 	if (batch) {
	// 		String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
	// 		try {
	// 			db.insertIntoTableBatch(schema + "." + ts.toLowerCase() + "_geom", batchInsertionAr, batchSize);
	// 		} catch (SQLException e) {
	// 			// TODO Auto-generated catch block
	// 			e.printStackTrace();
	// 		}
	// 	}
	// }

	// public static void extractLT_A_LC_SOM(String datFilePath, String codFilePath, String type, int kMax,
	// 		int distMeasure, String schema, PostgreSQLJDBC db, int normIdx, boolean batch, boolean interpolate) {
	// 	Dat dat = FileHandler.readDatFile(datFilePath, type);
	// 	Cod cod = FileHandler.readCodFile(codFilePath, type);

	// 	ArrayList<String> ivData = getIV2BMU(datFilePath, codFilePath, type, distMeasure);
	// 	ArrayList<String> batchInsertion = new ArrayList<String>();

	// 	int numAttributes = db.getTableLength(schema, "attribute_key");

	// 	// int numNormalizations = db.getTableLength(schema, "normalization_key");

	// 	int numTimes = db.getTableLength(schema, "time_key");

	// 	// iterate thru all neurons
	// 	// record input-vector to BMU assignments
	// 	for (int i = 1; i <= cod.neurons.length; i++) {
	// 		String newData = "";
	// 		String neuronId = i + "";
	// 		ArrayList<String> removeData = new ArrayList<String>();

	// 		// create comma-separated list of IV's
	// 		for (int j = 0; j < ivData.size(); j++) {
	// 			String[] split = ivData.get(j).split(",");

	// 			if (split[1].equalsIgnoreCase(neuronId)) {
	// 				removeData.add(ivData.get(j));
	// 				if (newData.equals("")) {
	// 					newData = split[0];
	// 				} else {
	// 					newData = newData + "," + split[0];
	// 				}
	// 			}
	// 		}

	// 		// update table if list of IV's is not empty
	// 		if (!newData.equals("")) {
	// 			for (int j = 0; j < removeData.size(); j++) {
	// 				ivData.remove(removeData.get(j));
	// 			}
	// 			String column = "ivs_n" + (normIdx + 1);
	// 			String update = "'" + newData + "'";

	// 			if (!batch) {
	// 				db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
	// 			} else {
	// 				batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id", neuronId,
	// 						column, update));
	// 			}
	// 		}

	// 		for (int j = 0; j < cod.neurons[i - 1].getAttributes().length; j++) {

	// 			String column = "";
	// 			String update = "" + cod.neurons[i - 1].getAttributes()[j];

	// 			// int computeIdx = j+1;

	// 			if (type.equalsIgnoreCase("L_AT")) {

	// 				if (j < numTimes) {
	// 					column = "a1_t" + (j + 1) + "_n" + (normIdx + 1);
	// 				} else {
	// 					int attrIdx = j / numTimes;
	// 					int timeIdx = j % numTimes;

	// 					column = "a" + (attrIdx + 1) + "_t" + (timeIdx + 1) + "_n" + (normIdx + 1);
	// 				}
	// 			} else if (type.equalsIgnoreCase("LA_T")) {
	// 				column = "t" + (j + 1) + "_n" + (normIdx + 1);
	// 			} else if (type.equalsIgnoreCase("LT_A")) {
	// 				column = "a" + (j + 1) + "_n" + (normIdx + 1);
	// 			}
	// 			// System.out.println("attribute: " + cod.neurons[i-1].getAttributes()[j]);

	// 			if (!batch) {
	// 				db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
	// 			} else {
	// 				batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id", neuronId,
	// 						column, update));
	// 			}
	// 		}
	// 	}

	// 	for (int j = 2; j <= kMax; j++) {
	// 		String[] tmpKMeans = getNeuronKMeans(datFilePath, codFilePath, type, j, distMeasure);

	// 		for (int k = 0; k < tmpKMeans.length - 1; k++) {
	// 			String neuronId = (k + 1) + "";
	// 			String column = "k" + j + "_n" + (normIdx + 1);
	// 			String update = "'" + tmpKMeans[k] + "'";

	// 			if (!batch) {
	// 				db.updateTable(schema, type.toLowerCase() + "_geom", "id", neuronId, column, update);
	// 			} else {
	// 				batchInsertion.add(db.updateQueryBuilder(schema, type.toLowerCase() + "_geom", "id", neuronId,
	// 						column, update));
	// 			}
	// 		}

	// 		String column = "k" + j + "_n" + (normIdx + 1);
	// 		String update = tmpKMeans[tmpKMeans.length - 1];

	// 		if (!batch) {
	// 			db.updateTable(schema, "sse", "id", type.toLowerCase(), column, update);
	// 		} else {
	// 			batchInsertion.add(db.updateQueryBuilder(schema, "sse", "id", type.toLowerCase(), column, update));
	// 		}

	// 	}

	// 	if (batch) {
	// 		String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
	// 		try {
	// 			db.insertIntoTableBatch(schema + "." + type.toLowerCase() + "_geom", batchInsertionAr);
	// 		} catch (SQLException e) {
	// 			// TODO Auto-generated catch block
	// 			e.printStackTrace();
	// 		}
	// 	}
	// }

	// public static String[] getNeuronKMeans(String datFilePath, String codFilePath, String type, int kVal,
	// 		int distMeasure) {
	// 	ArrayList<String> output = new ArrayList<String>();

	// 	Cod cod = FileHandler.readCodFile(codFilePath, type);

	// 	// ArrayList<KMeans> kMeansTotal = new ArrayList<KMeans>();
	// 	// for (int j = 2; j <= kMax; j++) {
	// 	KMeans kMeansClustering = null;
	// 	boolean kMeansComplete = false;
	// 	while (!kMeansComplete) {
	// 		try {
	// 			kMeansClustering = new KMeans(kVal, cod.neurons, distMeasure);
	// 			kMeansComplete = true;
	// 		} catch (IndexOutOfBoundsException e) {
	// 			System.out.println("Invalid option");
	// 			kMeansComplete = false;
	// 		}
	// 	}
	// 	// kMeansTotal.add(kMeansClustering);
	// 	// }

	// 	// ArrayList<ArrayList<ArrayList<Integer>>> allVals = new
	// 	// ArrayList<ArrayList<ArrayList<Integer>>>();
	// 	// for (int j = 0; j < kMeansTotal.size(); j++) {
	// 	ArrayList<ArrayList<Integer>> totalVals = new ArrayList<ArrayList<Integer>>();
	// 	for (int i = 0; i < kMeansClustering.kMeansClusters.length; i++) {
	// 		totalVals.add(kMeansClustering.kMeansClusters[i].memberIDs);
	// 	}
	// 	// allVals.add(totalVals);

	// 	// }
	// 	// System.out.println("!!!!!!!!!!!!!!");
	// 	int dataSize = 0;
	// 	for (int i = 0; i < totalVals.size(); i++) {
	// 		dataSize += totalVals.get(i).size();
	// 	}

	// 	String[] kClasses = new String[dataSize + 1];

	// 	for (int i = 0; i < totalVals.size(); i++) {
	// 		for (int j = 0; j < totalVals.get(i).size(); j++) {
	// 			kClasses[totalVals.get(i).get(j) - 1] = (i + 1) + "";
	// 		}
	// 	}

	// 	kClasses[dataSize] = kMeansClustering.calcSSE(kMeansClustering.distMatrix, kMeansClustering.clusterMatrix) + "";

	// 	return kClasses;
	// 	// return output.toArray(new String[output.size()]);
	// }

	// public static void extractBMU2CSV(String datFilePath, String codFilePath, String iv2BMUOut, String neuron2IVOut,
	// 		int simMeasure) {

	// 	// ArrayList<String> output2 = new ArrayList<String>();

	// 	// Dat dat = FileHandler.readDatFile(datFilePath, "L_AT");
	// 	Cod cod = FileHandler.readCodFile(codFilePath, "L_AT");

	// 	ArrayList<String> ivData = getIV2BMU(datFilePath, codFilePath, "L_AT", simMeasure);
	// 	// int counter = 1;
	// 	//
	// 	// for (InputVector inputVector : dat.inputVectors) {
	// 	// BestMatchingUnit.findBMU(inputVector, cod.neurons, simMeasure);
	// 	// output.add(inputVector.getName() + "," +
	// 	// (inputVector.getMatchingNeuronID()+1));
	// 	//// output2.add(counter + ",-1");
	// 	//// counter++;
	// 	// }
	// 	//
	// 	// System.out.println("FIRST");
	// 	// System.out.println(output.get(0));
	// 	// System.out.println("LAST");
	// 	// System.out.println(output.get(output.size()-1));

	// 	String[] neuron2IV = new String[cod.neurons.length];

	// 	for (int i = 0; i < neuron2IV.length; i++) {
	// 		neuron2IV[i] = "-1";
	// 	}

	// 	FileWriter fw = null;
	// 	try {
	// 		fw = new FileWriter(iv2BMUOut);

	// 		fw.append("inputVector,BMU\n");

	// 		for (int i = 0; i < ivData.size(); i++) {
	// 			fw.append(ivData.get(i) + "\n");
	// 			fw.flush();
	// 			String[] tmpSplit = ivData.get(i).split(",");

	// 			int neuronIdx = Integer.parseInt(tmpSplit[1]);
	// 			neuronIdx--;

	// 			if (neuron2IV[neuronIdx].equals("-1")) {
	// 				neuron2IV[neuronIdx] = tmpSplit[0];
	// 			} else {
	// 				neuron2IV[neuronIdx] = neuron2IV[neuronIdx] + "|" + tmpSplit[0];
	// 			}

	// 		}

	// 		fw.close();

	// 		fw = new FileWriter(neuron2IVOut);

	// 		fw.append("neuron,IVlist\n");

	// 		for (int i = 0; i < neuron2IV.length; i++) {
	// 			fw.append((i + 1) + "," + neuron2IV[i] + "\n");
	// 			fw.flush();
	// 		}
	// 		fw.close();

	// 	} catch (Exception e) {
	// 		e.printStackTrace();
	// 		return;
	// 	}

	// }

	// public static void extractKmeans2CSV(String datFilePath, String codFilePath, String type, int kStart, int kEnd,
	// 		String output, int simMeasure) {

	// 	if (kStart < 2)
	// 		return;

	// 	try {
	// 		FileWriter fw = new FileWriter(output);

	// 		fw.append("id");

	// 		for (int k = kStart; k <= kEnd; k++) {
	// 			fw.append(",k" + k);
	// 		}

	// 		fw.append("\n");

	// 		fw.flush();

	// 		ArrayList<String> lineData = new ArrayList<String>();

	// 		boolean firstTime = true;
	// 		for (int k = kStart; k <= kEnd; k++) {
	// 			String[] tmpKMeans = getNeuronKMeans(datFilePath, codFilePath, type, k, simMeasure);

	// 			if (firstTime) {
	// 				for (int i = 0; i < tmpKMeans.length; i++) {
	// 					lineData.add((i + 1) + "");
	// 				}

	// 				firstTime = false;
	// 			}

	// 			// fw.append()
	// 			for (int i = 0; i < tmpKMeans.length; i++) {
	// 				String currentLine = "" + lineData.get(i);
	// 				currentLine += "," + tmpKMeans[i];
	// 				lineData.set(i, currentLine);
	// 				// fw.append
	// 				// String column = "k" + k +
	// 			}
	// 		}

	// 		for (int i = 0; i < lineData.size(); i++) {
	// 			fw.append(lineData.get(i) + "\n");
	// 		}

	// 		fw.flush();
	// 		fw.close();

	// 	} catch (IOException e) {
	// 		// TODO Auto-generated catch block
	// 		e.printStackTrace();
	// 		return;
	// 	}
	// }

	// // public static void updateBMUColumn(String schema, PostgreSQLJDBC db, File
	// // bmuTable, String type, int normalization, boolean batch) {
	// //
	// // }
	}
}
