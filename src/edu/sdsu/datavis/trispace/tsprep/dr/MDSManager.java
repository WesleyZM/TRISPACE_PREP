package edu.sdsu.datavis.trispace.tsprep.dr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

import org.json.JSONArray;

import ca.pjer.ekmeans.EKmeans;
import edu.sdsu.datavis.trispace.tsprep.db.PostgreSQLJDBC;
import edu.sdsu.datavis.trispace.tsprep.utils.DistanceCalculator;
import mdsj.MDSJ;

public class MDSManager {
	final static String[] PERSPECTIVES = { "L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L" };
	
	public static void kMeansClustering(float[][] infData, String[] labels, int simIdx, PostgreSQLJDBC db, 
			String schema, String tsType, int normIdx, int kMax, int kIterations, boolean batch) {
		
		boolean compositeObject = false;
		
		if (tsType.equalsIgnoreCase("at_l") || tsType.equalsIgnoreCase("lt_a") || tsType.equalsIgnoreCase("la_t")) { 
			compositeObject = true;
		}
		
		ArrayList<String> batchInsertion = new ArrayList<String>();

		// double 2d array to hold the input vectors
		double[][] points = new double[infData.length][infData[0].length];

		// arrays to hold the min/max value of each vector dimension
		double[] minVals = new double[infData[0].length];
		double[] maxVals = new double[infData[0].length];

		// initialize with values
		for (int i = 0; i < points[0].length; i++) {
			minVals[i] = infData[0][i];
			maxVals[i] = infData[0][i];
		}

		// populate min/max arrays and convert float[] to double[]
		for (int i = 0; i < points.length; i++) {
			for (int j = 0; j < points[i].length; j++) {
				points[i][j] = infData[i][j];

				if (maxVals[j] < infData[i][j]) {
					maxVals[j] = infData[i][j];
				}

				if (minVals[j] > infData[i][j]) {
					minVals[j] = infData[i][j];
				}
			}
		}

		if (kMax != -1) {
			
			if (kMax > labels.length - 2) {
				kMax = labels.length - 1;
				
				if (kMax == 1) {
					kMax = 2;
				}
			}
			
			int numRuns = kIterations;
			double previousSSE = Double.MAX_VALUE;
//			ArrayList<Double> totalSSE = new ArrayList<Double>();
//			ArrayList<Integer[]> totalAssignments = new ArrayList<Integer[]>();
//			double[] totalSSE = new double[numRuns];
//			int[][] totalAssignments = new int[numRuns][labels.length];
			for (int i = 2; i <= kMax; i++) {
				ArrayList<Double> totalSSE = new ArrayList<Double>();
				ArrayList<int[]> totalAssignments = new ArrayList<int[]>();
//				double[][]
//				int minIdx = 0;
//				int sseIdx = 0;				
				int attempts = 0;
				boolean proceed = false;
				while (!proceed) {
					// instantiate centroids
					double[][] centroids = new double[i][minVals.length];
					
					// initialize centroids
					for (int j = 0; j < i; j++) {
						for (int k = 0; k < minVals.length; k++) {
							try {
								centroids[j][k] = ThreadLocalRandom.current().nextDouble(minVals[k], maxVals[k]);
							} catch (Exception e) {
								centroids[j][k] = minVals[k];
//								e.printStackTrace();
//								System.out.println("MIN: " + minVals[k] + "  |  MAX: " + maxVals[k]);
//								System.out.println("PROBLEM WITH " + tsType + " - " + normIdx + " :" + j + " " + k);
							}
							
	//						System.out.println("centroid " + j + " " + k + ": " + centroids[j][k]);
						}
					}
					
					// initialize EKmeans
					EKmeans eKmeans = new EKmeans(centroids,points);
					
					// set the number of iterations
					eKmeans.setIteration(kIterations);
					
					// set the distance function
					if (simIdx == 2) {
						EKmeans.DistanceFunction COSINE_SIMILARITY_FUNCTION = new EKmeans.DistanceFunction() {
	
							@Override
							public double distance(double[] attributesA, double[] attributesB) {
								double distMeasure = 0;
	
								double skalarSum = 0;  // the scalar product of IV and Neuron
								double ivLength = 0;  // the length of the attribute-vector of the InputVector
								double neuronLength = 0;  // the length of the attribute vector of the Neuron
								double cosine;			// the cosine of IV and Neuron
								//calculating the cosine similarity measure
								for (int i = 0; i<attributesB.length; i++){
									// only use attributes designated for training
									//if(globals.atts[i].forTraining){
									skalarSum += attributesB[i]*attributesA[i]; //summarize the attribute-wise products of IV * Neuron
									ivLength += (attributesA[i]*attributesA[i]); //summarize the squared attributes of the IV
									neuronLength += (attributesB[i]*attributesB[i]); //summarize the squared attributes of the Neuron
									//}
								}
								ivLength = Math.sqrt(ivLength);
								neuronLength = Math.sqrt(neuronLength);
								cosine = skalarSum / (ivLength*neuronLength); // range 0 to 1; the higher, the more similar; 1 = equal.
								//distMeasure = ((1/cosine) -1); // range 0 - inf; the lower the more similar. this way usable with the same BMU search for distances
	
								if (cosine > 1) {
									cosine = 1;
								}
								distMeasure = 1 - cosine;
								distMeasure = Math.sqrt(distMeasure);
								return distMeasure;
							}
						};
						
						eKmeans.setDistanceFunction(COSINE_SIMILARITY_FUNCTION);
					} else if (simIdx == 3) {
						eKmeans.setDistanceFunction(eKmeans.MANHATTAN_DISTANCE_FUNCTION);
					} else {
						eKmeans.setDistanceFunction(eKmeans.EUCLIDEAN_DISTANCE_FUNCTION);
					}
	
					// run eKmeans
					try {
						eKmeans.run();
					} catch (Exception e) {
						e.printStackTrace();
						continue;
					}
					
//					proceed = true;
					
					// get the input vector class assignments
					int[] assignments = eKmeans.getAssignments();
					
					// get the kmeans centroids
					double[][] kCentroids = eKmeans.getCentroids();
					
					// calculate the SSE
					double sse = calculateSSE(kCentroids,assignments,points,simIdx);
					
					if (Double.isNaN(sse)) {
						continue;
					}
					
//					if (sse < totalSSE[minIdx]) {
//						minIdx = sseIdx;
//					}
//					totalSSE[sseIdx] = sse;
//					totalAssignments[sseIdx] = assignments;
//					sseIdx++;
					
//					if (sse > previousSSE && attempts < 100) {
					if (attempts < numRuns) {
						totalSSE.add(sse);
						totalAssignments.add(assignments);
						attempts++;
						continue;						
					}
					previousSSE = sse;
					
//					if (sseIdx >= numRuns) {
//						proceed = true;
//					} else {
//						continue;
//					}
					proceed = true;
					
					double finalSSE = Collections.min(totalSSE);
					
					int finalIdx = totalSSE.indexOf(finalSSE);
					int[] finalAssignments = totalAssignments.get(finalIdx);
					
					// update the kX_nY column
					for (int j = 0; j < assignments.length; j++) {
						String column = "k" + i + "_n" + (normIdx + 1);
//						String update = totalAssignments[minIdx][j] + "";
//						String update = assignments[j] + "";
						String update = finalAssignments[j] + "";
//						db.updateTable(schema, tsType.toLowerCase() + "_geom", "id", labels[j], column, update);	
						
//						if (!batch) {
//							db.updateTable(schema, tsType.toLowerCase() + "_geom", "id", labels[j], column, update);
//						} else {
//							batchInsertion.add(db.updateQueryBuilder(schema, tsType.toLowerCase() + "_geom", "id", labels[j], column, update));
//						}
						
						if (!batch) { // if not using the batch insertion
							
							if (compositeObject) { // if composite obj (at_l, lt_a, la_t)
								
								String[] idColumns = {"id1", "id2"};
								String[] idSplit = labels[j].split("_");
								String[] idValues = {idSplit[0].substring(1), idSplit[1].substring(1)};
								
								db.updateTable(schema, tsType.toLowerCase() + "_geom", idColumns, idValues, column, update);
								
							} else { // if not composite object (a_lt, l_at, t_la)
								
								db.updateTable(schema, tsType.toLowerCase() + "_geom", "id", labels[j].substring(1), column, update);
							}
							
							
						} else { // if using batch insertion
							
							if (compositeObject) { // if composite obj (at_l, lt_a, la_t)
								
								String[] idColumns = {"id1", "id2"};
								String[] idSplit = labels[j].split("_");
								String[] idValues = {idSplit[0].substring(1), idSplit[1].substring(1)};
								
								batchInsertion.add(db.updateQueryBuilder(schema, tsType.toLowerCase() + "_geom", idColumns, idValues, column, update));
								
							} else { // if not composite object (a_lt, l_at, t_la)
								
								batchInsertion.add(db.updateQueryBuilder(schema, tsType.toLowerCase() + "_geom", "id", labels[j].substring(1), column, update));
							}					
						}
					}
					
					
					
					// update the SSE column
					String column = "k" + i + "_n" + (normIdx + 1);
//					String update = totalSSE[minIdx] + "";
//					String update = sse + "";
					String update = finalSSE + "";
//					db.updateTable(schema, "sse", "id", tsType.toLowerCase(), column, update);
					
					if (!batch) {
						db.updateTable(schema, "sse", "id", tsType.toLowerCase(), column, update);
					} else {
						batchInsertion.add(db.updateQueryBuilder(schema, "sse", "id", tsType.toLowerCase(), column, update));
					}
					
					
				}
			}
			
			if (batch) {
				String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
				try {
					db.insertIntoTableBatch(schema + ".sse", batchInsertionAr);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void kMeansClusteringNew(String perspective, String inputCSV, String output, 
			String schema, int normIdx, int kMin, int kMax, int kIterations, int simIdx) {
		
		boolean compositeObject = false;
		
		if (perspective.equalsIgnoreCase("at_l") || perspective.equalsIgnoreCase("lt_a") || perspective.equalsIgnoreCase("la_t")) { 
			compositeObject = true;
		}
		
		File input = new File(inputCSV);
		
		float[][] inputData = extractData(input, perspective);
		String[] labels = extractLabels(input, perspective);

		// double 2d array to hold the input vectors
		double[][] points = new double[inputData.length][inputData[0].length];

		// arrays to hold the min/max value of each vector dimension
		double[] minVals = new double[inputData[0].length];
		double[] maxVals = new double[inputData[0].length];

		// initialize with values
		for (int i = 0; i < points[0].length; i++) {
			minVals[i] = inputData[0][i];
			maxVals[i] = inputData[0][i];
		}

		// populate min/max arrays and convert float[] to double[]
		for (int i = 0; i < points.length; i++) {
			for (int j = 0; j < points[i].length; j++) {
				points[i][j] = inputData[i][j];

				if (maxVals[j] < inputData[i][j]) {
					maxVals[j] = inputData[i][j];
				}

				if (minVals[j] > inputData[i][j]) {
					minVals[j] = inputData[i][j];
				}
			}
		}

		if (kMax != -1) {
			
			if (kMax > labels.length - 2) {
				kMax = labels.length - 1;
				
				if (kMax == 1) {
					kMax = 2;
				}
			}
			
			int numRuns = kIterations;
			double previousSSE = Double.MAX_VALUE;
			for (int i = 2; i <= kMax; i++) {
				ArrayList<Double> totalSSE = new ArrayList<Double>();
				ArrayList<int[]> totalAssignments = new ArrayList<int[]>();		
				int attempts = 0;
				boolean proceed = false;
				while (!proceed) {
					// instantiate centroids
					double[][] centroids = new double[i][minVals.length];
					
					// initialize centroids
					for (int j = 0; j < i; j++) {
						for (int k = 0; k < minVals.length; k++) {
							try {
								centroids[j][k] = ThreadLocalRandom.current().nextDouble(minVals[k], maxVals[k]);
							} catch (Exception e) {
								centroids[j][k] = minVals[k];
							}
						}
					}
					
					// initialize EKmeans
					EKmeans eKmeans = new EKmeans(centroids,points);
					
					// set the number of iterations
					eKmeans.setIteration(kIterations);
					
					// set the distance function
					if (simIdx == 2) {
						EKmeans.DistanceFunction COSINE_SIMILARITY_FUNCTION = new EKmeans.DistanceFunction() {
	
							@Override
							public double distance(double[] attributesA, double[] attributesB) {
								double distMeasure = 0;
	
								double skalarSum = 0;  // the scalar product of IV and Neuron
								double ivLength = 0;  // the length of the attribute-vector of the InputVector
								double neuronLength = 0;  // the length of the attribute vector of the Neuron
								double cosine;			// the cosine of IV and Neuron
								//calculating the cosine similarity measure
								for (int i = 0; i<attributesB.length; i++){
									// only use attributes designated for training
									//if(globals.atts[i].forTraining){
									skalarSum += attributesB[i]*attributesA[i]; //summarize the attribute-wise products of IV * Neuron
									ivLength += (attributesA[i]*attributesA[i]); //summarize the squared attributes of the IV
									neuronLength += (attributesB[i]*attributesB[i]); //summarize the squared attributes of the Neuron
									//}
								}
								ivLength = Math.sqrt(ivLength);
								neuronLength = Math.sqrt(neuronLength);
								cosine = skalarSum / (ivLength*neuronLength); // range 0 to 1; the higher, the more similar; 1 = equal.
								//distMeasure = ((1/cosine) -1); // range 0 - inf; the lower the more similar. this way usable with the same BMU search for distances
	
								if (cosine > 1) {
									cosine = 1;
								}
								distMeasure = 1 - cosine;
								distMeasure = Math.sqrt(distMeasure);
								return distMeasure;
							}
						};
						
						eKmeans.setDistanceFunction(COSINE_SIMILARITY_FUNCTION);
					} else if (simIdx == 3) {
						eKmeans.setDistanceFunction(eKmeans.MANHATTAN_DISTANCE_FUNCTION);
					} else {
						eKmeans.setDistanceFunction(eKmeans.EUCLIDEAN_DISTANCE_FUNCTION);
					}
	
					// run eKmeans
					try {
						eKmeans.run();
					} catch (Exception e) {
						e.printStackTrace();
						continue;
					}
					
//					proceed = true;
					
					// get the input vector class assignments
					int[] assignments = eKmeans.getAssignments();
					
					// get the kmeans centroids
					double[][] kCentroids = eKmeans.getCentroids();
					
					// calculate the SSE
					double sse = calculateSSE(kCentroids,assignments,points,simIdx);
					
					if (Double.isNaN(sse)) {
						continue;
					}
					
//					if (sse < totalSSE[minIdx]) {
//						minIdx = sseIdx;
//					}
//					totalSSE[sseIdx] = sse;
//					totalAssignments[sseIdx] = assignments;
//					sseIdx++;
					
//					if (sse > previousSSE && attempts < 100) {
					if (attempts < numRuns) {
						totalSSE.add(sse);
						totalAssignments.add(assignments);
						attempts++;
						continue;						
					}
					previousSSE = sse;
					
//					if (sseIdx >= numRuns) {
//						proceed = true;
//					} else {
//						continue;
//					}
					proceed = true;
					
					double finalSSE = Collections.min(totalSSE);
					
					int finalIdx = totalSSE.indexOf(finalSSE);
					int[] finalAssignments = totalAssignments.get(finalIdx);
					
					// update the kX_nY column
					for (int j = 0; j < assignments.length; j++) {
						String column = "k" + i + "_n" + (normIdx + 1);
//						String update = totalAssignments[minIdx][j] + "";
//						String update = assignments[j] + "";
						String update = finalAssignments[j] + "";
//						db.updateTable(schema, tsType.toLowerCase() + "_geom", "id", labels[j], column, update);	
						
//						if (!batch) {
//							db.updateTable(schema, tsType.toLowerCase() + "_geom", "id", labels[j], column, update);
//						} else {
//							batchInsertion.add(db.updateQueryBuilder(schema, tsType.toLowerCase() + "_geom", "id", labels[j], column, update));
//						}
						
						if (!batch) { // if not using the batch insertion
							
							if (compositeObject) { // if composite obj (at_l, lt_a, la_t)
								
								String[] idColumns = {"id1", "id2"};
								String[] idSplit = labels[j].split("_");
								String[] idValues = {idSplit[0].substring(1), idSplit[1].substring(1)};
								
								db.updateTable(schema, tsType.toLowerCase() + "_geom", idColumns, idValues, column, update);
								
							} else { // if not composite object (a_lt, l_at, t_la)
								
								db.updateTable(schema, tsType.toLowerCase() + "_geom", "id", labels[j].substring(1), column, update);
							}
							
							
						} else { // if using batch insertion
							
							if (compositeObject) { // if composite obj (at_l, lt_a, la_t)
								
								String[] idColumns = {"id1", "id2"};
								String[] idSplit = labels[j].split("_");
								String[] idValues = {idSplit[0].substring(1), idSplit[1].substring(1)};
								
								batchInsertion.add(db.updateQueryBuilder(schema, tsType.toLowerCase() + "_geom", idColumns, idValues, column, update));
								
							} else { // if not composite object (a_lt, l_at, t_la)
								
								batchInsertion.add(db.updateQueryBuilder(schema, tsType.toLowerCase() + "_geom", "id", labels[j].substring(1), column, update));
							}					
						}
					}
					
					
					
					// update the SSE column
					String column = "k" + i + "_n" + (normIdx + 1);
//					String update = totalSSE[minIdx] + "";
//					String update = sse + "";
					String update = finalSSE + "";
//					db.updateTable(schema, "sse", "id", tsType.toLowerCase(), column, update);
					
//					if (!batch) {
//						db.updateTable(schema, "sse", "id", tsType.toLowerCase(), column, update);
//					} else {
//						batchInsertion.add(db.updateQueryBuilder(schema, "sse", "id", tsType.toLowerCase(), column, update));
//					}
					
					
				}
			}
			
//			if (batch) {
//				String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
//				try {
//					db.insertIntoTableBatch(schema + ".sse", batchInsertionAr);
//				} catch (SQLException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
		}
	}

	public static boolean performMDS(File inputFile, String perspective, String normalization, int simMeasure,
			PostgreSQLJDBC db, String schema, int kMax, int kIterations, boolean batch) {
		
		ArrayList<String> batchInsertion = new ArrayList<String>();

		String norm = "";
		String table = "";
		int normIdx = -1;
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			if (PERSPECTIVES[i].equalsIgnoreCase(perspective)) {
				table = PERSPECTIVES[i].toLowerCase() + "_geom";
			}
			if (PERSPECTIVES[i].equalsIgnoreCase(normalization)) {
				norm = PERSPECTIVES[i].toLowerCase();
				normIdx = i;
			}
		}

		float[][] inputData = extractData(inputFile, perspective);
		String[] labels = extractLabels(inputFile, perspective);
		
		System.out.println("MDS");

		// initialize dissimilarity matrix
		float[][] dMatrix;

		// select sim measure
		if (simMeasure == 1) {
			// Euclidean Distance
			dMatrix = DistanceMatrix.EUCLIDEAN(inputData);
		} else if (simMeasure == 2) {
			// Cosine Sim
			dMatrix = DistanceMatrix.COSINE(inputData);
		} else if (simMeasure == 3) {
			// Manhattan
			dMatrix = DistanceMatrix.MANHATTAN(inputData);
		} else {
			return false;
		}
		
		System.out.println("MDS");

		// convert to double
		double[][] input = new double[dMatrix.length][];
		for (int i = 0; i < dMatrix.length; i++) {
			String matValue = "";
			double[] in = new double[dMatrix[i].length];
			for (int j = 0; j < dMatrix[i].length; j++) {
				in[j] = (double) dMatrix[i][j];
				matValue = matValue + " " + dMatrix[i][j];
			}
			input[i] = in;
		}

		boolean filtered = false;
		System.out.println("MDS");
		double[][] mds = MDSJ.classicalScaling(input);
		System.out.println("MDS");
		
//		for (int i = 0; i < dMatrix.length; i++) {
//			String line = "";
//			String line2 = "";
//			for (int j = 0; j < dMatrix[i].length; j++) {
//				line += " " + dMatrix[i][j];
//				line2 += " " + mds[i][j];
//			}
//			System.out.println(line);
//			System.out.println(line2);
//		}
		
//		System.out.println(labels[0]);
//		System.out.println(dMatrix[0][0]);
//		System.out.println(labels[labels.length-1]);
//		System.out.println(dMatrix[dMatrix.length-1]);
		while (!filtered) {
			boolean fixed = true;
			for (int i = 0; i < mds[0].length; i++) {
				Double tmpNum = mds[0][i];
				if (tmpNum.isNaN()) {
					fixed = false;
					break;
				}
				tmpNum = mds[1][i];
				if (tmpNum.isNaN()) {
					fixed = false;
					break;
				}
			}
			if (fixed) {
				filtered = true;
			} else {
				mds = MDSJ.classicalScaling(input);
			}
		}
		// perform MDS
		// double[][]
		//
		// boolean
		
		boolean compositeObject = false;
		
		if (perspective.equalsIgnoreCase("at_l") || perspective.equalsIgnoreCase("lt_a") || perspective.equalsIgnoreCase("la_t")) { 
			compositeObject = true;
		}
		
		System.out.println("MDS2");

		int numCols = (kMax - 1) * 6;

		if (db.getTableLength(schema, table) == 0) {
			for (int i = 0; i < labels.length; i++) {

				try {
					String[] insertData = new String[db.getTableColumnIds(schema + "." + table).length];
					int startIdx = 2;
					
					if (!compositeObject) {
						startIdx = 1;
						insertData[0] = "'" + labels[i].substring(1) + "'";
					} else { 
						String[] insertSplit = labels[i].split("_");
						insertData[0] = "'" + insertSplit[0].substring(1) + "'";
						insertData[1] = "'" + insertSplit[1].substring(1) + "'";
					}
					
//					insertData[0] = "'" + labels[i].substring(1) + "'";

					for (int j = startIdx; j < insertData.length - 6; j++) {
						insertData[j] = -1 + "";
					}

					// "ST_GeomFromText('POINT(" + data[coordinates[0]];
					// } else if (coordinates.length == 3) {
					// insertData[1] = "ST_GeomFromText('POINTZ(" + data[coordinates[0]];
					// } else {
					// br.close();
					// return false;
					// }
					//
					//
					// for (int i = 1; i < coordinates.length; i++) {
					// insertData[1] = insertData[1] + " " + data[coordinates[i]];
					// }
					//
					// insertData[1] = insertData[1] + ")'," + epsg + ")";

					int counter = 0;
					for (int j = insertData.length - 6; j < insertData.length; j++) {
						if (counter == normIdx) {
							insertData[j] = "ST_GeomFromText('POINT(" + mds[0][i] + " " + mds[1][i] + ")')";
						} else {
							insertData[j] = "ST_GeomFromText('POINT(1 1)')";
						}
						counter++;
					}

					for (int j = 0; j < insertData.length; j++) {
						System.out.println(insertData[j]);
					}
					
//					db.insertIntoTable(schema + "." + table, insertData);
					
					if (!batch) { // if not using the batch insertion
						
						// use the non-batch insertion
						db.insertIntoTable(schema + "." + table, insertData);
						
					} else { // if using the batch insertion
						
						// use the batch insertion
						batchInsertion.add(db.insertQueryBuilder(schema + "." + table, insertData));
					}		
					// return true;
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return false;
				}

			}

			// insertData[0] = "'" + id + "'";
			// for (int j = 1; j < numCols-1; j++) {
			// insertData[j] = -1 + "";
			// }
			// JSONArray coords3 = coordinates2.getJSONArray(0);
			// insertData[numCols-1] = "ST_GeomFromText('POLYGON((" + coords3.getString(0) +
			// " " + coords3.getString(1);
			//
			//
			// for (int j = 1; j < coordinates2.length(); j++) {
			// coords3 = coordinates2.getJSONArray(j);
			//
			// insertData[numCols-1] = insertData[numCols-1] + "," + coords3.getString(0) +
			// " " + coords3.getString(1);
			// }
			//
			// insertData[numCols-1] = insertData[numCols-1] + "))')";
			//
			//
			// db.insertIntoTable(schema + "." + ts.toLowerCase() + "_geom",insertData);
		} else {
			for (int i = 0; i < labels.length; i++) {
				String column = "geom_n" + (normIdx + 1);
				String update = "ST_GeomFromText('Point(" + mds[0][i] + " " + mds[1][i] + ")')";
//				db.updateTable(schema, table, "id", labels[i], column, update);
				

				if (!batch) { // if not using the batch insertion
					
					if (compositeObject) { // if composite obj (at_l, lt_a, la_t)
						
						String[] idColumns = {"id1", "id2"};
						String[] idSplit = labels[i].split("_");
						String[] idValues = {idSplit[0].substring(1), idSplit[1].substring(1)};
						
						db.updateTable(schema, table, idColumns, idValues, column, update);
						
					} else { // if not composite object (a_lt, l_at, t_la)
						
						db.updateTable(schema, table, "id", labels[i].substring(1), column, update);
					}
					
					
				} else { // if using batch insertion
					
					if (compositeObject) { // if composite obj (at_l, lt_a, la_t)
						
						String[] idColumns = {"id1", "id2"};
						String[] idSplit = labels[i].split("_");
						String[] idValues = {idSplit[0].substring(1), idSplit[1].substring(1)};
						
						batchInsertion.add(db.updateQueryBuilder(schema, table, idColumns, idValues, column, update));
						
					} else { // if not composite object (a_lt, l_at, t_la)
						
						batchInsertion.add(db.updateQueryBuilder(schema, table, "id", labels[i].substring(1), column, update));
					}					
				}
			}

		}
		
		if (batch) {
			String[] batchInsertionAr = batchInsertion.toArray(new String[batchInsertion.size()]);
			try {
				db.insertIntoTableBatch(schema + "." + table, batchInsertionAr);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		kMeansClustering(inputData, labels, simMeasure, db, 
				schema, perspective, normIdx, kMax, kIterations, batch);

		return true;
		// make lowercase
		// String outputLC = output.toLowerCase();

		// perform the appropriate action based on file extension
		// if (outputLC.endsWith(".csv")) {
		// String[] content = formatCSV(mds,labels);
		// return writeToCSV(f,content);
		// } else {
		// System.out.println("Please specify a file with a valid extension: .csv");
		// return false;
		// }

	}
	
	public static boolean writeMDS(PostgreSQLJDBC db, String schema, File inputFile, String perspective, 
									String normalization, boolean batch) {
		
		ArrayList<String> batchInsertion = new ArrayList<String>();

		String norm = "";
		String table = perspective.toLowerCase();
		int normIdx = -1;
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			if (PERSPECTIVES[i].equalsIgnoreCase(perspective)) {
				table = PERSPECTIVES[i].toLowerCase() + "_geom";
			}
			if (PERSPECTIVES[i].equalsIgnoreCase(normalization)) {
				norm = PERSPECTIVES[i].toLowerCase();
				normIdx = i;
			}
		}

		float[][] inputData = extractData(inputFile, perspective);
		String[] labels = extractLabels(inputFile, perspective);
		
		System.out.println("MDS");

		// initialize dissimilarity matrix
		float[][] dMatrix;

		// select sim measure
		if (simMeasure == 1) {
			// Euclidean Distance
			dMatrix = DistanceMatrix.EUCLIDEAN(inputData);
		} else if (simMeasure == 2) {
			// Cosine Sim
			dMatrix = DistanceMatrix.COSINE(inputData);
		} else if (simMeasure == 3) {
			// Manhattan
			dMatrix = DistanceMatrix.MANHATTAN(inputData);
		} else {
			return false;
		}
		
		System.out.println("MDS");

		// convert to double
		double[][] input = new double[dMatrix.length][];
		for (int i = 0; i < dMatrix.length; i++) {
			String matValue = "";
			double[] in = new double[dMatrix[i].length];
			for (int j = 0; j < dMatrix[i].length; j++) {
				in[j] = (double) dMatrix[i][j];
				matValue = matValue + " " + dMatrix[i][j];
			}
			input[i] = in;
		}

		boolean filtered = false;
		System.out.println("MDS");
		double[][] mds = MDSJ.classicalScaling(input);
		System.out.println("MDS");
		
//		for (int i = 0; i < dMatrix.length; i++) {
//			String line = "";
//			String line2 = "";
//			for (int j = 0; j < dMatrix[i].length; j++) {
//				line += " " + dMatrix[i][j];
//				line2 += " " + mds[i][j];
//			}
//			System.out.println(line);
//			System.out.println(line2);
//		}
		
//		System.out.println(labels[0]);
//		System.out.println(dMatrix[0][0]);
//		System.out.println(labels[labels.length-1]);
//		System.out.println(dMatrix[dMatrix.length-1]);
		while (!filtered) {
			boolean fixed = true;
			for (int i = 0; i < mds[0].length; i++) {
				Double tmpNum = mds[0][i];
				if (tmpNum.isNaN()) {
					fixed = false;
					break;
				}
				tmpNum = mds[1][i];
				if (tmpNum.isNaN()) {
					fixed = false;
					break;
				}
			}
			if (fixed) {
				filtered = true;
			} else {
				mds = MDSJ.classicalScaling(input);
			}
		}
		// perform MDS
		// double[][]
		//
		// boolean
		
		boolean compositeObject = false;
		
		if (perspective.equalsIgnoreCase("at_l") || perspective.equalsIgnoreCase("lt_a") || perspective.equalsIgnoreCase("la_t")) { 
			compositeObject = true;
		}		
	}

	public static boolean createMDSFile(File inputFile, String tsType, int simMeasure, String output) {

		// instantiate a file
		File f = new File(output);
		if (f.exists()) {
			System.out.println("File Already Exists!");
			return true;
		}

		float[][] inputData = extractData(inputFile, tsType);
		String[] labels = extractLabels(inputFile, tsType);

		// initialize dissimilarity matrix
		float[][] dMatrix;

		// select sim measure
		if (simMeasure == 1) {
			// Euclidean Distance
			dMatrix = DistanceMatrix.EUCLIDEAN(inputData);
		} else if (simMeasure == 2) {
			// Cosine Sim
			dMatrix = DistanceMatrix.COSINE(inputData);
		} else if (simMeasure == 3) {
			// Manhattan
			dMatrix = DistanceMatrix.MANHATTAN(inputData);
		} else {
			return false;
		}

		// convert to double
		double[][] input = new double[dMatrix.length][];
		for (int i = 0; i < dMatrix.length; i++) {
			String matValue = "";
			double[] in = new double[dMatrix[i].length];
			for (int j = 0; j < dMatrix[i].length; j++) {
				in[j] = (double) dMatrix[i][j];
				matValue = matValue + " " + dMatrix[i][j];
			}
			input[i] = in;
		}

		// perform MDS
		double[][] mds = MDSJ.classicalScaling(input);

		// make lowercase
		String outputLC = output.toLowerCase();

		// perform the appropriate action based on file extension
		if (outputLC.endsWith(".csv")) {
			String[] content = formatCSV(mds, labels);
			return writeToCSV(f, content);
		} else {
			System.out.println("Please specify a file with a valid extension: .csv");
			return false;
		}

	}

	public static float[][] extractData(File f, String tsType) {
		ArrayList<ArrayList<String>> data = new ArrayList<ArrayList<String>>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(f.getPath()));
			String ts = tsType.toLowerCase();
			int[] primaryKey;
			if (ts.equals("l_at") || ts.equals("t_la") || ts.equals("a_lt")) {
				int[] pKey = { 0 };
				primaryKey = pKey;
				br.readLine();
				br.readLine();
			} else if (ts.equals("lt_a") || ts.equals("at_l") || ts.equals("la_t")) {
				int[] pKey = { 0, 1 };
				primaryKey = pKey;
				br.readLine();

			} else {
				primaryKey = null;
			}

			String line = "";
			while ((line = br.readLine()) != null) {
				String[] split = line.split(",");
				ArrayList<String> tmpData = new ArrayList<String>();
				int startIdx = primaryKey[primaryKey.length - 1] + 1;
				for (int i = startIdx; i < split.length; i++) {
					tmpData.add(split[i]);
				}

				data.add(tmpData);

			}
			br.close();

			float[][] returnData = new float[data.size()][];
			for (int i = 0; i < data.size(); i++) {
				float[] tmpReturn = new float[data.get(i).size()];
				for (int j = 0; j < data.get(i).size(); j++) {
					tmpReturn[j] = Float.parseFloat(data.get(i).get(j));
				}
				returnData[i] = tmpReturn;
			}
			// String[] returnLabels = labels.toArray(new String[labels.size()]);
			return returnData;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	public static String[] extractLabels(File f, String tsType) {

		ArrayList<String> labels = new ArrayList<String>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(f.getPath()));
			String ts = tsType.toLowerCase();
			int[] primaryKey;
			if (ts.equals("l_at") || ts.equals("t_la") || ts.equals("a_lt")) {
				int[] pKey = { 0 };
				primaryKey = pKey;
				br.readLine();
				br.readLine();
			} else if (ts.equals("lt_a") || ts.equals("at_l") || ts.equals("la_t")) {
				int[] pKey = { 0, 1 };
				primaryKey = pKey;
				br.readLine();
				// br.readLine();
			} else {
				primaryKey = null;
			}

			String line = "";
			while ((line = br.readLine()) != null) {
				String[] split = line.split(",");
				String label = split[primaryKey[0]];
				if (primaryKey.length > 1) {
					for (int i = 1; i < primaryKey.length; i++) {
						label = label + "_" + split[primaryKey[i]];
					}
				}

				labels.add(label);
			}
			br.close();
			String[] returnLabels = labels.toArray(new String[labels.size()]);
			return returnLabels;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	public static boolean createMDSFile(float[][] inputData, String[] labels, int simMeasure, String output) {

		// instantiate a file
		File f = new File(output);
		if (f.exists()) {
			System.out.println("File Already Exists!");
			return true;
		}

		// initialize dissimilarity matrix
		float[][] dMatrix;

		// select sim measure
		if (simMeasure == 1) {
			// Euclidean Distance
			dMatrix = DistanceMatrix.EUCLIDEAN(inputData);
		} else if (simMeasure == 2) {
			// Cosine Sim
			dMatrix = DistanceMatrix.COSINE(inputData);
		} else if (simMeasure == 3) {
			// Manhattan
			dMatrix = DistanceMatrix.MANHATTAN(inputData);
		} else {
			return false;
		}

		// testing
		// for (int i = 0; i < dMatrix.length; i++) {
		// String newLine = "";
		// for (int j = 0; j < dMatrix[i].length; j++) {
		// newLine = newLine + " " + dMatrix[i][j];
		// }
		// System.out.println(newLine);
		// }

		// convert to double
		double[][] input = new double[dMatrix.length][];
		for (int i = 0; i < dMatrix.length; i++) {
			String matValue = "";
			double[] in = new double[dMatrix[i].length];
			for (int j = 0; j < dMatrix[i].length; j++) {
				in[j] = (double) dMatrix[i][j];
				matValue = matValue + " " + dMatrix[i][j];
			}
			input[i] = in;
		}

		// perform MDS
		double[][] mds = MDSJ.classicalScaling(input);

		// make lowercase
		String outputLC = output.toLowerCase();

		// perform the appropriate action based on file extension
		if (outputLC.endsWith(".csv")) {
			String[] content = formatCSV(mds, labels);
			return writeToCSV(f, content);
			// } else if (outputLC.endsWith(".json") || outputLC.endsWith(".geojson")) {
			// String[] content = formatGeoJSON(mds,labels);
			// return writeToGeoJSON(f,content);
		} else {
			System.out.println("Please specify a file with a valid extension: .csv, .geojson, .json");
			return false;
		}

	}

	public static String[] formatCSV(double[][] input, String[] labels) {
		float[][] mds = dblToFloat(input);

		String[] output = new String[mds[0].length + 1];
		String colNames = "label,x,y";
		output[0] = colNames;
		for (int i = 0; i < mds[0].length; i++) {
			// String name = labels[i].substring(1, labels[i].length());
			// String name = labels[i];
			output[i + 1] = labels[i] + "," + mds[0][i] + "," + mds[1][i];
		}
		return output;
	}

	public static boolean writeToCSV(File f, String[] content) {
		try {
			FileWriter fw = new FileWriter(f);
			for (int i = 0; i < content.length; i++) {
				fw.append(content[i]);
				fw.append(System.lineSeparator());
			}
			fw.flush();
			fw.close();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("ERROR SAVING CSV");
			return false;
		}
	}

	public static float[][] dblToFloat(double[][] input) {
		float[][] output = new float[input.length][];
		for (int i = 0; i < input.length; i++) {
			float[] tempOutput = new float[input[i].length];

			for (int j = 0; j < input[i].length; j++) {
				tempOutput[j] = (float) input[i][j];
			}
			output[i] = tempOutput;
		}
		return output;
	}

	/**
	 * Calculates the SSE for a set of input vectors
	 *
	 * @param centroids
	 *            The kmeans centroids.
	 * @param assignments
	 *            The input vectors kmeans assignment.
	 * @param points
	 *            The input vectors.
	 */
	public static double calculateSSE(double[][] centroids, int[] assignments, double[][] points, int distMeasure) {
		// initialize the return value
		double sse = 0;
		
		float[][] newPts = dblToFloat(points);
		
		float[][] newCentroids = dblToFloat(centroids);

		// populate the return value
		for (int i = 0; i < points.length; i++) {
			
//			if (newPts.length == 6 && i == 5) {
//				System.out.println("BUG");
//			}

			// initialize the distance
			double distance = 0;

			// calculate the distance
//			for (int j = 0; j < points[i].length; j++) {
				double difference = 0;
				if (distMeasure == 1) {
					difference = DistanceCalculator.EUCLIDEAN(newPts[i], newCentroids[assignments[i]]);
				} else if (distMeasure == 2) {
					difference = DistanceCalculator.COSINE(newPts[i], newCentroids[assignments[i]]);
				} else if (distMeasure == 3) {
					difference = DistanceCalculator.MANHATTAN(newPts[i], newCentroids[assignments[i]]);
				} else {
					return (Double) null;
				}
//				if (Double.isNaN(difference)) {
//					System.out.println("BUG!");
//					difference = DistanceCalculator.COSINE(newPts[i], newCentroids[assignments[i]]);
//				}
//				double difference = points[i][j] - centroids[assignments[i]][j];
				difference = difference * difference;
				distance += difference;
//			}

			// add the distance to SSE
			sse += distance;
		}

		// return SSE
		return sse;
	}
}
