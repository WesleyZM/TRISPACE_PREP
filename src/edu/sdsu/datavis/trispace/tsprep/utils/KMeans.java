package edu.sdsu.datavis.trispace.tsprep.utils;


import java.util.ArrayList;
import java.util.Arrays;

//import somatic.viewer.clustering.Cluster;
//import somatic.viewer.utils.DistanceCalculator;

//import somatic.viewer.GlobalVariables;
//import somatic.viewer.clustering.Cluster;
//import somatic.viewer.som.entities.Neuron;
//import somatic.viewer.utils.DistanceCalculator;

public class KMeans {
//	GlobalVariables globals = GlobalVariables.getInstance();
	
	int kClusters;
	edu.sdsu.datavis.trispace.tsprep.som.Neuron[] data;
	float[][] centroids;
	public float[][] distMatrix;
	float[][] oldDistMatrix;
	public float[][] clusterMatrix;
	public Cluster[] kMeansClusters = new Cluster[0];

	public KMeans(int kValue, edu.sdsu.datavis.trispace.tsprep.som.Neuron[] neurons, int distMeasure, int kIterations) {
		this.kClusters = kValue;
//		data = globals.neurons;
		data = neurons;
//		globals.kMeansClusters = new Cluster[kClusters];
		kMeansClusters = new Cluster[kClusters];
		
		centroids = new float[kClusters][];
		distMatrix = new float[kClusters][];
		oldDistMatrix = new float[kClusters][];
		clusterMatrix = new float[kClusters][data.length];
		
		initCentroids();
		
		distMatrix = calcDistanceMatrix(data, centroids, distMeasure);
		
		clusterMatrix = calcClusterMatrix(distMatrix);
		
//		System.out.println("\nDistance Matrix");
//		printMatrix(distMatrix);
//		System.out.println("\nCluster Matrix");
//		printMatrix(clusterMatrix);
		
		
		int iteration = 0;
		while(!Arrays.deepEquals(distMatrix, oldDistMatrix) || iteration < kIterations) {
			System.out.println("\n------------\nIteration " + iteration + "\n------------\n");
			
			updateCentroids();
					
			oldDistMatrix = distMatrix;
			
			distMatrix = calcDistanceMatrix(data, centroids, distMeasure);
			
			clusterMatrix = calcClusterMatrix(distMatrix);
			
//			System.out.println("\nDistance Matrix");
//			printMatrix(distMatrix);
//			System.out.println("\nCluster Matrix");
//			printMatrix(clusterMatrix);
			
			iteration++;
		}
		
		System.out.println("\nSUCCESSFULL! \nk-Means clustering after " + iteration + " iterations finished!");
		System.out.println("Numbers of Clusters: " + kMeansClusters.length);
//		for (int i = 0; i < clusterMatrix.length; i++) {
//			System.out.println(centroids.length);
//			System.out.println(centroids[].length);
//		}
//		System.out.println(centroids + "");
//		System.out.println("\nDistance Matrix");
//		printMatrix(distMatrix);
//		System.out.println("\nCluster Matrix");
//		printMatrix(clusterMatrix);
//		System.out.println(distMatrix.length);
//		System.out.println(clusterMatrix.length);
//		calcSSE(distMatrix,clusterMatrix);
//		System.out.println(kMeansClusters[0].);
//		for (int i = 0; i < kMeansClusters.length; i++) {
//			if (i == 0) {
//				System.out.println("\nMembers of the first cluster are:");
//			} else if (i == 1) {
//				System.out.println("\nMembers of the second cluster are:");
//			} else if (i == 2) {
//				System.out.println("\nMembers of the third cluster are:");
//			} else if (i == 3) {
//				System.out.println("\nMembers of the fourth cluster are:");
//			} else if (i == 4) {
//				System.out.println("\nMembers of the fifth cluster are:");
//			}
//			
//			kMeansClusters[i].printClusterMembers();
//		}
		
	}
	
	private void initCentroids() {
		System.out.println("Initial centroids");
		
		for (int i = 0; i < kClusters; i++) {
			int randIndex = (int)(Math.random()*data.length);
			centroids[i] = data[randIndex].getAttributes();
			System.out.println(centroids[i][0] + "\t" + centroids[i][1]);		
		}
	}
	
	private void updateCentroids() {
		System.out.println("\nNew centroids");
		
		for (int i = 0; i < clusterMatrix.length; i++) {
			ArrayList<float[]> cluster = new ArrayList<float[]>();
			
			for (int j = 0; j < clusterMatrix[i].length; j++) {
				if(clusterMatrix[i][j] == 1) {
					cluster.add(data[j].getAttributes());
				}
			}
			centroids[i] = calcCentroid(cluster);
			
			
//			System.out.println(centroids[i][0] + "\t" + centroids[i][1]);
		}
	}
	
	private float[] calcCentroid(ArrayList<float[]> cluster) {
			
		float[] centroid = new float[cluster.get(0).length];
		
//		System.out.println("\nCluster Size: " + cluster.size());
		
		
		int index = 0;
		
		if(cluster.size() > 1) {
			
			for (int i = 0; i < cluster.get(0).length; i++) {
				float pointValues = 0;
				
				for(int j = 0; j < cluster.size(); j++) {
					pointValues += cluster.get(j)[i];
//					System.out.println(cluster.get(j)[i]);
				}
				centroid[index] = pointValues / cluster.size();	
				
				index++;
			}
		}
		else {
			centroid = cluster.get(0);
		}
		
		return centroid;
			
	}
	
	private float[][] calcDistanceMatrix(edu.sdsu.datavis.trispace.tsprep.som.Neuron[] data2, float[][] centroids, int distMeasure) {
		float[][] distanceMatrix = new float[centroids.length][];
		
		for (int j = 0; j < centroids.length; j++) {
			distanceMatrix[j] = calcDistances(data2, centroids[j], distMeasure);
		}
		
		return distanceMatrix;
	}
	
	private float[][] calcClusterMatrix(float[][] distanceMatrix) {
		for(int i = 0; i < kMeansClusters.length; i++) {
			kMeansClusters[i] = new Cluster();
		}
		float[][] clusterMatrix = new float[distanceMatrix.length][distanceMatrix[0].length];
				
		for (int k = 0; k < clusterMatrix.length; k++) {
			for (int l = 0; l < clusterMatrix[0].length; l++) {
				clusterMatrix[k][l] = 0;
			}
		}
		
		for (int k = 0; k < distanceMatrix[0].length; k++) {
			
			float min = Float.MAX_VALUE;
			int minIndex[] = new int[2];
						
			for (int l = 0; l < distanceMatrix.length; l++) {
				if (distanceMatrix[l][k] < min) {
					min = distanceMatrix[l][k];
					minIndex = new int[] { l, k };					
				}
			}
			
			// why mindIndex[1]+1? -> because Neuron IDs start with 1 and not 0
			kMeansClusters[minIndex[0]].setMemberIDs(minIndex[1]+1);
			
			clusterMatrix[minIndex[0]][minIndex[1]] = 1;
				
		}
		
		return clusterMatrix;
	}
	
	private float[] calcDistances(edu.sdsu.datavis.trispace.tsprep.som.Neuron[] data2, float[] centroid, int distMeasure) {
		float[] distances = new float[data2.length];
		
		for (int i = 0; i < data2.length; i++) {
			if (distMeasure == 1) {
				distances[i] = DistanceCalculator.EUCLIDEAN(centroid, data2[i].getAttributes());
			} else if (distMeasure == 2) {
				distances[i] = DistanceCalculator.COSINE(centroid, data2[i].getAttributes());
			} else if (distMeasure == 3) {
				distances[i] = DistanceCalculator.MANHATTAN(centroid, data2[i].getAttributes());
			} else {
				return null;
			}
		}
		
		return distances;
	}
	
	private float euclidDistance(float[] centroid, float[] vector) {
		float dist = 0;
		
		for (int i = 0; i < centroid.length; i++) {
			dist += ((vector[i] - centroid[i]) * (vector[i] - centroid[i]));
		}
		
		return (float) Math.sqrt(dist);
	}
	
	private void printMatrix(float[][] matrix) {
		for (int i = 0; i < matrix.length; i++) {
			for (int j = 0; j < matrix[i].length; j++) {
				System.out.print(matrix[i][j] + "\t");
			}
			System.out.println("");
		}
	}
	
	public float calcSSE(float[][] matrix, float[][] matrix2) {
		float sse = 0;
		for (int i = 0; i < matrix.length; i++) {
			for (int j = 0; j < matrix[i].length; j++) {
				float newSSE = matrix[i][j] * matrix[i][j] * matrix2[i][j];
				sse += newSSE;
//				System.out.print(matrix[i][j] + "\t");
//				System.out.println(matrix[i][j] * matrix[i][j]);
			}
//			System.out.println("");
		}
//		printMatrix(matrix);
//		printMatrix(matrix2);
		System.out.println(sse);
		return sse;
	}
	
//	public static void main (String[] args) {
//		float[][] testdata = new float[4][];
//		testdata[0] = new float[]{ 1,1 };
//		testdata[1] = new float[]{ 2,1 };
//		testdata[2] = new float[]{ 4,3 };
//		testdata[3] = new float[]{ 5,4 };
//		
//		KMeans test = new KMeans(testdata, 2);
//	}
}
