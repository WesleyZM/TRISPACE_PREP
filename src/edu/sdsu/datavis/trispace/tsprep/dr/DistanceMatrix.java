package edu.sdsu.datavis.trispace.tsprep.dr;

// import org.somatic.viewer.utils.DistanceCalculator;
import edu.sdsu.datavis.trispace.tsprep.utils.DistanceCalculator;


public class DistanceMatrix {
	
	private static float[][] disMatrix; //the final similarity matrix that will be returned
	
	public static float[][] EUCLIDEAN(float[][] input) {
		
		disMatrix = new float[input.length][input.length];
		
		for (int i = 0; i < disMatrix.length; i++) {
			for (int j = i; j < disMatrix.length; j++) {
				if (i == j) {
					disMatrix[i][j] = 0;
				} else {
					disMatrix[i][j] = DistanceCalculator.EUCLIDEAN(input[i], input[j]);
					disMatrix[j][i] = disMatrix[i][j];
				}
			}
		}
		
		return disMatrix;				
	}
	
	public static float[][] COSINE(float[][] input) {
		
		disMatrix = new float[input.length][input.length];
		
		for (int i = 0; i < disMatrix.length; i++) {
			for (int j = i; j < disMatrix.length; j++) {
				if (i == j) {
					disMatrix[i][j] = 0;
				} else {
//					if (i == disMatrix.length-2) {
//						System.out.println("YAY");
//					}
					disMatrix[i][j] = DistanceCalculator.COSINE(input[i], input[j]);
					disMatrix[j][i] = disMatrix[i][j];
					if (Float.isNaN(disMatrix[i][j])) {
						System.out.println("Object: " + i + " - " + j + " || " + disMatrix[i][j]);
					}
					if (disMatrix[i][j] < 0.0) {
						System.out.println("Less Than 0 Error");
					} else if (disMatrix[i][j] > 1.0) {
						System.out.println("More Than 1 Error");
					}
				}
			}
		}
		
		return disMatrix;				
	}
	
	public static float[][] MANHATTAN(float[][] input) {
		
		disMatrix = new float[input.length][input.length];
		
		for (int i = 0; i < disMatrix.length; i++) {
			for (int j = i; j < disMatrix.length; j++) {
				if (i == j) {
					disMatrix[i][j] = 0;
				} else {
					disMatrix[i][j] = DistanceCalculator.MANHATTAN(input[i], input[j]);
					disMatrix[j][i] = 0 + disMatrix[i][j];
				}
			}
		}
		
		return disMatrix;				
	}
	
	public static float[][] COSINE_DISTANCE(float[][] input) {
		
		disMatrix = new float[input.length][input.length];
		
		for (int i = 0; i < disMatrix.length; i++) {
			for (int j = i; j < disMatrix.length; j++) {
				if (i == j) {
					disMatrix[i][j] = 0;
				} else {
//					disMatrix[j][i] = 0 + disMatrix[i][j];
					disMatrix[i][j] = 1 - input[i][j];
					disMatrix[j][i] = 0 + disMatrix[i][j];
				}
			}
		}
		
		return disMatrix;
	}
		
}
