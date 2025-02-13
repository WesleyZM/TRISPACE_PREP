package edu.sdsu.datavis.trispace.tsprep.utils;

public class DescriptiveStatistics {
	
	public static double calculateMean(double[] data) {
		double mean = 0;
		for (int i = 0; i < data.length; i++) {
			mean += data[i];
		}
		mean /= data.length;
		
		return mean;
	}
	
	public static float calculateMean(float[] data) {
		float mean = 0;
		for (int i = 0; i < data.length; i++) {
			mean += data[i];
		}
		mean /= data.length;
		
		return mean;
	}
	
	public static double calculateStandardDeviation(double[] data) {
		double mean = calculateMean(data);
		double std = 0;
		
		for (int i = 0; i < data.length; i++) {
			double diff = data[i] - mean;
			diff = diff * diff;
			
			std += diff;
		}
		
		std /= data.length;
		
		return Math.sqrt(std);
	}
	
	public static float calculateStandardDeviation(float[] data) {
		float mean = calculateMean(data);
		float std = 0;
		
		for (int i = 0; i < data.length; i++) {
			float diff = data[i] - mean;
			diff = diff * diff;
			
			std += diff;
		}
		
		std /= data.length;
		
		return (float) Math.sqrt(std);
	}
}
