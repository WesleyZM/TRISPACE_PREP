package edu.sdsu.datavis.trispace.tsprep.app;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.cpu.nativecpu.NDArray;
import org.nd4j.linalg.dimensionalityreduction.PCA;
import org.nd4j.linalg.string.NDArrayStrings;

public class PCA_Test2 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		BasicConfigurator.configure();
		
//		double[] d1 = {1, 3, 5};
//		double[] d2 = {2, 4, 6};
//		
//		double[][] data = {d1, d2};
		
//		double[] d1 = {1, 2};
//		double[] d2 = {3, 4};
//		double[] d3 = {5, 6};
		
//		double[] d1 = {7, 4, 6, 8, 8, 7, 5, 9, 7, 8};
//		double[] d2 = {4, 1, 3, 6, 5, 2, 3, 5, 4, 2};
//		double[] d3 = {3, 8, 5, 1, 7, 9, 3, 8, 5, 2};
////		
//		double[][] data = {d1, d2, d3};
		
//		double[] d1 = {7, 4, 3};
//		double[] d2 = {4, 1, 8};
//		double[] d3 = {6, 3, 5};
//		double[] d4 = {8, 6, 1};
//		double[] d5 = {8, 5, 7};
//		double[] d6 = {7, 2, 9};
//		double[] d7 = {5, 3, 3};
//		double[] d8 = {9, 5, 8};
//		double[] d9 = {7, 4, 5};
//		double[] d10 = {8, 2, 2};
//		
//		double[][] data = {d1, d2, d3, d4, d5, d6, d7, d8, d9, d10};
		
//		double[][] data = new double[15][5];
//		
//		for (int i = 0; i < data.length; i++) {
//			data[i][0] = 1 + i;
//			data[i][1] = 1 - i*i;
//			data[i][2] = i * i * i;
//			data[i][3] = i * 8;
//			data[i][4] = i * i * 25;
//		}
		
		//Create points as NDArray instances
		List<INDArray> ndArrays = Arrays.asList(
		        new NDArray(new float [] {1F, 3F}),
		        new NDArray(new float [] {3F, 4F}),
		        new NDArray(new float [] {5F, 6F}));
		
		

		//Create matrix of points (rows are observations; columns are features)
		INDArray matrix = new NDArray(ndArrays, new int [] {3,2});
//		PCA myPCA = new PCA(matrix);

		//Execute PCA - again to 2 dimensions
		INDArray factors = PCA.pca_factor(matrix, 2, false);
		
		System.out.println(factors);
//		NDArrayStrings ns = new NDArrayStrings(5);
//        System.out.println("Eigenvectors:\n" + ns.format(myPCA.getEigenvectors()));
//        System.out.println("Eigenvalues:\n" + ns.format(myPCA.getEigenvalues()));
		
		System.exit(0);
		
//		PCA pca = new PCA(data);
//		
//		pca.setProjection(0.99);
//		
//		double[][] newy = pca.project(data);
//		
////		double[] varProportions = pca.getCumulativeVarianceProportion();
//		double[] varProportions = pca.getVarianceProportion();
//		
//		double[] center = pca.getCenter();
//		
//		for (int i = 0; i < center.length; i++) {
//			System.out.println(i + ": " + center[i]);
//		}
//		
//		System.out.println("");
//		
//		for (int i = 0; i < varProportions.length; i++) {
//			System.out.println(varProportions[i]);
//		}
//		
//		for (int i = 0; i < newy.length; i++) {
//			String newDat = i + ": ";
//			for (int j = 0; j < newy[i].length; j++) {
//				newDat = newDat + " " + newy[i][j]; 
//			}
//			System.out.println(newDat);
//		}
	}

}
