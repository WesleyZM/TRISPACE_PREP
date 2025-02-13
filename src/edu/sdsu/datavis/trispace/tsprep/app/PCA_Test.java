package edu.sdsu.datavis.trispace.tsprep.app;

//import java.util.Arrays;
//import java.util.List;

import smile.projection.PCA;

//import org.nd4j.linalg.api.ndarray.INDArray;
//import org.nd4j.linalg.dimensionalityreduction.PCA;
//import org.nd4j.linalg.factory.Nd4j;


public class PCA_Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		double[][] data = new double[5][2];
		
		for (int i = 0; i < data.length; i++) {
			data[i][0] = 1 + i;
			data[i][1] = 1 - i;
		}
		
		PCA pca = new PCA(data);
		
		double[] varProportions = pca.getCumulativeVarianceProportion();
		
		for (int i = 0; i < varProportions.length; i++) {
			System.out.println(varProportions[i]);
		}
		
		
//		INDArray matrix = Nd4j.create(data);
		
		
		
//		List<INDArray> ndArrays = Arrays.asList(
//		        new NDArray(new float [] {-1.0F, -1.0F}),
//		        new NDArray(new float [] {-1.0F, 1.0F}),
//		        new NDArray(new float [] {1.0F, 1.0F}));
		
		//Create matrix of points (rows are observations; columns are features)
//		INDArray matrix = new NDArray(ndArrays, new int [] {3,2});

		//Execute PCA - again to 2 dimensions
//		INDArray factors = PCA.pca_factor(matrix, 1, false);
//		
//		System.out.println(matrix.getRow(0));
//		
//		System.out.println(factors.getRow(0));
		
//		factors.get;
//		
//		System.out.println(factors.);

	}

}
