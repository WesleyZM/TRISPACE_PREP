package edu.sdsu.datavis.trispace.tsprep.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.apache.log4j.BasicConfigurator;

import smile.projection.PCA;

public class PCA_Test3 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		BasicConfigurator.configure();
		
		File input = new File("./data/sanelijo3/tables/A_LT_Normalized/A_LT_SanElijo.csv");
//		String output = "./crime-data/trispace/Unnormalized";
		
		// PERSPECTIVES = {"L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L"};
		double[][] data = new double [6][];
		int arrayCounter = 0;

		try {
			BufferedReader br = new BufferedReader(new FileReader(input.getPath()));
			
			br.readLine();
			String line = br.readLine();

			while ((line = br.readLine()) != null) {
				
				String[] split = line.split(",");
				double[] tmpData = new double[split.length - 1];
				
				for (int i = 1; i < split.length; i++) {
					tmpData[i-1] = Double.parseDouble(split[i]);
				}
				System.out.println(arrayCounter);
				data[arrayCounter++] = tmpData;
			}

			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println(data[0][0]);
		System.out.println(data[5][0]);


		PCA pca = new PCA(data);
		
		pca.setProjection(0.99);
		
		double[][] newy = pca.project(data);
		
//				double[] varProportions = pca.getCumulativeVarianceProportion();
		double[] varProportions = pca.getVarianceProportion();
		
		double[] center = pca.getCenter();
		
		for (int i = 0; i < center.length; i++) {
			System.out.println(i + ": " + center[i]);
		}
		
		System.out.println("");
		
		for (int i = 0; i < varProportions.length; i++) {
			System.out.println(varProportions[i]);
		}
		
		for (int i = 0; i < newy.length; i++) {
			String newDat = i + ": ";
			for (int j = 0; j < newy[i].length; j++) {
				newDat = newDat + " " + newy[i][j]; 
			}
			System.out.println(newDat);
		}
	}

}
