package edu.sdsu.datavis.trispace.tsprep.app;

import java.io.File;

import edu.sdsu.datavis.trispace.tsprep.xform.CSVManager;

public class MotlowWorkflowCSV {
	
	// L_AT, LT_A, T_LA study perspectives
	// Normalization TBD
	final static String[] PERSPECTIVES = {"L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L"};
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		// manually create ./data/motlow/trispace/ 
		// and ./data/motlow/trispace/Unnormalized/ folders

		// CHECK THE FILE PATHS!!!!!
		// ---------------------------------------------------------------------------
		File input = new File("./data/Random/1k/1k_no_zeros_ALL_locations_2018_2023.csv");
		// ---------------------------------------------------------------------------
		// specify the output location between motlow and trispace
		// String output = "./data/motlow/Cali/trispace/Unnormalized";
		// String output = "./data/motlow/Port/trispace/Unnormalized";
		// String output = "./data/motlow/Afri/trispace/Unnormalized";
		String output = "./data/motlow/All_2/trispace/Unnormalized";


		
		// CSVManager.fromL_AT2LT_A(input, "./data/motlow/Cali/LT_A.csv");
		// CSVManager.fromL_AT2LT_A(input, "./data/motlow/Port/LT_A.csv");
		// CSVManager.fromL_AT2LT_A(input, "./data/motlow/Afri/LT_A.csv");
		System.out.println("Stage 1: Initial CSV Transformations");
		CSVManager.fromL_AT2LT_A(input, "./data/motlow/All_2/LT_A.csv");
		
		System.out.println("L_AT to LT_A conversion complete.");
		

		// CSVManager.fromL_AT2LT_A(input, output + "/LT_A.csv");
		
//		input = new File(output + "/LT_A.csv");
		// add /studylocation/ to the output path after motlow


		input = new File("./data/motlow/All_2/LT_A.csv");
		
		CSVManager.processFile(input, PERSPECTIVES[4], output, true);
		
		System.out.println("perspective" + PERSPECTIVES[4] + " complete.");
		


		CSVManager.fromLT_A2All(input, output);
		
		// // normalize to all perspectives
		System.out.println("Stage 2: Normalization");
		String inDir = output + "";
		System.err.println("inDir: " + inDir);
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			output = "";
			String name = PERSPECTIVES[i] + ".csv";
			System.out.println("Processing file: " + name);
			input = new File(inDir + "/" + name);
			System.out.println("Input file path: " + input.getPath());
			String[] split = inDir.split("/");
			for (int j = 0; j < split.length-1; j++) {
				output = output + split[j] + "/";
			}
			output = output + PERSPECTIVES[i] + "_Normalized/" + name;
			System.out.println("Output file path: " + output);
			System.out.println("Normalizing perspective: " + PERSPECTIVES[i]);
			CSVManager.normalizeCSVMinMax(input, output, PERSPECTIVES[i], 0);
			System.out.println("Normalization complete for: " + PERSPECTIVES[i]);
		}
		
		// convert each normalized file to L_AT then to the remaining perspectives
		System.out.println("Stage 3: Converting Normalized Files");		
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			inDir = "./data/motlow/All_2/trispace/" + PERSPECTIVES[i] + "_Normalized";
			input = new File(inDir + "/" + PERSPECTIVES[i] + ".csv");
			System.out.println("Converting from perspective: " + PERSPECTIVES[i]);
			if (i == 0) {
				System.out.println("Starting conversion using fromL_AT2All");
				CSVManager.fromL_AT2All(input, inDir);
				System.out.println("Completed conversion from perspective: " + PERSPECTIVES[i]);
			} else if (i == 1) {
			 	System.out.println("Starting conversion using fromA_LT2All");
			 	CSVManager.fromA_LT2All(input, inDir);
				System.out.println("Completed conversion from perspective: " + PERSPECTIVES[i]);
			} else if (i == 2) {
				System.out.println("Starting conversion using fromT_LA2All");
				CSVManager.fromT_LA2All(input, inDir);
				System.out.println("Completed conversion from perspective: " + PERSPECTIVES[i]);
			} else if (i == 3) {
				System.out.println("Starting conversion using fromLA_T2All");
				CSVManager.fromLA_T2All(input, inDir);
				System.out.println("Completed conversion from perspective: " + PERSPECTIVES[i]);
			} else if (i == 4) {
				System.out.println("Starting conversion using fromLT_A2All");
				CSVManager.fromLT_A2All(input, inDir);
				System.out.println("Completed conversion from perspective: " + PERSPECTIVES[i]);
			} else if (i == 5) {
				System.out.println("Starting conversion using fromAT_L2All");
				CSVManager.fromAT_L2All(input, inDir);
				System.out.println("Completed conversion from perspective: " + PERSPECTIVES[i]);
			}
		}
		
		// convert each file into a .dat file for SOMatic
		System.out.println("Stage 4: Converting to .dat Files for SOMatic");
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			for (int j = 0; j < PERSPECTIVES.length; j++) {
				input = new File("./data/motlow/All_2/trispace/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[j] + ".csv");
				output = "./data/motlow/All_2/SOMaticIn/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[j] + ".dat";
				System.out.println("Processing file: " + input.getPath());
				CSVManager.fromCSV2DatFile(PERSPECTIVES[j], input, output);
				System.out.println("Completed processing file: " + input.getPath());
			}
		}
		
		System.out.println("End of Session.");
		System.exit(0);
	}

}
