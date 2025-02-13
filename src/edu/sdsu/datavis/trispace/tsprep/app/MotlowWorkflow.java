package edu.sdsu.datavis.trispace.tsprep.app;

import java.io.File;

import edu.sdsu.datavis.trispace.tsprep.xform.CSVManager;

public class MotlowWorkflow {
	
	// L_AT, LT_A, T_LA study perspectives
	// Normalization TBD
	final static String[] PERSPECTIVES = {"L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L"};
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		// manually create ./data/motlow/trispace/ 
		// and ./data/motlow/trispace/Unnormalized/ folders

		File input = new File("./data/Steps/100/PT_no_clouds_100.csv");
		String output = "./data/motlow/trispace/Unnormalized";
		
		CSVManager.fromL_AT2LA_T(input, output + "/LA_T.csv");
		
		input = new File(output + "/LA_T.csv");
		
		CSVManager.processFile(input, PERSPECTIVES[4], output, true);

		CSVManager.fromLA_T2All(input, output);
		
		// normalize to all perspectives
		String inDir = output + "";
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			output = "";
			String name = PERSPECTIVES[i] + ".csv";
			input = new File(inDir + "/" + name);
			String[] split = inDir.split("/");
			for (int j = 0; j < split.length-1; j++) {
				output = output + split[j] + "/";
			}
			output = output + PERSPECTIVES[i] + "_Normalized/" + name;
			CSVManager.normalizeCSVMinMax(input, output, PERSPECTIVES[i], 0);
		}
		
		// convert each normalized file to L_AT then to the remaining perspectives		
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			inDir = "./data/motlow/trispace/" + PERSPECTIVES[i] + "_Normalized";
			input = new File(inDir + "/" + PERSPECTIVES[i] + ".csv");
			if (i == 0) {
				CSVManager.fromL_AT2All(input, inDir);
			} else if (i == 1) {
				CSVManager.fromA_LT2All(input, inDir);
			} else if (i == 2) {
				CSVManager.fromT_LA2All(input, inDir);
			} else if (i == 3) {
				CSVManager.fromLA_T2All(input, inDir);
			} else if (i == 4) {
				CSVManager.fromLT_A2All(input, inDir);
			} else if (i == 5) {
				CSVManager.fromAT_L2All(input, inDir);
			}
		}
		
		// convert each file into a .dat file for SOMatic
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			for (int j = 0; j < PERSPECTIVES.length; j++) {
				input = new File("./data/motlow/trispace/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[j] + ".csv");
				output = "./data/motlow/SOMaticIn/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[j] + ".dat";
				CSVManager.fromCSV2DatFile(PERSPECTIVES[j], input, output);
			}
		}
		
		System.out.println("End of Session.");
		System.exit(0);
	}

}
