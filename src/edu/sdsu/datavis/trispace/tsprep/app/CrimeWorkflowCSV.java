package edu.sdsu.datavis.trispace.tsprep.app;

import java.io.File;

import edu.sdsu.datavis.trispace.tsprep.dr.MDSManager;
import edu.sdsu.datavis.trispace.tsprep.xform.CSVManager;

public class CrimeWorkflowCSV {
	
	final static String[] PERSPECTIVES = {"L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L"};
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// "C:\Users\wesle\Desktop\Thesis_files\Github\TriSpace_Prep\data\ca_step100_filtered_dates_copy.csv"
		// "C:\Users\wesle\Desktop\Thesis_files_2\working_files\excel\values\full\No_Clouds\CA.csv"
		// File input = new File("./data/ca_step100_filtered_dates_copy.csv");
		// File input = new File("./data/Steps/1200/CA.csv");
		
		File input = new File("./data/Steps/100/CA_no_clouds_100.csv");
		String output = "./data/trispace/CA_Normalized_3/No_Clouds_100";
		CSVManager.processFile(input, PERSPECTIVES[4], output, true);
		
		// input = new File(output + "/L_AT.csv");
		

		CSVManager.fromL_AT2All(input, output);
	
// ___________________ Start commenting out here for unnormalized csv _______________		
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
		
		// // // convert each normalized file to L_AT then to the remaining perspectives		
		// // for (int i = 0; i < PERSPECTIVES.length; i++) {
		// // 	// inDir = "./crime-data/trispace/" + PERSPECTIVES[i] + "_Normalized";

		// // 	inDir = "./crime-data/trispace/CA_Normalized/" + PERSPECTIVES[i] + "_Normalized";
		// // 	input = new File(inDir + "/" + PERSPECTIVES[i] + ".csv");
		// // 	if (i == 0) {
		// // 		CSVManager.fromL_AT2All(input, inDir);
		// // 	} else if (i == 1) {
		// // 		CSVManager.fromA_LT2All(input, inDir);
		// // 	} else if (i == 2) {
		// // 		CSVManager.fromT_LA2All(input, inDir);
		// // 	} else if (i == 3) {
		// // 		CSVManager.fromLA_T2All(input, inDir);
		// // 	} else if (i == 4) {
		// // 		CSVManager.fromLT_A2All(input, inDir);
		// // 	} else if (i == 5) {
		// // 		CSVManager.fromAT_L2All(input, inDir);
		// // 	}
		// // }
		
		// convert each file into a .dat file for SOMatic
		// can remove this // 
		// for (int i = 0; i < PERSPECTIVES.length; i++) {
		// 	for (int j = 0; j < PERSPECTIVES.length; j++) {
		// 		input = new File("./crime-data/trispace/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[j] + ".csv");
		// 		input = new File("./crime-data/trispace/Unnormalized/" + PERSPECTIVES[j] + ".csv");
		// 		output = "./crime-data/SOMaticIn/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[j] + ".dat";
		// 		CSVManager.fromCSV2DatFile(PERSPECTIVES[j], input, output);
		// 	}
		// }


		// this is an updated version of converting to a .dat file to conform with my file paths
		// -----------------------------------------------------------------------------------------
		for (int i = 0; i < PERSPECTIVES.length; i++) {
			for (int j = 0; j < PERSPECTIVES.length; j++) {
				// Update input file path
				input = new File("./data/Steps/100/CA_no_clouds_100.csv");
				
				// Update output file path
				output = "./data/trispace/CA_Normalized_3/" + PERSPECTIVES[i] + "_Normalized/" + PERSPECTIVES[i] + ".dat";
				
				// Convert CSV to .dat file
				CSVManager.fromCSV2DatFile(PERSPECTIVES[j], input, output);
			}
		}
		
// ___________________ End commenting out here for unnormalized csv _______________



		System.out.println("End of Session.");
		System.exit(0);
	}

}
