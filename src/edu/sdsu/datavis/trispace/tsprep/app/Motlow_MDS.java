package edu.sdsu.datavis.trispace.tsprep.app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import edu.sdsu.datavis.trispace.tsprep.dr.MDSManager;

public class Motlow_MDS {

    static int studyArea = 1;

    // Schema list
	final static String[] SCHEMAS = { "Port", "Cali", "Afri" };
	final static String SCHEMA = SCHEMAS[studyArea];

    final static boolean RESET = false;
	
	final static String[] PERSPECTIVES = { "L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L" };
	final static boolean BATCH = true;
	final static boolean HAS_LC = false;
	
	
	
	
	// specify columns that represent unique ID
	final static int[] PRIMARY_KEYS = { 0 };
	
	// normalization parameters
	// specify range of normalization (e.g. 0-1; .1-.9, etc.)
	final static float MIN_NORMALIZATION = 0.1f;
	final static float MAX_NORMALIZATION = 0.9f;
	
	// implemented distance measures
	final static int EUCLIDEAN = 1;
	final static int COSINE = 2;
	final static int MANHATTAN = 3;

	// specify distance measure
	final static int DIST_MEASURE = 2;
	
	// SOM training parameters
	final static int NITERATIONS1 = 10000;
	final static int NITERATIONS2 = 20000;
	final static int NITERATIONS3 = 50000;
	final static int NTHREADS = 4;
	final static boolean ROUNDING = true;
	final static int SCALING_FACTOR = 1;

	final static int MAX_K = 12;
	final static int K_ITERATIONS = 1000;

    final static int[] normalizations = { 1, 2};
    public static void main(String[] args) {

        for (int i = 0; i < normalizations.length; i++) {

			String normalization = PERSPECTIVES[normalizations[i]];

			// iterate thru each perspective
			for (int k = 0; k < PERSPECTIVES.length; k++) {
			// int k = 0;

            String mdsInput = "./data/motlow/" + SCHEMA + "/trispace/" + normalization + "_Normalized/" + PERSPECTIVES[k]
                                + ".csv";
                        File mdsFile = new File(mdsInput);

                        String newOutput = "./data/motlow/" + SCHEMA + "/MDSOut/" + normalization + "_Normalized";
                        try {
                            Files.createDirectories(Paths.get(newOutput));
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }

                        newOutput = newOutput + "/" + PERSPECTIVES[k] + ".csv";

                        MDSManager.createMDSFile(mdsFile, PERSPECTIVES[k], DIST_MEASURE, newOutput);
                        // MDSManager.performMDS(mdsFile, PERSPECTIVES[k], normalization, DIST_MEASURE, db, schema, MAX_K,
                                // K_ITERATIONS, BATCH);
                    }
                }
    }
    
}
