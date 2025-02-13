package edu.sdsu.datavis.trispace.tsprep.io.img;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public class ImageManager {

	public final static String PYTHON = "D:/Python34/python.exe";
	
	// Remove noData pixels
	public static void cleanData(File f, int noData, boolean headers) {

		// test if f is a Directory or a File
		if (f.isDirectory()) {
			cleanFolder(f, noData, "", headers);
		} else {
			cleanFile(f, noData, "", headers);
		}
	}

	// Remove noData pixels
	public static void cleanData(File f, int noData, String output, boolean headers) {
		// test if f is a Directory or a File
		if (f.isDirectory()) {
			cleanFolder(f, noData, output, headers);
		} else {
			cleanFile(f, noData, output, headers);
		}
	}
	
	// Remove noData pixels
	public static void cleanData(File f, int noData, String output, boolean headers, String dictionary) {
		// test if f is a Directory or a File
		if (f.isDirectory()) {
			cleanFolder(f, noData, output, headers, dictionary);
		} else {
			cleanFile(f, noData, output, headers, dictionary);
		}
	}

	// Remove noData pixels
	private static void cleanFile(File f, int noData, String output, boolean headers) {
		// variable to store the final output string
		String newOutput = "";

		// variable to store the final output directory structure
		String dirStructure = "";

		// if blank create a '/clean' subdirectory
		if (output.equals("")) {

			// variable to hold the '/clean' directory
			File cleanDir = new File(f.getParent() + "/clean");

			// make the directory
//			cleanDir.mkdir();

			// output file has same name as input
			newOutput = cleanDir.getPath() + "/" + f.getName();
			dirStructure = cleanDir.getPath();
		} else {

			newOutput = output + "/" + f.getName();
			dirStructure = output;
		}

		try {
			Files.createDirectories(Paths.get(dirStructure));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		clean(f.getPath(), noData, newOutput, headers);
	}
	
	// Remove noData pixels
	private static void cleanFile(File f, int noData, String output, boolean headers, String dictionary) {
		// variable to store the final output string
		String newOutput = "";

		// variable to store the final output directory structure
		String dirStructure = "";

		// if blank create a '/clean' subdirectory
		if (output.equals("")) {

			// variable to hold the '/clean' directory
			File cleanDir = new File(f.getParent() + "/clean");

			// make the directory
//				cleanDir.mkdir();

			// output file has same name as input
			newOutput = cleanDir.getPath() + "/" + f.getName();
			dirStructure = cleanDir.getPath();
		} else {

			newOutput = output + "/" + f.getName();
			dirStructure = output;
		}

		try {
			Files.createDirectories(Paths.get(dirStructure));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		clean(f.getPath(), noData, newOutput, headers, dictionary);
	}

	private static void clean(String input, int noData, String output, boolean headers) {

		try {

			// BufferedReader to read the input (CSV) file
			BufferedReader br = new BufferedReader(new FileReader(input));
			
			// FileWriter to write the output (CSV) file
			FileWriter writer = new FileWriter(output);
			
			// variable to hold the line-by-line data
			String line = "";
			
			if (headers) {
				
				// read 1st line of BufferedReader
				line = br.readLine();
				
				// split the line
				String[] lineData = line.split(",");
				
				// calculate the number of bands
				int numBands = lineData.length-2;	
				
				// write the headers
				writer.append("x,y,");
				
				for (int i = 1; i < numBands; i++) {
					writer.append("band" + i + ",");
				}
				writer.append("band" + numBands + "\n");
				
				// reset BufferedReader
				br.close();			
				br = new BufferedReader(new FileReader(input));
				line = "";
			}

			// iterate thru input file
			while ((line = br.readLine()) != null) {

				// capture data line-by-line as a "," delineated array
				String[] data = line.split(",");

				// test if row has data
				if (Integer.parseInt(data[data.length - 1]) != noData) {

					// if it has data then record the row in the output
					for (int i = 0; i < data.length - 1; i++) {
						writer.append(data[i]);
						writer.append(',');
					}

					// record the last column
					writer.append(data[data.length - 1]);
					// create a new line
					writer.append('\n');
				}
			}

			// close IO resources
			writer.flush();
			writer.close();
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void clean(String input, int noData, String output, boolean headers, String dictionary) {

		try {

			// BufferedReader to read the input (CSV) file
			BufferedReader br = new BufferedReader(new FileReader(input));
			
			// BufferedReader to read the dictionary (CSV) file
			BufferedReader dict = new BufferedReader(new FileReader(dictionary));
			ArrayList<String> attributes = new ArrayList<String>();
			
			// remove header
			String line = dict.readLine();
			
			// iterate thru dictionary file appending attribute labels to array list
			while ((line = dict.readLine()) != null) {
				if (!line.equals("")) {
					
					// only extract the first column
					attributes.add(line.split(",")[0]);
				}				
			}
			
			// reset variable;
			line = "";
			
			// FileWriter to write the output (CSV) file
			FileWriter writer = new FileWriter(output);
			
			if (headers) {
				
				// read 1st line of BufferedReader
				line = br.readLine();
				
				// split the line
				String[] lineData = line.split(",");
				
				// calculate the number of bands
				int numBands = lineData.length-2;	
				
				// write the headers
				writer.append("x,y,");
				
				for (int i = 0; i < attributes.size()-1; i++) {
					writer.append(attributes.get(i) + ",");
				}
				writer.append(attributes.get(attributes.size()-1) + "\n");
				
				// reset BufferedReader
				br.close();			
				br = new BufferedReader(new FileReader(input));
				line = "";
			}

			// iterate thru input file
			while ((line = br.readLine()) != null) {

				// capture data line-by-line as a "," delineated array
				String[] data = line.split(",");

				// test if row has data
				if (Integer.parseInt(data[data.length - 1]) != noData) {

					// if it has data then record the row in the output
					for (int i = 0; i < data.length - 1; i++) {
						writer.append(data[i]);
						writer.append(',');
					}

					// record the last column
					writer.append(data[data.length - 1]);
					// create a new line
					writer.append('\n');
				}
			}

			// close IO resources
			writer.flush();
			writer.close();
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// Remove noData pixels
	private static void cleanFolder(File dir, int noData, String output, boolean headers) {

		// variable to store the final output directory structure
		String dirStructure = "";

		// get the csv files in the directory
		File[] files = dir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".csv");
			}
		});

		// array to hold the filepaths & names of outputs
		String[] newOutput = new String[files.length];

		if (output.equals("")) {

			// variable to hold the '/clean' directory
			File cleanDir = new File(dir.getPath() + "/clean");

			// make the directory
//			cleanDir.mkdir();

			for (int i = 0; i < newOutput.length; i++) {
				// output file has same name as input
				newOutput[i] = cleanDir.getPath() + "/" + files[i].getName();
			}

			dirStructure = cleanDir.getPath();
		} else {
			for (int i = 0; i < newOutput.length; i++) {
				newOutput[i] = output + "/" + files[i].getName();
			}

			dirStructure = output;
		}

		try {
			Files.createDirectories(Paths.get(dirStructure));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		for (int i = 0; i < files.length; i++) {
			clean(files[i].getPath(), noData, newOutput[i], headers);
		}
	}
	
	// Remove noData pixels
	private static void cleanFolder(File dir, int noData, String output, boolean headers, String dictionary) {

		// variable to store the final output directory structure
		String dirStructure = "";

		// get the csv files in the directory
		File[] files = dir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".csv");
			}
		});

		// array to hold the filepaths & names of outputs
		String[] newOutput = new String[files.length];

		if (output.equals("")) {

			// variable to hold the '/clean' directory
			File cleanDir = new File(dir.getPath() + "/clean");

			// make the directory
//				cleanDir.mkdir();

			for (int i = 0; i < newOutput.length; i++) {
				// output file has same name as input
				newOutput[i] = cleanDir.getPath() + "/" + files[i].getName();
			}

			dirStructure = cleanDir.getPath();
		} else {
			for (int i = 0; i < newOutput.length; i++) {
				newOutput[i] = output + "/" + files[i].getName();
			}

			dirStructure = output;
		}

		try {
			Files.createDirectories(Paths.get(dirStructure));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		for (int i = 0; i < files.length; i++) {
			clean(files[i].getPath(), noData, newOutput[i], headers, dictionary);
		}
	}

	// Convert file(s) into CSV format via gdal2xyz.py
	public static void performGDAL2XYZ(File f) {

		// test if f is a Directory or a File
		if (f.isDirectory()) {
			xyzFolder(f, "");
		} else {
			xyzFile(f, "");
		}
	}

	// Convert file(s) into CSV format via gdal2xyz.py
	public static void performGDAL2XYZ(File f, String output) {

		// test if f is a Directory or a File
		if (f.isDirectory()) {
			xyzFolder(f, output);
		} else {
			xyzFile(f, output);
		}
	}

	private static void xyzFile(File f, String output) {
		// get the file path
		String fPath = f.getPath();

		// get the number of bands
		int numBands = getNumBands(fPath);

		// variable to hold the final output String
		String newOutput = "";

		// if blank use same directory for output as for input
		if (output.equals("")) {
			newOutput = fPath.substring(0, fPath.length() - 4) + ".csv";
		} else {
			newOutput = output + "/" + f.getName().substring(0, f.getName().length() - 4) + ".csv";
		}

		// System.out.println(numBands);
		// System.out.println(fPath);
		// System.out.println(newOutput);

		// perform gdal2xyz.py
		System.out.println("numBands: " + numBands);
		runPython(numBands, fPath, newOutput);
//		runPython(6, fPath, newOutput);
	}

	private static void xyzFolder(File dir, String output) {

		// get the tif files in the directory
		File[] files = dir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".tif");
			}
		});

		// get the number of bands
		int numBands = getNumBands(files[0].getPath());
//		int numBands = 6;

		String[] newOutput = new String[files.length];
		
		

		if (output.equals("")) {
			for (int i = 0; i < newOutput.length; i++) {
				newOutput[i] = files[i].getPath().substring(0, files[i].getPath().length() - 4) + ".csv";
			}
		} else {
			try {
				Files.createDirectories(Paths.get(output));
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			for (int i = 0; i < newOutput.length; i++) {
				newOutput[i] = output + "/" + files[i].getName().substring(0, files[i].getName().length() - 4) + ".csv";
			}
		}

		System.out.println("numBands: " + numBands);
		// perform gdal2xyz.py
		for (int i = 0; i < files.length; i++) {
			runPython(numBands, files[i].getPath(), newOutput[i]);
//			runPython(6, files[i].getPath(), newOutput[i]);
		}
	}

	// Performs the gdal2xyz.py script with dynamic parameters
	// Usage: gdal2xyz.py [-skip factor] [-srcwin xoff yoff width height]
	// [-band b] [-csv] srcfile [dstfile]
	private static void runPython(int numBands, String imagery, String csv) {
		String[] bands = new String[numBands];
		for (int i = 0; i < numBands; i++) {
			bands[i] = String.valueOf((i + 1));
			// cols[i + 1] = String.valueOf((i + 1));
			// cols2[i + 2] = String.valueOf((i + 1));
			// bList.add(new ArrayList<Integer>());
		}
		System.out.println("Running gdal2xyz.py script on " + imagery);
		String filePath = ".";
		String argBand = "-band";
		String argCSV = "-csv";
		String argsrc = imagery;
		String argdest = csv;
		String argScript = filePath + "/lib/gdal2xyz.py";
		int numBands2 = numBands * 2;

		// String array to hold all arguments
		String[] cmd = new String[5 + numBands2];

		// Call python
//		cmd[0] = "python";
		cmd[0] = PYTHON;

		// Call gdal2xys.py
		cmd[1] = argScript;

		// [-band b] argument(s)
		int counter = 1;
		int counter2 = 2;
		for (int i = 0; i < numBands2; i++) {
			if (i == 0 || i % 2 == 0) {
				cmd[i + 2] = argBand;
			} else {
				cmd[i + 2] = bands[i - counter++];
			}
			counter2++;
		}
		// [-csv] argument
		cmd[counter2++] = argCSV;
		// srcfile argument
		cmd[counter2++] = argsrc;
		// [dstfile] argument
		cmd[counter2] = argdest;

		// Execute .py script
		try {
			Runtime rt = Runtime.getRuntime();
			Process pr = rt.exec(cmd);
			pr.waitFor();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// GDAL Method to get the number of bands from input TIF files via
	// rasterCount.py
	// Returns -1 if file cannot be read
	public static int getNumBands(String path) {
		System.out.println("BANDS");
		String fullPath = "D:\\Workspace\\OxygenProjects\\tsprep";
		// initialize bands variable
		int bands = -1;
		String argument = path;
//		String argument = fullPath + "\\data\\inputImagery\\1993_SanElijo.tif";
		String filePath = ".";
		String argScript1 = filePath + "\\lib\\rasterCoun2.py";
//		String argScript1 = fullPath + "\\lib\\rasterCount.py";
//		String argScript1 = "-V";
//		String[] cmd = { "D:\\Python34\\python.exe", argScript1, argument };
		String[] cmd = { "python", argScript1, argument };
		
//		for (int i = 0; i < cmd.length; i++) {
//			System.out.println(cmd[i]);
//		}

		try {
//			ProcessBuilder pb = new ProcessBuilder(PYTHON,"D:\\Workspace\\OxygenProjects\\tsprep\\lib\\rasterCount.py");
//			Process pr = pb.start();
			Runtime rt = Runtime.getRuntime();
			Process pr = rt.exec(cmd);
			pr.waitFor();
//			System.out.println(pr.);
			for (int i = 0; i < cmd.length; i++) {
				System.out.println(cmd[i]);
			}
			BufferedReader bfr = new BufferedReader(new InputStreamReader(pr.getInputStream()));
			String line = "";
			while ((line = bfr.readLine()) != null) {
				 System.out.println(line);
				bands = Integer.parseInt(line);
			}
			bfr.close();
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			bands = -1;
		}
		System.out.println(bands);
		return (bands);
	}
	
	public static int getNumColumns(String path) {
		// initialize bands variable
			int cols = -1;
			String argument = path;
			String filePath = ".";
			String argScript1 = filePath + "\\lib\\columnCount3.py";
			String[] cmd = { "python", argScript1, argument };
//			String[] cmd = { PYTHON, argScript1, argument };
			for (int i = 0; i < cmd.length; i++) {
				System.out.println(cmd[i]);
			}

			try {
				Runtime rt = Runtime.getRuntime();
				Process pr = rt.exec(cmd);
				pr.waitFor();
				BufferedReader bfr = new BufferedReader(new InputStreamReader(pr.getInputStream()));
				String line = "";
				while ((line = bfr.readLine()) != null) {
					 System.out.println(line);
					cols = Integer.parseInt(line);
				}
				bfr.close();
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				cols = -2;
			}
			return (cols);
	}
	
	public static int getNumRows(String path) {
		// initialize bands variable
			int rows = -1;
			String argument = path;
			String filePath = ".";
			String argScript1 = filePath + "\\lib\\rowCount3.py";
			String[] cmd = { "python", argScript1, argument };
//			String[] cmd = { PYTHON, argScript1, argument };
			for (int i = 0; i < cmd.length; i++) {
				System.out.println(cmd[i]);
			}

			try {
				Runtime rt = Runtime.getRuntime();
				Process pr = rt.exec(cmd);
				pr.waitFor();
				BufferedReader bfr = new BufferedReader(new InputStreamReader(pr.getInputStream()));
				String line = "";
				while ((line = bfr.readLine()) != null) {
					 System.out.println(line);
					rows = Integer.parseInt(line);
				}
				bfr.close();
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				rows = -2;
			}
			return (rows);
	}
}
