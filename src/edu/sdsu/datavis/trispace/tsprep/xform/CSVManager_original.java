package edu.sdsu.datavis.trispace.tsprep.xform;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.analysis.function.Atanh;
import org.apache.commons.math3.analysis.function.Tanh;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;

import edu.sdsu.datavis.trispace.tsprep.db.PostgreSQLJDBC;
import edu.sdsu.datavis.trispace.tsprep.som.Cod;
import edu.sdsu.datavis.trispace.tsprep.som.FileHandler;
import edu.sdsu.datavis.trispace.tsprep.utils.DescriptiveStatistics;
import edu.sdsu.datavis.trispace.tsprep.utils.KMeans;

/**
 * CSVManager is an abstract class for all Tri-Space conversions that include
 * transformation and formatting methods.
 * 
 * @author Tim Schempp
 * @version %I%, %G%
 * @since 1.0
 */
public abstract class CSVManager_original {

	final static String[] PERSPECTIVES = { "L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L" };

	/**
	 * Counts the number of rows in a .csv or .txt file
	 *
	 * @param fPath
	 *            Path to file.
	 */
	public static int rowCount(String fPath) {

		try {

			// create BufferedReader
			BufferedReader br = new BufferedReader(new FileReader(fPath));

			// initialize variable to store number of rows
			int rowCounter = 0;

			// read thru all lines
			while ((br.readLine()) != null) {
				// increment row counter
				rowCounter++;
			}

			// close the BR
			br.close();

			// return the number of rows
			return rowCounter;
		} catch (IOException e) {

			// TODO Auto-generated catch block
			e.printStackTrace();

			// return -1 if any IOException occurs
			return -1;
		}
	}

	/**
	 * Compares the Loci, located at the PK columns, of files contained within
	 * directory.
	 *
	 * @param outDir
	 *            the directory containing files to compare.
	 * @param primaryKeys
	 *            column indexes to check for comparison.
	 */
	public static boolean commonLociCheck(String outDir, int[] primaryKeys) {

		File f = new File(outDir);
		// test if file is a directory
		if (f.isDirectory()) {

			// store children to array
			File[] children = f.listFiles();

			// loop thru each child-combination to perform common loci test
			for (int i = 0; i < children.length; i++) {
				for (int j = i + 1; j < children.length; j++) {

					// System.out.println(children[i].getName());
					// System.out.println(children[j].getName());

					// if any comparison is false, return false
					if (!compareLoci(children[0], children[i], primaryKeys)) {
						// System.out.println(children[0]);
						// System.out.println(children[1]);
						return false;
					}
				}
			}
		} else {
			// return false if File is not a directory
			return false;
		}

		// return true if no loci comparisons are false
		return true;
	}

	/**
	 * Compares the Loci, located at the PK columns, contained within 2 files.
	 *
	 * @param f1
	 *            first file to compare.
	 * @param f2
	 *            second file to compare.
	 * @param primaryKeys
	 *            column indexes to check for comparison.
	 */
	public static boolean compareLoci(File f1, File f2, int[] primaryKeys) {
		try {
			// initialize BufferedReader variables
			BufferedReader br1 = new BufferedReader(new FileReader(f1.getPath()));
			BufferedReader br2 = new BufferedReader(new FileReader(f2.getPath()));

			// compensate for headers
			// while (headers > 0) {
			// System.out.println("HEADERS");
			// System.out.println(br1.readLine());
			// System.out.println(br2.readLine());
			// headers--;
			// }

			// initialize variables to hold the line data
			String line1 = "";
			String line2 = "";

			int testCounter = 0;
			// loop thru all lines in file
			while ((line1 = br1.readLine()) != null) {
				testCounter++;
				line2 = br2.readLine();

				// String array variable to split each line
				String[] split1 = line1.split(",");
				String[] split2 = line2.split(",");

				// compare primary keys b/w files
				for (int i = 0; i < primaryKeys.length; i++) {
					// System.out.println(split1[i]);
					// System.out.println(split2[i]);
					// if not equal, close BRs and return false
					if (!split1[i].equals(split2[i])) {
						System.out.println(testCounter);
						System.out.println(f1.getName());
						System.out.println(f2.getName());
						System.out.println(split1[i]);
						System.out.println(split2[i]);
						br1.close();
						br2.close();
						return false;
					}
				}
			}

			// close BRs
			br1.close();
			br2.close();

			// return true since all comparisons were equal
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

			// return false if any IOException occurs
			return false;
		}
	}

	/**
	 * Deletes a directory with file contents
	 *
	 * @param path
	 *            directory file path.
	 */
	public static void deleteDir(String path) {
		Path rootPath = Paths.get(path);
		try {
			Files.walk(rootPath, FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).map(Path::toFile)
					.peek(System.out::println).forEach(File::delete);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Deletes a directory with file contents
	 *
	 * @param dir
	 *            directory file.
	 */
	public static boolean deleteDirectory2(File dir) {
		File[] allContents = dir.listFiles();
		if (allContents != null) {
			for (File file : allContents) {
				deleteDirectory2(file);
			}
		}
		return dir.delete();
	}

	/**
	 * Deletes a directory with file contents
	 *
	 * @param dir
	 *            directory file.
	 */
	public static boolean deleteDirectory(File dir) {
		// System.out.println("DELETING FILE/DIRECTORY: " + dir.getName());
		if (dir.isDirectory()) {
			File[] children = dir.listFiles();
			for (int i = 0; i < children.length; i++) {
				// System.out.println(children[i].getName());
				boolean success = deleteDirectory(children[i]);
				if (!success) {
					return false;
				}
			}
		}

		// either file or an empty directory
		System.out.println("removing file or directory : " + dir.getName());
		return dir.delete();
	}

	/**
	 * Converts a CSV file to a .dat file for SOMatic
	 *
	 * @param perspective
	 *            the CSV file's current perspective.
	 * @param csv
	 *            the CSV file.
	 * @param output
	 *            the output path including .dat extension.
	 */
	public static void fromCSV2DatFile(String perspective, File csv, String output) {
		try {
			// initialize BufferedReader for csv File
			BufferedReader br = new BufferedReader(new FileReader(csv.getPath()));

			// split the output path
			String[] outSplit = output.split("/");

			// get the parent folder of output file
			String newOutput = outSplit[0];
			for (int i = 1; i < outSplit.length - 1; i++) {
				newOutput = newOutput + "/" + outSplit[i];
			}

			// create directory structure
			Files.createDirectories(Paths.get(newOutput));

			// initialize FileWriter variable
			FileWriter writer = new FileWriter(output);

			// initialize variable to store line data
			// store the first line
			String line = br.readLine();

			// String array to split the line
			String[] split = line.split(",");

			// perform operation on LA_T, LT_A, and AT_L perspectives
			if (perspective.equals("LA_T") || perspective.equals("LT_A") || perspective.equals("AT_L")) {

				// create .dat file headers
				writer.append((split.length - 2) + "\r\n");
				writer.append("#att ");
				for (int i = 2; i < split.length; i++) {
					writer.append(split[i] + " ");
				}
				writer.append("\r\n");

				// initialize counter variable
				int counter = 1;

				// loop thru all lines
				while ((line = br.readLine()) != null) {

					// split current line
					split = line.split(",");

					// loop thru split
					for (int i = 2; i < split.length; i++) {

						// append split data
						writer.append(split[i] + " ");
					}

					// append IV labels
					writer.append(split[0] + "_" + split[1] + " " + counter + "\r\n");

					// increment counter
					counter++;
				}

				// perform operation on L_AT, T_LA, A_LT perspectives
			} else if (perspective.equals("T_LA") || perspective.equals("L_AT") || perspective.equals("A_LT")) {

				// initialize variable to store line data
				// store the first line
				// String line = br.readLine();

				// String array to split the line
				// String[] split = line.split(",");

				// create .dat file headers
				writer.append((split.length - 1) + "\r\n");
				line = br.readLine();
				String[] split2 = line.split(",");
				writer.append("#att ");
				for (int i = 1; i < split.length; i++) {
					writer.append(split[i] + "_" + split2[i] + " ");
				}
				writer.append("\r\n");

				// initialize counter variable
				int counter = 1;

				// loop thru all lines
				while ((line = br.readLine()) != null) {

					// split the current line
					split = line.split(",");

					// loop thru split
					for (int i = 1; i < split.length; i++) {

						// append split data
						writer.append(split[i] + " ");
					}

					// append IV labels
					writer.append(split[0] + " " + counter + "\r\n");

					// increment counter
					counter++;
				}
			}

			// flush the writer
			writer.flush();

			// close the writer
			writer.close();

			// close the BR
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Converts a AT_L CSV file to the remaining perspectives.
	 *
	 * @param f
	 *            input AT_L CSV file.
	 * @param output
	 *            output directory.
	 */
	public static void fromAT_L2All(File f, String output) {
		fromAT_L2L_AT(f, output + "/L_AT.csv");

		File fileL_AT = new File(output + "/L_AT.csv");

		fromL_AT2A_LT(fileL_AT, output + "/A_LT.csv");
		fromL_AT2T_LA(fileL_AT, output + "/T_LA.csv");
		fromL_AT2LT_A(fileL_AT, output + "/LT_A.csv");
		fromL_AT2LA_T(fileL_AT, output + "/LA_T.csv");
	}

	/**
	 * Converts a AT_L CSV file to the remaining perspectives.
	 *
	 * @param f
	 *            input AT_L CSV file.
	 * @param output
	 *            output directory.
	 * @param name
	 *            name extension.
	 */
	public static void fromAT_L2All(File f, String output, String name) {
		fromAT_L2L_AT(f, output + "/L_AT_" + name + ".csv");

		File fileL_AT = new File(output + "/L_AT_" + name + ".csv");

		fromL_AT2A_LT(fileL_AT, output + "/A_LT_" + name + ".csv");
		fromL_AT2T_LA(fileL_AT, output + "/T_LA_" + name + ".csv");
		fromL_AT2LT_A(fileL_AT, output + "/LT_A_" + name + ".csv");
		fromL_AT2LA_T(fileL_AT, output + "/LA_T_" + name + ".csv");
	}

	/**
	 * Converts a LA_T CSV file to the remaining perspectives.
	 *
	 * @param f
	 *            input LA_T CSV file.
	 * @param output
	 *            output directory.
	 */
	public static void fromLA_T2All(File f, String output) {
		fromLA_T2L_AT(f, output + "/L_AT.csv");

		File fileL_AT = new File(output + "/L_AT.csv");

		fromL_AT2A_LT(fileL_AT, output + "/A_LT.csv");
		fromL_AT2T_LA(fileL_AT, output + "/T_LA.csv");
		fromL_AT2LT_A(fileL_AT, output + "/LT_A.csv");
		fromL_AT2AT_L(fileL_AT, output + "/AT_L.csv");
	}

	/**
	 * Converts a LA_T CSV file to the remaining perspectives.
	 *
	 * @param f
	 *            input LA_T CSV file.
	 * @param output
	 *            output directory.
	 * @param name
	 *            name extension.
	 */
	public static void fromLA_T2All(File f, String output, String name) {
		fromLA_T2L_AT(f, output + "/L_AT_" + name + ".csv");

		File fileL_AT = new File(output + "/L_AT_" + name + ".csv");

		fromL_AT2A_LT(fileL_AT, output + "/A_LT_" + name + ".csv");
		fromL_AT2T_LA(fileL_AT, output + "/T_LA_" + name + ".csv");
		fromL_AT2LT_A(fileL_AT, output + "/LT_A_" + name + ".csv");
		fromL_AT2AT_L(fileL_AT, output + "/AT_L_" + name + ".csv");
	}

	/**
	 * Converts a T_LA CSV file to the remaining perspectives.
	 *
	 * @param f
	 *            input T_LA CSV file.
	 * @param output
	 *            output directory.
	 */
	public static void fromT_LA2All(File f, String output) {
		fromT_LA2L_AT(f, output + "/L_AT.csv");

		File fileL_AT = new File(output + "/L_AT.csv");

		fromL_AT2A_LT(fileL_AT, output + "/A_LT.csv");
		fromL_AT2LA_T(fileL_AT, output + "/LA_T.csv");
		fromL_AT2LT_A(fileL_AT, output + "/LT_A.csv");
		fromL_AT2AT_L(fileL_AT, output + "/AT_L.csv");
	}

	/**
	 * Converts a T_LA CSV file to the remaining perspectives.
	 *
	 * @param f
	 *            input T_LA CSV file.
	 * @param output
	 *            output directory.
	 * @param name
	 *            name extension.
	 */
	public static void fromT_LA2All(File f, String output, String name) {
		fromT_LA2L_AT(f, output + "/L_AT_" + name + ".csv");

		File fileL_AT = new File(output + "/L_AT_" + name + ".csv");

		fromL_AT2A_LT(fileL_AT, output + "/A_LT_" + name + ".csv");
		fromL_AT2LA_T(fileL_AT, output + "/LA_T_" + name + ".csv");
		fromL_AT2LT_A(fileL_AT, output + "/LT_A_" + name + ".csv");
		fromL_AT2AT_L(fileL_AT, output + "/AT_L_" + name + ".csv");
	}

	/**
	 * Converts a LT_A CSV file to the remaining perspectives.
	 *
	 * @param f
	 *            input LT_A CSV file.
	 * @param output
	 *            output directory.
	 */
	public static void fromLT_A2All(File f, String output) {
		fromLT_A2L_AT(f, output + "/L_AT.csv");

		File fileL_AT = new File(output + "/L_AT.csv");

		fromL_AT2A_LT(fileL_AT, output + "/A_LT.csv");
		fromL_AT2LA_T(fileL_AT, output + "/LA_T.csv");
		fromL_AT2T_LA(fileL_AT, output + "/T_LA.csv");
		fromL_AT2AT_L(fileL_AT, output + "/AT_L.csv");
	}

	/**
	 * Converts a LT_A CSV file to the remaining perspectives.
	 *
	 * @param f
	 *            input LT_A CSV file.
	 * @param output
	 *            output directory.
	 * @param name
	 *            name extension.
	 */
	public static void fromLT_A2All(File f, String output, String name) {
		fromLT_A2L_AT(f, output + "/L_AT_" + name + ".csv");

		File fileL_AT = new File(output + "/L_AT_" + name + ".csv");

		fromL_AT2A_LT(fileL_AT, output + "/A_LT_" + name + ".csv");
		fromL_AT2LA_T(fileL_AT, output + "/LA_T_" + name + ".csv");
		fromL_AT2T_LA(fileL_AT, output + "/T_LA_" + name + ".csv");
		fromL_AT2AT_L(fileL_AT, output + "/AT_L_" + name + ".csv");
	}

	/**
	 * Converts a A_LT CSV file to the remaining perspectives.
	 *
	 * @param f
	 *            input A_LT CSV file.
	 * @param output
	 *            output directory.
	 */
	public static void fromA_LT2All(File f, String output) {
		// 5 conversions done in parallel
		fromA_LT2L_AT(f, output + "/L_AT.csv");

		File fileL_AT = new File(output + "/L_AT.csv");

		fromL_AT2LT_A(fileL_AT, output + "/LT_A.csv");
		fromL_AT2LA_T(fileL_AT, output + "/LA_T.csv");
		fromL_AT2T_LA(fileL_AT, output + "/T_LA.csv");
		fromL_AT2AT_L(fileL_AT, output + "/AT_L.csv");
	}

	/**
	 * Converts a A_LT CSV file to the remaining perspectives.
	 *
	 * @param f
	 *            input A_LT CSV file.
	 * @param output
	 *            output directory.
	 * @param name
	 *            name extension.
	 */
	public static void fromA_LT2All(File f, String output, String name) {
		fromA_LT2L_AT(f, output + "/L_AT_" + name + ".csv");

		File fileL_AT = new File(output + "/L_AT_" + name + ".csv");

		fromL_AT2LT_A(fileL_AT, output + "/LT_A_" + name + ".csv");
		fromL_AT2LA_T(fileL_AT, output + "/LA_T_" + name + ".csv");
		fromL_AT2T_LA(fileL_AT, output + "/T_LA_" + name + ".csv");
		fromL_AT2AT_L(fileL_AT, output + "/AT_L_" + name + ".csv");
	}

	/**
	 * Converts a L_AT CSV file to the remaining perspectives.
	 *
	 * @param input
	 *            input L_AT CSV file.
	 * @param output
	 *            output directory.
	 */
	public static void fromL_AT2All(File input, String output) {
		fromL_AT2LT_A(input, output + "/LT_A.csv");
		fromL_AT2LA_T(input, output + "/LA_T.csv");
		fromL_AT2T_LA(input, output + "/T_LA.csv");
		fromL_AT2A_LT(input, output + "/A_LT.csv");
		fromL_AT2AT_L(input, output + "/AT_L.csv");
	}

	/**
	 * Converts a L_AT CSV file to the remaining perspectives.
	 *
	 * @param f
	 *            input L_AT CSV file.
	 * @param output
	 *            output directory.
	 * @param name
	 *            name extension.
	 */
	public static void fromL_AT2All(File input, String output, String name) {
		fromL_AT2LT_A(input, output + "/LT_A_" + name + ".csv");
		fromL_AT2LA_T(input, output + "/LA_T_" + name + ".csv");
		fromL_AT2T_LA(input, output + "/T_LA_" + name + ".csv");
		fromL_AT2A_LT(input, output + "/A_LT_" + name + ".csv");
		fromL_AT2AT_L(input, output + "/AT_L_" + name + ".csv");
	}

	/**
	 * Re-scales a number in an original range to a new range. The same as the
	 * .map() function in Processing.
	 *
	 * @param unscaled
	 *            the value to rescale.
	 * @param oldMin
	 *            minimum of original scale.
	 * @param oldMax
	 *            maximum of original scale.
	 * @param newMin
	 *            minimum of new scale.
	 * @param newMax
	 *            maximum of new scale.
	 */
	private static float rescale(float unscaled, float oldMin, float oldMax, float newMin, float newMax) {
		if (unscaled == oldMin) {
			return newMin;
		} else if (unscaled == oldMax) {
			return newMax;
		} else {
			return (newMax - newMin) * (unscaled - oldMin) / (oldMax - oldMin) + newMin;
		}		
	}

	private static float getUnitVector(ArrayList<Float> data) {
		float value = 0;

		for (int i = 0; i < data.size(); i++) {
			float dataSquared = data.get(i) * data.get(i);
			value += dataSquared;
		}

		return (float) Math.sqrt(value);
	}

	/**
	 * Normalizes a CSV file across each row.
	 *
	 * @param f
	 *            input CSV file.
	 * @param output
	 *            output CSV filepath.
	 * @param perspective
	 *            perspective of input file.
	 * @param zeroValue
	 *            set the minimum value.
	 */
	public static void normalizeCSVUnitVector(File f, String output, String perspective) {
		int numHeaders = -1;
		int numColLeaders = -1;
		if (perspective.equalsIgnoreCase("L_AT") || perspective.equalsIgnoreCase("T_LA")
				|| perspective.equalsIgnoreCase("A_LT")) {
			numHeaders = 2;
			numColLeaders = 1;
		} else if (perspective.equalsIgnoreCase("LT_A") || perspective.equalsIgnoreCase("AT_L")
				|| perspective.equalsIgnoreCase("LA_T")) {
			numHeaders = 1;
			numColLeaders = 2;
		}

		try {
			BufferedReader br = new BufferedReader(new FileReader(f.getPath()));
			String[] splitOut = output.split("/");
			String dirStructure = "";

			for (int i = 0; i < splitOut.length - 1; i++) {
				dirStructure = dirStructure + splitOut[i] + "/";
			}
			Files.createDirectories(Paths.get(dirStructure));
			FileWriter writer = new FileWriter(output);

			String line = br.readLine();
			writer.append(line);
			writer.append("\n");

			if (numHeaders == 2) {
				line = br.readLine();
				writer.append(line);
				writer.append("\n");
			}

			while ((line = br.readLine()) != null) {

				String[] split = line.split(",");
				String[] newLine = new String[split.length];

				for (int i = 0; i < numColLeaders; i++) {
					newLine[i] = split[i];
				}

				ArrayList<Float> allVals = new ArrayList<Float>();

				for (int i = numColLeaders; i < split.length; i++) {

					float newValue = Float.parseFloat(split[i]);
					allVals.add(newValue);
				}

				float unitVector = getUnitVector(allVals);

				int counter = 0;
				for (int i = numColLeaders; i < split.length; i++) {
					float newData = allVals.get(counter) / unitVector;
					newLine[i] = "" + newData;
					counter++;
				}

				writer.append(newLine[0]);

				for (int i = 1; i < newLine.length; i++) {
					writer.append("," + newLine[i]);
				}
				writer.append("\n");
			}

			br.close();
			writer.flush();
			writer.close();
			// br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Normalizes a CSV file across each row.
	 *
	 * @param f
	 *            input CSV file.
	 * @param output
	 *            output CSV filepath.
	 * @param perspective
	 *            perspective of input file.
	 * @param zeroValue
	 *            set the minimum value.
	 */
	public static void normalizeCSVTanH(File f, String output, String perspective) {
		int numHeaders = -1;
		int numColLeaders = -1;
		if (perspective.equalsIgnoreCase("L_AT") || perspective.equalsIgnoreCase("T_LA")
				|| perspective.equalsIgnoreCase("A_LT")) {
			numHeaders = 2;
			numColLeaders = 1;
		} else if (perspective.equalsIgnoreCase("LT_A") || perspective.equalsIgnoreCase("AT_L")
				|| perspective.equalsIgnoreCase("LA_T")) {
			numHeaders = 1;
			numColLeaders = 2;
		}

		try {
			BufferedReader br = new BufferedReader(new FileReader(f.getPath()));
			String[] splitOut = output.split("/");
			String dirStructure = "";

			for (int i = 0; i < splitOut.length - 1; i++) {
				dirStructure = dirStructure + splitOut[i] + "/";
			}
			Files.createDirectories(Paths.get(dirStructure));
			FileWriter writer = new FileWriter(output);

			String line = br.readLine();
			writer.append(line);
			writer.append("\n");

			if (numHeaders == 2) {
				line = br.readLine();
				writer.append(line);
				writer.append("\n");
			}

			int lineCount = 0;
			while ((line = br.readLine()) != null) {

				String[] split = line.split(",");
				String[] newLine = new String[split.length];

				for (int i = 0; i < numColLeaders; i++) {
					newLine[i] = split[i];
				}

				float maxVal = Integer.MIN_VALUE;
				float minVal = Integer.MAX_VALUE;
				// ArrayList<Double> allVals = new ArrayList<Double>();
				double[] allVals = new double[split.length - numColLeaders];

				int counter = 0;
				for (int i = numColLeaders; i < split.length; i++) {

					double newValue = Double.parseDouble(split[i]);
					allVals[counter++] = newValue;
					// allVals.add(newValue);
					// if (newValue > maxVal) {
					// maxVal = newValue;
					// }
					//
					// if (newValue < minVal) {
					// minVal = newValue;
					// }
				}

				Mean mean = new Mean();
				StandardDeviation std = new StandardDeviation();
				Tanh tanh = new Tanh();

				// double meanValue = mean.evaluate(allVals);
				double meanValue = DescriptiveStatistics.calculateMean(allVals);

				double stdValue = DescriptiveStatistics.calculateStandardDeviation(allVals);
				// std.evaluate(values, mean)
				// double stdValue = std.evaluate(allVals);
				// double stdValue = std.evaluate(allVals,meanValue);

				if (lineCount == 0) {
					System.out.println("perspective: " + perspective);
					System.out.println("mean: " + meanValue);
					// System.out.println("mean2: " + mean2);
					System.out.println("std: " + stdValue);
					// System.out.println("std2: " + std2);
				}

				counter = 0;
				for (int i = numColLeaders; i < split.length; i++) {
					double intermediateVal = (allVals[counter] - meanValue) / stdValue;

					double normVal = 0.5 * ((tanh.value(0.01 * intermediateVal)) + 1);

					newLine[i] = normVal + "";

					// newLine[i] = "" + rescale(allVals.get(counter), minVal, maxVal, min, max);
					counter++;
				}

				writer.append(newLine[0]);

				for (int i = 1; i < newLine.length; i++) {
					writer.append("," + newLine[i]);
				}
				writer.append("\n");
				lineCount++;
			}

			br.close();
			writer.flush();
			writer.close();
			// br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Normalizes a CSV file across each row.
	 *
	 * @param f
	 *            input CSV file.
	 * @param output
	 *            output CSV filepath.
	 * @param perspective
	 *            perspective of input file.
	 * @param zeroValue
	 *            set the minimum value.
	 */
	public static void normalizeCSVMinMax(File f, String output, String perspective, float min, float max) {
		int numHeaders = -1;
		int numColLeaders = -1;
		if (perspective.equalsIgnoreCase("L_AT") || perspective.equalsIgnoreCase("T_LA")
				|| perspective.equalsIgnoreCase("A_LT")) {
			numHeaders = 2;
			numColLeaders = 1;
		} else if (perspective.equalsIgnoreCase("LT_A") || perspective.equalsIgnoreCase("AT_L")
				|| perspective.equalsIgnoreCase("LA_T")) {
			numHeaders = 1;
			numColLeaders = 2;
		}

		try {
			BufferedReader br = new BufferedReader(new FileReader(f.getPath()));
			String[] splitOut = output.split("/");
			String dirStructure = "";

			for (int i = 0; i < splitOut.length - 1; i++) {
				dirStructure = dirStructure + splitOut[i] + "/";
			}
			Files.createDirectories(Paths.get(dirStructure));
			FileWriter writer = new FileWriter(output);

			String line = br.readLine();
			writer.append(line);
			writer.append("\n");

			if (numHeaders == 2) {
				line = br.readLine();
				writer.append(line);
				writer.append("\n");
			}

			while ((line = br.readLine()) != null) {

				String[] split = line.split(",");
				String[] newLine = new String[split.length];

				for (int i = 0; i < numColLeaders; i++) {
					newLine[i] = split[i];
				}

				float maxVal = Integer.MIN_VALUE;
				float minVal = Integer.MAX_VALUE;
				ArrayList<Float> allVals = new ArrayList<Float>();

				for (int i = numColLeaders; i < split.length; i++) {

					float newValue = Float.parseFloat(split[i]);
					allVals.add(newValue);
					if (newValue > maxVal) {
						maxVal = newValue;
					}

					if (newValue < minVal) {
						minVal = newValue;
					}
				}

				int counter = 0;
				for (int i = numColLeaders; i < split.length; i++) {
					if (minVal != maxVal) {
						newLine[i] = "" + rescale(allVals.get(counter), minVal, maxVal, min, max);
					} else {
						newLine[i] = "" + 0.5f;
					}

					counter++;
				}

				writer.append(newLine[0]);

				for (int i = 1; i < newLine.length; i++) {
					writer.append("," + newLine[i]);
				}
				writer.append("\n");
			}

			br.close();
			writer.flush();
			writer.close();
			// br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Normalizes a CSV file across each row.
	 *
	 * @param f
	 *            input CSV file.
	 * @param output
	 *            output CSV filepath.
	 * @param perspective
	 *            perspective of input file.
	 * @param zeroValue
	 *            set the minimum value.
	 */
	public static void normalizeCSVMinMax(File f, String output, String perspective, float zeroValue) {
		int numHeaders = -1;
		int numColLeaders = -1;
		if (perspective.equalsIgnoreCase("L_AT") || perspective.equalsIgnoreCase("T_LA")
				|| perspective.equalsIgnoreCase("A_LT")) {
			numHeaders = 2;
			numColLeaders = 1;
		} else if (perspective.equalsIgnoreCase("LT_A") || perspective.equalsIgnoreCase("AT_L")
				|| perspective.equalsIgnoreCase("LA_T")) {
			numHeaders = 1;
			numColLeaders = 2;
		}

		try {
			BufferedReader br = new BufferedReader(new FileReader(f.getPath()));
			String[] splitOut = output.split("/");
			String dirStructure = "";

			for (int i = 0; i < splitOut.length - 1; i++) {
				dirStructure = dirStructure + splitOut[i] + "/";
			}
			Files.createDirectories(Paths.get(dirStructure));
			FileWriter writer = new FileWriter(output);

			String line = br.readLine();
			writer.append(line);
			writer.append("\n");

			if (numHeaders == 2) {
				line = br.readLine();
				writer.append(line);
				writer.append("\n");
			}

			while ((line = br.readLine()) != null) {

				String[] split = line.split(",");
				String[] newLine = new String[split.length];

				for (int i = 0; i < numColLeaders; i++) {
					newLine[i] = split[i];
				}

				float maxVal = Integer.MIN_VALUE;
				float minVal = Integer.MAX_VALUE;
				ArrayList<Float> allVals = new ArrayList<Float>();

				for (int i = numColLeaders; i < split.length; i++) {

					float newValue = Float.parseFloat(split[i]);
					allVals.add(newValue);
					if (newValue > maxVal) {
						maxVal = newValue;
					}

					if (newValue < minVal) {
						minVal = newValue;
					}
				}

				int counter = 0;
				for (int i = numColLeaders; i < split.length; i++) {
					newLine[i] = "" + rescale(allVals.get(counter), minVal, maxVal, zeroValue, 1);
					counter++;
				}

				writer.append(newLine[0]);

				for (int i = 1; i < newLine.length; i++) {
					writer.append("," + newLine[i]);
				}
				writer.append("\n");
			}

			br.close();
			writer.flush();
			writer.close();
			// br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Converts a double-header input LT_A CSV file to a L_AT CSV file.
	 *
	 * @param f
	 *            the input CSV file.
	 * @param output
	 *            the string to display. If the text is null, the tool tip is turned
	 *            off for this component.
	 */
	public static void fromLA_T2L_AT(File f, String output) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(f.getPath()));
			FileWriter writer = new FileWriter(output);

			ArrayList<String> attributes = new ArrayList<String>();
			ArrayList<String> times = new ArrayList<String>();

			String line = br.readLine();
			String[] split = line.split(",");

			// extract time labels
			for (int i = 2; i < split.length; i++) {
				times.add(split[i]);
			}

			// extract attribute labels
			line = br.readLine();
			split = line.split(",");
			String firstLoci = split[0];
			attributes.add(split[1]);
			while ((line = br.readLine()) != null) {
				split = line.split(",");

				if (split[0].equals(firstLoci)) {
					attributes.add(split[1]);
				} else {
					break;
				}
			}

			br.close();

			writer.append("L_AT");
			for (int i = 0; i < attributes.size(); i++) {
				for (int j = 0; j < times.size(); j++) {
					writer.append("," + attributes.get(i));
				}
			}

			writer.append("\n");

			for (int i = 0; i < attributes.size(); i++) {
				for (int j = 0; j < times.size(); j++) {
					writer.append("," + times.get(j));
				}
			}

			br = new BufferedReader(new FileReader(f.getPath()));
			br.readLine();

			int counter = 0;
			while ((line = br.readLine()) != null) {
				split = line.split(",");
				if (counter == 0) {
					writer.append("\n");
					writer.append(split[0]);
				}

				for (int i = 2; i < split.length; i++) {
					writer.append("," + split[i]);
				}

				counter++;
				if (counter == attributes.size()) {
					counter = 0;
				}
			}
			br.close();
			writer.flush();
			writer.close();
			// br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Converts a double-header input LT_A CSV file to a L_AT CSV file.
	 *
	 * @param f
	 *            the input CSV file.
	 * @param output
	 *            the string to display. If the text is null, the tool tip is turned
	 *            off for this component.
	 */
	public static void fromLT_A2L_AT(File f, String output) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(f.getPath()));
			FileWriter writer = new FileWriter(output);

			ArrayList<String> attributes = new ArrayList<String>();
			ArrayList<String> times = new ArrayList<String>();

			String line = br.readLine();
			String[] split = line.split(",");

			// extract attribute labels
			for (int i = 2; i < split.length; i++) {
				attributes.add(split[i]);
			}

			// extract time labels
			line = br.readLine();
			split = line.split(",");
			String firstLoci = split[0];
			times.add(split[1]);
			while ((line = br.readLine()) != null) {
				split = line.split(",");

				if (split[0].equals(firstLoci)) {
					times.add(split[1]);
				}
			}

			br.close();

			writer.append("L_AT");
			for (int i = 0; i < attributes.size(); i++) {
				for (int j = 0; j < times.size(); j++) {
					writer.append("," + attributes.get(i));
				}
			}

			writer.append("\n");

			String[] newLine = new String[(attributes.size() * times.size()) + 1];

			newLine[0] = "";

			int counter = 1;
			for (int i = 0; i < attributes.size(); i++) {
				for (int j = 0; j < times.size(); j++) {
					newLine[counter] = times.get(j);
					counter++;
				}
			}

			br = new BufferedReader(new FileReader(f.getPath()));
			br.readLine();

			counter = 0;
			while ((line = br.readLine()) != null) {
				split = line.split(",");
				if (counter == 0) {
					writer.append(newLine[0]);
					for (int i = 1; i < newLine.length; i++) {
						writer.append("," + newLine[i]);
					}
					writer.append("\n");
					newLine = new String[(attributes.size() * times.size()) + 1];

					newLine[0] = split[0];
				}

				for (int i = 0; i < attributes.size(); i++) {
					newLine[1 + counter + (i * times.size())] = split[2 + i];
				}

				counter++;
				if (counter == times.size()) {
					counter = 0;
				}
			}

			writer.append(newLine[0]);
			for (int i = 1; i < newLine.length; i++) {
				writer.append("," + newLine[i]);
			}
			writer.append("\n");
			newLine = new String[(attributes.size() * times.size()) + 1];

			newLine[0] = split[0];

			br.close();
			writer.flush();
			writer.close();
			// br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Converts a double-header input T_LA CSV file to a L_AT CSV file.
	 *
	 * @param f
	 *            the input CSV file.
	 * @param output
	 *            the string to display. If the text is null, the tool tip is turned
	 *            off for this component.
	 */
	public static void fromT_LA2L_AT(File f, String output) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(f.getPath()));
			FileWriter writer = new FileWriter(output);

			ArrayList<String> attributes = new ArrayList<String>();
			ArrayList<String> times = new ArrayList<String>();

			String line = br.readLine();
			String[] split = line.split(",");

			int numLocus = split.length - 1;

			int numAttributes = 1;

			for (int i = 2; i < split.length; i++) {
				if (split[i].equals(split[1])) {
					numAttributes++;
				} else {
					break;
				}
			}

			numLocus = numLocus / numAttributes;

			line = br.readLine();
			split = line.split(",");

			for (int i = 0; i < numAttributes; i++) {
				attributes.add(split[i + 1]);
			}

			while ((line = br.readLine()) != null) {
				split = line.split(",");
				if (!times.contains(split[0])) {
					times.add(split[0]);
				}
			}

			br.close();

			writer.append("L_AT");
			for (int i = 0; i < attributes.size(); i++) {
				for (int j = 0; j < times.size(); j++) {
					writer.append("," + attributes.get(i));
				}
			}

			writer.append("\n");

			for (int i = 0; i < attributes.size(); i++) {
				for (int j = 0; j < times.size(); j++) {
					writer.append("," + times.get(j));
				}
			}

			writer.append("\n");

			int finIdx = 1;
			for (int i = 0; i < numLocus; i++) {
				String[] newLine = new String[(attributes.size() * times.size()) + 1];
				br = new BufferedReader(new FileReader(f.getPath()));
				line = br.readLine();
				split = line.split(",");
				newLine[0] = split[finIdx];
				br.readLine();

				int counter = 1;
				while ((line = br.readLine()) != null) {
					split = line.split(",");
					for (int j = 0; j < numAttributes; j++) {
						newLine[counter + (j * times.size())] = split[finIdx + j];
					}
					counter++;
				}

				writer.append(newLine[0]);
				for (int j = 1; j < newLine.length; j++) {
					writer.append("," + newLine[j]);
				}
				writer.append("\n");
				finIdx += numAttributes;
				br.close();
			}

			writer.flush();
			writer.close();
			// br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Converts a double-header input T_LA CSV file to a L_AT CSV file.
	 *
	 * @param f
	 *            the input CSV file.
	 * @param output
	 *            the string to display. If the text is null, the tool tip is turned
	 *            off for this component.
	 * @deprecated
	 */
	public static void fromT_LA2L_AT_OLD(File f, String output) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(f.getPath()));
			FileWriter writer = new FileWriter(output);

			ArrayList<String> attributes = new ArrayList<String>();
			ArrayList<String> times = new ArrayList<String>();

			String line = br.readLine();
			String[] split = line.split(",");

			int numLocus = split.length - 1;

			int numAttributes = 1;

			for (int i = 2; i < split.length; i++) {
				if (split[i].equals(split[1])) {
					numAttributes++;
				} else {
					break;
				}
			}

			numLocus = numLocus / numAttributes;

			line = br.readLine();
			split = line.split(",");

			for (int i = 0; i < numAttributes; i++) {
				attributes.add(split[i + 1]);
			}

			while ((line = br.readLine()) != null) {
				split = line.split(",");
				if (!times.contains(split[0])) {
					times.add(split[0]);
				}
			}

			br.close();

			writer.append("L_AT");
			for (int i = 0; i < attributes.size(); i++) {
				for (int j = 0; j < times.size(); j++) {
					writer.append("," + attributes.get(i));
				}
			}

			writer.append("\n");

			for (int i = 0; i < attributes.size(); i++) {
				for (int j = 0; j < times.size(); j++) {
					writer.append("," + times.get(j));
				}
			}

			writer.append("\n");

			int finIdx = 1;
			for (int i = 0; i < numLocus; i++) {
				br = new BufferedReader(new FileReader(f.getPath()));
				line = br.readLine();
				split = line.split(",");
				writer.append(split[finIdx]);
				br.close();

				for (int j = 0; j < numAttributes; j++) {
					br = new BufferedReader(new FileReader(f.getPath()));
					line = br.readLine();
					line = br.readLine();

					while ((line = br.readLine()) != null) {
						split = line.split(",");
						writer.append("," + split[finIdx]);
						// finIdx++;
					}
					br.close();
					finIdx++;
				}
				writer.append("\n");
				// br = new BufferedReader(new FileReader(f.getPath()));
				// line = br.readLine();
				// line = br.readLine();

				// split = line.split(",");
				// writer.append(split[finIdx]);
				//
				// while ((line = br.readLine()) != null) {
				// split = line.split(",");
				// writer.append("," + split[finIdx]);
				// }
				// writer.append("\n");
				// finIdx++;
			}

			writer.flush();
			writer.close();
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Converts a double-header input AT_L CSV file to a L_AT CSV file.
	 *
	 * @param f
	 *            the input CSV file.
	 * @param output
	 *            the string to display. If the text is null, the tool tip is turned
	 *            off for this component.
	 */
	public static void fromAT_L2L_AT(File f, String output) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(f.getPath()));
			FileWriter writer = new FileWriter(output);

			ArrayList<String> attributes = new ArrayList<String>();
			ArrayList<String> times = new ArrayList<String>();

			String line = br.readLine();
			String[] split = line.split(",");

			int numLocus = split.length - 2;

			while ((line = br.readLine()) != null) {
				split = line.split(",");
				if (!attributes.contains(split[0])) {
					attributes.add(split[0]);
				}
				if (!times.contains(split[1])) {
					times.add(split[1]);
				}
			}

			writer.append("L_AT");
			for (int i = 0; i < attributes.size(); i++) {
				for (int j = 0; j < times.size(); j++) {
					writer.append("," + attributes.get(i));
				}
			}

			writer.append("\n");

			for (int i = 0; i < attributes.size(); i++) {
				for (int j = 0; j < times.size(); j++) {
					writer.append("," + times.get(j));
				}
			}

			writer.append("\n");

			int finIdx = 2;
			for (int i = 0; i < numLocus; i++) {
				br = new BufferedReader(new FileReader(f.getPath()));
				line = br.readLine();

				split = line.split(",");
				writer.append(split[finIdx]);

				while ((line = br.readLine()) != null) {
					split = line.split(",");
					writer.append("," + split[finIdx]);
				}
				writer.append("\n");
				finIdx++;
			}

			writer.flush();
			writer.close();
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Converts a double-header input L_AT CSV file to a AT_L CSV file.
	 *
	 * @param f
	 *            the input CSV file.
	 * @param output
	 *            the string to display. If the text is null, the tool tip is turned
	 *            off for this component.
	 */
	public static void fromL_AT2AT_L(File f, String output) {
		try {
			// initialize reader and writer
			BufferedReader br = new BufferedReader(new FileReader(f.getPath()));
			FileWriter writer = new FileWriter(output);

			// list to hold attributes
			ArrayList<String> attributes = new ArrayList<String>();
			// list to hold times
			ArrayList<String> times = new ArrayList<String>();

			// read first line (attributes)
			String line = br.readLine();
			// split first line
			String[] split = line.split(",");

			// add the first attribute
			// attributes.add(split[1]);

			// add the rest of the attributes
			for (int i = 1; i < split.length; i++) {
				if (!attributes.contains(split[i])) {
					attributes.add(split[i]);
				}
			}

			// read the second line (time)
			line = br.readLine();
			// split the second line
			split = line.split(",");

			// times.add(split[1]);

			for (int i = 1; i < split.length; i++) {
				if (!times.contains(split[i])) {
					times.add(split[i]);
				}
			}

			writer.append("AT_L,");
			int locusCount = 0;
			// iterate thru input file
			while ((line = br.readLine()) != null) {
				locusCount++;
				split = line.split(",");
				writer.append("," + split[0]);
			}
			writer.append("\n");

			int finIdx = 1;
			for (int i = 0; i < attributes.size(); i++) {
				for (int j = 0; j < times.size(); j++) {
					writer.append(attributes.get(i) + "," + times.get(j));
					br = new BufferedReader(new FileReader(f.getPath()));
					line = br.readLine();
					line = br.readLine();

					while ((line = br.readLine()) != null) {
						split = line.split(",");
						// int finIdx = ()
						writer.append("," + split[finIdx]);
					}

					writer.append("\n");
					finIdx++;
				}
			}

			writer.flush();
			writer.close();
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Converts a double-header input A_LT CSV file to a L_AT CSV file.
	 *
	 * @param f
	 *            the input CSV file.
	 * @param output
	 *            the string to display. If the text is null, the tool tip is turned
	 *            off for this component.
	 */
	public static void fromA_LT2L_AT(File f, String output) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(f.getPath()));
			FileWriter writer = new FileWriter(output);

			ArrayList<String> attributes = new ArrayList<String>();
			ArrayList<String> times = new ArrayList<String>();

			String line = br.readLine();
			String[] split = line.split(",");

			int numLocus = split.length - 1;

			line = br.readLine();
			split = line.split(",");

			times.add(split[1]);

			for (int i = 2; i < split.length; i++) {
				if (!times.contains(split[i])) {
					times.add(split[i]);
				} else {
					break;
				}
			}

			while ((line = br.readLine()) != null) {
				split = line.split(",");
				if (!attributes.contains(split[0])) {
					attributes.add(split[0]);
				}
			}

			numLocus = numLocus / times.size();

			writer.append("L_AT");
			for (int i = 0; i < attributes.size(); i++) {
				for (int j = 0; j < times.size(); j++) {
					writer.append("," + attributes.get(i));
				}
			}

			writer.append("\n");

			for (int i = 0; i < attributes.size(); i++) {
				for (int j = 0; j < times.size(); j++) {
					writer.append("," + times.get(j));
				}
			}

			writer.append("\n");

			int finIdx = 1;
			for (int i = 0; i < numLocus; i++) {
				br = new BufferedReader(new FileReader(f.getPath()));
				line = br.readLine();
				split = line.split(",");
				writer.append(split[finIdx]);
				line = br.readLine();

				while ((line = br.readLine()) != null) {
					split = line.split(",");
					for (int j = 0; j < times.size(); j++) {
						writer.append("," + split[finIdx + j]);
					}
				}

				finIdx += times.size();
				writer.append("\n");
			}

			writer.flush();
			writer.close();
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Converts a double-header input L_AT CSV file to a A_LT CSV file.
	 *
	 * @param f
	 *            the input CSV file.
	 * @param output
	 *            the string to display. If the text is null, the tool tip is turned
	 *            off for this component.
	 */
	public static void fromL_AT2A_LT(File f, String output) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(f.getPath()));
			FileWriter writer = new FileWriter(output);

			ArrayList<String> attributes = new ArrayList<String>();
			ArrayList<String> times = new ArrayList<String>();

			String line = br.readLine();
			String[] split = line.split(",");

			attributes.add(split[1]);

			for (int i = 2; i < split.length; i++) {
				if (!attributes.contains(split[i])) {
					attributes.add(split[i]);
				}
			}

			line = br.readLine();
			split = line.split(",");

			times.add(split[1]);

			for (int i = 2; i < split.length; i++) {
				if (!times.contains(split[i])) {
					times.add(split[i]);
				}
			}

			writer.append("A_LT");
			int locusCount = 0;
			// iterate thru input file
			while ((line = br.readLine()) != null) {
				locusCount++;
				split = line.split(",");
				for (int i = 0; i < times.size(); i++) {
					writer.append("," + split[0]);
				}
				// split = line.split(",");
			}
			writer.append("\n");
			// writer.append)

			for (int i = 0; i < locusCount; i++) {
				for (int j = 0; j < times.size(); j++) {
					writer.append("," + times.get(j));
				}
			}

			writer.append("\n");

			for (int i = 0; i < attributes.size(); i++) {
				writer.append(attributes.get(i));

				br = new BufferedReader(new FileReader(f.getPath()));
				line = br.readLine();
				line = br.readLine();

				while ((line = br.readLine()) != null) {

					split = line.split(",");

					for (int j = 0; j < times.size(); j++) {
						int finIdx = (j + 1) + (i * times.size());
						writer.append("," + split[finIdx]);
					}
				}
				writer.append("\n");

			}

			writer.flush();
			writer.close();
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Converts a double-header input L_AT CSV file to a T_LA CSV file.
	 *
	 * @param f
	 *            the input CSV file.
	 * @param output
	 *            the string to display. If the text is null, the tool tip is turned
	 *            off for this component.
	 */
	public static void fromL_AT2T_LA(File f, String output) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(f.getPath()));
			FileWriter writer = new FileWriter(output);

			ArrayList<String> attributes = new ArrayList<String>();
			ArrayList<String> times = new ArrayList<String>();

			String line = br.readLine();
			String[] split = line.split(",");

			attributes.add(split[1]);

			for (int i = 2; i < split.length; i++) {
				if (!attributes.contains(split[i])) {
					attributes.add(split[i]);
				}
			}

			line = br.readLine();
			split = line.split(",");

			times.add(split[1]);

			for (int i = 2; i < split.length; i++) {
				if (!times.contains(split[i])) {
					times.add(split[i]);
				}
			}

			writer.append("T_LA");
			int locusCount = 0;
			// iterate thru input file
			while ((line = br.readLine()) != null) {
				locusCount++;
				split = line.split(",");
				for (int i = 0; i < attributes.size(); i++) {
					writer.append("," + split[0]);
				}
			}
			writer.append("\n");

			for (int i = 0; i < locusCount; i++) {
				for (int j = 0; j < attributes.size(); j++) {
					writer.append("," + attributes.get(j));
				}
			}

			writer.append("\n");

			for (int i = 0; i < times.size(); i++) {
				writer.append(times.get(i));

				br = new BufferedReader(new FileReader(f.getPath()));
				line = br.readLine();
				line = br.readLine();

				while ((line = br.readLine()) != null) {

					split = line.split(",");

					for (int j = 0; j < attributes.size(); j++) {
						int finIdx = (i + 1) + (j * times.size());
						writer.append("," + split[finIdx]);
					}
				}
				writer.append("\n");

			}

			writer.flush();
			writer.close();
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Converts a double-header input L_AT CSV file to a LT_A CSV file.
	 *
	 * @param f
	 *            the input CSV file.
	 * @param output
	 *            the string to display. If the text is null, the tool tip is turned
	 *            off for this component.
	 */
	public static void fromL_AT2LT_A(File f, String output) {
		try {
			// String list to hold attributes
			ArrayList<String> attributes = new ArrayList<String>();

			// String list to hold times
			ArrayList<String> times = new ArrayList<String>();

			// initialize BufferedReader to parse file
			BufferedReader br = new BufferedReader(new FileReader(f.getPath()));

			// initialize FileWriter to write output
			FileWriter writer = new FileWriter(output);

			// initialize variable to store line data
			// store first line
			String line = br.readLine();

			// initialize String array to split line data
			String[] split = line.split(",");

			// add the first attribute
			attributes.add(split[1]);

			// loop thru split data (attributes)
			for (int i = 2; i < split.length; i++) {

				// if attributes doesn't contain split element
				if (!attributes.contains(split[i])) {

					// add split element to attribute list
					attributes.add(split[i]);
				}
			}

			// store second line
			line = br.readLine();

			// split the line data
			split = line.split(",");

			// add the first time
			times.add(split[1]);

			// loop thru split data (times)
			for (int i = 2; i < split.length; i++) {

				// if times doesn't contain split element
				if (!times.contains(split[i])) {

					// add split element to time list
					times.add(split[i]);
				}
			}

			System.out.println(times.size());
			System.out.println(attributes.size());

			// append perspective
			writer.append("LT_A,");

			// loop thru attribute list
			for (int i = 0; i < attributes.size(); i++) {

				// append attributes
				writer.append("," + attributes.get(i));
			}

			// new line
			writer.append("\n");

			// loop thru each line in input file
			while ((line = br.readLine()) != null) {

				// split current line
				split = line.split(",");

				// int dataIdx = 1;
				// loop thru times
				for (int i = 0; i < times.size(); i++) {

					// append locus/time
					writer.append(split[0] + "," + times.get(i));

					// loop thru attributes
					for (int j = 0; j < attributes.size(); j++) {

						// calculate appropriate index
						int finIdx = (i + 1) + (j * times.size());

						// append data to LT_A object
						// System.out.println(split[0] + " - " + split[finIdx] + ": " + finIdx);
						// System.out.println(split[0]);
						// System.out.println(finIdx);
						writer.append("," + split[finIdx]);
					}

					// new line
					writer.append("\n");
				}
			}

			// flush the writer
			writer.flush();

			// close the writer
			writer.close();

			// close the BR
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Converts a double-header input L_AT CSV file to a LA_T CSV file.
	 *
	 * @param f
	 *            the input CSV file.
	 * @param output
	 *            the string to display. If the text is null, the tool tip is turned
	 *            off for this component.
	 */
	public static void fromL_AT2LA_T(File f, String output) {
		try {

			// String list to hold attributes
			ArrayList<String> attributes = new ArrayList<String>();

			// String list to hold times
			ArrayList<String> times = new ArrayList<String>();

			// initialize BufferedReader to parse file
			BufferedReader br = new BufferedReader(new FileReader(f.getPath()));

			// initialize FileWriter to write output
			FileWriter writer = new FileWriter(output);

			// initialize variable to store line data
			// store first line
			String line = br.readLine();

			// initialize String array to split line data
			String[] split = line.split(",");

			// add the first attribute
			attributes.add(split[1]);

			// loop thru split data (attributes)
			for (int i = 2; i < split.length; i++) {

				// if attributes doesn't contain split element
				if (!attributes.contains(split[i])) {

					// add split element to attribute list
					attributes.add(split[i]);
				}
			}

			// store second line
			line = br.readLine();

			// split the line data
			split = line.split(",");

			// add the first time
			times.add(split[1]);

			// loop thru split data (times)
			for (int i = 2; i < split.length; i++) {

				// if times doesn't contain split element
				if (!times.contains(split[i])) {

					// add split element to time list
					times.add(split[i]);
				}
			}

			// append perspective
			writer.append("LA_T,");

			// loop thru time list
			for (int i = 0; i < times.size(); i++) {

				// append times
				writer.append("," + times.get(i));
			}

			// next line
			writer.append("\n");

			// loop thru each line in input file
			while ((line = br.readLine()) != null) {

				// split current line
				split = line.split(",");

				// loop thru attributes
				for (int i = 0; i < attributes.size(); i++) {

					// append locus/attribute
					writer.append(split[0] + "," + attributes.get(i));

					// loop thru times
					for (int j = 0; j < times.size(); j++) {

						// calculate appropriate index
						int finIdx = (j + 1) + (i * times.size());

						// append data to LA_T object
						writer.append("," + split[finIdx]);
					}
					// attrChange += times.size();
					// new line
					writer.append("\n");
				}
			}

			// flush the writer
			writer.flush();

			// close the writer
			writer.close();

			// close the BR
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Converts the folder contents (e.g. each file represents a single T) of
	 * double-header input CSV files to a L_AT CSV file. File is saved to the input
	 * directory.
	 *
	 * @param format
	 *            the data format: {"L_A_T"}
	 * @param f
	 *            the input directory.
	 * @param primaryKey
	 *            integer array that holds the primary key(s).
	 */
	public static void convertToL_AT(String format, File f, int[] primaryKey) {
		// ensure f is a Directory or
		if (f.isDirectory()) {

			// if format is L_A_T
			if (format.equalsIgnoreCase("L_A_T")) {

				// perform export to L_AT function
				exportL_A_T2L_AT(f, primaryKey, "");
			}

		} else {
			System.out.println("ERROR: f is not a directory!");
		}
	}

	/**
	 * Converts the folder contents (e.g. each file represents a single T) of
	 * double-header input CSV files to a L_AT CSV file.
	 *
	 * @param format
	 *            the data format: {"L_A_T"}
	 * @param input
	 *            the input directory.
	 * @param primaryKey
	 *            integer array that holds the primary key(s).
	 * @param output
	 *            the output file path.
	 */
	public static boolean convertToL_AT(String format, File input, int[] primaryKey, String output) {
		// ensure f is a Directory or
		if (input.isDirectory()) {
			if (format.equalsIgnoreCase("L_A_T")) {
				return exportL_A_T2L_AT(input, primaryKey, output);
			}

			return false;
		} else {
			System.out.println("ERROR: f is not a directory!");
			return false;
		}
	}

	public static boolean processFile(File f, String perspective, String output, boolean dictionaryHeaders) {
		// PERSPECTIVES = {"L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L"};
		if (perspective.equalsIgnoreCase(PERSPECTIVES[4])) {
			ArrayList<String> loci = new ArrayList<String>();
			ArrayList<String> times = new ArrayList<String>();
			ArrayList<String> attributes = new ArrayList<String>();

			try {
				BufferedReader br = new BufferedReader(new FileReader(f.getPath()));

				String line = br.readLine();

				String[] split = line.split(",");

				for (int i = 2; i < split.length; i++) {
					attributes.add(split[i]);
				}

				while ((line = br.readLine()) != null) {
					split = line.split(",");
					if (!loci.contains(split[0])) {
						loci.add(split[0]);
					}

					if (!times.contains(split[1])) {
						times.add(split[1]);
					}
				}

				br.close();

				String[] newOutput = output.split("/");
				String dictOutput = newOutput[0];
				for (int i = 1; i < newOutput.length - 1; i++) {
					dictOutput = dictOutput + "/" + newOutput[i];
				}
				dictOutput = dictOutput + "/dictionaries";

				Files.createDirectories(Paths.get(output));
				Files.createDirectories(Paths.get(dictOutput));

				// createDictionary(String type, String[] data, String output, boolean headers)
				// {
				createDictionary("l", loci.toArray((new String[loci.size()])), dictOutput, dictionaryHeaders);
				createDictionary("a", attributes.toArray((new String[attributes.size()])), dictOutput,
						dictionaryHeaders);
				createDictionary("t", times.toArray((new String[times.size()])), dictOutput, dictionaryHeaders);

				FileWriter writer = new FileWriter(output + "/LT_A.csv");

				br = new BufferedReader(new FileReader(f.getPath()));
				br.readLine();

				writer.append("LT_A,");
				for (int i = 0; i < attributes.size(); i++) {
					writer.append(",a" + (i + 1));
				}
				writer.append("\n");

				while ((line = br.readLine()) != null) {
					split = line.split(",");
					String[] newRow = new String[split.length];

					int locusIdx = loci.indexOf(split[0]);
					locusIdx++;
					newRow[0] = "l" + locusIdx;

					int timeIdx = times.indexOf(split[1]);
					timeIdx++;
					newRow[1] = "t" + timeIdx;

					for (int i = 2; i < split.length; i++) {
						newRow[i] = split[i];
					}

					writer.append(newRow[0]);

					for (int i = 1; i < newRow.length; i++) {
						writer.append("," + newRow[i]);
					}

					writer.append("\n");

				}

				br.close();
				writer.flush();
				writer.close();

				return true;

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}

		}
		return false;
	}

	// Method to convert a table of: 
	public static boolean convertLC2LT_A(File dir, File dictionary, String output) throws IOException {
		// get the csv files in the directory
		File[] files = dir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".csv");
			}
		});

		// return false if there are no files in directory
		if (files.length == 0) {
			return false;
		}

		String newOutput = "";

		if (output.equals("")) {
			newOutput = dir.getPath();
		} else {
			newOutput = output;
		}

		try {
			Files.createDirectories(Paths.get(newOutput));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}

		BufferedReader lociDictionary = new BufferedReader(new FileReader(dictionary.getPath()));
		ArrayList<String[]> lociDict = new ArrayList<String[]>();
		String line = "";

		while ((line = lociDictionary.readLine()) != null) {
			lociDict.add(line.split(","));
		}

		lociDictionary.close();

		FileWriter writer = new FileWriter(newOutput + "/LT_A_LC.csv");
		writer.append("LT_A,lc\n");

		BufferedReader[] bfs = new BufferedReader[files.length];
		String[] lines = new String[files.length];

		for (int i = 0; i < files.length; i++) {
			bfs[i] = new BufferedReader(new FileReader(files[i].getPath()));
			lines[i] = bfs[i].readLine();
		}

		// iterate thru input file
		while ((lines[0] = bfs[0].readLine()) != null) {
			for (int i = 1; i < bfs.length; i++) {
				lines[i] = bfs[i].readLine();
			}

			String[] tmpSplit = lines[0].split(",");
			String[][] lineData = new String[bfs.length][tmpSplit.length];

			for (int i = 0; i < lines.length; i++) {
				lineData[i] = lines[i].split(",");
			}

			String testX = lineData[0][0];
			String testY = lineData[0][1];
			for (int i = 1; i < lines.length; i++) {
				if (!testX.equals(lineData[i][0]) || !testY.equals(lineData[i][1])) {
					for (int j = 0; j < bfs.length; j++) {
						bfs[j].close();
					}
					writer.flush();
					writer.close();
					return false;
				}
			}

			String label = "";
			DecimalFormat df = new DecimalFormat("0.0000");

			try {
				String textX1 = df.format(Double.parseDouble(testX));
				double finalTX = (Double) df.parse(textX1);

				String textY1 = df.format(Double.parseDouble(testY));
				double finalTY = (Double) df.parse(textY1);

				for (int i = 0; i < lociDict.size(); i++) {
					String textX2 = df.format(Double.parseDouble(lociDict.get(i)[1]));
					double finalTX2 = (Double) df.parse(textX2);

					String textY2 = df.format(Double.parseDouble(lociDict.get(i)[2]));
					double finalTY2 = (Double) df.parse(textY2);
					if (finalTX2 - finalTX == 0 && finalTY2 - finalTY == 0) {
						label = lociDict.get(i)[0];
					}
					// if (lociDict.get(i)[1].equals(testX) && lociDict.get(i)[2].equals(testY)) {
					// label = lociDict.get(i)[0];
					// }
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// System.out.println(label);

			for (int i = 0; i < files.length; i++) {
				writer.append(label + "_t" + (i + 1));
				writer.append("," + lineData[i][2] + "\n");
			}

			// String dictionaryLine = lociDictionary.readLine();
			// String[] dictionarySplit = dictionaryLine.split(",");

		}

		for (int i = 0; i < bfs.length; i++) {
			bfs[i].close();
		}
		writer.flush();
		writer.close();
		return true;
	}

	private static boolean exportL_A_T2L_AT(File f, int[] primaryKey, String output) {

		// get the csv files in the directory
		File[] files = f.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".csv");
			}
		});

		// return false if there are no files in directory
		if (files.length == 0) {
			return false;
		}

		String newOutput = "";
		String dictOutput = "";

		if (output.equals("")) {
			newOutput = f.getPath();
		} else {
			newOutput = output;
		}

		try {
			Files.createDirectories(Paths.get(newOutput));
			File dictionaryDir = new File(newOutput + File.separator + ".." + File.separator + "dictionaries");

			dictOutput = dictionaryDir.getPath();
			Files.createDirectories(Paths.get(dictOutput));
			int numTimes = createDictionary("t", files, dictOutput, primaryKey);
			int numAttributes = createDictionary("a", files, dictOutput, primaryKey);
			int numLoci = createDictionary("l", files, dictOutput, primaryKey);

			BufferedReader[] bfs = new BufferedReader[files.length];
			String[] lines = new String[files.length];

			for (int i = 0; i < files.length; i++) {
				bfs[i] = new BufferedReader(new FileReader(files[i].getPath()));
				lines[i] = bfs[i].readLine();
			}

			String[] split2 = lines[0].split(",");

			FileWriter writer = new FileWriter(newOutput + "/L_AT.csv");

			int numCols = split2.length - primaryKey.length;

			int totalCols = numCols * files.length;

			writer.append("L_AT");

			for (int i = 0; i < totalCols; i++) {
				writer.append(",a" + ((i / numTimes) + 1));
			}

			writer.append("\n");

			for (int i = 0; i < totalCols; i++) {
				writer.append(",t" + ((i % numTimes) + 1));
			}

			writer.append("\n");

			int locusIdx = 1;

			// iterate thru input file
			while ((lines[0] = bfs[0].readLine()) != null) {
				for (int i = 1; i < bfs.length; i++) {
					lines[i] = bfs[i].readLine();
				}

				String[] tmpSplit = lines[0].split(",");

				// boolean samePixel = true;

				String[][] lineData = new String[bfs.length][tmpSplit.length];

				for (int i = 0; i < lines.length; i++) {
					lineData[i] = lines[i].split(",");
				}

				String[] keys = new String[primaryKey.length];

				for (int i = 0; i < primaryKey.length; i++) {
					keys[i] = lineData[0][primaryKey[i]];
				}

				for (int i = 0; i < primaryKey.length; i++) {
					for (int j = 1; j < files.length; j++) {
						if (!keys[i].equals(lineData[j][primaryKey[i]])) {
							writer.flush();
							writer.close();
							return false;
						}
					}
				}

				writer.append("l" + locusIdx);

				for (int i = 0; i < lineData[0].length; i++) {
					boolean isKey = false;
					for (int j = 0; j < primaryKey.length; j++) {
						if (i == primaryKey[j]) {
							isKey = true;
						}
					}

					if (!isKey) {
						for (int j = 0; j < lineData.length; j++) {
							writer.append("," + lineData[j][i]);
						}

					}
				}

				writer.append("\n");
				locusIdx++;

			}
			writer.flush();
			writer.close();

		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		return true;
	}

	public static void createDictionary(String type, String[] data, String output, boolean headers) {
		String alias = "";
		String outCSV = "";
		if (type.equalsIgnoreCase("t")) {
			alias = "t";
			outCSV = "timeDictionary";
		} else if (type.equalsIgnoreCase("l")) {
			alias = "l";
			outCSV = "locusDictionary";
		} else if (type.equalsIgnoreCase("a")) {
			alias = "a";
			outCSV = "attributeDictionary";
		}

		try {
			FileWriter writer = new FileWriter(output + "/" + outCSV + ".csv");

			if (headers) {
				writer.append("id,alias");
				writer.append("\n");
			}

			for (int i = 0; i < data.length; i++) {
				writer.append(alias + (i + 1 + "," + data[i]));
				writer.append("\n");
			}

			writer.flush();
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static int createDictionary(String type, File[] files, String output, int[] primaryKey) {
		int numItems = -1;
		if (type.equalsIgnoreCase("t")) {
			try {
				numItems = 0;
				FileWriter writer = new FileWriter(output + "/timeDictionary.csv");
				for (int i = 0; i < files.length; i++) {
					String[] nameSplit = files[i].getName().split("_");
					writer.append("t" + (i + 1) + "," + nameSplit[0] + "\n");
					numItems++;
				}
				writer.flush();
				writer.close();
				return numItems;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return -1;
			}
		} else if (type.equalsIgnoreCase("a")) {
			try {
				numItems = 0;
				FileWriter writer = new FileWriter(output + "/attrDictionary.csv");
				BufferedReader br = new BufferedReader(new FileReader(files[0].getPath()));
				int attribute = 1;
				String line = br.readLine();
				String[] data = line.split(",");
				for (int i = 0; i < data.length; i++) {
					boolean isKey = false;
					for (int j = 0; j < primaryKey.length; j++) {
						if (i == primaryKey[j]) {
							isKey = true;
						}
					}

					if (!isKey) {
						writer.append("a" + attribute + "," + data[i] + "\n");
						attribute++;
					}

				}
				writer.flush();
				writer.close();
				br.close();
				return attribute - 1;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return -1;
			}
		} else if (type.equalsIgnoreCase("l")) {
			try {
				FileWriter writer = new FileWriter(output + "/lociDictionary.csv");
				BufferedReader br = new BufferedReader(new FileReader(files[0].getPath()));
				int locus = 1;
				String line = br.readLine();

				// iterate thru input file
				while ((line = br.readLine()) != null) {
					String[] data = line.split(",");
					writer.append("l" + locus);
					for (int i = 0; i < primaryKey.length; i++) {
						writer.append("," + data[primaryKey[i]]);
					}
					writer.append("\n");
					locus++;
				}
				writer.flush();
				writer.close();
				br.close();
				return locus - 1;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return -1;
			}
		} else {
			return -1;
		}
	}

	public static double[][] retrieve2DArray(String perspective, File f) throws IOException {
		ArrayList<ArrayList<String>> data = new ArrayList<ArrayList<String>>();
		BufferedReader br = new BufferedReader(new FileReader(f.getPath()));

		String line = br.readLine();
		int startIdx = 2;
		if (perspective.equalsIgnoreCase("L_AT") || perspective.equalsIgnoreCase("T_LA")
				|| perspective.equalsIgnoreCase("A_LT")) {
			br.readLine();
			startIdx = 1;
		}

		while ((line = br.readLine()) != null) {
			String[] lineSplit = line.split(",");
			ArrayList<String> lineData = new ArrayList<String>();

			for (int i = startIdx; i < lineSplit.length; i++) {
				lineData.add(lineSplit[i]);
			}

			data.add(lineData);
		}

		double[][] returnData = new double[data.size()][data.get(0).size()];

		for (int i = 0; i < returnData.length; i++) {
			for (int j = 0; j < returnData[i].length; j++) {
				returnData[i][j] = Double.parseDouble(data.get(i).get(j));
			}
		}

		return returnData;
	}

	public static void collapseHeader(File f, String output) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(f.getPath()));

		String line = br.readLine();
		String[] lineSplit = line.split(",");
		line = br.readLine();
		String[] lineSplit2 = line.split(",");
		// String[] newLine = new String[lineSplit.length];
		// newLine[0] = lineSplit[0];
		for (int i = 1; i < lineSplit.length; i++) {
			lineSplit[i] = lineSplit[i] + "_" + lineSplit2[i];
		}

		FileWriter writer = new FileWriter(output);

		for (int i = 0; i < lineSplit.length - 1; i++) {
			writer.append(lineSplit[i] + ",");
		}
		writer.append(lineSplit[lineSplit.length - 1] + "\n");

		while ((line = br.readLine()) != null) {
			writer.append(line + "\n");
		}

		writer.flush();
		writer.close();
		br.close();
	}

	public static void extractBMU(File datFilePath, File codFilePath, String iv2BMUOut, String neuron2IVOut) {
		ArrayList<String> output = new ArrayList<String>();
	}

	public static String[] getNeuronKMeans(String codFilePath, String type, int kVal,
			int distMeasure, int kIterations) {
		ArrayList<String> output = new ArrayList<String>();

		Cod cod = FileHandler.readCodFile(codFilePath, type);

		// ArrayList<KMeans> kMeansTotal = new ArrayList<KMeans>();
		// for (int j = 2; j <= kMax; j++) {
		KMeans kMeansClustering = null;
		boolean kMeansComplete = false;
		while (!kMeansComplete) {
			try {
				kMeansClustering = new KMeans(kVal, cod.neurons, distMeasure, kIterations);
				kMeansComplete = true;
			} catch (IndexOutOfBoundsException e) {
				System.out.println("Invalid option");
				kMeansComplete = false;
			}
		}
		// kMeansTotal.add(kMeansClustering);
		// }

		// ArrayList<ArrayList<ArrayList<Integer>>> allVals = new
		// ArrayList<ArrayList<ArrayList<Integer>>>();
		// for (int j = 0; j < kMeansTotal.size(); j++) {
		ArrayList<ArrayList<Integer>> totalVals = new ArrayList<ArrayList<Integer>>();
		for (int i = 0; i < kMeansClustering.kMeansClusters.length; i++) {
			totalVals.add(kMeansClustering.kMeansClusters[i].memberIDs);
		}
		// allVals.add(totalVals);

		// }
		// System.out.println("!!!!!!!!!!!!!!");
		int dataSize = 0;
		for (int i = 0; i < totalVals.size(); i++) {
			dataSize += totalVals.get(i).size();
		}

		String[] kClasses = new String[dataSize + 1];

		for (int i = 0; i < totalVals.size(); i++) {
			for (int j = 0; j < totalVals.get(i).size(); j++) {
				kClasses[totalVals.get(i).get(j) - 1] = (i + 1) + "";
			}
		}

		kClasses[dataSize] = kMeansClustering.calcSSE(kMeansClustering.distMatrix, kMeansClustering.clusterMatrix) + "";

		return kClasses;
		// return output.toArray(new String[output.size()]);
	}
	
	public static String[] getNeuronKMeans2(edu.sdsu.datavis.trispace.tsprep.som.Neuron[] neurons, String type, int kVal,
			int distMeasure, int kIterations) {
		ArrayList<String> output = new ArrayList<String>();

		// ArrayList<KMeans> kMeansTotal = new ArrayList<KMeans>();
		// for (int j = 2; j <= kMax; j++) {
		KMeans kMeansClustering = null;
		boolean kMeansComplete = false;
		while (!kMeansComplete) {
			try {
				kMeansClustering = new KMeans(kVal, neurons, distMeasure, kIterations);
				kMeansComplete = true;
			} catch (IndexOutOfBoundsException e) {
				System.out.println("Invalid option");
				kMeansComplete = false;
			}
		}
		// kMeansTotal.add(kMeansClustering);
		// }

		// ArrayList<ArrayList<ArrayList<Integer>>> allVals = new
		// ArrayList<ArrayList<ArrayList<Integer>>>();
		// for (int j = 0; j < kMeansTotal.size(); j++) {
		ArrayList<ArrayList<Integer>> totalVals = new ArrayList<ArrayList<Integer>>();
		for (int i = 0; i < kMeansClustering.kMeansClusters.length; i++) {
			totalVals.add(kMeansClustering.kMeansClusters[i].memberIDs);
		}
		// allVals.add(totalVals);

		// }
		// System.out.println("!!!!!!!!!!!!!!");
		int dataSize = 0;
		for (int i = 0; i < totalVals.size(); i++) {
			dataSize += totalVals.get(i).size();
		}

		String[] kClasses = new String[dataSize + 1];

		for (int i = 0; i < totalVals.size(); i++) {
			for (int j = 0; j < totalVals.get(i).size(); j++) {
				kClasses[totalVals.get(i).get(j) - 1] = (i + 1) + "";
			}
		}

		kClasses[dataSize] = kMeansClustering.calcSSE(kMeansClustering.distMatrix, kMeansClustering.clusterMatrix) + "";

		return kClasses;
		// return output.toArray(new String[output.size()]);
	}

	public static void performKMeans(String datFilePath, String codFilePath, String type, int kMax, int distMeasure,
			int normIdx, String output, int kIterations) throws IOException {

		// files to store output
		FileWriter kmeansOutput = null;
		FileWriter sseOutput = null;

		String[] columns = new String[kMax];
		columns[0] = "neuron";

		ArrayList<int[]> kmeansData = new ArrayList<int[]>();
		ArrayList<Float> sseData = new ArrayList<Float>();

		kmeansOutput = new FileWriter(output + ".csv");
		sseOutput = new FileWriter(output + "_sse.csv");

		// iterate thru from k=2 to k=kMax
		for (int j = 2; j <= kMax; j++) {
			String[] tmpKMeans = getNeuronKMeans(codFilePath, type, j, distMeasure, kIterations);

			String column = "k" + j + "_n" + (normIdx + 1);
			columns[j - 1] = column;
			
			// on the first run initialize the dictionary keys
			if (j == 2) {
				
				
				for (int k = 0; k < tmpKMeans.length - 1; k++) {

					String neuronId = (k + 1) + "";
					

					int[] tmpArray = new int[kMax - 1];

					tmpArray[j - 2] = Integer.parseInt(tmpKMeans[k]);

					kmeansData.add(tmpArray);

					

				}
			} else {
//				String column = "k" + j + "_n" + (normIdx + 1);
//				columns[j - 1] = column;
				
				for (int k = 0; k < tmpKMeans.length - 1; k++) {

					String neuronId = (k + 1) + "";
					

					kmeansData.get(k)[j - 2] = Integer.parseInt(tmpKMeans[k]);

					

				}
			}

			sseData.add(Float.parseFloat(tmpKMeans[tmpKMeans.length - 1]));

		}

		kmeansOutput.write(columns[0]);
		for (int i = 1; i < columns.length; i++) {
			kmeansOutput.write("," + columns[i]);
		}
		kmeansOutput.write("\n");

		for (int i = 0; i < kmeansData.size(); i++) {
			String neuronId = (i + 1) + "";
			kmeansOutput.write(neuronId);

			for (int j = 0; j < kmeansData.get(i).length; j++) {
				kmeansOutput.write("," + kmeansData.get(i)[j]);
			}
			kmeansOutput.write("\n");
		}

		kmeansOutput.flush();
		kmeansOutput.close();

		sseOutput.write("k,sse\n");

		for (int i = 0; i < sseData.size(); i++) {
			String sseAttribute = "k" + (i + 1) + "_n" + (normIdx + 1);
			sseOutput.write(sseAttribute + "," + sseData.get(i) + "\n");
		}

		sseOutput.flush();
		sseOutput.close();
	}
	
	public static void performKMeans(String datFilePath, String codFilePath, String type, int kMin, int kMax, int distMeasure,
			int normIdx, String output, int kIterations) throws IOException {

		// files to store output
		FileWriter kmeansOutput = null;
		FileWriter sseOutput = null;
		
		int totalCols = kMax - kMin;
		totalCols += 2;

		String[] columns = new String[totalCols];
		
		int colCounter = 0;
		columns[colCounter++] = "neuron";

		ArrayList<int[]> kmeansData = new ArrayList<int[]>();
		ArrayList<Float> sseData = new ArrayList<Float>();

		kmeansOutput = new FileWriter(output + ".csv");
		sseOutput = new FileWriter(output + "_sse.csv");

		// iterate thru from k=2 to k=kMax
		for (int j = kMin; j <= kMax; j++) {
			String[] tmpKMeans = getNeuronKMeans(codFilePath, type, j, distMeasure, kIterations);

			String column = "k" + j + "_n" + (normIdx + 1);
			columns[colCounter++] = column;
			
			// on the first run initialize the dictionary keys
			if (j == kMin) {
				
				
				
				for (int k = 0; k < tmpKMeans.length - 1; k++) {

					String neuronId = (k + 1) + "";
					

					int[] tmpArray = new int[kMax - kMin + 1];

					tmpArray[j - kMin] = Integer.parseInt(tmpKMeans[k]);

					kmeansData.add(tmpArray);

					

				}
			} else {
				for (int k = 0; k < tmpKMeans.length - 1; k++) {

					String neuronId = (k + 1) + "";
//					String column = "k" + j + "_n" + (normIdx + 1);

					kmeansData.get(k)[j - kMin] = Integer.parseInt(tmpKMeans[k]);

//					columns[colCounter++] = column;

				}
			}

			sseData.add(Float.parseFloat(tmpKMeans[tmpKMeans.length - 1]));

		}

		kmeansOutput.write(columns[0]);
		for (int i = 1; i < columns.length; i++) {
			kmeansOutput.write("," + columns[i]);
		}
		kmeansOutput.write("\n");

		for (int i = 0; i < kmeansData.size(); i++) {
			String neuronId = (i + 1) + "";
			kmeansOutput.write(neuronId);

			for (int j = 0; j < kmeansData.get(i).length; j++) {
				kmeansOutput.write("," + kmeansData.get(i)[j]);
			}
			kmeansOutput.write("\n");
		}

		kmeansOutput.flush();
		kmeansOutput.close();

		sseOutput.write("k,sse\n");

		for (int i = 0; i < sseData.size(); i++) {
			String sseAttribute = "k" + (i + kMin) + "_n" + (normIdx + 1);
			sseOutput.write(sseAttribute + "," + sseData.get(i) + "\n");
		}

		sseOutput.flush();
		sseOutput.close();
	}
	
	public static void performKMeansNew(String perspective, String codFilePath, String output, 
										int kMin, int kMax, int kIterations, int distMeasure) throws IOException {

		// files to store output
		FileWriter kmeansOutput = null;
		FileWriter sseOutput = null;
		
		int totalCols = kMax - kMin;
		totalCols += 2;

		String[] columns = new String[totalCols];
		
		int colCounter = 0;
		columns[colCounter++] = "neuron";

		ArrayList<int[]> kmeansData = new ArrayList<int[]>();
		ArrayList<Float> sseData = new ArrayList<Float>();

		kmeansOutput = new FileWriter(output + "_kmeans.csv");
		sseOutput = new FileWriter(output + "_sse.csv");

		// iterate thru from k=2 to k=kMax
		for (int j = kMin; j <= kMax; j++) {
			String[] tmpKMeans = getNeuronKMeans(codFilePath, perspective, j, distMeasure, kIterations);

			String column = "k" + j;
			columns[colCounter++] = column;
			
			// on the first run initialize the dictionary keys
			if (j == kMin) {
				
				
				
				for (int k = 0; k < tmpKMeans.length - 1; k++) {

					String neuronId = (k + 1) + "";
					

					int[] tmpArray = new int[kMax - kMin + 1];

					tmpArray[j - kMin] = Integer.parseInt(tmpKMeans[k]);

					kmeansData.add(tmpArray);

					

				}
			} else {
				for (int k = 0; k < tmpKMeans.length - 1; k++) {

					String neuronId = (k + 1) + "";
//					String column = "k" + j + "_n" + (normIdx + 1);

					kmeansData.get(k)[j - kMin] = Integer.parseInt(tmpKMeans[k]);

//					columns[colCounter++] = column;

				}
			}

			sseData.add(Float.parseFloat(tmpKMeans[tmpKMeans.length - 1]));

		}

		kmeansOutput.write(columns[0]);
		for (int i = 1; i < columns.length; i++) {
			kmeansOutput.write("," + columns[i]);
		}
		kmeansOutput.write("\n");

		for (int i = 0; i < kmeansData.size(); i++) {
			String neuronId = (i + 1) + "";
			kmeansOutput.write(neuronId);

			for (int j = 0; j < kmeansData.get(i).length; j++) {
				kmeansOutput.write("," + kmeansData.get(i)[j]);
			}
			kmeansOutput.write("\n");
		}

		kmeansOutput.flush();
		kmeansOutput.close();

		sseOutput.write("k,sse\n");

		for (int i = 0; i < sseData.size(); i++) {
			String sseAttribute = "k" + (i + kMin);
			sseOutput.write(sseAttribute + "," + sseData.get(i) + "\n");
		}

		sseOutput.flush();
		sseOutput.close();
	}
	
	public static void performKMeansNew2(String perspective, edu.sdsu.datavis.trispace.tsprep.som.Neuron[] neurons, 
							String[] labels, String output, int kMin, int kMax, int kIterations, int distMeasure) throws IOException {

		// files to store output
		FileWriter kmeansOutput = null;
		FileWriter sseOutput = null;
		
		int totalCols = kMax - kMin;
		totalCols += 2;
		
		String[] columns = new String[totalCols];
		
		int colCounter = 0;
		columns[colCounter++] = "input_vector";
		
		ArrayList<int[]> kmeansData = new ArrayList<int[]>();
		ArrayList<Float> sseData = new ArrayList<Float>();
		
		kmeansOutput = new FileWriter(output + "_kmeans.csv");
		sseOutput = new FileWriter(output + "_sse.csv");
		
		// iterate thru from k=2 to k=kMax
		for (int j = kMin; j <= kMax; j++) {
			String[] tmpKMeans = getNeuronKMeans2(neurons, perspective, j, distMeasure, kIterations);
			
			String column = "k" + j;
			columns[colCounter++] = column;
			
			// on the first run initialize the dictionary keys
			if (j == kMin) {
				
				
				
				for (int k = 0; k < tmpKMeans.length - 1; k++) {
				
					String neuronId = (k + 1) + "";
					
					
					int[] tmpArray = new int[kMax - kMin + 1];
					
					tmpArray[j - kMin] = Integer.parseInt(tmpKMeans[k]);
					
					kmeansData.add(tmpArray);
				
				
				
				}
			} else {
				for (int k = 0; k < tmpKMeans.length - 1; k++) {
					
					String neuronId = (k + 1) + "";
					//String column = "k" + j + "_n" + (normIdx + 1);
					
					kmeansData.get(k)[j - kMin] = Integer.parseInt(tmpKMeans[k]);
					
					//columns[colCounter++] = column;
					
				}
			}
			
			sseData.add(Float.parseFloat(tmpKMeans[tmpKMeans.length - 1]));
		
		}
		
		kmeansOutput.write(columns[0]);
		for (int i = 1; i < columns.length; i++) {
			kmeansOutput.write("," + columns[i]);
		}
		kmeansOutput.write("\n");
		
		for (int i = 0; i < kmeansData.size(); i++) {
			String neuronId = labels[i];
			kmeansOutput.write(neuronId);
			
			for (int j = 0; j < kmeansData.get(i).length; j++) {
				kmeansOutput.write("," + kmeansData.get(i)[j]);
			}
			kmeansOutput.write("\n");
		}
		
		kmeansOutput.flush();
		kmeansOutput.close();
		
		sseOutput.write("k,sse\n");
		
		for (int i = 0; i < sseData.size(); i++) {
			String sseAttribute = "k" + (i + kMin);
			sseOutput.write(sseAttribute + "," + sseData.get(i) + "\n");
		}
		
		sseOutput.flush();
		sseOutput.close();
	}	
}
