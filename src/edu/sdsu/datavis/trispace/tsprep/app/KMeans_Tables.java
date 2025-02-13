package edu.sdsu.datavis.trispace.tsprep.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;

import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import edu.sdsu.datavis.trispace.tsprep.db.PostgreSQLJDBC;
import edu.sdsu.datavis.trispace.tsprep.dr.SOMaticManager;
import edu.sdsu.datavis.trispace.tsprep.io.img.ImageManager;
import edu.sdsu.datavis.trispace.tsprep.som.*;
import edu.sdsu.datavis.trispace.tsprep.xform.CSVManager;
import edu.sdsu.datavis.trispace.tsprep.xform.PostgreSQLManager;

public class KMeans_Tables {

	/* Store an ID for the computer that is performing the training */
	static int computerStation = 1337;


	/* Email Updates */
	final static boolean emailUpdate = false;

	/* Select the study area to train */
	static int studyArea = 1;

	// Scene name list
	final static String[] SCENES = { "SanElijo", "CocosFire", "Batiquitos" };
	final static String SCENE = SCENES[studyArea];

	// Schema list
	final static String[] SCHEMAS = { "sanelijo", "cocos", "batiquitos" };
	final static String SCHEMA = SCHEMAS[studyArea];

	// Tri-Space perspectives
	final static String[] PERSPECTIVES = { "L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L" };

	/* Tri-Space perspectives to train SOMs for */
	// final static int[] TRAINING_PERSPECTIVES = { 0, 3, 4 };
	// final static int[] TRAINING_NORMALIZATIONS = { 0, 1, 2, 3, 4, 5 };
	final static int[] TRAINING_PERSPECTIVES = { 4 };
	final static int[] TRAINING_NORMALIZATIONS = { 0 };

	// Nodata value
	final static int NO_DATA = -9999;

	// Tri-Space element count initialized - computed in program
	static int lociCount = -1;
	static int attributeCount = -1;
	static int timeCount = -1;

	// threshold of input vector count to determine if SOM or MDS
	final static int SOM_THRESHOLD = 100;
	// upper limit on SOM resolution
	final static int SOM_NEURON_CAP = 250000;

	// Set normalization boundaries: 0-1; 0.1-0.9
	final static float MIN_NORMALIZATION = 0.1f;
	final static float MAX_NORMALIZATION = 0.9f;

	// Distance measures
	final static int EUCLIDEAN = 1;
	final static int COSINE = 2;
	final static int MANHATTAN = 3;
	final static int DIST_MEASURE = COSINE;

	// Geographic Coordinate System OR Projection
	final static String EPSG = "4326";

	// Number of threads for training
	final static int NTHREADS = 1;

	// Use rounding
	final static boolean ROUNDING = true;

	// Scaling for SOM geometry (Kowatsch)
	final static int SCALING_FACTOR = 1;

	// Compute kmeans from MIN to MAX
	final static int MIN_K = 7;
	final static int MAX_K = 12;

	// Maximum number of iterations per k-class computation
	final static int K_ITERATIONS = 1000;

	public static void main(String[] args) throws IOException {

		performKMeans();
		System.out.println("Exiting Program");
		System.exit(0);
	}

	public static void performKMeans() throws IOException {
		if (emailUpdate)
			sendEmailStart();

		lociCount = CSVManager.rowCount("./data/" + SCHEMA + "/tables/Non_normalized/dictionaries/lociDictionary.csv");
		attributeCount = CSVManager
				.rowCount("./data/" + SCHEMA + "/tables/Non_normalized/dictionaries/attrDictionary.csv");

		timeCount = CSVManager.rowCount("./data/" + SCHEMA + "/tables/Non_normalized/dictionaries/timeDictionary.csv");
		
		String normalization = PERSPECTIVES[TRAINING_NORMALIZATIONS[0]];
		String perspective = PERSPECTIVES[TRAINING_PERSPECTIVES[0]];
		
		String datInput = "data/" + SCHEMA + "/SOMaticIn/" + normalization + "_Normalized/" + perspective + "_" + SCENE + ".dat";
		String codInput = "./data/" + SCHEMA + "/SOMaticOut/" + normalization + "_Normalized/" + perspective + "/trainedSom.cod";
		String outputFolder = "./data/" + SCHEMA + "/kmeans/" + normalization + "_Normalized";
		
		try {
			Files.createDirectories(Paths.get(outputFolder));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String output = outputFolder + "/" + perspective + "_" + normalization + "_Normalized";
//		CSVManager.performKMeans(testDAT, testCOD, "L_AT", 3, DIST_MEASURE, 0, "./data/cocos/kmeans/L_AT");
		CSVManager.performKMeans(datInput, codInput, perspective, MIN_K, MAX_K, DIST_MEASURE, TRAINING_NORMALIZATIONS[0], output);


		if (emailUpdate) {
			sendEmailAlert(output + ".csv", perspective + "_" + normalization + "_Normalized.csv", output + "_sse.csv", perspective + "_" + normalization + "_Normalized_sse.csv");
//			sendEmailAlert(output + "_sse.csv", perspective + "_" + normalization + "_Normalized_sse.csv");
		}
			
	}


	public static ArrayList<String> getIV2BMU(String datFilePath, String codFilePath) {

		ArrayList<String> output = new ArrayList<String>();

		Dat dat = FileHandler.readDatFile(datFilePath, "L_AT");
		Cod cod = FileHandler.readCodFile(codFilePath, "L_AT");

		for (InputVector inputVector : dat.inputVectors) {
			float minDist = BestMatchingUnit.findBMU(inputVector, cod.neurons, DIST_MEASURE);
			output.add(inputVector.getName() + "," + (inputVector.getMatchingNeuronID() + 1) + "," + minDist);
		}

		return output;
	}

	public static float getAQE(String datFilePath, String codFilePath) {

		ArrayList<String> output = new ArrayList<String>();

		Dat dat = FileHandler.readDatFile(datFilePath, "L_AT");
		Cod cod = FileHandler.readCodFile(codFilePath, "L_AT");

		float aqe = 0f;
		int aqeCount = 0;

		for (InputVector inputVector : dat.inputVectors) {
			float next = BestMatchingUnit.findBMU(inputVector, cod.neurons, DIST_MEASURE);

			if (!Float.isNaN(next)) {
				aqe += next;
				aqeCount++;
			}
			// aqe += BestMatchingUnit.findBMU(inputVector, cod.neurons, DIST_MEASURE);
			// float minDist = BestMatchingUnit.findBMU(inputVector, cod.neurons,
			// DIST_MEASURE);
			// output.add(inputVector.getName() + "," +
			// (inputVector.getMatchingNeuronID()+1) + "," + minDist);
		}

		aqe /= aqeCount;

		return aqe;
	}

	public static float getTE(String datFilePath, String codFilePath) {

		ArrayList<String> output = new ArrayList<String>();

		Dat dat = FileHandler.readDatFile(datFilePath, "L_AT");
		Cod cod = FileHandler.readCodFile(codFilePath, "L_AT");

		int numIncorrect = 0;

		for (InputVector inputVector : dat.inputVectors) {
			if (BestMatchingUnit.topographicErrorBMU(inputVector, cod, DIST_MEASURE) == false) {
				numIncorrect++;
			}
		}

		float incorrect = (float) numIncorrect;
		float ivLength = (float) dat.inputVectors.length;

		float topoError = incorrect / ivLength;

		return topoError;
	}

	public static void sendEmailAlert(String filePath1, String fileName1, String filePath2, String fileName2) {
		final String username = "schemppthesis@gmail.com";
		final String send2User = "tschempp@gmail.com";
		final String password = "G30gthesis";

		String updateMsg = "Complete";
		String updateMsg2 = "completed";

		Properties props = new Properties();
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");

		Session session = Session.getInstance(props, new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(username, password);
			}
		});

		try {

			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress(username));
			message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(send2User));
			message.setSubject("Computer Station " + computerStation + " KMeans Process " + updateMsg);
			
			Multipart multipart = new MimeMultipart();

	        MimeBodyPart textBodyPart = new MimeBodyPart();
	        textBodyPart.setText("FYI," + "\n\n The " + SCENE + " process at computer station " + computerStation + " is " + updateMsg2
					+ "!");


	        MimeBodyPart attachmentBodyPart= new MimeBodyPart();
	        DataSource source = new FileDataSource(filePath1); // ex : "C:\\test.pdf"
	        attachmentBodyPart.setDataHandler(new DataHandler(source));
	        attachmentBodyPart.setFileName(fileName1); // ex : "test.pdf"

	        multipart.addBodyPart(textBodyPart);  // add the text part
	        multipart.addBodyPart(attachmentBodyPart); // add the attachement part
	        MimeBodyPart attachmentBodyPart2 = new MimeBodyPart();
	        DataSource source2  = new FileDataSource(filePath2); // ex : "C:\\test.pdf"
	        attachmentBodyPart2.setDataHandler(new DataHandler(source2));
	        attachmentBodyPart2.setFileName(fileName2); // ex : "test.pdf"
	        
	        multipart.addBodyPart(attachmentBodyPart2); 

	        message.setContent(multipart);
			
			

			Transport.send(message);

			System.out.println("Done");

		} catch (MessagingException e) {
			throw new RuntimeException(e);
		}
	}

	public static void sendEmailStart() {
		final String username = "schemppthesis@gmail.com";
		final String send2User = "tschempp@gmail.com";
		final String password = "G30gthesis";

		String updateMsg = "Start";
		String updateMsg2 = "starting";

		Properties props = new Properties();
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");

		Session session = Session.getInstance(props, new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(username, password);
			}
		});

		try {

			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress(username));
			message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(send2User));
			message.setSubject("Computer Station " + computerStation + " Process " + updateMsg);

			message.setText(
					"FYI," + "\n\n The process at computer station " + computerStation + " is " + updateMsg2 + "!");

			Transport.send(message);

			System.out.println("Done");

		} catch (MessagingException e) {
			throw new RuntimeException(e);
		}
	}
}
