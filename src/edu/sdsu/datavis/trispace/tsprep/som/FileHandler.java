package edu.sdsu.datavis.trispace.tsprep.som;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class FileHandler {
	
	private static int settingsLine = 0;
    private static int attrNamesLine = 1;

    public FileHandler() {
    }
    
    public static String[] readCodAttributesToArrayList(String filepath) throws IOException {

    	BufferedReader reader = new BufferedReader(new FileReader(filepath));
    	String[] attributes;
        try {
            
            
            reader.readLine();
            String line = reader.readLine();
            String[] lineSplit = line.split(" ");
            attributes = new String[lineSplit.length - 1];
            
            for (int i = 1; i < lineSplit.length; i++) {
            	attributes[i-1] = lineSplit[i];
            }

        }
        catch (Exception e) {
            e.printStackTrace();
            attributes = null;
        }
        finally {
            reader.close();
        }
        return attributes;

    }
    

    private static ArrayList<String[]> readFileToArrayList(String filepath) throws IOException {

        attrNamesLine = 0;

        ArrayList<String[]> filecontent = new ArrayList<String[]>();

        BufferedReader reader = new BufferedReader(new FileReader(filepath));

        try {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                // skip comment lines BUT read attribute line
                if(line.substring(0,1).equals("#")) {

                    if(line.substring(0,4).toLowerCase().equals("#att")) {
                        // cut the #att from the line String
                        //System.out.println("Attribute line found!");
                        line = line.substring(4,line.length());
                        filecontent.add(line.trim().split(" "));
                        attrNamesLine = 1;
                    }
                    else {
                        //System.out.println("Comment line found!");
                    }
                }
                else if(!line.equals("")) {
                    // only read non-empty lines
                    filecontent.add(line.split(" "));
                }
            }
            //System.out.println("DONE: " + filecontent.size() + " lines in ArrayList");
            if(attrNamesLine == 0) {
                //System.out.println("No attribute names!");
            }

        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            reader.close();
        }
        return filecontent;

    }
    
    public static Dat readDatFile(String datFilePath, String type) {

        ArrayList<String[]> rawFileContent = new ArrayList<String[]>();
        InputVector[] inputVectors;
        String[] ivAttributeNames;

        try {
            rawFileContent = readFileToArrayList(datFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        int vectorDimension = Integer.parseInt(rawFileContent.get(0)[0]);

        inputVectors = new InputVector[rawFileContent.size()-2];

        ivAttributeNames = new String[vectorDimension];

        int idIndex = 0;
        int arrayIndex = 0;

        for(int i = 1; i < rawFileContent.size(); i++)	{
            float[] attributes = new float[vectorDimension];
            String inputVectorLabel = "Vector" + (i-1);
            String geoID = "noID";

            for(int j = 0; j < rawFileContent.get(i).length; j++) {
                if(attrNamesLine != 0 && i == 1) {
                    String attrName = rawFileContent.get(i)[j];
//                    String type = datFilePath.substring(datFilePath.lastIndexOf("/") + 1, datFilePath.lastIndexOf("crime"));
//                    String type = datFilePath.substring(datFilePath.lastIndexOf("/") + 1, datFilePath.lastIndexOf("image"));
                    String den = type.split("_")[1];
                    if (den.equals("AT")) {
                        if (attrName.contains("2000")) {
                            attrName = attrName.replace("_2", ",2");
                        } else {
                            attrName = attrName.replace("_1", ",1");
                        }

                    } else if (den.equals("LA")) {
                        attrName = attrName.replaceFirst("_", ",");

                    } else if (den.equals("LT")) {
                        attrName = attrName.replace("_", ",");
                    }
                    ivAttributeNames[j] = attrName;
                }
                else {

                    if(j < vectorDimension) {
                        if(attrNamesLine == 0) {
                            ivAttributeNames[j] = "attr" + (j+1);
                        }
                        if(rawFileContent.get(i)[j].equals("x")) {
                            attributes[j] = -99;
                        }
                        else {
                            attributes[j] = Float.parseFloat(rawFileContent.get(i)[j]);
                        }
                    }
                    else {

                        if(j == vectorDimension) {
                            inputVectorLabel = rawFileContent.get(i)[j];
//                            String type = datFilePath.substring(datFilePath.lastIndexOf("/") + 1, datFilePath.lastIndexOf("image"));
                            if (type.equals("AT_L")) {
                                if (inputVectorLabel.contains("2000")) {
                                    inputVectorLabel = inputVectorLabel.replace("_2", ",2");
                                } else {
                                    inputVectorLabel = inputVectorLabel.replace("_1", ",1");
                                }

                            } else if (type.equals("LA_T")) {
                                inputVectorLabel = inputVectorLabel.replaceFirst("_", ",");

                            } else if (type.equals("LT_A")) {
                                inputVectorLabel = inputVectorLabel.replace("_", ",");
                            }
                        }
                        else if(j == vectorDimension+1) {
                            geoID = rawFileContent.get(i)[j];
                        }

                    }

                }
            }
            if(attrNamesLine != 0 && i == 1) {
                // do nothing
            }
            else {
                inputVectors[arrayIndex] = new InputVector(idIndex, attributes, inputVectorLabel, geoID);
                idIndex++;
                arrayIndex++;
            }

        }

        System.out.println("DONE: " + inputVectors.length + " inputVectors created.");
        System.out.println("First input Vector -> ID: " + inputVectors[0].getID() + ", "
                + inputVectors[0].getAttributes().length + " attributes, name: "
                + inputVectors[0].getName() + ", geoID: " + inputVectors[0].getGeoID() + "\n");
        System.out.println("Attribute labels 0 and 1: " + ivAttributeNames[0] + " " + ivAttributeNames[1] + "\n");

        return new Dat(vectorDimension, inputVectors, ivAttributeNames);
    }
    
    public static Cod readCodFile(String codFilePath, String type) {

        int idIndex = 0;
        int arrayIndex = 0;

        Neuron[] neurons;
        String[] codAttributeNames;

        ArrayList<String[]> rawFileContent = new ArrayList<String[]>();

        try {
            rawFileContent = readFileToArrayList(codFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        int vectorDimension = Integer.parseInt(rawFileContent.get(0)[0]);
//        System.out.println("vectorDimension " + vectorDimension);

        int rows = Integer.parseInt(rawFileContent.get(0)[2]);
        int cols = Integer.parseInt(rawFileContent.get(0)[3]);
//        System.out.println("rawFileContent Size: " + rawFileContent.size());
//        System.out.println(rawFileContent.get(1)[0]);
//        System.out.println(attrNamesLine);

        neurons = new Neuron[rawFileContent.size()-1];
//        System.out.println("TEST : " + neurons.length);

        codAttributeNames = new String[vectorDimension];

        for(int i = 1; i < rawFileContent.size(); i++)	{
            float[] attributes = new float[vectorDimension];

            for(int j = 0; j < vectorDimension; j++) {
                if(attrNamesLine != 0 && i == 1) {
                    String attrName = rawFileContent.get(i)[j];
//                    String type = codFilePath.substring(codFilePath.lastIndexOf("/") + 1, codFilePath.lastIndexOf("image"));
                    String den = type.split("_")[1];
                    if (den.equals("AT")) {
                        if (attrName.contains("2000")) {
                            attrName = attrName.replace("_2", ",2");
                        } else {
                            attrName = attrName.replace("_1", ",1");
                        }

                    } else if (den.equals("LA")) {
                        attrName = attrName.replaceFirst("_", ",");

                    } else if (den.equals("LT")) {
                        attrName = attrName.replace("_", ",");
                    }
                    codAttributeNames[j] = attrName;
                }
                else {
                    if(attrNamesLine == 0) {
                        codAttributeNames[j] = "attr" + (j+1);
                    }
                    if(rawFileContent.get(i)[j].equals("x")) {
                        attributes[j] = -99;
                    }
                    else {
                        attributes[j] = Float.parseFloat(rawFileContent.get(i)[j]);
                    }
                }
            }
            if (i >= 1) {
                neurons[arrayIndex] = new Neuron(idIndex, attributes);
                idIndex++;
                arrayIndex++;
            }
        }

        System.out.println("1st attribute: " + codAttributeNames[0]);
        System.out.println("DONE: " + neurons.length + " neurons created.");
        System.out.println("First neuron -> ID: " + neurons[0].getID() + " "
                + neurons[0].getAttributes().length + " attributes\n" + neurons[0].getAttributes()[0] + "\n");
        System.out.println("Last neuron -> ID: " + neurons[neurons.length-1].getID() + " "
        		+ neurons[neurons.length-1].getAttributes().length + " attributes\n" + neurons[neurons.length-1].getAttributes()[0] + "\n");

        return new Cod(neurons, codAttributeNames, rows, cols);

    }

    public static Dat readDatFile(String datFilePath) {

        ArrayList<String[]> rawFileContent = new ArrayList<String[]>();
        InputVector[] inputVectors;
        String[] ivAttributeNames;

        try {
            rawFileContent = readFileToArrayList(datFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        int vectorDimension = Integer.parseInt(rawFileContent.get(0)[0]);

        inputVectors = new InputVector[rawFileContent.size()-2];

        ivAttributeNames = new String[vectorDimension];

        int idIndex = 0;
        int arrayIndex = 0;

        for(int i = 1; i < rawFileContent.size(); i++)	{
            float[] attributes = new float[vectorDimension];
            String inputVectorLabel = "Vector" + (i-1);
            String geoID = "noID";

            for(int j = 0; j < rawFileContent.get(i).length; j++) {
                if(attrNamesLine != 0 && i == 1) {
                    String attrName = rawFileContent.get(i)[j];
//                    String type = datFilePath.substring(datFilePath.lastIndexOf("/") + 1, datFilePath.lastIndexOf("crime"));
                    String type = datFilePath.substring(datFilePath.lastIndexOf("/") + 1, datFilePath.lastIndexOf("image"));
                    String den = type.split("_")[1];
                    if (den.equals("AT")) {
                        if (attrName.contains("2000")) {
                            attrName = attrName.replace("_2", ",2");
                        } else {
                            attrName = attrName.replace("_1", ",1");
                        }

                    } else if (den.equals("LA")) {
                        attrName = attrName.replaceFirst("_", ",");

                    } else if (den.equals("LT")) {
                        attrName = attrName.replace("_", ",");
                    }
                    ivAttributeNames[j] = attrName;
                }
                else {

                    if(j < vectorDimension) {
                        if(attrNamesLine == 0) {
                            ivAttributeNames[j] = "attr" + (j+1);
                        }
                        if(rawFileContent.get(i)[j].equals("x")) {
                            attributes[j] = -99;
                        }
                        else {
                            attributes[j] = Float.parseFloat(rawFileContent.get(i)[j]);
                        }
                    }
                    else {

                        if(j == vectorDimension) {
                            inputVectorLabel = rawFileContent.get(i)[j];
                            String type = datFilePath.substring(datFilePath.lastIndexOf("/") + 1, datFilePath.lastIndexOf("image"));
                            if (type.equals("AT_L")) {
                                if (inputVectorLabel.contains("2000")) {
                                    inputVectorLabel = inputVectorLabel.replace("_2", ",2");
                                } else {
                                    inputVectorLabel = inputVectorLabel.replace("_1", ",1");
                                }

                            } else if (type.equals("LA_T")) {
                                inputVectorLabel = inputVectorLabel.replaceFirst("_", ",");

                            } else if (type.equals("LT_A")) {
                                inputVectorLabel = inputVectorLabel.replace("_", ",");
                            }
                        }
                        else if(j == vectorDimension+1) {
                            geoID = rawFileContent.get(i)[j];
                        }

                    }

                }
            }
            if(attrNamesLine != 0 && i == 1) {
                // do nothing
            }
            else {
                inputVectors[arrayIndex] = new InputVector(idIndex, attributes, inputVectorLabel, geoID);
                idIndex++;
                arrayIndex++;
            }

        }

        System.out.println("DONE: " + inputVectors.length + " inputVectors created.");
        System.out.println("First input Vector -> ID: " + inputVectors[0].getID() + ", "
                + inputVectors[0].getAttributes().length + " attributes, name: "
                + inputVectors[0].getName() + ", geoID: " + inputVectors[0].getGeoID() + "\n");
        System.out.println("Attribute labels 0 and 1: " + ivAttributeNames[0] + " " + ivAttributeNames[1] + "\n");

        return new Dat(vectorDimension, inputVectors, ivAttributeNames);
    }

    public static Cod readCodFile(String codFilePath) {

        int idIndex = 0;
        int arrayIndex = 0;

        Neuron[] neurons;
        String[] codAttributeNames;

        ArrayList<String[]> rawFileContent = new ArrayList<String[]>();

        try {
            rawFileContent = readFileToArrayList(codFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        int vectorDimension = Integer.parseInt(rawFileContent.get(0)[0]);
//        System.out.println("vectorDimension " + vectorDimension);

        int rows = Integer.parseInt(rawFileContent.get(0)[2]);
        int cols = Integer.parseInt(rawFileContent.get(0)[3]);
//        System.out.println("rawFileContent Size: " + rawFileContent.size());
//        System.out.println(rawFileContent.get(1)[0]);
//        System.out.println(attrNamesLine);

        neurons = new Neuron[rawFileContent.size()-1];
//        System.out.println("TEST : " + neurons.length);

        codAttributeNames = new String[vectorDimension];

        for(int i = 1; i < rawFileContent.size(); i++)	{
            float[] attributes = new float[vectorDimension];

            for(int j = 0; j < vectorDimension; j++) {
                if(attrNamesLine != 0 && i == 1) {
                    String attrName = rawFileContent.get(i)[j];
                    String type = codFilePath.substring(codFilePath.lastIndexOf("/") + 1, codFilePath.lastIndexOf("image"));
                    String den = type.split("_")[1];
                    if (den.equals("AT")) {
                        if (attrName.contains("2000")) {
                            attrName = attrName.replace("_2", ",2");
                        } else {
                            attrName = attrName.replace("_1", ",1");
                        }

                    } else if (den.equals("LA")) {
                        attrName = attrName.replaceFirst("_", ",");

                    } else if (den.equals("LT")) {
                        attrName = attrName.replace("_", ",");
                    }
                    codAttributeNames[j] = attrName;
                }
                else {
                    if(attrNamesLine == 0) {
                        codAttributeNames[j] = "attr" + (j+1);
                    }
                    if(rawFileContent.get(i)[j].equals("x")) {
                        attributes[j] = -99;
                    }
                    else {
                        attributes[j] = Float.parseFloat(rawFileContent.get(i)[j]);
                    }
                }
            }
            if (i >= 1) {
                neurons[arrayIndex] = new Neuron(idIndex, attributes);
                idIndex++;
                arrayIndex++;
            }
        }

        System.out.println("1st attribute: " + codAttributeNames[0]);
        System.out.println("DONE: " + neurons.length + " neurons created.");
        System.out.println("First neuron -> ID: " + neurons[0].getID() + " "
                + neurons[0].getAttributes().length + " attributes\n" + neurons[0].getAttributes()[0] + "\n");
        System.out.println("Last neuron -> ID: " + neurons[neurons.length-1].getID() + " "
        		+ neurons[neurons.length-1].getAttributes().length + " attributes\n" + neurons[neurons.length-1].getAttributes()[0] + "\n");

        return new Cod(neurons, codAttributeNames, rows, cols);

    }
}
