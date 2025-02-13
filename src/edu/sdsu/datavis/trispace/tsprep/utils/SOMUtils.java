package edu.sdsu.datavis.trispace.tsprep.utils;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

class Som {

    Dat dat;
    Cod cod;
    String name;
    int index;
    int maxHits;
    Neuron[] neurons;
    InputVector[] inputVectors;
    int selectedCP;

    public Som(String datFilePath, String codFilePath, String name_, int index_, String iV2BMU, String bMU2IV, String sSE_CSV, int kValue) {
        dat = FileHandler.readDatFile(datFilePath);
        cod = FileHandler.readCodFile(codFilePath);
        name = name_;
        index = index_;
        neurons = cod.neurons;
        inputVectors = dat.inputVectors;
        ArrayList<String> iVList = new ArrayList<String>();
        
        try {
			FileWriter writerIV = new FileWriter(iV2BMU);
	        for (InputVector inputVector : dat.inputVectors) {
	//        	System.out.println("IV to BMU");
	        	iVList.add(inputVector.getName().replace(",", "_"));
	            BestMatchingUnit.findBMU(inputVector, cod.neurons);
	            writerIV.append(inputVector.getName());
	            writerIV.append(',');
	            writerIV.append("" + (inputVector.getMatchingNeuronID()+1));
	            writerIV.append('\n');
//	            System.out.println(inputVector.getName().replaceAll(",", "_"));
//	            System.out.println(inputVector.getMatchingNeuronID());
	        }
	        writerIV.flush();
			writerIV.close();
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        maxHits = 0;
        System.out.println("Number of Neurons: " + neurons.length);
        ArrayList<KMeans> kMeansTotal = new ArrayList<KMeans>();
        for (int j = 2; j <= kValue; j++) {
        	KMeans kMeansClustering = null;
            boolean kMeansComplete = false;
            while (!kMeansComplete) {
            	try {
                	kMeansClustering = new KMeans(j,neurons);
                	kMeansComplete = true;
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("Invalid option");
                    kMeansComplete = false;
                }
            }
            kMeansTotal.add(kMeansClustering);
        }
        ArrayList<ArrayList<ArrayList<Integer>>> allVals = new ArrayList<ArrayList<ArrayList<Integer>>>();
        for (int j = 0; j < kMeansTotal.size(); j++) {
        	ArrayList<ArrayList<Integer>> totalVals = new ArrayList<ArrayList<Integer>>();
        	for (int i = 0; i < kMeansTotal.get(j).kMeansClusters.length; i++) {
            	totalVals.add(kMeansTotal.get(j).kMeansClusters[i].memberIDs);
            }
        	allVals.add(totalVals);
        	
        }
        
        try {
        	FileWriter writerSSE = new FileWriter(sSE_CSV);
        	for (int j = 0; j < kMeansTotal.size(); j++) {
        		writerSSE.append(""+kMeansTotal.get(j).calcSSE(kMeansTotal.get(j).distMatrix, kMeansTotal.get(j).clusterMatrix));
        		if (j < kMeansTotal.size()-1) {
        			writerSSE.append('\n');
        		}        		
            }
        	writerSSE.flush();
	        writerSSE.close();
        }  catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
        
        
//        ArrayList<Integer> vals = kMeansClustering.kMeansClusters[0].memberIDs;
        try {
			FileWriter writerBMU = new FileWriter(bMU2IV);
		
	        for (Neuron neuron : cod.neurons) {
	            ArrayList<Integer> hits = neuron.getMatchingVectorIDs();
	            if (hits != null && hits.size() > maxHits) {
	                maxHits = hits.size();
	            }
	            String[] mVIDs = new String[neuron.getMatchingVectorIDs().size()];
	           
	            for (int ii = 0; ii < neuron.getMatchingVectorIDs().size(); ii++) {
//	            	if (targetData == "L_AT") {
//	            		mVIDs[ii] = "P" + (neuron.getMatchingVectorIDs().get(ii)+1);
//	            	} else if (targetData == "A_LT") {
//	            		mVIDs[ii] = "Band" + (neuron.getMatchingVectorIDs().get(ii)+1);
//	            	} else {
	            		mVIDs[ii] = "" + iVList.get(neuron.getMatchingVectorIDs().get(ii));
//	            	}
	            	
//	            	mVIDs[ii] = "" + neuron.getMatchingVectorIDs().get(ii);
	            }
	            int neuronID = neuron.getID()+1;
	            writerBMU.append("" + neuronID);
	            writerBMU.append(',');
	            for (int ii = 0; ii < neuron.getMatchingVectorIDs().size(); ii++) {
	            	writerBMU.append(mVIDs[ii]);
	            	if (ii < mVIDs.length-1) {
	            		writerBMU.append(" ");
	            	}
	            }
	            ArrayList<Integer> kClusters = new ArrayList<Integer>();
	            for (int ii = 2; ii <= kValue; ii++) {
	            	kClusters.add(-1);
	            }
//	            int kCluster = -1;
	            for (int kk = 0; kk < allVals.size(); kk++) {
	            	for (int ii = 0; ii < allVals.get(kk).size(); ii++) {
	            		for (int jj = 0; jj < allVals.get(kk).get(ii).size(); jj++) {
	            			if (allVals.get(kk).get(ii).get(jj) == neuronID) {
	            				kClusters.set(kk,  ii);
	            				break;
//	            				set(kk) = ii;
	            			}
	            		}
	            	}
	            }
	            for (int kk = 0; kk < kClusters.size(); kk++) {
	            	writerBMU.append(',');
		            writerBMU.append("" + (kClusters.get(kk)+1));
	            }
//	            for (int ii = 0; ii < totalVals.size(); ii++) {
//	            	for (int iii = 0; iii < totalVals.get(ii).size(); iii++) {
//	            		if (totalVals.get(ii).get(iii) == neuronID) {
//	            			kCluster = ii;
//	            			break;
//	            		}
//	            	}
//	            }
//	            writerBMU.append(',');
//	            writerBMU.append("" + (kCluster+1));
//	            writerBMU.append(mVIDs);
	            writerBMU.append('\n');
	//            if (neuron.getID() == 0) {
	//            	System.out.println(neuron.getID());
	//            	System.out.println(neuron.getMatchingVectorIDs());
	//            }
	//            System.out.println(neuron.getID());
	//            System.out.println(neuron.getMatchingVectorIDs());
	        }
	        writerBMU.flush();
	        writerBMU.close();
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        selectedCP = -1;
        
        
//        kMeansClustering.
    }

    public void setSelectedCP(int i) {
        selectedCP = i;
    }

    public int getSelectedCP() {
        return selectedCP;
    }

}

class InputVector {

    private int id;
    private float[] attributes;
    private String name;
    private String geoID;
    private int matchingNeuronID;
    private ArrayList<Integer> colorNeuronIDs = new ArrayList<Integer>();

    public InputVector(int id, float[] attributes, String name, String geoID) {
        this.id = id;
        this.attributes = attributes;
        this.name = name;
        this.geoID = geoID;
        this.matchingNeuronID = -1;
    }

    public void setID(int id) {
        this.id = id;
    }
    public int getID() {
        return id;
    }
    public void setAttributes(float[] attributes) {
        this.attributes = attributes;
    }
    public float[] getAttributes() {
        return attributes;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getName() {
        return name;
    }
    public void setGeoID(String geoID) {
        this.geoID = geoID;
    }
    public String getGeoID() {
        return geoID;
    }
    public void setMatchingNeuronID(int matchingNeuronID) {
        this.matchingNeuronID = matchingNeuronID;
    }
    public int getMatchingNeuronID() {
        return matchingNeuronID;
    }

    public void addColorNeuronID(int neuronID) {
        colorNeuronIDs.add(neuronID);
    }
    public ArrayList<Integer> getColorNeuronIDs() {
        return colorNeuronIDs;
    }
    public void clearColorNeuronIDs() {
        colorNeuronIDs.clear();
    }
}

class Neuron {

    private int id;
    private float[] attributes;
    private ArrayList<Integer> matchingVectorIDs = new ArrayList<Integer>();
    private int matchingSearchVectorID;

    public Neuron(int id, float[] attributes) {
        this.id = id;
        this.attributes = attributes;
    }

    public void setID(int id) {
        this.id = id;
    }
    public int getID() {
        return id;
    }
    public void setAttributes(float[] attributes) {
        this.attributes = attributes;
    }
    public float[] getAttributes() {
        return attributes;
    }
    public void addMatchingVectorID(int vectorID) {
        matchingVectorIDs.add(vectorID);
    }
    public ArrayList<Integer> getMatchingVectorIDs() {
        return matchingVectorIDs;
    }
    public void clearMatchingVectorIDs() {
        matchingVectorIDs.clear();
    }
    public void setMatchingSearchVectorID(int id_) {
        matchingSearchVectorID = id_;
    }
    public int getMatchingSearchVectorID() {
        return matchingSearchVectorID;
    }
    public float getDistance(float[] inputVector) {
        float distance = 0;
        for (int i = 0; i < attributes.length; i++) {
            distance += (inputVector[i] - attributes[i]) * (inputVector[i] - attributes[i]);
        }
        return (float) Math.sqrt(distance);
    }
}

class FileHandler {
    
    private static int settingsLine = 0;
    private static int attrNamesLine = 1;

    public FileHandler() {
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

class Dat {

    int vectorDimension;
    InputVector[] inputVectors;
    String[] ivAttributeNames;

    public Dat(int vd, InputVector[] iv, String[] ivan) {
        vectorDimension = vd;
        inputVectors = iv;
        ivAttributeNames = ivan;
    }
}

class Cod {

    int rows;
    int cols;
    public Neuron[] neurons;
    public String[] codAttributeNames;

    public Cod(Neuron[] n, String[] codan, int r, int c) {
        neurons = n;
        codAttributeNames = codan;
        rows = r;
        cols = c;
    }
}

class BestMatchingUnit {

    public static void findBMU(InputVector inputVector, Neuron[] neurons) {
        float minDistance = neurons[0].getDistance(inputVector.getAttributes());
        Neuron bmu = neurons[0];
        for (int i = 1; i < neurons.length; i++) {
            Neuron neuron = neurons[i];
            float dist = neuron.getDistance(inputVector.getAttributes());
            if (dist < minDistance) {
                minDistance = dist;
                bmu = neuron;
            }
        }
        bmu.addMatchingVectorID(inputVector.getID());
        inputVector.setMatchingNeuronID(bmu.getID());
    }
}

