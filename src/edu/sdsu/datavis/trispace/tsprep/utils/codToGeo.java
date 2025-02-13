package edu.sdsu.datavis.trispace.tsprep.utils;


import edu.sdsu.datavis.trispace.tsprep.dr.SOMaticManager;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.somatic.som.SOMatic;
import org.somatic.trainer.Global;
import org.somatic.trainer.io.FileWriter;
import org.somatic.entities.Neuron;
import org.somatic.entities.SOM;
import org.somatic.trainer.Global;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.somatic.entities.Neuron;
import org.somatic.entities.SOM;
import org.somatic.entities.TrainingVector;
import org.somatic.som.SOMatic;
import org.somatic.trainer.Global;




public class codToGeo {

    static Global g = Global.getInstance();
    SOMatic s;
    public int ID;
    private float xCoor;
    private float yCoor;

    

    // Main method - entry point for the application
    public static void main(String[] args) {
        org.somatic.trainer.io.FileWriter fw = new org.somatic.trainer.io.FileWriter();
        
        // ====================================================================================================
        
        String inputPath = "C:/Users/wesle/Desktop/Thesis_files/Github/TriSpace_Prep/data/motlow/completed_som_trainings/10k/All_2/test/L_AT/L_AT_1/1/L_AT_Normalized/trainedSom.cod";
        
        // String inputPath = "C:/Users/wesle/Desktop/Thesis_files/Github/TriSpace_Prep/data/motlow/All_2_50k/test/L_AT/L_AT_1/1/L_AT_Normalized/trainedSom.cod";
        String outputPath = "C:/Users/wesle/Desktop/Thesis_files/Github/TriSpace_Prep/data/motlow/completed_som_trainings/10k/All_2/test/L_AT/L_AT_1/1/L_AT_Normalized/";
        org.somatic.trainer.io.FileReader fr = new org.somatic.trainer.io.FileReader();
        new ArrayList();
        Global g = Global.getInstance();
        ArrayList<String[]> fileContent = fr.readCodFile(inputPath);
        String[] settingLine = (String[])fileContent.get(0);
        int somX = Integer.parseInt(settingLine[2]);
        int somY = Integer.parseInt(settingLine[3]);
        int nrAtts = Integer.parseInt(settingLine[0]);
        fileContent.remove(0);
        fileContent.remove(0);
        g.getClass();
        // System.err.println("somX: " + somX + " somY: " + somY + " nrAtts: " + nrAtts);
        // System.out.println("Intended dimensions - X: " + 388 + " Y: " + somY + " Attributes: " + nrAtts);
        // g.nNeuronsX = 388;
        // g.nNeuronsY = 388;
        g.som = new org.somatic.entities.SOM(somY, somX, nrAtts, 4, fileContent);
        // System.out.println("Actual SOM dimensions - Width: " + g.som.neurons.length + " Height: " + g.som.neurons[0].length);
        System.err.println("g.som: " + g.som.neurons.length + " " + g.som.neurons[0].length);
        codToGeo.writeSomToGeoJson(outputPath);
        // ====================================================================================================

        // fw.fromCodToGeojson("C:/Users/wesle/Desktop/Thesis_files/Github/TriSpace_Prep/data/motlow/completed_som_trainings/10k/All_2/test/L_AT/L_AT_1/1/L_AT_Normalized/trainedSom.cod",
                            // "C:/Users/wesle/Desktop/Thesis_files/Github/TriSpace_Prep/data/motlow/completed_som_trainings/10k/All_2/test/L_AT/L_AT_1/1/L_AT_Normalized/");
        // fw.writeBmusToGeoJson("C:/Users/wesle/Desktop/Thesis_files/Github/TriSpace_Prep/data/motlow/All_2_50k/test/L_AT/L_AT_1/1/L_AT_Normalized/");
        // fw.writeSomToGeoJson("C:/Users/wesle/Desktop/Thesis_files/Github/TriSpace_Prep/data/motlow/completed_som_trainings/10k/All_2/test/L_AT/L_AT_1/1/L_AT_Normalized/");
        
        System.out.println("codToGeo is running!");
    }


    // works but causes java heap space error sometimes
    // // ====================================================================================================
    // public static void writeSomToGeoJson(String filePath) {
    //     org.somatic.trainer.io.FileWriter fw = new org.somatic.trainer.io.FileWriter();
    //     org.somatic.som.SOMatic s = new org.somatic.som.SOMatic();
    //     // org.somatic.trainer.Global g = new org.somatic.trainer.Global();
    //     File file = fw.prepareFile(filePath, "trainedSom", "geojson", true);
    //     org.json.simple.JSONObject featureColl = new org.json.simple.JSONObject();
    //     org.json.simple.JSONArray features = new org.json.simple.JSONArray();
    //     featureColl.put("type", "FeatureCollection");
    //     featureColl.put("features", features);
    //     s.modifyGeometry();
  
    //     for(int i = 0; i < g.som.neurons.length; ++i) {
    //        for(int j = 0; j < g.som.neurons[i].length; ++j) {
    //         org.json.simple.JSONObject neuron = new org.json.simple.JSONObject();
    //         org.json.simple.JSONObject geometry = new org.json.simple.JSONObject();
    //         org.json.simple.JSONArray outerCoords = new org.json.simple.JSONArray();
    //         org.json.simple.JSONArray innerCoords = new org.json.simple.JSONArray();
    //         org.json.simple.JSONObject properties = new org.json.simple.JSONObject();
    //         org.somatic.entities.Neuron currNeuron = g.som.neurons[j][i];
    //           double[] coords = s.createHexaPolygon(currNeuron);
  
    //           int k;
    //           for(k = 0; k < coords.length; k += 2) {
    //             org.json.simple.JSONArray pointCoords = new org.json.simple.JSONArray();
    //              pointCoords.add(coords[k]);
    //              pointCoords.add(coords[k + 1]);
    //              innerCoords.add(pointCoords);
    //           }
  
    //           outerCoords.add(innerCoords);
    //           neuron.put("type", "Feature");
    //           neuron.put("geometry", geometry);
    //           neuron.put("properties", properties);
    //           properties.put("id", currNeuron.ID);
    //           geometry.put("type", "Polygon");
    //           geometry.put("coordinates", outerCoords);
  
    //           for(k = 0; k < g.som.neurons[i][j].getAttributeLength(); ++k) {
    //              double value = g.som.neurons[i][j].getAttribute(k);
    //              if (g.rounding) {
    //                 value = (double)Math.round(value * 100000.0) / 100000.0;
    //              }
  
    //              if (g.Attributes[k].name != "") {
    //                 properties.put(g.Attributes[k].name, value);
    //              }
    //           }
  
    //           features.add(neuron);
    //        }
    //     }
  
    //     String json = featureColl.toJSONString();
  
    //     try {
    //         java.io.FileWriter fw2 = new java.io.FileWriter(file.getAbsoluteFile());
    //         java.io.BufferedWriter bw = new BufferedWriter(fw2);
    //         bw.write(json);
    //         bw.flush();
    //         bw.close();
    //         } catch (Exception var20) {
    //         var20.printStackTrace();
    //         g.status = var20.getMessage();
    //         }
  
    //  }

    // ====================================================================================================

    public static void writeSomToGeoJson(String filePath) {
        org.somatic.trainer.io.FileWriter fw = new org.somatic.trainer.io.FileWriter();
        org.somatic.som.SOMatic s = new org.somatic.som.SOMatic();
        File file = fw.prepareFile(filePath, "trainedSom", "geojson", true);
        
        try (BufferedWriter bw = new BufferedWriter(new java.io.FileWriter(file))) {
            // Write header
            bw.write("{\"type\":\"FeatureCollection\",\"features\":[");
            
            boolean firstFeature = true;
            modifyGeometry();
            
            for(int i = 0; i < g.som.neurons.length; ++i) {
                for(int j = 0; j < g.som.neurons[i].length; ++j) {
                    if (!firstFeature) {
                        bw.write(",");
                    }
                    firstFeature = false;
                    
                    org.json.simple.JSONObject neuron = new org.json.simple.JSONObject();
                    org.json.simple.JSONObject geometry = new org.json.simple.JSONObject();
                    org.json.simple.JSONArray outerCoords = new org.json.simple.JSONArray();
                    org.json.simple.JSONArray innerCoords = new org.json.simple.JSONArray();
                    org.json.simple.JSONObject properties = new org.json.simple.JSONObject();
                    Neuron currNeuron = g.som.neurons[j][i];
                    // System.out.println(" currNeuron: " + currNeuron +  " x cord " + currNeuron.getxCoor() + " y cord " + currNeuron.getyCoor()
                    // + " j: " + j + " i: " + i);
                    double[] coords = s.createHexaPolygon(currNeuron);
                
                    
                    // Build the neuron JSON object
                    for(int k = 0; k < coords.length; k += 2) {
                        org.json.simple.JSONArray pointCoords = new org.json.simple.JSONArray();
                        pointCoords.add(coords[k]);
                        pointCoords.add(coords[k + 1]);
                        innerCoords.add(pointCoords);
                    }
                    
                    outerCoords.add(innerCoords);
                    neuron.put("type", "Feature");
                    neuron.put("geometry", geometry);
                    neuron.put("properties", properties);
                    properties.put("id", currNeuron.ID);
                    geometry.put("type", "Polygon");
                    geometry.put("coordinates", outerCoords);
                    
                    for(int k = 0; k < g.som.neurons[i][j].getAttributeLength(); ++k) {
                        double value = g.som.neurons[i][j].getAttribute(k);
                        if (g.rounding) {
                            value = (double)Math.round(value * 1000.0) / 1000.0;

                            // value = (double)Math.round(value * 100000.0) / 100000.0;
                        }
                        if (g.Attributes[k].name != "") {
                            properties.put(g.Attributes[k].name, value);
                        }
                    }
                    
                    // Write this neuron immediately
                    bw.write(neuron.toJSONString());
                    bw.flush();
                }
            }
            
            // Write footer
            bw.write("]}");
            
        } catch (IOException e) {
            e.printStackTrace();
            g.status = e.getMessage();
        }
    }


    public static void modifyGeometry() {
        Global g = Global.getInstance();
        int numRows = g.nNeuronsX;
        int numCols = g.nNeuronsY;  // Add this to properly handle dimensions
    
        for(int i = 0; i < g.som.neurons.length; ++i) {
            for(int j = 0; j < g.som.neurons[i].length; ++j) {
                Neuron currNeuron = g.som.neurons[j][i];
                float x = 0.0F;
                // Use numCols instead of rows for proper scaling
                int row = (currNeuron.ID - 1) / numCols;
                int col = currNeuron.ID - 1 - row * numCols;
                
                if ((row & 1) == 0) {
                    x = (float)col;
                } else {
                    x = (float)((double)col + 0.5);
                }
    
                float y = (float)((double)row * 0.8660254037844);
                currNeuron.setxCoor(x * (float)g.scalingFactor);
                currNeuron.setyCoor(y * (float)g.scalingFactor);
                g.som.neurons[j][i] = currNeuron;
            }
        }
    }

    public float getxCoor() {
        return this.xCoor;
     }
  
     public void setxCoor(float xCoor) {
        this.xCoor = xCoor;
     }
  
     public float getyCoor() {
        return this.yCoor;
     }
  
     public void setyCoor(float yCoor) {
        this.yCoor = yCoor;
     }




}