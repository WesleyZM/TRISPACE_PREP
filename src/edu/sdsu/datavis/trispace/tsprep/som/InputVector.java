package edu.sdsu.datavis.trispace.tsprep.som;

import java.util.ArrayList;

public class InputVector {
	
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
