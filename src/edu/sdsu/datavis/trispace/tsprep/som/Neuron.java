package edu.sdsu.datavis.trispace.tsprep.som;

import java.util.ArrayList;

public class Neuron {

    private int id;
    private float[] attributes;
    private ArrayList<Integer> matchingVectorIDs = new ArrayList<Integer>();
    private int matchingSearchVectorID;
//    private ArrayList<Integer> neighbors = new ArrayList<Integer>();

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
    public ArrayList<Integer> calculateNeighbors(Cod cod) {
    	ArrayList<Integer> neighbors = new ArrayList<Integer>();
    	
    	if (isBottomEdge(cod)) {
    		if (isLeftEdge(cod)) {
    			neighbors.add(1);
        		neighbors.add(cod.cols);
    		} else if (isRightEdge(cod)) {
    			neighbors.add(cod.cols-2);
    			neighbors.add(this.id + cod.cols - 1);
    			neighbors.add(this.id + cod.cols);
    		} else {
    			neighbors.add(this.id - 1);
    			neighbors.add(this.id + 1);
    			neighbors.add(this.id + cod.cols - 1);
    			neighbors.add(this.id + cod.cols);
    		}
    	} else if (isTopEdge(cod)) {
			if (isLeftEdge(cod)) {
				neighbors.add(this.id + 1);
				neighbors.add(this.id - cod.cols);
	    		if (isOffset(cod)) neighbors.add(this.id - cod.cols + 1);
    		} else if (isRightEdge(cod)) {
    			neighbors.add(this.id - 1);
    			neighbors.add(this.id - cod.cols);
    			if (!isOffset(cod)) neighbors.add(this.id - cod.cols - 1);
    		} else {
    			neighbors.add(this.id - 1);
    			neighbors.add(this.id + 1);
    			neighbors.add(this.id - cod.cols);
    			if (isOffset(cod)) neighbors.add(this.id - cod.cols + 1);
    			else neighbors.add(this.id - cod.cols - 1);
    		}
    	} else if (isLeftEdge(cod)) {
    		neighbors.add(this.id + cod.cols);
    		neighbors.add(this.id - cod.cols);
    		neighbors.add(this.id + 1);
    		if (isOffset(cod)) {
    			neighbors.add(this.id + cod.cols + 1);
        		neighbors.add(this.id - cod.cols + 1);
    		}
    	} else if (isRightEdge(cod)) {
    		neighbors.add(this.id + cod.cols);
    		neighbors.add(this.id - cod.cols);
    		neighbors.add(this.id - 1);
    		if (!isOffset(cod)) {
    			neighbors.add(this.id + cod.cols - 1);
        		neighbors.add(this.id - cod.cols - 1);
    		}
    	} else {
    		neighbors.add(this.id - 1);
    		neighbors.add(this.id + 1);
    		neighbors.add(this.id - cod.cols);
			neighbors.add(this.id + cod.cols);
    		if (isOffset(cod)) {    			
    			neighbors.add(this.id - cod.cols + 1);
    			neighbors.add(this.id + cod.cols + 1);
    		} else {
    			neighbors.add(this.id - cod.cols - 1);
    			neighbors.add(this.id + cod.cols - 1);
    		}
    	}
    		    
//    	System.out.println("Neighbors:");
//    	
//    	for (int i = 0; i < neighbors.size(); i++) {
//    		System.out.println(neighbors.get(i));
//    	}
    	
    	return neighbors;
    }
    private boolean isBottomEdge(Cod cod) {
    	if (this.id < cod.cols) return true;
    	return false;
    }
    private boolean isTopEdge(Cod cod) {
    	int minVal = cod.cols * (cod.rows-1);
    	if (this.id >= minVal) return true;
    	return false;
    }
    private boolean isLeftEdge(Cod cod) {
    	if (this.id == 0 || this.id % cod.cols == 0) return true;
    	return false;
    }
    private boolean isRightEdge(Cod cod) {
    	int rEdge = cod.cols-1;
    	if (this.id == rEdge || this.id % cod.cols == rEdge) return true;
    	return false;
    }
    private boolean isOffset(Cod cod) {
    	if (this.id < cod.cols) return false;
    	int rowNum = this.id / cod.rows;
    	if (rowNum % 2 == 0) return false;
    	return true;
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
    public float getDistance(float[] inputVector, int similarityMeasure) {
        if (similarityMeasure == 1) {
        	return getEuclideanDistance(inputVector);
        } else if (similarityMeasure == 2) {
        	return getCosineDistance(inputVector);
        } else if (similarityMeasure == 3) {
        	return getManhattanDistance(inputVector);
        } else {
        	return -1;
        }
    }
    
    public float getEuclideanDistance(float[] inputVector) {
    	float distance = 0;
        for (int i = 0; i < attributes.length; i++) {
            distance += (inputVector[i] - attributes[i]) * (inputVector[i] - attributes[i]);
        }
        return (float) Math.sqrt(distance);
    }
    
    public float getManhattanDistance(float[] inputVector) {
    	float distance = 0;
        for (int i = 0; i < attributes.length; i++) {
            distance += Math.abs((inputVector[i] - attributes[i]));
        }
        return (float) Math.sqrt(distance);
    }
    
    public float getCosineDistance(float[] inputVector) {
    	float distance = 0;

		float skalarSum = 0;  // the scalar product of IV and Neuron
		float ivLength = 0;  // the length of the attribute-vector of the InputVector
		float neuronLength = 0;  // the length of the attribute vector of the Neuron
		float cosine;			// the cosine of IV and Neuron


		for (int i = 0; i < attributes.length; i++) {
			//	distance += (inputVector[i] - attributes[i]) * (inputVector[i] - attributes[i]);
			skalarSum += attributes[i]*inputVector[i]; //summarize the attribute-wise products of IV * Neuron
			ivLength += (inputVector[i]*inputVector[i]); //summarize the squared attributes of the IV
			neuronLength += (attributes[i]*attributes[i]); //summarize the squared attributes of the Neuron
		}

		ivLength = (float) Math.sqrt(ivLength);
		neuronLength = (float) Math.sqrt(neuronLength);
		cosine = skalarSum / (ivLength*neuronLength); // range 0 to 1; the higher, the more similar; 1 = equal.
		distance = ((1/cosine) -1); // range 0 - inf; the lower the more similar. this way usable with the same BMU search for distances
		
		return (float) Math.sqrt(distance);
    }
}
