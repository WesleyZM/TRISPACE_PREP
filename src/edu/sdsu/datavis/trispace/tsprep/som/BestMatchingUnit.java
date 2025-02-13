package edu.sdsu.datavis.trispace.tsprep.som;

import java.util.ArrayList;

public class BestMatchingUnit {
	
	public static float findBMU(InputVector inputVector, Neuron[] neurons, int similarityMeasure) {
        float minDistance = neurons[0].getDistance(inputVector.getAttributes(), similarityMeasure);
        Neuron bmu = neurons[0];
        for (int i = 1; i < neurons.length; i++) {
            Neuron neuron = neurons[i];
            float dist = neuron.getDistance(inputVector.getAttributes(), similarityMeasure);
        	if (!Float.isNaN(dist) && dist < minDistance) {
                minDistance = dist;
                bmu = neuron;
            }
        }
        bmu.addMatchingVectorID(inputVector.getID());
        inputVector.setMatchingNeuronID(bmu.getID());
        return minDistance;
    }
	
	public static boolean topographicErrorBMU(InputVector inputVector, Cod cod, int similarityMeasure) {
		Neuron[] neurons = cod.neurons;
        float minDistance = neurons[0].getDistance(inputVector.getAttributes(), similarityMeasure);
        float minDistance2 = neurons[1].getDistance(inputVector.getAttributes(), similarityMeasure);
        int bmu1 = 0;
        int bmu2 = 1;
//        Neuron bmu = neurons[0];
//        Neuron bmu2 = neurons[1];
        
        if (minDistance2 < minDistance) {
        	float tmpNum = minDistance + 0;
        	minDistance = minDistance2 + 0;
        	minDistance2 = tmpNum + 0;
        	bmu1 = 1;
        	bmu2 = 0;
        }
        
        for (int i = 2; i < neurons.length; i++) {
            Neuron neuron = neurons[i];
            float dist = neuron.getDistance(inputVector.getAttributes(), similarityMeasure);
            if (dist < minDistance) {
            	minDistance2 = minDistance + 0;
                minDistance = dist;
                bmu2 = bmu1 + 0;
                bmu1 = i;
            }
        }
        
        Neuron bmu = neurons[bmu1];
        
//        neurons[0].
        
        bmu.addMatchingVectorID(inputVector.getID());
        inputVector.setMatchingNeuronID(bmu.getID());
        
        ArrayList<Integer> bmuNeighbors = bmu.calculateNeighbors(cod);
        
//        System.out.println("BMU: " + bmu1);
//        System.out.println("BMU2: " + bmu2);
//        
//        System.out.println("Neighbors!");
        for (int i = 0; i < bmuNeighbors.size(); i++) {
//        	System.out.println(bmuNeighbors.get(i));
        	if (bmuNeighbors.get(i) == bmu2) return true;
        }
        
//        return bmuNeighbors.contains(bmu2);
        return false;
        
//        System.out.println("rows: " + cod.rows);
//        System.out.println("columns: " + cod.cols);
//        System.out.println("first neuron id: " + neurons[0].getID());
//        System.out.println("last neuron id: " + neurons[neurons.length-1].getID());
        
//        neurons[256].calculateNeighbors(cod);
   
//        return true;
    }
}
