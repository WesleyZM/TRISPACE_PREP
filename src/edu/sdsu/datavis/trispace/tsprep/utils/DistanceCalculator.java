package edu.sdsu.datavis.trispace.tsprep.utils;


public class DistanceCalculator {

	private static float distMeasure = 0; //the final distance value that will be returned
	private static float oneAttDist = 0; // the difference in one dimension/attribute

	public static float EUCLIDEAN(float[] attributesA, float[] attributesB) {
	   distMeasure = 0;
	   
		for(int i = 0; i<attributesB.length; i++){ // for each attribute calculate the difference and add it to the distance
			// only use attributes designated for training
			//if(globals.atts[i].forTraining){
				oneAttDist = (attributesA[i]-attributesB[i]);
				if(!Float.isNaN(oneAttDist)){ // check if a number was found
					//System.out.println(TAG+" getDistance: distance in attribute "+i+":"+oneAttDist);

					distMeasure += (oneAttDist*oneAttDist); //square the difference, like Pythagoras taught us
				}
				//else System.out.println(TAG+" getDistance: found NOT a number:"+oneAttDist);
			//{
		}
		distMeasure = (float) Math.sqrt(distMeasure);
		
		return distMeasure;
	} 
   
   // Cosine similarity measure: http://en.wikipedia.org/wiki/Cosine_similarity
	public static float COSINE(float[] attributesA, float[] attributesB) {
		distMeasure = 0;
		
	   float skalarSum = 0;  // the scalar product of IV and Neuron
	   float ivLength = 0;  // the length of the attribute-vector of the InputVector
	   float neuronLength = 0;  // the length of the attribute vector of the Neuron
	   float cosine;			// the cosine of IV and Neuron
	   //calculating the cosine similarity measure
	   for (int i = 0; i<attributesB.length; i++){
			// only use attributes designated for training
			//if(globals.atts[i].forTraining){
			   skalarSum += attributesB[i]*attributesA[i]; //summarize the attribute-wise products of IV * Neuron
			   ivLength += (attributesA[i]*attributesA[i]); //summarize the squared attributes of the IV
			   neuronLength += (attributesB[i]*attributesB[i]); //summarize the squared attributes of the Neuron
			//}
	   }
	   ivLength = (float) Math.sqrt(ivLength);
	   neuronLength = (float) Math.sqrt(neuronLength);
	   cosine = skalarSum / (ivLength*neuronLength); // range 0 to 1; the higher, the more similar; 1 = equal.
	   //distMeasure = ((1/cosine) -1); // range 0 - inf; the lower the more similar. this way usable with the same BMU search for distances
	   
	   if (cosine > 1) {
		   cosine = 1;
	   }
	   distMeasure = 1 - cosine;
	   distMeasure = (float) Math.sqrt(distMeasure);
	   return distMeasure;
	} 
   
	public static float MANHATTAN(float[] attributesA, float[] attributesB) {
		distMeasure = 0;
		
		for(int i = 0; i<attributesB.length; i++){ // for each attribute calculate the difference and add it to the distance
			// only use attributes designated for training
			if(attributesA[i] != -99 && attributesB[i] != -99){
				oneAttDist = (attributesA[i]-attributesB[i]);
				if(!Float.isNaN(oneAttDist)) { 
					distMeasure += Math.abs(oneAttDist); // sum up all differences over all dimensions
				}
			}
		}
		return distMeasure;
	}
}
