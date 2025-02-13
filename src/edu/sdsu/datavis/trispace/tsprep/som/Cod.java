package edu.sdsu.datavis.trispace.tsprep.som;

public class Cod {
	
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
