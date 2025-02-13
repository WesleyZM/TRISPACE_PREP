package edu.sdsu.datavis.trispace.tsprep.som;

public class Dat {
	int vectorDimension;
    public InputVector[] inputVectors;
    String[] ivAttributeNames;

    public Dat(int vd, InputVector[] iv, String[] ivan) {
        vectorDimension = vd;
        inputVectors = iv;
        ivAttributeNames = ivan;
    }
}
