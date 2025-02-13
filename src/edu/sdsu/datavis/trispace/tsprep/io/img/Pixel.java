package edu.sdsu.datavis.trispace.tsprep.io.img;

public class Pixel {
	public String name;
	public int [][] bandData;
	public float [][] bandDataNormalized;
	  
	public Pixel(String nm, int years, int bands) {
		name = nm;
	    bandData = new int[years][bands];
	    bandDataNormalized = new float[years][bands];
	}
	  
	public void setData(int yr, int att, int val) {
		bandData[yr][att] = val;  
	}
	
	public void setNormData(int yr, int att, float val) {
		bandDataNormalized[yr][att] = val;  
	}
	
	public void clearNormData() {
		bandDataNormalized = new float[bandData.length][bandData[0].length];
	}
}
