package edu.sdsu.datavis.trispace.tsprep.utils;


import java.util.ArrayList;

/**
 * Class to represent a cluster of coordinates.
 */
public class Cluster {
	
	// Indices of the member coordinates.
    public ArrayList<Integer> memberIDs;
    
    /**
     * Constructor.
     * 
     * @param memberIndexes indices of the member coordinates.
     * @param center the cluster center.
     */
    public Cluster() {
    	memberIDs = new ArrayList<Integer>();
    }
    
    /**
     * Set the member indices.
     * 
     */
    public void setMemberIDs(int ID) {
        memberIDs.add(ID);
    }
    
    /**
     * Get the member indices.
     * 
     * @return an array containing the indices of the member coordinates.
     */
    public ArrayList<Integer> getMemberIDs() {
        return memberIDs;
    }
    
    public void printClusterMembers() {
    	for(int member : memberIDs) {
    		System.out.println("NeuronID " + member);
    	}
    }
    
}
