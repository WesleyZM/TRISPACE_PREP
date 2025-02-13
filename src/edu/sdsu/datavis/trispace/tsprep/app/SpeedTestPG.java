package edu.sdsu.datavis.trispace.tsprep.app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.sdsu.datavis.trispace.tsprep.db.PostgreSQLJDBC;

public class SpeedTestPG {

	final static int NO_DATA = -9999;
	final static int[] PRIMARY_KEYS = {0,1};
	final static String SCENE = "SanElijo";
	final static String[] PERSPECTIVES = {"L_AT", "A_LT", "T_LA", "LA_T", "LT_A", "AT_L"};
	
	final static String CREDENTIALS = "./environments/postgresql.txt";
	static String url = "";
	static String user = "";
	static String pw = "";
	
	final static String SCHEMA = "newthesis";
	final static String EPSG = "4326";
	
	private static boolean loadEnvironments() {
		try {
			BufferedReader br = new BufferedReader(new FileReader(CREDENTIALS));
			
			url = br.readLine();
			user = br.readLine();
			pw = br.readLine();
			
			br.close();
			
			if (!url.equals("") && !user.equals("") && !pw.equals("")) {
				return true;
			} else {
				return false;
			}
			
		} catch (IOException e) {
			return false;
		}
	}
	
	public static void main(String[] args) {
		if (!CREDENTIALS.equals("")) {
			if (!loadEnvironments()) {
				System.out.println("Incomplete data for URL/USER/PW");
				System.out.println("System Exiting");
				System.exit(0);
			}
		}
		
		PostgreSQLJDBC db = null;
		try {
			db = new PostgreSQLJDBC(url,user,pw);
			System.out.println(db.changeDatabase("schempp17"));
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//start
		long lStartTime = System.nanoTime();
//		JSONArray q1 = queryT_LA1();
//		JSONArray q1 = queryA_LT1();
		JSONArray q1 = queryAT_L2();
		
		
		//end
	      long lEndTime = System.nanoTime();
	      
	      //time elapsed
	      long output = lEndTime - lStartTime;

//	      String timeTaken = "time in milliseconds: " + output / 1000000;
	      System.out.println("Process 1 Elapsed time in milliseconds: " + output / 1000000);
	      
			//start
			lStartTime = System.nanoTime();
//			JSONArray q2 = queryT_LA2();
//			JSONArray q2 = queryA_LT2();
			JSONArray q2 = queryAT_L1();
			
			
			//end
		      lEndTime = System.nanoTime();
		      
		      //time elapsed
		      output = lEndTime - lStartTime;

//		      String timeTaken = "time in milliseconds: " + output / 1000000;
		      System.out.println("Process 2 Elapsed time in milliseconds: " + output / 1000000);

		      boolean equals = true;
		      
		      System.out.println(q1.equals(q2));
		      int breakIdx = -1;
		      
		      for (int i = 0; i < q1.length(); i++) {
		    	  try {
					if (!q1.getJSONObject(i).equals(q2.getJSONObject(i))) {
						  equals = false;
						  breakIdx = i;
						  break;
					  }
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
////		    	  System.out.println(q1[i]);
////		    	  System.out.println(q2[i]);
////		    	  System.out.println("");
		      }
		      
		      System.out.println("SOLUTIONS ARE EQUAL? " + breakIdx);
	}
	
	public static JSONArray queryT_LA2() {
		JSONArray json = new JSONArray();
		Connection c = null;
		try {
			Class.forName("org.postgresql.Driver");
			c = DriverManager.getConnection(url + "schempp17", user, pw);
			c.setAutoCommit(false);
		} catch (ClassNotFoundException | SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		String[] data;
		try {
			Statement stmt = c.createStatement();		
			
			ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + SCHEMA + "." + "time_key;");
			int numTimes = 0;			
			while (rs.next()) {
				numTimes = rs.getInt(1);
			}
			rs.close();
			data = new String[numTimes];
			
			int numAttributes = 0;
			rs = stmt.executeQuery("SELECT COUNT(*) FROM " + SCHEMA + "." + "attribute_key;");
			while (rs.next()) {
				numAttributes = rs.getInt(1);
			}
			rs.close();
			
			int numColsPerNorm = 7 * numAttributes;
			
			int startIdx = 2;
			
			for (int i = 0; i < numTimes; i++) {
				JSONObject obj = new JSONObject();
				obj.put("id", "t" + (i+1));
				json.put(obj);
			}
				
				rs = stmt.executeQuery("SELECT * FROM " + SCHEMA + "." + "l_at;");
				ResultSetMetaData rsmd = rs.getMetaData();
				int lCount = 1;
				while (rs.next()) {
							
					startIdx = 2;
					for (int i = 0; i < numTimes; i++) {
//						String obj = data[i];
						JSONObject obj = json.getJSONObject(i);
								
						int offsetIdx = 0;			
						for (int j = 0; j < numColsPerNorm; j++) {
							int dataIdx = startIdx + offsetIdx;
							String tmpColName = rsmd.getColumnName(dataIdx);
							String[] tmpColNames = tmpColName.split("_");
//							obj = obj + "," + "l" + lCount + "_" + tmpColNames[0] + "_" + tmpColNames[2] + "," + rs.getObject(dataIdx);
							obj.put("l" + lCount + "_" + tmpColNames[0] + "_" + tmpColNames[2], rs.getObject(dataIdx));
							offsetIdx += numTimes;
						}
//						data[i] = obj;
						startIdx++;
					}
//					System.out.println(lCount++);
					lCount++;
				}
				rs.close();
				stmt.close();
				c.close();
				return json;

		} catch (SQLException | JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}		
	}
	
	public static JSONArray queryT_LA1() {
		JSONArray json = new JSONArray();
		Connection c = null;
		try {
			Class.forName("org.postgresql.Driver");
			c = DriverManager.getConnection(url + "schempp17", user, pw);
			c.setAutoCommit(false);
		} catch (ClassNotFoundException | SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		String[] data;
		
		try {
			
			Statement stmt = c.createStatement();		
			
			ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + SCHEMA + "." + "time_key;");
			int numTimes = 0;			
			while (rs.next()) {
				numTimes = rs.getInt(1);
			}
			rs.close();
			data = new String[numTimes];
			
			int numAttributes = 0;
			rs = stmt.executeQuery("SELECT COUNT(*) FROM " + SCHEMA + "." + "attribute_key;");
			while (rs.next()) {
				numAttributes = rs.getInt(1);
			}
			rs.close();
			
			int numColsPerNorm = 7 * numAttributes;
			
			int startIdx = 2;
			
			for (int i = 0; i < numTimes; i++) {
				JSONObject obj = new JSONObject();
				obj.put("id", "t" + (i+1));
//				String obj = data[i];
//				obj = "id:t" + (i+1); 
//				stmt = c.createStatement();		
				rs = stmt.executeQuery("SELECT * FROM " + SCHEMA + "." + "l_at;");
				ResultSetMetaData rsmd = rs.getMetaData();
				int lCount = 1;
				while (rs.next()) {
					int offsetIdx = 0;					
					for (int j = 0; j < numColsPerNorm; j++) {
						int dataIdx = startIdx + offsetIdx;
						String tmpColName = rsmd.getColumnName(dataIdx);
						String[] tmpColNames = tmpColName.split("_");
//						obj = obj + "," + "l" + lCount + "_" + tmpColNames[0] + "_" + tmpColNames[2] + ": " + rs.getObject(dataIdx);
//						obj.put("l" + lCount + "_" + tmpColNames[0] + "_" + tmpColNames[2], rs.getObject(dataIdx));
						obj.put("l" + lCount + "_" + tmpColNames[0] + "_" + tmpColNames[2], rs.getObject(dataIdx));
						offsetIdx += numTimes;
					}
//					System.out.println(lCount++);
					lCount++;
				}
				rs.close();
//				data
				json.put(obj);
//				data[i] = obj;
				startIdx++;
			}		
			stmt.close();
			c.close();
			return json;
		} catch (SQLException | JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}	
	}

	public static JSONArray queryA_LT1() {
		JSONArray json = new JSONArray();
		Connection c = null;
		try {
			Class.forName("org.postgresql.Driver");
			c = DriverManager.getConnection(url + "schempp17", user, pw);
			c.setAutoCommit(false);
		} catch (ClassNotFoundException | SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
		Statement stmt = c.createStatement();		
		
		ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + SCHEMA + "." + "time_key;");
		int numTimes = 0;			
		while (rs.next()) {
			numTimes = rs.getInt(1);
		}
		rs.close();
		
		int numAttributes = 0;
		rs = stmt.executeQuery("SELECT COUNT(*) FROM " + SCHEMA + "." + "attribute_key;");
		while (rs.next()) {
			numAttributes = rs.getInt(1);
		}
		rs.close();
		
		int numColsPerNorm = 7 * numTimes;
		
		int colCombo = numTimes * numAttributes;
		
		int startIdx = 2;
		
		for (int i = 0; i < numAttributes; i++) {
			JSONObject obj = new JSONObject();
			obj.put("id", "a" + (i+1));
			json.put(obj);
		}
			
			rs = stmt.executeQuery("SELECT * FROM " + SCHEMA + "." + "l_at;");
			ResultSetMetaData rsmd = rs.getMetaData();
			int lCount = 1;
			while (rs.next()) {
				
				startIdx = 2;
				for (int i = 0; i < numAttributes; i++) {
					int offsetIdx = 0;					
					int normOffsetIdx = 0;
					JSONObject obj = json.getJSONObject(i);
					for (int j = 0; j < numColsPerNorm; j++) {
						int dataIdx = startIdx + offsetIdx + (normOffsetIdx * colCombo);
						String tmpColName = rsmd.getColumnName(dataIdx);
						String[] tmpColNames = tmpColName.split("_");
						obj.put("l" + lCount + "_" + tmpColNames[1] + "_" + tmpColNames[2], rs.getObject(dataIdx));
						offsetIdx++;
						if (offsetIdx >= 3) {
							offsetIdx = 0;
							normOffsetIdx++;
						}
					}
					startIdx+=numTimes;
				}
				
				lCount++;
			}
			rs.close();
		stmt.close();
		c.close();
		return json;
	} catch (SQLException | JSONException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		return json;
	}	
	}
	
	public static JSONArray queryA_LT2() {
		JSONArray json = new JSONArray();
		Connection c = null;
		try {
			Class.forName("org.postgresql.Driver");
			c = DriverManager.getConnection(url + "schempp17", user, pw);
			c.setAutoCommit(false);
		} catch (ClassNotFoundException | SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
Statement stmt = c.createStatement();		
			
			ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + SCHEMA + "." + "time_key;");
			int numTimes = 0;			
			while (rs.next()) {
				numTimes = rs.getInt(1);
			}
			rs.close();
			
			int numAttributes = 0;
			rs = stmt.executeQuery("SELECT COUNT(*) FROM " + SCHEMA + "." + "attribute_key;");
			while (rs.next()) {
				numAttributes = rs.getInt(1);
			}
			rs.close();
			
			int numColsPerNorm = 7 * numTimes;
			
			int colCombo = numTimes * numAttributes;
			
			int startIdx = 2;
			
			for (int i = 0; i < numAttributes; i++) {
				JSONObject obj = new JSONObject();
				obj.put("id", "a" + (i+1));
				
				rs = stmt.executeQuery("SELECT * FROM " + SCHEMA + "." + "l_at;");
				ResultSetMetaData rsmd = rs.getMetaData();
				int lCount = 1;
				while (rs.next()) {
					int offsetIdx = 0;					
					int normOffsetIdx = 0;
					for (int j = 0; j < numColsPerNorm; j++) {
						int dataIdx = startIdx + offsetIdx + (normOffsetIdx * colCombo);
						String tmpColName = rsmd.getColumnName(dataIdx);
						String[] tmpColNames = tmpColName.split("_");
						obj.put("l" + lCount + "_" + tmpColNames[1] + "_" + tmpColNames[2], rs.getObject(dataIdx));
						offsetIdx++;
						if (offsetIdx >= 3) {
							offsetIdx = 0;
							normOffsetIdx++;
						}
					}
					lCount++;
				}
				rs.close();
				json.put(obj);
				startIdx+=numTimes;
			}		
			stmt.close();
			return json;
		} catch (SQLException | JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return json;
		}		
		
	}
	
	public static JSONArray queryAT_L1() {
		JSONArray json = new JSONArray();
		Connection c = null;
		try {
			Class.forName("org.postgresql.Driver");
			c = DriverManager.getConnection(url + "schempp17", user, pw);
			c.setAutoCommit(false);
		} catch (ClassNotFoundException | SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			Statement stmt = c.createStatement();

			ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + SCHEMA + "." + "time_key;");
			int numTimes = 0;
			while (rs.next()) {
				numTimes = rs.getInt(1);
			}
			rs.close();

			int numAttributes = 0;
			rs = stmt.executeQuery("SELECT COUNT(*) FROM " + SCHEMA + "." + "attribute_key;");
			while (rs.next()) {
				numAttributes = rs.getInt(1);
			}
			rs.close();

			int colCombo = numTimes * numAttributes;

			int startIdx = 2;

			for (int i = 0; i < numAttributes; i++) {
				String at_lObjPrefix = "a" + (i + 1);
				// int startIdx = 2;

				for (int j = 0; j < numTimes; j++) {
					String at_lObject = at_lObjPrefix + "_t" + (j + 1);

					JSONObject obj = new JSONObject();
					obj.put("id", at_lObject);

					rs = stmt.executeQuery("SELECT * FROM " + SCHEMA + "." + "l_at;");
					int lCount = 1;
					while (rs.next()) {
						for (int k = 0; k < 7; k++) {
							int dataIdx = startIdx + j + (k * colCombo) + (i * 3);
							obj.put("l" + lCount + "_n" + (k + 1), rs.getObject(dataIdx));
						}
						lCount++;
					}
					rs.close();
					json.put(obj);
				}
			}
			stmt.close();
			return json;
		} catch (SQLException | JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return json;
		}
		
	}
	
	public static JSONArray queryAT_L2() {
		JSONArray json = new JSONArray();
		Connection c = null;
		try {
			Class.forName("org.postgresql.Driver");
			c = DriverManager.getConnection(url + "schempp17", user, pw);
			c.setAutoCommit(false);
		} catch (ClassNotFoundException | SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		 try {
			 Statement stmt = c.createStatement();
			 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + SCHEMA + "." + "time_key;");
				int numTimes = 0;
				while (rs.next()) {
					numTimes = rs.getInt(1);
				}
				rs.close();

				int numAttributes = 0;
				rs = stmt.executeQuery("SELECT COUNT(*) FROM " + SCHEMA + "." + "attribute_key;");
				while (rs.next()) {
					numAttributes = rs.getInt(1);
				}
				rs.close();

				int colCombo = numTimes * numAttributes;

				int startIdx = 2;

				for (int i = 0; i < numAttributes; i++) {
					String at_lObjPrefix = "a" + (i + 1);
					// int startIdx = 2;

					for (int j = 0; j < numTimes; j++) {
						String at_lObject = at_lObjPrefix + "_t" + (j + 1);

						JSONObject obj = new JSONObject();
						obj.put("id", at_lObject);
						json.put(obj);
					}
				}

				rs = stmt.executeQuery("SELECT * FROM " + SCHEMA + "." + "l_at;");
				int lCount = 1;
				while (rs.next()) {
					startIdx = 2;
					int idCounter = 0;
					for (int i = 0; i < numAttributes; i++) {
						for (int j = 0; j < numTimes; j++) {
							JSONObject obj = json.getJSONObject(idCounter);
							for (int k = 0; k < 7; k++) {
								int dataIdx = startIdx + j + (k * colCombo) + (i * 3);
								obj.put("l" + lCount + "_n" + (k + 1), rs.getObject(dataIdx));
							}
							idCounter++;
						}
					}
					
					lCount++;
				}
				rs.close();
//				json.put(obj);
					
				
				stmt.close();
				c.close();
				return json;
		 } catch (SQLException | JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return json;
			}
	}
}
