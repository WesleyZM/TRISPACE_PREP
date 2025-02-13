package edu.sdsu.datavis.trispace.tsprep.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

public class PostgreSQLJDBC {
	public Connection c = null;
	String path = "";
	String username;
	String pw;
	
	public PostgreSQLJDBC(String url, String user, String password) {
		this.path = url;
		this.username = user;
		this.pw = password;
		try {
			Class.forName("org.postgresql.Driver");
			this.c = DriverManager.getConnection(url + "postgres", user, password);
			this.c.setAutoCommit(false);
//			this.path = url;
			System.out.println("Opened database successfully");	
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
					
	}
	
	public Connection getConnection() {
		return this.c;
	}
	
	public String findID(PostgreSQLJDBC db,String schema, String inputVector) {
		
		Statement stmt;
		try {
			stmt = this.c.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM " + schema + ".lt_a_geom;");
			String timeAlias = "";
			
			while (rs.next()) {
				timeAlias = rs.getString(2);
				if (!timeAlias.equals("-1")) {
					String[] split = timeAlias.split(",");
					for (int i = 0; i < split.length; i++) {
						if (split[i].equals(inputVector)) {
							return rs.getString(1);
						}
					}
				}
				
//				System.out.println(timeAlias);
			}
			return "";
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "";
		}
		
	}
	
	public void updateLT_A_LC(PostgreSQLJDBC db, File f, String schema) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(f.getPath()));
			
			String line = br.readLine();
			String[] split = line.split(",");
			String[] times = new String[3];
			times[0] = "t1";
			times[1] = "t2";
			times[2] = "t3";
			
			while ((line = br.readLine()) != null) {
				split = line.split(",");
				for (int i = 0; i < times.length; i++) {
					String ivID = split[0];
					String ivID2 = ivID.substring(1, ivID.length());
					String ivID3 = "l" + ivID2 + "_" + times[i];
					String neuronId = findID(db,schema,ivID3);
					
					db.updateTable(schema, "lt_a_geom", "id", neuronId, "k3_n1", split[3 + i]);
				}
				
				
//				Statement stmt = this.c.createStatement();
//				ResultSet rs = stmt.executeQuery("SELECT * FROM " + schema + ".lt_a_geom");
//				ResultSetMetaData rsmd = rs.getMetaData();
//				String[] colIds = new String[rsmd.getColumnCount()];
//				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
//					colIds[i-1] = rsmd.getColumnName(i);
//				}
//				return colIds;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
			
		
//		 updateTable(String schema, String table, String key, String id, String column, String data) {
	}
	
	/**
	 * Executes a SQL statement:
	 * executeSQL("CREATE TABLE crime.l_at;")
	 *
	 * @param	sql		- SQL statement to execute
	 * @return			true if successful, otherwise false
	 */
	public boolean executeSQL(String sql) {
		try {
			Statement stmt = c.createStatement();		
			stmt.executeUpdate(sql);
			stmt.close();
			c.commit();
			System.out.println("Update successful");
			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Update failed");
			return false;
		}
		
	}
	
	/**
	 * Creates a new table
	 *
	 * @param	name	- name of the table to create
	 * @param	cols	- array of String values containing column name & type 
	 *  				["class INTEGER", "landcover TEXT"]
	 * @return			true if successful, otherwise false
	 */
	public boolean createTable(String name, String[] cols) throws SQLException {
		String sql = "CREATE TABLE " + name;
		for (int i = 0; i < cols.length-1; i++) {
			sql += cols[i] + ", ";
		}
		sql += cols[cols.length-1];
		return executeSQL(sql);
	}
	
	/**
	 * Creates a new table
	 *
	 * @param	name		- name of the table to create
	 * @param	cols		- array of String values containing column name & type 
	 *  					["class INTEGER", "landcover TEXT"]
	 * @param	schema		- schema to create table in
	 * @return				true if successful, otherwise false
	 */
	public boolean createTable(String name, String[] cols, String schema) throws SQLException {
		String sql = "CREATE TABLE " + schema + "." + name;
		for (int i = 0; i < cols.length-1; i++) {
			sql += cols[i] + ", ";
		}
		sql += cols[cols.length-1];
		return executeSQL(sql);
	}
	
	/**
	 * Connects JDBC to a different database
	 *
	 * @param	db	- name of the database to connect to
	 * @return		true if successful, otherwise false
	 */
	public boolean changeDatabase (String db) {
		try {
			this.c = DriverManager.getConnection(this.path + db, this.username, this.pw);
			this.c.setAutoCommit(false);
			System.out.println("Connected with the " + db + " database.");
			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	/**
	 * Adds a new column to an existing PostgreSQL table
	 *
	 * @param	table		- name of the table to alter
	 * @param	columnName	- name of the new column
	 * @param	dataType	- datatype of the column
	 * @return				true if successful, otherwise false
	 */
	public boolean addColumn(String table, String columnName, String dataType) {
		String sql = "ALTER TABLE " + table + " ADD COLUMN " + columnName + " " + dataType + ";";
		return executeSQL(sql);
	}
	

	public boolean insertIntoTable(String table, String[] newData) throws SQLException {
		Statement stmt = c.createStatement();
		String[] colIds = getTableColumnIds(table);
		String allColIds = " (";
		for (int i = 0; i < colIds.length; i++) {
			allColIds = allColIds + colIds[i];
			if (i < colIds.length - 1) {
				allColIds = allColIds + ", ";
			}
		}
		allColIds = allColIds + ") ";
		
		String allData = "VALUES (";
		for (int i = 0; i < newData.length; i++) {
			allData = allData + newData[i];
			if (i < newData.length - 1) {
				allData = allData + ", ";
			}
		}
		allData = allData + ");";
		
		String sql = "INSERT INTO " + table + allColIds + allData;
//		System.out.println("table: " + table);
//		System.out.println("allColIds: " + allColIds);
//		System.out.println("allData: " + allData);
		return executeSQL(sql);
	}
	
	public void insertIntoTableBatch(String table, String[] newData) throws SQLException {
		System.out.println("Beginning Database Insertion");
		Statement stmt = c.createStatement();
		for (int i = 0; i < newData.length; i++) {
			stmt.addBatch(newData[i]);
		}
		stmt.executeBatch();
		System.out.println("Batch insertion completed.");
		c.commit();
		stmt.close();
	}
	
	public void insertIntoTableBatch(String table, String[] newData, int batchSize) throws SQLException {
		System.out.println("Beginning Database Insertion (Batch)");
		Statement stmt = c.createStatement();
		for (int i = 0; i < newData.length; i++) {
			stmt.addBatch(newData[i]);
			
			if (i % batchSize == 0) {
				stmt.executeBatch();
				System.out.println("Batch " + ((i / batchSize) + 1) + " inserted.");
			}
		}
		stmt.executeBatch();
		System.out.println("Batch insertion completed.");
		c.commit();
		stmt.close();
	}
	
	public boolean insertIntoTable(String table, String[][] newData) throws SQLException {
		
		String[] colIds = getTableColumnIds(table);
		String allColIds = " (";
		for (int j = 0; j < colIds.length; j++) {
			allColIds = allColIds + colIds[j];
			if (j < colIds.length - 1) {
				allColIds = allColIds + ", ";
			}
		}
		allColIds = allColIds + ") ";
		
		boolean returnBool = true;
		
		for (int i = 0; i < newData.length; i++) {
					
			String allData = "VALUES (";
			for (int j = 0; j < newData[i].length; j++) {
				allData = allData + newData[i][j];
				if (j < newData[i].length - 1) {
					allData = allData + ", ";
				}
			}
			allData = allData + ");";
			
			String sql = "INSERT INTO " + table + allColIds + allData;
			if (!executeSQL(sql)) {
				returnBool = false;
			}
//			return executeSQL(sql);
		}		
		return returnBool;
	}
	
	public boolean updateTable(String schema, String table, String key, String id, String column, String data) {
		String sql = "UPDATE " + schema + "." + table + " SET " + column + " = " + data + " WHERE " + key + " = '" + id + "'";
		System.out.println(sql);
		if (!executeSQL(sql)) {
			return false;
		}
		return true;
	}
	
	public boolean updateTable(String schema, String table, String[] key, String[] id, String column, String data) {
		String sql = "UPDATE " + schema + "." + table + " SET " + column + " = " + data + " WHERE " + key[0] + " = '" + id[0] + "'";
		if (key.length > 1) {
			for (int i = 1; i < key.length; i++) {
				sql += " AND " + key[i] + " = '" + id[i] + "'";
			}
		}
		
//		+ key + " = '" + id + "'";
		if (!executeSQL(sql)) {
			return false;
		}
		return true;
	}
	
	// Returns the table column ids
	public String[] getTableColumnIds(String table) throws SQLException {
		Statement stmt = c.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT * FROM " + table);
		ResultSetMetaData rsmd = rs.getMetaData();
		String[] colIds = new String[rsmd.getColumnCount()];
		for (int i = 1; i <= rsmd.getColumnCount(); i++) {
			colIds[i-1] = rsmd.getColumnName(i);
		}
		return colIds;
	}
	
	// Returns the table column labels
	public String[] getTableColumnLabels(String table) throws SQLException {
		Statement stmt = c.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT * FROM " + table);
		ResultSetMetaData rsmd = rs.getMetaData();
		String[] colIds = new String[rsmd.getColumnCount()];
		for (int i = 1; i <= rsmd.getColumnCount(); i++) {
			colIds[i-1] = rsmd.getColumnLabel(i);
		}
		rs.close();
		return colIds;
	}
	
	// Returns the table column types
	public String[] getTableColumnTypes(String table) throws SQLException {
		Statement stmt = c.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT * FROM " + table);
		ResultSetMetaData rsmd = rs.getMetaData();
		String[] colIds = new String[rsmd.getColumnCount()];
		for (int i = 1; i <= rsmd.getColumnCount(); i++) {
			colIds[i-1] = rsmd.getColumnTypeName(i);
		}
		rs.close();
		return colIds;
	}
	
	public void createCentroidsTable() throws SQLException {
		String[] tableCols = new String[2];
		tableCols[0] = "(UID TEXT PRIMARY KEY NOT NULL";
		tableCols[1] = "point_coords geometry(POINT,4326))";
		this.createTable("final_project.pixel_centroids", tableCols);
	}
	
//	public void createCentroidsTable(String espg) throws SQLException {
//		String[] tableCols = new String[2];
//		tableCols[0] = "(UID TEXT PRIMARY KEY NOT NULL";
//		tableCols[1] = "point_coords geometry(POINT," + espg + "))";
//		this.createTable("final_project.pixel_centroids", tableCols);
//	}
	
//	public void insertCentroidCoordsFromCSV(String filePath) throws IOException, SQLException {
//		BufferedReader br = new BufferedReader(new FileReader(filePath));
//		br.readLine();
//		String line = "";
//		while ((line = br.readLine()) != null) {
//			String[] data = line.split(",");
//			String[] insertData = new String[2];
//			String uid = data[0];
////			uid = uid.substring(1, uid.length());
//			insertData[0] = "'" + uid + "'";
//			insertData[1] = "ST_GeomFromText('POINT(" + data[1] + " " + data[2] + ")',4326)";
//			insertIntoTable("final_project.pixel_centroids",insertData);
//		}
//		br.close();
//	}
	
	public void queryL_ATObject(int id, int normalization) throws SQLException {
		Statement stmt = c.createStatement();
		String[] query = getTableColumnIds("final_project.l_at");
		// number of colums per normalization
		int numColsPerNorm = query.length/7;
		ResultSet rs = stmt.executeQuery( "SELECT * FROM final_project.l_at WHERE lid = 'P" + id + "';");
		int offset = normalization * numColsPerNorm;
         while ( rs.next() ) {
        	 Double[] result = new Double[numColsPerNorm];
        	 String idx = rs.getString(1);//query[0];
        	 System.out.println(idx);
        	 for (int i = 1; i <= result.length; i++) {
        		 result[i-1] = rs.getDouble(i+offset+1);
        		 System.out.println(result[i-1]);
        	 }
         }
         rs.close();
         stmt.close();
	}
	
	public void disconnect() {
		try {
			this.c.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void queryLT_AObject(int id, int time, int normalization) throws SQLException {
		Statement stmt = c.createStatement();
		String[] query = getTableColumnIds("final_project.l_at");
		ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM final_project.time_key");
		
		// number of colums per normalization
		int numColsPerNorm = query.length/7;
		int numTimes = 0;
		
		while (rs.next()) {
			numTimes = rs.getInt(1);
			numColsPerNorm /= numTimes;
		}
		rs.close();
		
		rs = stmt.executeQuery( "SELECT alias FROM final_project.time_key WHERE id = 't" + time + "';");
		String timeAlias = "";
		
		while (rs.next()) {
			timeAlias = rs.getString(1);
			System.out.println(timeAlias);
		}
		rs.close();
		rs = stmt.executeQuery( "SELECT * FROM final_project.l_at WHERE lid = 'P" + id + "';");
		int offset = normalization * numTimes*numColsPerNorm;
		int offset2 = 0;
         while ( rs.next() ) {
        	 Double[] result = new Double[numColsPerNorm];
        	 String idx = rs.getString(1) + "_" + timeAlias;//query[0];
        	 System.out.println(idx);
        	 for (int i = 1; i <= result.length; i++) {
        		 result[i-1] = rs.getDouble(i+offset+offset2+time);
        		 offset2 += 2;
        		 System.out.println(result[i-1]);
        	 }
         }	
         rs.close();
	}
	
	public void queryT_LAObject(int time, int normalization) throws SQLException {
		Statement stmt = c.createStatement();
		String[] query = getTableColumnIds("final_project.l_at");
		ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM final_project.time_key");
		
		// number of colums per normalization
		int numColsPerNorm = query.length/7;
		int numTimes = 0;
		
		while (rs.next()) {
			numTimes = rs.getInt(1);
			numColsPerNorm /= numTimes;
		}
		rs.close();
		
		rs = stmt.executeQuery( "SELECT alias FROM final_project.time_key WHERE id = 't" + time + "';");
		String timeAlias = "";
		
		while (rs.next()) {
			timeAlias = rs.getString(1);
			System.out.println(timeAlias);
		}
		rs.close();
		rs = stmt.executeQuery( "SELECT * FROM final_project.l_at;");
		int offset = normalization * numTimes*numColsPerNorm;
		
         while ( rs.next() ) {
        	 int offset2 = 0;
        	 Double[] result = new Double[numColsPerNorm];
        	 String idx = rs.getString(1);// + "_" + timeAlias;//query[0];
        	 System.out.println();
        	 System.out.println(idx);
        	 for (int i = 1; i <= result.length; i++) {
        		 result[i-1] = rs.getDouble(i+offset+offset2+time);
        		 offset2 += 2;
        		 System.out.println(result[i-1]);
        	 }
         }	
         rs.close();
	}
	
	public void queryA_LTObject(int attribute, int normalization) throws SQLException {
		Statement stmt = c.createStatement();
		String[] query = getTableColumnIds("final_project.l_at");
		ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM final_project.attribute_key");
		
		// number of colums per normalization
		int numColsPerNorm = query.length/7;
		int numAttributes = 0;
		
		while (rs.next()) {
			numAttributes = rs.getInt(1);
			numColsPerNorm /= numAttributes;
		}
		rs.close();
		
		rs = stmt.executeQuery( "SELECT alias FROM final_project.attribute_key WHERE id = 'a" + attribute + "';");
		String attributeAlias = "";
		
		while (rs.next()) {
			attributeAlias = rs.getString(1);
			System.out.println(attributeAlias);
		}
		rs.close();
		rs = stmt.executeQuery( "SELECT * FROM final_project.l_at;");
		int offset = normalization * numAttributes*numColsPerNorm;
		
         while ( rs.next() ) {
        	 int offset2 = 2*(attribute-1);
        	 Double[] result = new Double[numColsPerNorm];
        	 String idx = rs.getString(1);// + "_" + attributeAlias;//query[0];
        	 System.out.println();
        	 System.out.println(idx);
        	 for (int i = 1; i <= result.length; i++) {
        		 result[i-1] = rs.getDouble(i+offset+offset2+attribute);
//        		 offset2 += 2;
        		 System.out.println(result[i-1]);
        	 }
         }	
         rs.close();
	}
	
	public void queryAT_LObject(int attribute, int time, int normalization) throws SQLException {
		Statement stmt = c.createStatement();
		String[] query = getTableColumnIds("final_project.l_at");
		ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM final_project.attribute_key");
		
		// number of colums per normalization
		int numColsPerNorm = query.length/7;
		int numAttributes = 0;
		
		while (rs.next()) {
			numAttributes = rs.getInt(1);
			numColsPerNorm /= numAttributes;
		}
		rs.close();
		
		rs = stmt.executeQuery( "SELECT alias FROM final_project.attribute_key WHERE id = 'a" + attribute + "';");
		String attributeAlias = "";
		
		while (rs.next()) {
			attributeAlias = rs.getString(1);
//			System.out.println(attributeAlias);
		}
		rs.close();
		
		rs = stmt.executeQuery( "SELECT alias FROM final_project.time_key WHERE id = 't" + time + "';");
		String timeAlias = "";
		
		while (rs.next()) {
			timeAlias = rs.getString(1);
//			System.out.println(timeAlias);
		}
		rs.close();
		System.out.println(attributeAlias + "_" + timeAlias);
		
		rs = stmt.executeQuery( "SELECT * FROM final_project.l_at;");
		int offset = normalization * numAttributes*numColsPerNorm;
		int offset2 = 2*(attribute-1);
         while ( rs.next() ) {
        	
//        	 int offset2 = at;
        	 Double[] result = new Double[numColsPerNorm];
        	 String idx = rs.getString(1);// + "_" + attributeAlias;//query[0];
        	 System.out.println();
        	 System.out.println(idx);
        	 System.out.println(rs.getDouble(offset+attribute+time+offset2));
         }	
         rs.close();
	}
	
	public boolean schemaExists(String schema) {
		
		try {
			Statement stmt = c.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '" + schema + "';");
			
			int schemaCounter = 0;
			while (rs.next()) {
				schemaCounter++;
			}
			rs.close();
			return schemaCounter > 0;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	public int getTableLength(String schema, String table) {
		try {
			Statement stmt = c.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM " + schema + "." + table + ";");
			
			int rowCount = 0;
			while (rs.next()) {
				rowCount++;
			}
			rs.close();
			stmt.close();
			return rowCount;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
	}
	
	public String getESPG(String schema, String table) {
		try {
			Statement stmt = c.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT ST_SRID(geom) FROM " + schema + "." + table + " LIMIT 1;");
			
//			int rowCount = 0;
			String result = "";
			while (rs.next()) {
//				rowCount++;
				result = rs.getString(1);
			}
			rs.close();
			return result;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "";
		}
	}
	
	public String getObjectColumnValue(String schema, String table, String idColumn, String id, String queryColumn) {
		try {
			Statement stmt = c.createStatement();
			String statement = "SELECT " + queryColumn + " FROM " + schema + "." + table + " WHERE " + idColumn + " = '" + id + "';";
//			System.out.println(statement);
			ResultSet rs = stmt.executeQuery(statement);
			
			String result = "";
			while (rs.next()) {
				result = rs.getString(1);
			}
			rs.close();
			return result;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "";
		}
	}
	
	public String getObjectColumnValue(String schema, String table, String[] idColumn, String[] id, String queryColumn) {
		try {
			Statement stmt = c.createStatement();	
			
			String statement = "SELECT " + queryColumn + " FROM " + schema + "." + table + " WHERE " + idColumn[0] + " = '" + id[0] + "'";
			if (idColumn.length > 1) {
				for (int i = 1; i < idColumn.length; i++) {
					statement += " AND " + idColumn[i] + " = '" + id[i] + "'";
				}
			}
			statement += ";";
//			System.out.println(statement);
			ResultSet rs = stmt.executeQuery(statement);
			
			String result = "";
			while (rs.next()) {
				result = rs.getString(1);
			}
			rs.close();
			return result;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "";
		}
	}
	
	public boolean extractFishnetGeom(String schema, String id) {
		String query = "INSERT INTO " + schema + ".locus_poly "
				+ "WITH pt_table AS (SELECT * FROM " + schema + ".locus_pt WHERE id = '" + id + "'),"
				+ " poly_table AS (SELECT * FROM " + schema + ".tmp_fishnet) " 
				+ "SELECT pt_table.id, poly_table.geom FROM pt_table, poly_table WHERE ST_Contains(poly_table.geom, pt_table.geom);";
		return executeSQL(query);
	}
	
	public String extractFishnetGeomQuery(String schema, String id) {
		String query = "INSERT INTO " + schema + ".locus_poly "
				+ "WITH pt_table AS (SELECT * FROM " + schema + ".locus_pt WHERE id = '" + id + "'),"
				+ " poly_table AS (SELECT * FROM " + schema + ".tmp_fishnet) " 
				+ "SELECT pt_table.id, poly_table.geom FROM pt_table, poly_table WHERE ST_Contains(poly_table.geom, pt_table.geom);";
		return query;
	}
	
	public boolean tableExists(String schema, String table) {
		try {
			Statement stmt = c.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT EXISTS ("
					+ "SELECT 1 FROM information_schema.tables "
					+ "WHERE table_schema = '" + schema + "' "
					+ "AND table_name = '" + table + "');");
			
			String result = "";
			
			while (rs.next()) {
				result = rs.getString(1);
			}
			rs.close();
			if (result.equals("t")) {
				return true;
			} else {
				return false;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	/**
	 * Tests if a table exists
	 *
	 * @param	table	- name of the table to test
	 * @return			true if successful, otherwise false
	 */
	public boolean tableExists(String table) {
		try {
			Statement stmt = c.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT EXISTS ("
					+ "SELECT 1 FROM information_schema.tables "
					+ "WHERE table_name = '" + table + "');");
			
			String result = "";
			
			while (rs.next()) {
				result = rs.getString(1);
			}
			rs.close();
			if (result.equals("t")) {
				return true;
			} else {
				return false;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	/**
	 * Creates a new schema within the database
	 *
	 * @param	schema	- name of the schema to create
	 * @return			true if successful, otherwise false
	 */
	public boolean createSchema(String schema) {
		
//		Connection conn = DriverManager.getConnection(()
		
		boolean exists = schemaExists(schema);
		
		if (!exists) {
			try {
				Statement stmt = c.createStatement();
				String query = "CREATE SCHEMA " + schema;
//				System.out.println(query);
				stmt.execute(query);
				System.out.println("Schema created.");
				c.commit();
				stmt.close();
				return true;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		} else {
			System.out.println("Schema already exists.");
			return false;
		}				
	}
	
	public boolean columnExists(String schema, String table, String column) {		
		 
		try {
			String query = "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE "
					+ "table_schema='" + schema + "' AND table_name='" + table 
					+ "' AND column_name='" + column + "');";
			
			System.out.println(query);

			Statement stmt = this.c.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			
			while (rs.next()) {
				return rs.getBoolean(1);
			}
			return false;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}		
	}
	
	public String getColumnDataType(String schema, String table, String column) {		
		 
		try {
			String query = "SELECT data_type FROM information_schema.columns WHERE "
					+ "table_schema='" + schema + "' AND table_name='" + table 
					+ "' AND column_name='" + column + "';";
			
			System.out.println(query);

			Statement stmt = this.c.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			
			while (rs.next()) {
				return rs.getString(1);
			}
			return null;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}		
	}
	
	public boolean dropSchema(String schema) {
		boolean exists = schemaExists(schema);
		
		if (exists) {
			try {
				Statement stmt = c.createStatement();
				String query = "DROP SCHEMA " + schema;
				stmt.execute(query);
				System.out.println("Schema dropped.");
				c.commit();
				stmt.close();
				return true;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		} else {
			return false;
		}
	}
	
	public boolean dropSchemaCascade(String schema) {
		boolean exists = schemaExists(schema);
		
		if (exists) {
			try {
				Statement stmt = c.createStatement();
				String query = "DROP SCHEMA " + schema + " CASCADE";
				stmt.execute(query);
				System.out.println("Schema dropped (Cascade).");
				c.commit();
				stmt.close();
				return true;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		} else {
			return false;
		}
	}
	
	/**
	 * Drop a table from the database
	 *
	 * @param	table	- name of the table to drop
	 * @return			true if successful, otherwise false
	 */
	public boolean dropTable(String table) {
		boolean exists = tableExists(table);
		
		if (exists) {
			try {
				Statement stmt = c.createStatement();
				ResultSet rs = stmt.executeQuery("DROP TABLE " + table + ";");
				rs.close();
				return true;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		} else {
			return false;
		}
	}
	
	/**
	 * Drop a table from the database
	 *
	 * @param	schema	- name of the schema that holds table
	 * @param	table	- name of the table to drop
	 * @return			true if successful, otherwise false
	 */
	public boolean dropTable(String schema, String table) {
		boolean exists = tableExists(schema,table);
		
		if (exists) {
			try {
				Statement stmt = c.createStatement();
				ResultSet rs = stmt.executeQuery("DROP TABLE " + schema + "." + table + ";");
				rs.close();
				return true;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		} else {
			return false;
		}
	}
	
	/**
	 * Drop a table from the database using CASCADE.
	 * Drops all other related tables.
	 *
	 * @param	table	- name of the table to drop
	 * @return			true if successful, otherwise false
	 */
	public boolean dropTableCascade(String table) {
		boolean exists = tableExists(table);
		
		if (exists) {
			try {
				Statement stmt = c.createStatement();
				ResultSet rs = stmt.executeQuery("DROP TABLE " + table + " CASCADE;");
				rs.close();
				return true;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		} else {
			return false;
		}
	}
	
	/**
	 * Drop a table from the database using CASCADE.
	 * Drops all other related tables.
	 *
	 * @param	schema	- name of the schema that holds table
	 * @param	table	- name of the table to drop
	 * @return			true if successful, otherwise false
	 */
	public boolean dropTableCascade(String schema, String table) {
		boolean exists = tableExists(schema,table);
		
		if (exists) {
			try {
				Statement stmt = c.createStatement();
				ResultSet rs = stmt.executeQuery("DROP TABLE " + schema + "." + table + " CASCADE;");
				rs.close();
				return true;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		} else {
			return false;
		}
	}
	
	
	public void queryLA_TObject(int id, int attribute, int normalization) throws SQLException {
		Statement stmt = c.createStatement();
		String[] query = getTableColumnIds("final_project.l_at");
		ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM final_project.attribute_key");
		
		// number of colums per normalization
		int numColsPerNorm = query.length/7;
		int numTimes = 0;
		
		while (rs.next()) {
			numTimes = rs.getInt(1);
			numColsPerNorm /= numTimes;
		}
		rs.close();
		
		rs = stmt.executeQuery( "SELECT alias FROM final_project.attribute_key WHERE id = 'a" + attribute + "';");
		String timeAlias = "";
		
		while (rs.next()) {
			timeAlias = rs.getString(1);
			System.out.println(timeAlias);
		}
		rs.close();
		rs = stmt.executeQuery( "SELECT * FROM final_project.l_at WHERE lid = 'P" + id + "';");
		// normalization offset
		int offset = normalization * numTimes*numColsPerNorm;
		int offset2 = (attribute - 1) * 2;
         while ( rs.next() ) {
        	 Double[] result = new Double[numColsPerNorm];
        	 String idx = rs.getString(1) + "_" + timeAlias;//query[0];
        	 System.out.println(idx);
        	 for (int i = 1; i <= result.length; i++) {
        		 int thing = i+offset+attribute+offset2;
        		 result[i-1] = rs.getDouble(thing);
        		 System.out.println(result[i-1]);
        	 }
         }	
         rs.close();
	}
	
	public void createLocusAliasTable() throws SQLException, IOException {
		String[] tableCols = new String[2];
		tableCols[0] = "(ID TEXT PRIMARY KEY NOT NULL";
		tableCols[1] = "ALIAS TEXT NOT NULL)";
		this.createTable("final_project.locus_key", tableCols);
		insertLocusAlias();
	}
	
	public void insertLocusAlias() throws IOException, SQLException {
		String filePath = "./data/dictionary/locusDictionary.csv";
		BufferedReader br = new BufferedReader(new FileReader(filePath));
		String line = "";
		while ((line = br.readLine()) != null) {
			String[] data = line.split(",");
			String[] insertData = new String[2];
			insertData[0] = "'" + data[0] + "'";
			insertData[1] = "'" + data[1] + "'";
			insertIntoTable("final_project.locus_key",insertData);
		}
		br.close();		
	}
	
	public void createAttrAliasTable() throws SQLException, IOException {
		String[] tableCols = new String[2];
		tableCols[0] = "(ID TEXT PRIMARY KEY NOT NULL";
		tableCols[1] = "ALIAS TEXT NOT NULL)";
		this.createTable("final_project.attribute_key", tableCols);
		insertAttrAlias();
	}
	
	public void insertAttrAlias() throws IOException, SQLException {
		String filePath = "./data/dictionary/attDictionary.csv";
		BufferedReader br = new BufferedReader(new FileReader(filePath));
		String line = "";
		while ((line = br.readLine()) != null) {
			String[] data = line.split(",");
			String[] insertData = new String[2];
			insertData[0] = "'" + data[0] + "'";
			insertData[1] = "'" + data[1] + "'";
			insertIntoTable("final_project.attribute_key",insertData);
		}
		br.close();		
	}	
	
	public void createTimeAliasTable() throws SQLException, IOException {
		String[] tableCols = new String[2];
		tableCols[0] = "(ID TEXT PRIMARY KEY NOT NULL";
		tableCols[1] = "ALIAS TEXT NOT NULL)";
		this.createTable("final_project.time_key", tableCols);
		insertTimeAlias();
	}
	
	public void insertTimeAlias() throws IOException, SQLException {
		String filePath = "./data/dictionary/timeDictionary.csv";
		BufferedReader br = new BufferedReader(new FileReader(filePath));
		String line = "";
		while ((line = br.readLine()) != null) {
			String[] data = line.split(",");
			String[] insertData = new String[2];
			insertData[0] = "'" + data[0] + "'";
			insertData[1] = "'" + data[1] + "'";
			insertIntoTable("final_project.time_key",insertData);
		}
		br.close();		
	}
	
	public void queryTSObject(int index, String ts, int normalization) throws SQLException {
		index++;
		Statement stmt = c.createStatement();
		String[] query = getTableColumnIds("final_project.l_at");
		if (ts.equals("L_AT")) {
			// number of colums per normalization
			int numColsPerNorm = query.length/7;
			ResultSet rs = stmt.executeQuery( "SELECT * FROM final_project.l_at WHERE lid = 'P" + index + "';");
			int offset = normalization * 7;
	         while ( rs.next() ) {
	        	 Double[] result = new Double[numColsPerNorm];
	        	 String idx = rs.getString(1);//query[0];
	        	 System.out.println(idx);
	        	 for (int i = 1; i <= result.length; i++) {
	        		 result[i-1] = rs.getDouble(i+offset+1);
	        		 System.out.println(result[i-1]);
	        	 }
	         }
		} else if (ts.equals("LT_A")) {
			int numColsPerNorm = query.length/7;
			numColsPerNorm /= 3;
			System.out.println(numColsPerNorm);
		} else if (ts.equals("LA_T")) {
			int numColsPerNorm = query.length/7;
			numColsPerNorm /= 6;
			System.out.println(numColsPerNorm);
		}
	}
	
	public void createTSTable(String ts, String filePath) throws IOException, SQLException {
		
		if (ts.equals("L_AT")) {
			BufferedReader br = new BufferedReader(new FileReader(filePath));			
			String line = br.readLine();
			String[] header = line.split(",");
			line = br.readLine();
			String[] header2 = line.split(",");			
			String[] tableCols = new String[1 + (header.length-1)*7];			
			tableCols[0] = "(LID TEXT PRIMARY KEY NOT NULL";
			int index = 1;
			for (int j = 0; j < 7; j++) {
				for (int i = 1; i < header.length; i++) {
					if (j == 0) {
						tableCols[index] = header[i]+"_"+header2[i] + "_" + j + " INT NOT NULL"; 
						index++;
					} else {
						tableCols[index] = header[i]+"_"+header2[i] + "_" + j + " NUMERIC NOT NULL"; 
						index++;
					}					
				}	
			}
			
			for (int i = 0; i < tableCols.length; i++) {
				System.out.println(tableCols[i]);
			}
			tableCols[tableCols.length-1] = tableCols[tableCols.length-1] + ")";
			this.createTable("final_project.l_at", tableCols);
			br.close();
		} else if (ts.equals("AT_L")) {
			BufferedReader br = new BufferedReader(new FileReader(filePath));			
			String line = br.readLine();
			String[] header = line.split(",");
			line = br.readLine();
			String[] header2 = line.split(",");			
			String[] tableCols = new String[1 + (header.length-1)*7];			
			tableCols[0] = "(AID TEXT PRIMARY KEY NOT NULL";
			int index = 1;
			for (int j = 0; j < 7; j++) {
				for (int i = 1; i < header.length; i++) {
					if (j == 0) {
						tableCols[index] = header[i]+"_"+header2[i] + "_" + j + " INT NOT NULL"; 
						index++;
					} else {
						tableCols[index] = header[i]+"_"+header2[i] + "_" + j + " NUMERIC NOT NULL"; 
						index++;
					}					
				}	
			}
			
			for (int i = 0; i < tableCols.length; i++) {
				System.out.println(tableCols[i]);
			}
			tableCols[tableCols.length-1] = tableCols[tableCols.length-1] + ")";
			this.createTable("final_project.at_l", tableCols);
			br.close();
		}
	}
	
	public void insertTSFromCSV(String ts, String parentPath) throws IOException, SQLException {
		if (ts.equals("L_AT")) {
			String miniPath = "output_TS/transformations/L_AT.csv";
			BufferedReader[] br = new BufferedReader[7];
			br[0] = new BufferedReader(new FileReader(parentPath + "/Non_Normalized/" + miniPath));
			br[1] = new BufferedReader(new FileReader(parentPath + "/L_AT_Normalized/" + miniPath));
			br[2] = new BufferedReader(new FileReader(parentPath + "/A_LT_Normalized/" + miniPath));
			br[3] = new BufferedReader(new FileReader(parentPath + "/T_LA_Normalized/" + miniPath));
			br[4] = new BufferedReader(new FileReader(parentPath + "/LA_T_Normalized/" + miniPath));
			br[5] = new BufferedReader(new FileReader(parentPath + "/LT_A_Normalized/" + miniPath));
			br[6] = new BufferedReader(new FileReader(parentPath + "/AT_L_Normalized/" + miniPath));
			
			for (int i = 0; i < br.length; i++) {
				br[i].readLine();
				br[i].readLine();
			}
			
			String[] line =  new String[7];
			while ((line[0] = br[0].readLine()) != null) {
				for (int i = 1; i < br.length; i++) {
					line[i] = br[i].readLine();
				}
				String[][] data = new String[7][];
				for (int i = 0; i < data.length; i++) {
					data[i] = line[i].split(",");
				}
//				String[] data = line.split(",");
				String[] insertData = new String[this.getTableColumnIds("final_project.l_at").length];
				String uid = data[0][0];
//				uid = uid.substring(1, uid.length());
				insertData[0] = "'"+uid+"'";
				int counter1 = 0;
				int counter2 = 1;
				
				for (int i = 1; i < insertData.length; i++) {
					insertData[i] = data[counter1][counter2];
					if (counter2 >= data[counter1].length-1) {
						counter1++;
						counter2 = 1;
					} else {
						counter2++;
					}
				}
				insertIntoTable("final_project.l_at",insertData);
			}
			for (int i = 0; i < br.length; i++) {
				br[i].close();
			}			
		}
	}
	
	public String insertQueryBuilder(String table, String[] newData) throws SQLException {
//		Statement stmt = c.createStatement();
		String[] colIds = getTableColumnIds(table);
		String allColIds = " (";
		for (int i = 0; i < colIds.length; i++) {
			allColIds = allColIds + colIds[i];
			if (i < colIds.length - 1) {
				allColIds = allColIds + ", ";
			}
		}
		allColIds = allColIds + ") ";
		
		String allData = "VALUES (";
		for (int i = 0; i < newData.length; i++) {
			allData = allData + newData[i];
			if (i < newData.length - 1) {
				allData = allData + ", ";
			}
		}
		allData = allData + ");";
		
		String sql = "INSERT INTO " + table + allColIds + allData;
//		System.out.println("table: " + table);
//		System.out.println("allColIds: " + allColIds);
//		System.out.println("allData: " + allData);
		return sql;
	}
	
	public String updateQueryBuilder(String schema, String table, String key, String id, String column, String data) {
		String sql = "UPDATE " + schema + "." + table + " SET " + column + " = " + data + " WHERE " + key + " = '" + id + "';";

		return sql;
	}
	
	public String updateQueryBuilder(String schema, String table, String[] key, String[] id, String column, String data) {
//		String sql = "UPDATE " + schema + "." + table + " SET " + column + " = " + data + " WHERE " + key + " = '" + id + "';";
		
		
		String sql = "UPDATE " + schema + "." + table + " SET " + column + " = " + data + " WHERE " + key[0] + " = '" + id[0] + "'";
		if (key.length > 1) {
			for (int i = 1; i < key.length; i++) {
				sql += " AND " + key[i] + " = '" + id[i] + "'";
			}
		}
		
		return sql + ";";
	}
	
	
}
