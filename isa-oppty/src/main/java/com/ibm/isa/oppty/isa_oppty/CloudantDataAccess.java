package com.ibm.isa.oppty.isa_oppty;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.cloudant.client.api.ClientBuilder;
import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.Database;
import com.cloudant.client.api.Replication;
import com.cloudant.client.api.model.Index;

public class CloudantDataAccess{
	
	public String dburl; // Cloudent database url:

	public String dbusername; // Cloudent database username

	public String dbpassword; // Cloudent database password
	
	Connection conn = null;

	private String url;
	
	private String userid;
	
	private String passwd;
	
	private String schema;
	
	public String getDburl() {
		return dburl;
	}

	public void setDburl(String dburl) {
		this.dburl = dburl;
	}

	public String getDbusername() {
		return dbusername;
	}

	public void setDbusername(String dbusername) {
		this.dbusername = dbusername;
	}

	public String getDbpassword() {
		return dbpassword;
	}

	public void setDbpassword(String dbpassword) {
		this.dbpassword = dbpassword;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUserid() {
		return userid;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public String getPasswd() {
		return passwd;
	}

	public void setPasswd(String passwd) {
		this.passwd = passwd;
	}
	
	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public Database getDBCollection(String database) {
		CloudantClient client = null;
		try {
			client = ClientBuilder.url(new URL(dburl))
					.username(dbusername)
					.password(dbpassword)
					.build();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		Database db = client.database(database, true);
		return db;
	}
	
	public void createDatabase(String database) {
		CloudantClient client = null;
		try {
			client = ClientBuilder.url(new URL(dburl))
					.username(dbusername)
					.password(dbpassword)
					.build();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		client.createDB(database);
	}

	
	
	public void deleteDatabase(String dataBass){
		CloudantClient client = null;
		try {
			client = ClientBuilder.url(new URL(dburl))
					.username(dbusername)
					.password(dbpassword)
					.build();
			client.deleteDB(dataBass);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}
	
	public void createIndex(Database db,String fieldName) {
		org.json.JSONObject queryJson = new org.json.JSONObject();
		org.json.JSONObject fields = new org.json.JSONObject();
		String[] field = {fieldName};
		fields.put("fields", field);
		queryJson.put("index", fields);
		queryJson.put("type", "json");
		
		List<Index> indexList = db.listIndices();
		List<String> indexNames = new ArrayList<String>(); 
		for(Index index: indexList){
			indexNames.add(index.getName());
		}
		if(!indexNames.contains("opptyNum"+"_index")){
			db.createIndex(queryJson.toString());
		}
	}
		
	
	
	public Connection getConn() throws SQLException {
		if (null != this.conn)
			return this.conn;

		// Class.forName("com.ibm.db2.jcc.DB2Driver");
		this.conn = DriverManager.getConnection(url, userid, passwd);

		return this.conn;
	}
	
	public void replication(String source, String target) throws Exception{
		CloudantClient client = null;
		try {
			client = ClientBuilder.url(new URL(dburl))
					.username(dbusername)
					.password(dbpassword)
					.build();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}

		Replication rep = client.replication();
		rep.source(source).target(target).createTarget(false).trigger(); 
	}
	
	
	public boolean dbExists(String dataBass){
		CloudantClient client = null;
		try {
			client = ClientBuilder.url(new URL(dburl)).username(dbusername)
					.password(dbpassword).build();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		List<String> dbs = client.getAllDbs();
		if (dbs.size() > 0) {
			if (dbs.contains(dataBass)) {
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}
	
	public void createSubOpptyIndex(Database db,String fieldName) {
		org.json.JSONObject queryJson = new org.json.JSONObject();
		org.json.JSONObject fields = new org.json.JSONObject();
		String[] field = {fieldName};
		fields.put("fields", field);
		queryJson.put("index", fields);
		queryJson.put("type", "json");
		queryJson.put("name", fieldName);
		
		List<Index> indexList = db.listIndices();
		List<String> indexNames = new ArrayList<String>(); 
		for(Index index: indexList){
			indexNames.add(index.getName());
		}
		if(!indexNames.contains("repEmail")){
			db.createIndex(queryJson.toString());
		}
	}
	
	public boolean getSubOpptyIndex(Database db,String fieldName) {
		
		boolean result = false;
		
		List<Index> indexList = db.listIndices();
		List<String> indexNames = new ArrayList<String>(); 
		for(Index index: indexList){
			indexNames.add(index.getName());
		}
		if(indexNames.contains("SN")&&indexNames.contains("opptyNum")){
			result =true;
		}
		return result;
	}
	
}
