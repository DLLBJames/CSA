package com.ibm.isa.oppty.isa_oppty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;


public class CloudantOpptyWriter implements ItemWriter<List<DBOppty>>,ItemStream{
	static final Logger log = Logger.getLogger(CloudantOpptyWriter.class);
	
	private String url;

	private String userid;

	private String passwd;
	
	private String database;

	Connection conn = null;

	protected CloudantDataAccess dataAccess ;

	public void open(ExecutionContext executionContext)
			throws ItemStreamException {
		try {
			log.info("writer open ...... clean "+database);
			conn = getConn();
			PreparedStatement ps = conn.prepareStatement(" delete from "+database);
			ps.execute();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			log.info(" connection error or "+database+" not exist");
			e.printStackTrace();
		}
		
	}

	public void update(ExecutionContext executionContext)
			throws ItemStreamException {
		// TODO Auto-generated method stub
		
	}

	public void close() throws ItemStreamException {
		// TODO Auto-generated method stub
		log.info("writer close ......");
		
	}

	public void write(List<? extends List<DBOppty>> items) throws Exception {
		// TODO Auto-generated method stub
		log.info("write......");
		StringBuffer sqlbuffer = new StringBuffer();
		sqlbuffer.append("INSERT INTO ");
		sqlbuffer.append(database);
		sqlbuffer.append(" values (?,?,?)");
		PreparedStatement ps = null;
		ps = conn.prepareStatement(sqlbuffer.toString());
		for (List<DBOppty> item:items) {
			for(DBOppty i:item){
				ps.setString(1, i.get_id());
				ps.setString(2, i.getOpptyNum());
				ps.setString(3, i.get_rev());
				ps.addBatch();
			}
		}
		try{
		ps.executeBatch();
		ps.close();
		}
		catch(SQLException e){
			e.printStackTrace();
		}
	}

	public void setUrl(String url) {
		this.url = url;
	}


	public void setUserid(String userid) {
		this.userid = userid;
	}

	public void setPasswd(String passwd) {
		this.passwd = passwd;
	}


	public void setConn(Connection conn) {
		this.conn = conn;
	}

	public void setDataAccess(CloudantDataAccess dataAccess) {
		this.dataAccess = dataAccess;
	}
	
	private Connection getConn() throws SQLException {
		if (null != this.conn)
			return this.conn;
		// Class.forName("com.ibm.db2.jcc.DB2Driver");
		this.conn = DriverManager.getConnection(url, userid, passwd);

		return this.conn;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}
	

}
