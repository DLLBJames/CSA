package com.ibm.isa.oppty.isa_oppty;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;

import com.cloudant.client.api.Database;


public class OpptyProcessWriter implements ItemWriter<DBOppty>,ItemStream{
	static final Logger log = Logger.getLogger(OpptyProcessWriter.class);
	protected String dburl; // 

	protected String dbusername; // database username

	protected String dbpassword; // database password
	
	protected String database;
	
	protected CloudantDataAccess dataAccess ;

	public void open(ExecutionContext executionContext)
			throws ItemStreamException {
			log.info("writer open ......");
	}

	public void update(ExecutionContext executionContext)
			throws ItemStreamException {
		// TODO Auto-generated method stub
		
	}

	public void close() throws ItemStreamException {
		// TODO Auto-generated method stub
		log.info("writer close ......");
		
	}

	public void write(List<? extends DBOppty> items) throws Exception {
		// TODO Auto-generated method stub
		log.info("write......");
		if(dataAccess.dbExists(database)){
			Database db = dataAccess.getDBCollection(database);
				for(DBOppty oppty : items){
					db.remove(oppty.get_id(), oppty.get_rev());
				}
			
			
		}
	}

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

	public CloudantDataAccess getDataAccess() {
		return dataAccess;
	}

	public void setDataAccess(CloudantDataAccess dataAccess) {
		this.dataAccess = dataAccess;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	

}
