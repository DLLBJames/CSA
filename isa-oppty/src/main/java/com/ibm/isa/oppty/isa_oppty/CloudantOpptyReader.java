package com.ibm.isa.oppty.isa_oppty;

import java.sql.Connection;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import com.cloudant.client.api.Database;
import com.cloudant.client.api.model.FindByIndexOptions;


public class CloudantOpptyReader implements ItemReader<List<DBOppty>>,ItemStream{

	static final Logger log = Logger.getLogger(CloudantOpptyReader.class);
	protected String dburl; // 

	protected String dbusername; // database username

	protected String dbpassword; // database password
	
	protected String database;
	
	protected Connection collection;
	
	protected CloudantDataAccess dataAccess ;
	
	protected Integer skip ;
	
	protected Integer limit ;
	 

	public void setDburl(String dburl) {
		this.dburl = dburl;
	}
	public void setDbusername(String dbusername) {
		this.dbusername = dbusername;
	}
	public void setDbpassword(String dbpassword) {
		this.dbpassword = dbpassword;
	}

	public void setSkip (Integer skip){
		this.skip = skip;
	}
	
	
	public String getDatabase() {
		return database;
	}
	public void setDatabase(String database) {
		this.database = database;
	}
	public void setLimit(Integer limit) {
		this.limit = limit;
	}
	public CloudantDataAccess getDataAccess() {
		return dataAccess;
	}
	public void setDataAccess(CloudantDataAccess dataAccess) {
		this.dataAccess = dataAccess;
	}
	public Connection getCollection () throws Exception{
		return dataAccess.getConn();
	}

	public void open(ExecutionContext executionContext)
			throws ItemStreamException {
		log.info("reader open......");
		if(executionContext.containsKey("skip")){
			skip = new Long(executionContext.getLong("skip")).intValue();
		}else{
			skip = 0;
		}
		
	}
	public void update(ExecutionContext executionContext)
			throws ItemStreamException {
		executionContext.putLong("skip",skip);
		
	}
	public void close() throws ItemStreamException {
		// TODO Auto-generated method stub
		log.info("reader close......");
		
	}
	public List<DBOppty> read() throws Exception, UnexpectedInputException,
			ParseException, NonTransientResourceException {
		log.info("read......");
		JSONObject selector = new JSONObject();
		JSONObject index = new JSONObject();
		index.put("$gt", index.NULL);
		selector.put("opptyNum",index);
		Database db = dataAccess.getDBCollection(database);
		List<DBOppty> list = db.findByIndex(selector.toString(),DBOppty.class,new FindByIndexOptions().fields("opptyNum").fields("_id").fields("_rev").limit(limit).skip(skip));
		if(list!=null&&list.size()>0){
			skip = skip+list.size();
			return list;
		}else{
			return null;
		}
	}
	



} 