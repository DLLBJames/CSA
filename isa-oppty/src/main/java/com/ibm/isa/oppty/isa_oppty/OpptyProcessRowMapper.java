package com.ibm.isa.oppty.isa_oppty;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.simple.ParameterizedRowMapper;

public class OpptyProcessRowMapper  implements ParameterizedRowMapper<DBOppty>{

	public DBOppty mapRow(ResultSet rs, int rowNum) throws SQLException {
		DBOppty entity = new DBOppty();
		entity.setOpptyNum(rs.getString("num"));
		entity.set_id(rs.getString("id"));
		entity.set_rev(rs.getString("rev"));
		return entity;
	}

}
