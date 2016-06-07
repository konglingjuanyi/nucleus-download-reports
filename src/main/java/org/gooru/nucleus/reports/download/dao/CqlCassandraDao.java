package org.gooru.nucleus.reports.download.dao;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.ResultSet;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;

public interface CqlCassandraDao {

	static CqlCassandraDao instance(){
		return new CqlCassandraDaoImpl();
	}
	
	ResultSet getArchievedClassMembers(String classId) throws InterruptedException, ExecutionException;

	ColumnList<String> readByKey(String columnFamilyName, String key) throws ConnectionException;

	ColumnList<String> read(String columnFamilyName, String key, Collection<String> columnList) throws ConnectionException;
}
