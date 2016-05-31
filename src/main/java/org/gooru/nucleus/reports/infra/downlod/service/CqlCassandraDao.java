package org.gooru.nucleus.reports.infra.downlod.service;

import java.util.Collection;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.netflix.astyanax.model.ColumnList;

public interface CqlCassandraDao {

	static CqlCassandraDao instance(){
		return new CqlCassandraDaoImpl();
	}
	
	ResultSet getArchievedClassMembers(String classId);

	ResultSet getArchievedClassData(String rowKey);

	ResultSet getArchievedContentTitle(String contentId);

	ProtocolVersion getClusterProtocolVersion();

	ResultSet getArchievedUserDetails(String userId);

	ResultSet getArchievedCollectionItem(String contentId);

	ResultSet getArchievedCollectionRecentSessionId(String rowKey);
	
	ResultSet getArchievedSessionData(String sessionId);

	ColumnList<String> readByKey(String columnFamilyName, String key);

	ColumnList<String> read(String columnFamilyName, String key, Collection<String> columnList);
}
