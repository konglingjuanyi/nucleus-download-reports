package org.gooru.nucleus.reports.downlod.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

public interface DataService {

	static DataService instance(){
		 return new DataServiceImpl();
	}

	void setUsageData(Map<String, Object> dataMap, String title, String rowKey, String collectionType)
			throws ConnectionException;

	String getSessionId(String rowKey) throws ConnectionException;

	String getContentTitle(String contentId) throws ConnectionException;

	void setUserDetails(Map<String, Object> dataMap, String userId) throws ConnectionException;

	List<String> getClassMembers(String classId) throws InterruptedException, ExecutionException;

	List<String> getCollectionItems(String contentId) throws ConnectionException;

	List<String> getClassMembersList(String classId, String userId, String userRole)
			throws InterruptedException, ExecutionException;

	String getUserRole(String classId, String userId) throws ConnectionException;

}
