package org.gooru.nucleus.reports.infra.downlod.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipOutputStream;

import org.apache.commons.lang.StringUtils;
import org.gooru.nucleus.reports.infra.component.UtilityManager;
import org.gooru.nucleus.reports.infra.constants.ColumnFamilyConstants;
import org.gooru.nucleus.reports.infra.constants.ConfigConstants;
import org.gooru.nucleus.reports.infra.constants.ExportConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

import io.vertx.core.json.JsonObject;

public class ClassExportServiceImpl implements ClassExportService {
	
	CSVFileGenerator csvFileGenerator = new CSVFileGenerator();
	ZipFileGenerator zipFileGenerator = new ZipFileGenerator();
	private CqlCassandraDao cqlDAO = CqlCassandraDao.instance();
	
	private static UtilityManager um = UtilityManager.getInstance();
	
	protected final Logger LOG = LoggerFactory.getLogger(ClassExportServiceImpl.class);

	@Override
	public JsonObject exportCsv(String classId, String courseId, String userId, String zipFileName) {
		try {
			if (StringUtils.isBlank(zipFileName)) {
				zipFileName = classId;
			}
			JsonObject result = new JsonObject();
			LOG.info("FileName : " + um.getFileSaveRealPath() + zipFileName + ConfigConstants.ZIP_EXT);
			List<String> classMembersList = getClassMembersList(classId, userId);
			ZipOutputStream zip = zipFileGenerator.createZipFile(um.getFileSaveRealPath() + zipFileName + ConfigConstants.ZIP_EXT);
			String courseTitle = getContentTitle(courseId);
			this.export(classId, courseId, null, null, null, ConfigConstants.COURSE,courseTitle,null,null,null, classMembersList, zipFileName,zip);
			for (String unitId : getCollectionItems(courseId)) {
				String unitTitle = getContentTitle(unitId);
				LOG.info("unit : " + unitTitle);
				this.export(classId, courseId, unitId, null, null, ConfigConstants.UNIT,courseTitle,unitTitle,null,null, classMembersList, zipFileName,zip);
				for (String lessonId : getCollectionItems(unitId)) {
					String lessonTitle = getContentTitle(lessonId);
					LOG.info("lesson : " + lessonTitle);
					this.export(classId, courseId, unitId, lessonId, null, ConfigConstants.LESSON,courseTitle,unitTitle,lessonTitle,null, classMembersList, zipFileName, zip);
					/*
					 * for(String assessmentId : getCollectionItems(lessonId)){
					 * LOG.info("			assessment : " + assessmentId);
					 * this.export(classId, courseId,unitId, lessonId,
					 * assessmentId, ConfigConstants.COLLECTION,
					 * userId,zipFileName); }
					 */
				}
			}
			
			um.getCacheMemory().put(zipFileName, ConfigConstants.AVAILABLE);
			LOG.info("CSV generation completed...........");
			//result.put(ConfigConstants.URL, um.getDownloadAppUrl() + zipFileName +ConfigConstants.ZIP_EXT);
			result.put(ConfigConstants.STATUS, ConfigConstants.AVAILABLE);
			zip.closeEntry();
			zip.close();
			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	private JsonObject export(String classId, String courseId, String unitId, String lessonId, String collectionId,
			String type, String courseTitle,String unitTitle, String lessonTitle, String assessmentTitle,List<String> classMembersList, String zipFileName, ZipOutputStream zip) {
		JsonObject result = new JsonObject();
		try {
			result.put(ConfigConstants.STATUS, ConfigConstants.IN_PROGRESS);
			List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
			
			
			for (String studentId : classMembersList) {
				Map<String, Object> dataMap = getDataMap();
				setUserDetails(dataMap, studentId);
				String usageRowKey = appendTilda(classId, courseId, unitId, lessonId, collectionId, studentId);
				LOG.info("usageRowKey" + usageRowKey);
				String leastTitle = getLeastTitle(courseTitle, unitTitle, lessonTitle, assessmentTitle, type);
				setDefaultUsage(leastTitle, dataMap);
				setUsageData(dataMap, leastTitle, usageRowKey, ConfigConstants.COLLECTION);
				setUsageData(dataMap, leastTitle, usageRowKey, ConfigConstants.ASSESSMENT);
				dataList.add(dataMap);
			}
			LOG.info("CSV generation started...........");
			String csvName = appendSlash(zipFileName,courseTitle,unitTitle, lessonTitle, assessmentTitle,ConfigConstants.DATA);
			String folderName =  appendSlash(zipFileName,courseTitle,unitTitle, lessonTitle, assessmentTitle);
			LOG.info("csvName:" + csvName);
			csvFileGenerator.generateCSVReport(true,folderName,csvName, dataList);
			zipFileGenerator.addFileInZip(csvName+ConfigConstants.CSV_EXT, zip);
		} catch (Exception e) {
			LOG.error("Exception while generating CSV", e);
		}
		return result;
	}

	private String getLeastTitle(String courseTitle, String unitTitle, String lessonTitle, String collectionTitle, String type) {
		String title = courseTitle;
		if (type.equalsIgnoreCase(ConfigConstants.COURSE)) {
			title = courseTitle;
		} else if (type.equalsIgnoreCase(ConfigConstants.UNIT)) {
			title = unitTitle;
		} else if (type.equalsIgnoreCase(ConfigConstants.LESSON)) {
			title = unitTitle;
		} else if (type.equalsIgnoreCase(ConfigConstants.COLLECTION)) {
			title = lessonTitle;
		}
		return title;
	}

	private List<String> getClassMembersList(String classId, String userId) {
		List<String> classMembersList = null;
		if (StringUtils.isBlank(userId)) {
			classMembersList = getClassMembers(classId);
		} else {
			classMembersList = new ArrayList<String>();
			classMembersList.add(userId);
		}
		return classMembersList;
	}

	private List<String> getCollectionItems(String contentId) {
		List<String> collectionItems = new ArrayList<String>();
		ColumnList<String> collectionItemSet = cqlDAO.readByKey(ColumnFamilyConstants.COLLECTION_ITEM_ASSOC, contentId);
		for (Column<String> column : collectionItemSet) {
			collectionItems.add(column.getName());
		}
		return collectionItems;
	}

	private List<String> getClassMembers(String classId) {
		List<String> classMembersList = new ArrayList<String>();
		ResultSet classMemberSet = cqlDAO.getArchievedClassMembers(classId);
		for (Row collectionItemRow : classMemberSet) {
			classMembersList.add(collectionItemRow.getString(ConfigConstants.COLUMN_1));
		}
		return classMembersList;
	}

	private void setUserDetails(Map<String, Object> dataMap, String userId) {
		ColumnList<String> userDetailSet = cqlDAO.readByKey(ColumnFamilyConstants.USER, userId);
		dataMap.put(ExportConstants.FIRST_NAME,
				userDetailSet.getStringValue("firstname", ConfigConstants.STRING_EMPTY));
		dataMap.put(ExportConstants.LAST_NAME, userDetailSet.getStringValue("lastname", ConfigConstants.STRING_EMPTY));
	}

	private String getContentTitle(String contentId) {
		Collection<String> resourceColumns = new ArrayList<String>();
		resourceColumns.add(ConfigConstants.TITLE);
		ColumnList<String> contentDetails = cqlDAO.read(ColumnFamilyConstants.RESOURCE, contentId, resourceColumns);
		return contentDetails.getStringValue(ConfigConstants.TITLE, ConfigConstants.STRING_EMPTY);
	}

	private String getSessionId(String rowKey) {
		String sessionId = null;
		ResultSet sessionIdSet = cqlDAO.getArchievedCollectionRecentSessionId(rowKey);
		for (Row sessionIdRow : sessionIdSet) {
			sessionId = TypeCodec.varchar().deserialize(sessionIdRow.getBytes(ConfigConstants.VALUE),
					cqlDAO.getClusterProtocolVersion());
		}
		return sessionId;
	}

	private void setUsageData(Map<String, Object> dataMap, String title, String rowKey, String collectionType) {
		String columnNames = ConfigConstants.COLUMNS_TO_EXPORT;;
		boolean splitColumnName = false;
		ColumnList<String> usageDataSet = cqlDAO.readByKey(ColumnFamilyConstants.CLASS_ACTIVITY,appendTilda(rowKey, collectionType));
		processResultSet(usageDataSet, splitColumnName, columnNames, dataMap, title, collectionType);
	}

	private void processResultSet(ColumnList<String> usageDataSet, boolean splitColumnName, String columnNames,Map<String, Object> dataMap, String title, String collectionType){
		dataMap.put(appendHyphen(title, collectionType, ExportConstants.VIEWS), usageDataSet.getLongValue("views", 0L));
		dataMap.put(appendHyphen(title, collectionType, ExportConstants.TIME_SPENT), usageDataSet.getLongValue("time_spent", 0L));
		dataMap.put(appendHyphen(title, collectionType, ExportConstants.SCORE_IN_PERCENTAGE), usageDataSet.getLongValue("score_in_percentage", 0L));
	}
	private Map<String, Object> getDataMap() {
		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		dataMap.put(ExportConstants.FIRST_NAME, "");
		dataMap.put(ExportConstants.LAST_NAME, "");
		return dataMap;
	}

	private void setDefaultUsage(String title, Map<String, Object> dataMap) {
		dataMap.put(appendHyphen(title, ConfigConstants.COLLECTION, ExportConstants.VIEWS), 0);
		dataMap.put(appendHyphen(title, ConfigConstants.ASSESSMENT, ExportConstants.VIEWS), 0);
		dataMap.put(appendHyphen(title, ConfigConstants.COLLECTION, ExportConstants.TIME_SPENT), 0);
		dataMap.put(appendHyphen(title, ConfigConstants.ASSESSMENT, ExportConstants.TIME_SPENT), 0);
		dataMap.put(appendHyphen(title, ConfigConstants.COLLECTION, ExportConstants.SCORE_IN_PERCENTAGE), 0);
		dataMap.put(appendHyphen(title, ConfigConstants.ASSESSMENT, ExportConstants.SCORE_IN_PERCENTAGE), 0);
	}

	private void setDefaultResourceUsage(String title, Map<String, Object> dataMap) {
		dataMap.put(appendHyphen(title, ExportConstants.VIEWS), 0);
		dataMap.put(appendHyphen(title, ExportConstants.TIME_SPENT), 0);
		dataMap.put(appendHyphen(title, ExportConstants.SCORE_IN_PERCENTAGE), 0);
		dataMap.put(appendHyphen(title, ExportConstants.ANSWER_STATUS), ConfigConstants.NA);
	}
	private String appendTilda(String... texts) {
		StringBuffer sb = new StringBuffer();
		for (String text : texts) {
			if (StringUtils.isNotBlank(text)) {
				if (sb.length() > 0) {
					sb.append(ConfigConstants.TILDA);
				}
				sb.append(text);
			}
		}
		return sb.toString();
	}
	private String appendSlash(String... texts) {
		StringBuffer sb = new StringBuffer();
		for (String text : texts) {
			if (StringUtils.isNotBlank(text)) {
				if (sb.length() > 0) {
					sb.append(ConfigConstants.SLASH);
				}
				sb.append(text);
			}
		}
		return sb.toString();
	}
	
	private String appendHyphen(String... texts) {
		StringBuffer sb = new StringBuffer();
		for (String text : texts) {
			if (StringUtils.isNotBlank(text)) {
				if (sb.length() > 0) {
					sb.append(ConfigConstants.HYPHEN);
				}
				sb.append(text);
			}
		}
		return sb.toString();
	}
}
