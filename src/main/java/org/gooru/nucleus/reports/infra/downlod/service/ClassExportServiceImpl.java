package org.gooru.nucleus.reports.infra.downlod.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
			LOG.debug("FileName : " + um.getFileSaveRealPath() + zipFileName + ConfigConstants.ZIP_EXT);
			List<String> classMembersList = getClassMembersList(classId, userId);
			String courseTitle = getContentTitle(courseId);
			this.export(classId, courseId, null, null, null, ConfigConstants.COURSE,courseTitle,null,null,null, classMembersList, zipFileName);
			for (String unitId : getCollectionItems(courseId)) {
				String unitTitle = getContentTitle(unitId);
				this.export(classId, courseId, unitId, null, null, ConfigConstants.UNIT,courseTitle,unitTitle,null,null, classMembersList, zipFileName);
				for (String lessonId : getCollectionItems(unitId)) {
					String lessonTitle = getContentTitle(lessonId);
					this.export(classId, courseId, unitId, lessonId, null, ConfigConstants.LESSON,courseTitle,unitTitle,lessonTitle,null, classMembersList, zipFileName);
					this.exportCollection(classId, courseId, unitId, lessonId,  ConfigConstants.COLLECTION, courseTitle, unitTitle, lessonTitle,  classMembersList, zipFileName);
				}
			}
			um.getCacheMemory().put(zipFileName, ConfigConstants.AVAILABLE);
			result.put(ConfigConstants.STATUS, ConfigConstants.AVAILABLE);
			
			return result;
		} catch (Exception e) {
			LOG.error("exception",e);
		}
		return new JsonObject();
	}
	private JsonObject export(String classId, String courseId, String unitId, String lessonId, String collectionId,
			String type, String courseTitle, String unitTitle, String lessonTitle, String assessmentTitle,
			List<String> classMembersList, String zipFileName) {
		JsonObject result = new JsonObject();
		try {
			result.put(ConfigConstants.STATUS, ConfigConstants.IN_PROGRESS);
			List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
			String leastTitle = getLeastTitle(courseTitle, unitTitle, lessonTitle, assessmentTitle, type);
			for (String studentId : classMembersList) {
				Map<String, Object> dataMap = getDataMap();
				setUserDetails(dataMap, studentId);
				String usageRowKey = appendTilda(classId, courseId, unitId, lessonId, collectionId, studentId);
				setDefaultUsage(leastTitle, dataMap);
				setUsageData(dataMap, leastTitle, usageRowKey, ConfigConstants.COLLECTION);
				setUsageData(dataMap, leastTitle, usageRowKey, ConfigConstants.ASSESSMENT);
				dataList.add(dataMap);
			}
			//writing into csv and add into zip
			generateFile(zipFileName, courseTitle, unitTitle, lessonTitle, assessmentTitle, leastTitle, dataList);
		} catch (Exception e) {
			LOG.error("Exception while preparing CUL data list for CSV", e);
		}
		return result;
	}

	private JsonObject exportCollection(String classId, String courseId, String unitId, String lessonId,
			String type, String courseTitle, String unitTitle, String lessonTitle,
			List<String> classMembersList, String zipFileName) {
		JsonObject result = new JsonObject();
		try {
			result.put(ConfigConstants.STATUS, ConfigConstants.IN_PROGRESS);

			for (String collectionId : getCollectionItems(lessonId)) {
				List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
				String collectionTitle = getContentTitle(collectionId);
				for (String studentId : classMembersList) {
					Map<String, Object> dataMap = getDataMap();
					String usageRowKey = appendTilda(classId, courseId, unitId, lessonId, studentId);
					ColumnList<String> usageDataSet = cqlDAO.readByKey(ColumnFamilyConstants.CLASS_ACTIVITY,
							usageRowKey);
					setUserDetails(dataMap, studentId);
					setDefaultResourceUsage(collectionTitle, dataMap);
					setMetrics(usageDataSet, dataMap, collectionTitle, collectionId);
					dataList.add(dataMap);
				}
				//writing into csv and add into zip
				generateFile(zipFileName, courseTitle, unitTitle, lessonTitle, collectionTitle, collectionTitle, dataList);
			}
		} catch (Exception e) {
			LOG.error("Exception while preparing collection/assessment data list for CSV", e);
		}
		return result;
	}
	private void generateFile(String zipFileName,String courseTitle,String unitTitle,String lessonTitle,String collectionTitle,String leastTitle,List<Map<String, Object>> dataList){
		String csvName = appendSlash(zipFileName, courseTitle, unitTitle, lessonTitle, collectionTitle,
				appendHyphen(leastTitle,ConfigConstants.DATA));
		String folderName = appendSlash(zipFileName, courseTitle, unitTitle, lessonTitle, collectionTitle);
		try {
			csvFileGenerator.generateCSVReport(true, folderName, csvName, dataList);
		} catch (Exception e) {
			LOG.error("exception while writing into csv", e);
		}
		try {
			zipFileGenerator.zipDirectory(um.getFileSaveRealPath() + zipFileName + ConfigConstants.ZIP_EXT,
					(um.getFileSaveRealPath() + zipFileName));
		} catch (Exception e) {
			LOG.error("exception while generating zip", e);
		}
	}
	private JsonObject exportResource(String classId, String courseId, String unitId, String lessonId, String collectionId,
			String type, String courseTitle, String unitTitle, String lessonTitle, String assessmentTitle,
			List<String> classMembersList, String zipFileName) {
		return null;
	}
	private String getLeastTitle(String courseTitle, String unitTitle, String lessonTitle, String collectionTitle, String type) {
		String title = courseTitle;
		if (type.equalsIgnoreCase(ConfigConstants.COURSE)) {
			title = courseTitle;
		} else if (type.equalsIgnoreCase(ConfigConstants.UNIT)) {
			title = unitTitle;
		} else if (type.equalsIgnoreCase(ConfigConstants.LESSON)) {
			title = lessonTitle;
		} else if (type.equalsIgnoreCase(ConfigConstants.COLLECTION)) {
			title = collectionTitle;
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
		ColumnList<String> usageDataSet = cqlDAO.readByKey(ColumnFamilyConstants.CLASS_ACTIVITY,appendTilda(rowKey, collectionType));
		long views = 0;
		long scoreInPercentage = 0;
		long timespent = 0;
		for (Column<String> metricsColumn : usageDataSet) {
			if (!metricsColumn.getName().matches(ConfigConstants.IGNORE_COLUMNS)) {
				if (metricsColumn.getName().endsWith(ConfigConstants.VIEWS)) {
					views += metricsColumn.getLongValue();
				} else if (metricsColumn.getName().endsWith(ConfigConstants.SCORE_IN_PERCENTAGE)) {
					scoreInPercentage += metricsColumn.getLongValue();
				} else if (metricsColumn.getName().endsWith(ConfigConstants.TIME_SPENT)) {
					timespent += metricsColumn.getLongValue();
				}
			}
		}
		scoreInPercentage = views != 0 ? (scoreInPercentage / views) : 0;
		dataMap.put(appendHyphen(title, collectionType, ExportConstants.VIEWS), views);
		dataMap.put(appendHyphen(title, collectionType, ExportConstants.TIME_SPENT), timespent);
		dataMap.put(appendHyphen(title, collectionType, ExportConstants.SCORE_IN_PERCENTAGE), scoreInPercentage);
	}
	
	private void setMetrics(ColumnList<String> usageDataSet,Map<String, Object> dataMap, String title,String leastId){
		dataMap.put(appendHyphen(title, ExportConstants.VIEWS), usageDataSet.getLongValue(appendTilda(leastId,ConfigConstants.VIEWS),0L));
		dataMap.put(appendHyphen(title, ExportConstants.TIME_SPENT), usageDataSet.getLongValue(appendTilda(leastId,ConfigConstants.TIME_SPENT),0L));
		dataMap.put(appendHyphen(title, ExportConstants.SCORE_IN_PERCENTAGE), usageDataSet.getLongValue(appendTilda(leastId,ConfigConstants.SCORE_IN_PERCENTAGE),0L));
	}
	
	

	private Map<String, Object> getDataMap() {
		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		dataMap.put(ExportConstants.FIRST_NAME, "");
		dataMap.put(ExportConstants.LAST_NAME, "");
		return dataMap;
	}

	private void setDefaultUsage(String title, Map<String, Object> dataMap) {
		dataMap.put(appendHyphen(ConfigConstants.COLLECTION, ExportConstants.VIEWS), 0);
		dataMap.put(appendHyphen(ConfigConstants.COLLECTION, ExportConstants.TIME_SPENT), 0);
		dataMap.put(appendHyphen(ConfigConstants.COLLECTION, ExportConstants.SCORE_IN_PERCENTAGE), 0);
		dataMap.put(appendHyphen(ConfigConstants.ASSESSMENT, ExportConstants.VIEWS), 0);
		dataMap.put(appendHyphen(ConfigConstants.ASSESSMENT, ExportConstants.TIME_SPENT), 0);
		dataMap.put(appendHyphen(ConfigConstants.ASSESSMENT, ExportConstants.SCORE_IN_PERCENTAGE), 0);
	}

	private void setDefaultResourceUsage(String title, Map<String, Object> dataMap) {
		/*dataMap.put(appendHyphen(title, ExportConstants.VIEWS), 0);
		dataMap.put(appendHyphen(title, ExportConstants.TIME_SPENT), 0);
		dataMap.put(appendHyphen(title, ExportConstants.SCORE_IN_PERCENTAGE), 0);*/
		dataMap.put(ExportConstants.VIEWS, 0);
		dataMap.put(ExportConstants.TIME_SPENT, 0);
		dataMap.put(ExportConstants.SCORE_IN_PERCENTAGE, 0);
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
