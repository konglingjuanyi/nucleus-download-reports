package org.gooru.nucleus.reports.downlod.service;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.StringUtils;
import org.gooru.nucleus.reports.download.dao.CqlCassandraDao;
import org.gooru.nucleus.reports.generator.component.CSVFileGenerator;
import org.gooru.nucleus.reports.generator.component.ZipFileGenerator;
import org.gooru.nucleus.reports.infra.component.UtilityManager;
import org.gooru.nucleus.reports.infra.constants.ColumnFamilyConstants;
import org.gooru.nucleus.reports.infra.constants.ConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;

import io.vertx.core.json.JsonObject;

public class ClassExportServiceImpl implements ClassExportService {

	private CSVFileGenerator csvFileGenerator = new CSVFileGenerator();
	
	private ZipFileGenerator zipFileGenerator = new ZipFileGenerator();
	
	private CqlCassandraDao cqlDAO = CqlCassandraDao.instance();

	private BaseService baseService = BaseService.instance();
	
	private DataService dataService = DataService.instance();
	
	private static UtilityManager um = UtilityManager.getInstance();

	protected final Logger LOG = LoggerFactory.getLogger(ClassExportServiceImpl.class);

	@Override
	public JsonObject exportCsv(String classId, String courseId, String userId, String userRole, String zipFileName)
			throws ParseException, IOException, ConnectionException, InterruptedException, ExecutionException {
		if (StringUtils.isBlank(zipFileName)) {
			zipFileName = baseService.appendHyphen(classId, userId);
		}
		JsonObject result = new JsonObject();
		LOG.debug("FileName : " + um.getFileSaveRealPath() + zipFileName + ConfigConstants.ZIP_EXT);
		List<String> classMembersList = dataService.getClassMembersList(classId, userId, userRole);
		LOG.debug("classMembersList: " + classMembersList);
		String courseTitle = dataService.getContentTitle(courseId);
		this.export(classId, courseId, null, null, null, ConfigConstants.COURSE, courseTitle, null, null, null,
				classMembersList, zipFileName);
		for (String unitId : dataService.getCollectionItems(courseId)) {
			String unitTitle = dataService.getContentTitle(unitId);
			this.export(classId, courseId, unitId, null, null, ConfigConstants.UNIT, courseTitle, unitTitle, null, null,
					classMembersList, zipFileName);
			for (String lessonId : dataService.getCollectionItems(unitId)) {
				String lessonTitle = dataService.getContentTitle(lessonId);
				this.export(classId, courseId, unitId, lessonId, null, ConfigConstants.LESSON, courseTitle, unitTitle,
						lessonTitle, null, classMembersList, zipFileName);
				this.exportCollection(classId, courseId, unitId, lessonId, ConfigConstants.COLLECTION, courseTitle,
						unitTitle, lessonTitle, classMembersList, zipFileName);
			}
		}
		um.getCacheMemory().put(zipFileName, ConfigConstants.AVAILABLE);
		result.put(ConfigConstants.STATUS, ConfigConstants.AVAILABLE);

		return result;
	}

	private JsonObject export(String classId, String courseId, String unitId, String lessonId, String collectionId,
			String type, String courseTitle, String unitTitle, String lessonTitle, String assessmentTitle,
			List<String> classMembersList, String zipFileName) throws ParseException, IOException, ConnectionException {
		JsonObject result = new JsonObject();

		result.put(ConfigConstants.STATUS, ConfigConstants.IN_PROGRESS);
		List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
		String leastTitle = baseService.getLeastTitle(courseTitle, unitTitle, lessonTitle, assessmentTitle, type);
		for (String studentId : classMembersList) {
			Map<String, Object> dataMap = baseService.getDataMap();
			dataService.setUserDetails(dataMap, studentId);
			String usageRowKey = baseService.appendTilda(classId, courseId, unitId, lessonId, collectionId, studentId);
			baseService.setDefaultUsage(leastTitle, dataMap);
			dataService.setUsageData(dataMap, leastTitle, usageRowKey, ConfigConstants.COLLECTION);
			dataService.setUsageData(dataMap, leastTitle, usageRowKey, ConfigConstants.ASSESSMENT);
			dataList.add(dataMap);
		}
		// writing into csv and add into zip
		generateFile(zipFileName, courseTitle, unitTitle, lessonTitle, assessmentTitle, leastTitle, dataList);

		return result;
	}

	private JsonObject exportCollection(String classId, String courseId, String unitId, String lessonId, String type,
			String courseTitle, String unitTitle, String lessonTitle, List<String> classMembersList, String zipFileName)
			throws ParseException, IOException, ConnectionException {
		JsonObject result = new JsonObject();
		result.put(ConfigConstants.STATUS, ConfigConstants.IN_PROGRESS);

		for (String collectionId : dataService.getCollectionItems(lessonId)) {
			List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
			List<Map<String, Object>> resourceDataList = new ArrayList<Map<String, Object>>();
			String collectionTitle = dataService.getContentTitle(collectionId);
			for (String studentId : classMembersList) {
				Map<String, Object> dataMap = baseService.getDataMap();

				String usageRowKey = baseService.appendTilda(classId, courseId, unitId, lessonId, studentId);
				ColumnList<String> usageDataSet = cqlDAO.readByKey(ColumnFamilyConstants.CLASS_ACTIVITY, usageRowKey);
				dataService.setUserDetails(dataMap, studentId);

				baseService.setDefaultCollectionUsage(collectionTitle, dataMap);
				baseService.setMetrics(usageDataSet, dataMap, collectionTitle, collectionId);
				dataList.add(dataMap);

				Map<String, Object> resourceDataMap = baseService.getDataMap();
				dataService.setUserDetails(resourceDataMap, studentId);
				exportResource(classId, courseId, unitId, lessonId, collectionId, studentId, ConfigConstants.RESOURCE,
						resourceDataMap);
				resourceDataList.add(resourceDataMap);
			}
			// writing collection/assessment into csv and add into zip
			generateFile(zipFileName, courseTitle, unitTitle, lessonTitle, collectionTitle, collectionTitle, dataList);
			// writing collection/assessment into csv and add into zip
			generateFile(zipFileName, courseTitle, unitTitle, lessonTitle, collectionTitle,
					baseService.appendHyphen(collectionTitle, ConfigConstants.ITEMS), resourceDataList);
		}

		return result;
	}

	private void generateFile(String zipFileName, String courseTitle, String unitTitle, String lessonTitle,
			String collectionTitle, String leastTitle, List<Map<String, Object>> dataList)
			throws ParseException, IOException {
		String csvName = baseService.appendSlash(zipFileName, courseTitle, unitTitle, lessonTitle, collectionTitle,
				baseService.appendHyphen(leastTitle, ConfigConstants.DATA));
		String folderName = baseService.appendSlash(zipFileName, courseTitle, unitTitle, lessonTitle, collectionTitle);
		csvFileGenerator.generateCSVReport(true, folderName, csvName, dataList);
		zipFileGenerator.zipDirectory(um.getFileSaveRealPath() + zipFileName + ConfigConstants.ZIP_EXT,
				(um.getFileSaveRealPath() + zipFileName));
	}

	private void exportResource(String classId, String courseId, String unitId, String lessonId, String collectionId,
			String studentId, String type, Map<String, Object> dataMap) throws ConnectionException {

		LOG.debug("get recent session id key : {}",
				baseService.appendTilda(ConfigConstants.RS, classId, courseId, unitId, lessonId, collectionId, studentId));

		String sessionId = dataService.getSessionId(
				baseService.appendTilda(ConfigConstants.RS, classId, courseId, unitId, lessonId, collectionId, studentId));
		LOG.debug("session id : {}", sessionId);

		ColumnList<String> usageDataSet = null;
		if (StringUtils.isNotBlank(sessionId) && !sessionId.equalsIgnoreCase(ConfigConstants.NA)) {
			usageDataSet = cqlDAO.readByKey(ColumnFamilyConstants.SESSION_ACTIVITY, sessionId);
		}
		for (String resourceId : dataService.getCollectionItems(collectionId)) {
			String resourceTitle = dataService.getContentTitle(resourceId);
			baseService.setDefaultResourceUsage(resourceTitle, dataMap);
			if (usageDataSet != null) {
				baseService.setResourceMetrics(usageDataSet, dataMap, resourceTitle, resourceId);
			}
		}
	}
}
