package org.gooru.nucleus.reports.infra.routes;

import org.gooru.nucleus.reports.infra.constants.ColumnFamilyConstants;
import org.gooru.nucleus.reports.infra.constants.ConfigConstants;
import org.gooru.nucleus.reports.infra.constants.HttpConstants;
import org.gooru.nucleus.reports.infra.constants.MessageConstants;
import org.gooru.nucleus.reports.infra.constants.MessagebusEndpoints;
import org.gooru.nucleus.reports.infra.constants.RouteConstants;
import org.gooru.nucleus.reports.infra.downlod.service.CqlCassandraDao;
import org.gooru.nucleus.reports.infra.routes.util.RouteRequestUtility;
import org.gooru.nucleus.reports.infra.routes.util.RouteResponseUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.model.ColumnList;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;

/**
 * Created by ashish on 26/4/16.
 */
final class RouteDownloadReportConfigurator implements RouteConfigurator {

	private static final Logger LOGGER = LoggerFactory.getLogger(RouteDownloadReportConfigurator.class);
	private CqlCassandraDao cqlDAO = CqlCassandraDao.instance();
	private String userRole = null;
	 
	@Override
	public void configureRoutes(Vertx vertx, Router router, JsonObject config) {
		final EventBus eb = vertx.eventBus();
		final long mbusTimeout = config.getLong(ConfigConstants.MBUS_TIMEOUT, 30L);
		router.get(RouteConstants.DOWNLOAD_REQUEST).handler(routingContext -> {
			String classId = routingContext.request().getParam(RouteConstants.CLASS_ID);
			String courseId = routingContext.request().getParam(RouteConstants.COURSE_ID);
			String userId = routingContext.request().getParam(RouteConstants.USER_ID);
			LOGGER.info("classId : " + classId + " - courseId:" + courseId + " - userId : " + userId);
			if (!isTeacher(classId, userId)) {
				if (!isStudent(classId, userId)) {
					LOGGER.info("user is not a valide teacher or student...");
					routingContext.response().setStatusCode(HttpConstants.HttpStatus.UNAUTHORIZED.getCode())
					.setStatusMessage(HttpConstants.HttpStatus.UNAUTHORIZED.getMessage()).end();
				}
			} else {
				LOGGER.info("User authrized. Process csv generation...");
				DeliveryOptions options = new DeliveryOptions().setSendTimeout(mbusTimeout * 1000);
				routingContext.response().putHeader("content-type", "application/json; charset=utf-8");
				JsonObject rru = new RouteRequestUtility().getBodyForMessage(routingContext);
				rru.put(RouteConstants.USER_ROLE, userRole);
				eb.send(MessagebusEndpoints.MBEP_DOWNLOAD_REQUEST, rru, options,
						reply -> new RouteResponseUtility().responseHandler(routingContext, reply, LOGGER));
			}
		});

		router.get(RouteConstants.DOWNLOAD_FILE).handler(routingContext -> {
			String classId = routingContext.request().getParam(RouteConstants.CLASS_ID);
			String courseId = routingContext.request().getParam(RouteConstants.COURSE_ID);
			String userId = routingContext.request().getParam(RouteConstants.USER_ID);
			LOGGER.info("classId : " + classId + " - courseId:" + courseId + " - userId : " + userId);
			try {
				Buffer zipFile = vertx.fileSystem().readFileBlocking(
						config.getString(ConfigConstants.FILE_SAVE_REAL_PATH) + classId + ConfigConstants.ZIP_EXT);
				routingContext.response().putHeader("Content-Length", "" + zipFile.length());
				routingContext.response().putHeader("content-type", "application/zip");
				routingContext.response().putHeader("Content-Disposition",
						"attachment; filename=\"" + classId + ConfigConstants.ZIP_EXT + "\"");
				routingContext.response().write(zipFile);
				routingContext.response().setStatusCode(200);
			} catch (Exception e) {
				routingContext.response().setStatusCode(404);
				LOGGER.error("Exception ", e);
			}
			routingContext.response().end();
		});

	}
	private boolean isTeacher(String classId, String userId) {
		boolean isTeacher = false;
		String teacherId = null;
		ColumnList<String> classData = cqlDAO.readByKey(ColumnFamilyConstants.CLASS, classId);
		if (classData != null) {
			teacherId = classData.getStringValue(ConfigConstants._CREATOR_UID, null);
			LOGGER.debug("teacherId : " + teacherId);
		}
		if (teacherId != null && userId.equalsIgnoreCase(teacherId)) {
			userRole = MessageConstants.MSG_TEACHER;
			isTeacher = true;
		}
		LOGGER.debug("isTeacher : " + isTeacher);
		return isTeacher;
	}
	private boolean isStudent(String classId, String userId) {
		boolean isStudent = false;
		String studentId = null;
		ColumnList<String> classData = cqlDAO.readByKey(ColumnFamilyConstants.USER_GROUP_ASSOCIATION, classId);
		if (classData != null) {
			studentId = classData.getColumnByName(userId).getName();
			LOGGER.debug("studentId : " + studentId);
		}
		if (studentId != null) {
			isStudent = true;
			userRole = MessageConstants.MSG_STUDENT;
		}
		LOGGER.debug("studentId : " + studentId);
		return isStudent;
	}
}
