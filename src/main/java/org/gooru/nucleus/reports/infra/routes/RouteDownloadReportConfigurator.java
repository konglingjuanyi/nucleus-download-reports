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

	@Override
	public void configureRoutes(Vertx vertx, Router router, JsonObject config) {
		final EventBus eb = vertx.eventBus();
		final long mbusTimeout = config.getLong(ConfigConstants.MBUS_TIMEOUT, 30L);
		router.get(RouteConstants.DOWNLOAD_REQUEST).handler(routingContext -> {
			String classId = routingContext.request().getParam(RouteConstants.CLASS_ID);
			String courseId = routingContext.request().getParam(RouteConstants.COURSE_ID);
			String userId = routingContext.request().getParam(RouteConstants.USER_ID);
			LOGGER.debug("classId : " + classId + " - courseId:" + courseId + " - userId : " + userId);
			String userRole = getUserRole(classId, userId);
			if (userRole == null) {
				LOGGER.debug("user is not a valid teacher or student...");
				routingContext.response().setStatusCode(HttpConstants.HttpStatus.UNAUTHORIZED.getCode())
						.setStatusMessage(HttpConstants.HttpStatus.UNAUTHORIZED.getMessage()).end();
			} else {
				LOGGER.debug("User authorized. Process CSV generation...");
				DeliveryOptions options = new DeliveryOptions().setSendTimeout(mbusTimeout * 1000);
				routingContext.response().putHeader("content-type", "application/json; charset=utf-8");
				routingContext.request().params().add(RouteConstants.USER_ROLE, userRole);
				JsonObject rru = new RouteRequestUtility().getBodyForMessage(routingContext);
				eb.send(MessagebusEndpoints.MBEP_DOWNLOAD_REQUEST, rru, options,
						reply -> new RouteResponseUtility().responseHandler(routingContext, reply, LOGGER));
			}
		});

		router.get(RouteConstants.DOWNLOAD_FILE).handler(routingContext -> {
			String classId = routingContext.request().getParam(RouteConstants.CLASS_ID);
			String courseId = routingContext.request().getParam(RouteConstants.COURSE_ID);
			String userId = routingContext.request().getParam(RouteConstants.USER_ID);
			LOGGER.debug("classId : " + classId + " - courseId:" + courseId + " - userId : " + userId);
			try {
				String userRole = getUserRole(classId, userId);
				if (userRole == null) {
					LOGGER.debug("user is not a valid teacher or student...");
					routingContext.response().setStatusCode(HttpConstants.HttpStatus.UNAUTHORIZED.getCode())
							.setStatusMessage(HttpConstants.HttpStatus.UNAUTHORIZED.getMessage()).end();
				} else {
					LOGGER.debug("User authorized. Dowloading ZIP file...");
					Buffer zipFile = vertx.fileSystem()
							.readFileBlocking(config.getString(ConfigConstants.FILE_SAVE_REAL_PATH) + classId
									+ ConfigConstants.HYPHEN + userId + ConfigConstants.ZIP_EXT);
					routingContext.response().putHeader("Content-Length", "" + zipFile.length());
					routingContext.response().putHeader("content-type", "application/zip");
					routingContext.response().putHeader("Content-Disposition", "attachment; filename=\"" + classId
							+ ConfigConstants.HYPHEN + userId + ConfigConstants.ZIP_EXT + "\"");
					routingContext.response().write(zipFile);
					routingContext.response().setStatusCode(200);
				}
			} catch (Exception e) {
				routingContext.response().setStatusCode(404);
				LOGGER.error("Exception ", e);
			}
			routingContext.response().end();
		});

	}

	private String getUserRole(String classId, String userId) {
		String userRole = null;
		if (isTeacher(classId, userId)) {
			userRole = MessageConstants.MSG_TEACHER;
		} else if (isStudent(classId, userId)) {
			userRole = MessageConstants.MSG_TEACHER;
		}
		return userRole;
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
			isTeacher = true;
		}
		LOGGER.debug("isTeacher : " + isTeacher);
		return isTeacher;
	}

	private boolean isStudent(String classId, String userId) {
		boolean isStudent = false;
		ColumnList<String> classData = cqlDAO.readByKey(ColumnFamilyConstants.USER_GROUP_ASSOCIATION, classId);
		if (classData != null && classData.getColumnByName(userId) != null) {
			isStudent = true;
		}

		LOGGER.debug("isStudent : " + isStudent);
		return isStudent;
	}
}
