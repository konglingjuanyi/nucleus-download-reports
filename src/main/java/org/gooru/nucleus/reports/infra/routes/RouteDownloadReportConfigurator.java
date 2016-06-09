package org.gooru.nucleus.reports.infra.routes;

import org.gooru.nucleus.reports.downlod.service.DataService;
import org.gooru.nucleus.reports.infra.constants.ConfigConstants;
import org.gooru.nucleus.reports.infra.constants.HttpConstants;
import org.gooru.nucleus.reports.infra.constants.MessagebusEndpoints;
import org.gooru.nucleus.reports.infra.constants.RouteConstants;
import org.gooru.nucleus.reports.infra.routes.util.RouteRequestUtility;
import org.gooru.nucleus.reports.infra.routes.util.RouteResponseUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final DataService dataService = DataService.instance();

    @Override
    public void configureRoutes(Vertx vertx, Router router, JsonObject config) {
        final EventBus eb = vertx.eventBus();
        final long mbusTimeout = config.getLong(ConfigConstants.MBUS_TIMEOUT, 30L);
        router.get(RouteConstants.DOWNLOAD_REQUEST).handler(routingContext -> {
            String classId = routingContext.request().getParam(RouteConstants.CLASS_ID);
            String courseId = routingContext.request().getParam(RouteConstants.COURSE_ID);
            String userId = routingContext.request().getParam(RouteConstants.USER_ID);
            LOGGER.debug("classId : " + classId + " - courseId:" + courseId + " - userId : " + userId);
            String userRole = null;
            try {
                userRole = dataService.getUserRole(classId, userId);
            } catch (Exception e) {
                LOGGER.error("Exception while getting user role..", e);
                routingContext.response().setStatusCode(HttpConstants.HttpStatus.ERROR.getCode())
                    .setStatusMessage(e.getMessage()).end();
            }
            if (userRole == null) {
                LOGGER.debug("user is not a valid teacher or student...");
                routingContext.response().setStatusCode(HttpConstants.HttpStatus.UNAUTHORIZED.getCode())
                    .setStatusMessage(HttpConstants.HttpStatus.UNAUTHORIZED.getMessage()).end();
            } else {
                LOGGER.debug("User authorized. Process CSV generation...");
                DeliveryOptions options = new DeliveryOptions().setSendTimeout(mbusTimeout * 1000);
                routingContext.response().putHeader(HttpConstants.HEADER_CONTENT_TYPE, HttpConstants.CONTENT_TYPE_JSON);
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
                String userRole = dataService.getUserRole(classId, userId);
                // TODO: AM: If userRole is not null what should do?
                // Do we download different report for teacher and student?
                if (userRole == null) {
                    LOGGER.debug("user is not a valid teacher or student...");
                    routingContext.response().setStatusCode(HttpConstants.HttpStatus.UNAUTHORIZED.getCode())
                        .setStatusMessage(HttpConstants.HttpStatus.UNAUTHORIZED.getMessage()).end();
                } else {
                    LOGGER.debug("User authorized. Dowloading ZIP file...");
                    Buffer zipFile = vertx.fileSystem().readFileBlocking(
                        config.getString(ConfigConstants.FILE_SAVE_REAL_PATH) + classId + ConfigConstants.HYPHEN
                            + userId + ConfigConstants.ZIP_EXT);
                    routingContext.response()
                        .putHeader(HttpConstants.HEADER_CONTENT_LENGTH, String.valueOf(zipFile.length()));
                    routingContext.response()
                        .putHeader(HttpConstants.HEADER_CONTENT_TYPE, HttpConstants.CONTENT_TYPE_ZIP);
                    routingContext.response().putHeader(HttpConstants.HEADER_CONTENT_DISPOSITION,
                        "attachment; filename=\"" + classId + ConfigConstants.HYPHEN + userId + ConfigConstants.ZIP_EXT
                            + '"');
                    routingContext.response().write(zipFile);
                    routingContext.response().setStatusCode(200);
                }
            } catch (Exception e) {
                LOGGER.error("Exception ", e);
                routingContext.response().setStatusCode(404);
            }
            routingContext.response().end();
        });

    }
}
