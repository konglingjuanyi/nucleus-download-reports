package org.gooru.nucleus.reports.infra.download.verticles;

import org.apache.commons.lang.StringUtils;
import org.gooru.nucleus.reports.infra.component.UtilityManager;
import org.gooru.nucleus.reports.infra.constants.ConfigConstants;
import org.gooru.nucleus.reports.infra.constants.MessageConstants;
import org.gooru.nucleus.reports.infra.constants.MessagebusEndpoints;
import org.gooru.nucleus.reports.infra.constants.RouteConstants;
import org.gooru.nucleus.reports.infra.downlod.service.ClassExportService;
import org.gooru.nucleus.reports.infra.util.MessageResponse;
import org.gooru.nucleus.reports.infra.util.MessageResponseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;

/**
 * @author ashish
 */
public class DownloadReportVerticle extends AbstractVerticle {
	private static final Logger LOGGER = LoggerFactory.getLogger(DownloadReportVerticle.class);

	private static final ClassExportService classExportService = ClassExportService.instance();
	private static UtilityManager um = UtilityManager.getInstance();

	@Override
	public void start(Future<Void> startFuture) throws Exception {
		EventBus eb = vertx.eventBus();
	
		MessageConsumer<Object> status = null;
		status = eb.localConsumer(MessagebusEndpoints.MBEP_DOWNLOAD_REQUEST, message -> {
			LOGGER.debug("Received message: '{}'", message.body());
			vertx.executeBlocking(future -> {
				MessageResponse result = null;
				try {
					JsonObject body = getHttpBody(message.body().toString());
					if (StringUtils.isNotBlank(body.getString(RouteConstants.USER_ID))) {
						LOGGER.debug("User ID is null.....");
						result = MessageResponseFactory.createForbiddenResponse(MessageConstants.MSG_USER_NULL);
					} else {
						String fileName = getFileName(message.body().toString());
						LOGGER.debug("zipFileName : " + fileName);
						if (!um.getCacheMemory().containsKey(fileName)) {
							um.getCacheMemory().put(ConfigConstants.STATUS, ConfigConstants.IN_PROGRESS);
							result = MessageResponseFactory.createOkayResponse(classExportService.exportCsv(
									body.getString(RouteConstants.CLASS_ID), body.getString(RouteConstants.COURSE_ID),
									body.getString(RouteConstants.USER_ID), body.getString(RouteConstants.USER_ROLE), fileName));
							/**
							 * delete folder
							 * vertx.fileSystem().deleteBlocking(config().
							 * getString( ConfigConstants.FILE_SAVE_REAL_PATH) +
							 * ConfigConstants.SLASH + zipFileName);
							 */
						} else {
							JsonObject resultObject = new JsonObject();
							resultObject.put(ConfigConstants.STATUS, um.getCacheMemory().get(fileName));
							result = MessageResponseFactory.createOkayResponse(resultObject);
						}
					}
				} catch (Exception e) {
					LOGGER.error("exception", e);
				}
				future.complete(result);
			}, res -> {
				MessageResponse result = (MessageResponse) res.result();
				LOGGER.debug("Sending response: '{}'", result.reply());
				message.reply(result.reply(), result.deliveryOptions());
			});
		});


		status.completionHandler(result -> {
			if (result.succeeded()) {
				LOGGER.info("User end point ready to listen");
				startFuture.complete();
			} else {
				LOGGER.error("Error registering the User handler on message bus");
				startFuture.fail(result.cause());
			}
		});

	}

	private String getFileName(String message) {
		JsonObject body = getHttpBody(message);
		String fileName = body.getString(RouteConstants.CLASS_ID) + ConfigConstants.HYPHEN +body.getString(RouteConstants.USER_ID);
		return fileName;
	}
	private JsonObject getHttpBody(String message) {
		JsonObject body = null;
		try {
			body = new JsonObject(message).getJsonObject(MessageConstants.MSG_HTTP_BODY);
			LOGGER.debug("body : {}",body);
		} catch (Exception e) {
			LOGGER.error("unable to parse json object", e);
		}
		return body;
	}

	@Override
	public void stop(Future<Void> stopFuture) throws Exception {
		// Currently a noop
		stopFuture.complete();
	}

}
