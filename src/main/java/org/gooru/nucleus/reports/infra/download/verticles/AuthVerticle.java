package org.gooru.nucleus.reports.infra.download.verticles;

import org.gooru.nucleus.reports.infra.component.RedisClient;
import org.gooru.nucleus.reports.infra.constants.ColumnFamilyConstants;
import org.gooru.nucleus.reports.infra.constants.ConfigConstants;
import org.gooru.nucleus.reports.infra.constants.HttpConstants;
import org.gooru.nucleus.reports.infra.constants.MessageConstants;
import org.gooru.nucleus.reports.infra.constants.MessagebusEndpoints;
import org.gooru.nucleus.reports.infra.downlod.service.CqlCassandraDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.model.ColumnList;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;

public class AuthVerticle extends AbstractVerticle {

	 private static final Logger LOG = LoggerFactory.getLogger(AuthVerticle.class);
	 
	 private static final String ACCESS_TOKEN_VALIDITY = "access_token_validity";
	 
	 private CqlCassandraDao cqlDAO = CqlCassandraDao.instance();
	 
	    @Override
	    public void start(Future<Void> voidFuture) throws Exception {
	        EventBus eb = vertx.eventBus();
	        eb.localConsumer(MessagebusEndpoints.MBEP_AUTH, message -> {
	            LOG.debug("Received message: " + message.headers());
			vertx.executeBlocking(future -> {
				String classId = message.headers().get(MessageConstants.MSG_CLASS_ID);
				String token = message.headers().get(MessageConstants.MSG_HEADER_TOKEN);
				JsonObject result = getAccessToken(token);

				if (result != null) {
					String userId = result.getString(MessageConstants.MSG_USER_ID);
					// Check if user is a teacher
					result.put(MessageConstants.MSG_IS_TEACHER, isTeacher(classId, userId));
					result.put(MessageConstants.MSG_IS_STUDENT, isStudent(classId, userId));
					
					if (!result.getBoolean(MessageConstants.MSG_IS_TEACHER)) {
						if (!result.getBoolean(MessageConstants.MSG_IS_STUDENT)) {
							result = null;
							LOG.info("user is not a valide teacher or student...");
						}
					}
				}
				future.complete(result);
			}, res -> {
				if (res.result() != null) {
					JsonObject result = (JsonObject) res.result();
					DeliveryOptions options = null;
					if (!result.isEmpty()) {
						options = new DeliveryOptions().addHeader(MessageConstants.MSG_OP_STATUS,
								MessageConstants.MSG_OP_STATUS_SUCCESS);
					} else {
						options = new DeliveryOptions().addHeader(MessageConstants.MSG_OP_STATUS,
								MessageConstants.MSG_OP_STATUS_ERROR);
					}
					message.reply(result, options);
				} else {
					LOG.debug("Unhandled exception. It could be redis issue...");
					message.fail(HttpConstants.HttpStatus.ERROR.getCode(), HttpConstants.HttpStatus.ERROR.getMessage());
				}
			});

	        }).completionHandler(result -> {
	            if (result.succeeded()) {
	            	LOG.debug("Application component initialization successful");
	            	LOG.info("Auth end point ready to listen");
	            	voidFuture.complete();
	            } else {
	                LOG.error("Error registering the auth handler. Halting the Auth machinery");
	                voidFuture.fail(result.cause());
	                Runtime.getRuntime().halt(1);
	            }
	        });
	    }
	    
	private JsonObject getAccessToken(String token) {
		JsonObject accessTokenInfo = null;
		if (token != null) {
			try {
				accessTokenInfo = RedisClient.instance().getJsonObject(token);
				if (accessTokenInfo != null) {
					int expireAtInSeconds = accessTokenInfo.getInteger(ACCESS_TOKEN_VALIDITY);
					RedisClient.instance().expire(token, expireAtInSeconds);
				} else{
					accessTokenInfo = new JsonObject();
					LOG.debug("Not able to find token in redis. Sessing might be expired...");
				}
			} catch (Exception e) {
				LOG.error("Exception while writing or writing in redis", e);
			}
		}
		//Temporarily added for testing....
		if (accessTokenInfo == null) {
			accessTokenInfo = new JsonObject();
			accessTokenInfo.put("user_id", "daniel");
			accessTokenInfo.put("sessionToken", "token");
		}
		LOG.debug("accessTokenInfo : {}", accessTokenInfo);
		return accessTokenInfo;
	}
	
	private boolean isTeacher(String classId, String userId) {
		boolean isTeacher = false;
		String teacherId = null;
		ColumnList<String> classData = cqlDAO.readByKey(ColumnFamilyConstants.CLASS_ACTIVITY, classId);
		if (classData != null) {
			teacherId = classData.getStringValue(ConfigConstants._CREATOR_UID, null);
			LOG.debug("teacherId : " + teacherId);
		}
		if (teacherId != null && userId.equalsIgnoreCase(teacherId)) {
			isTeacher = true;
		}
		return isTeacher;
	}
	private boolean isStudent(String classId, String userId) {
		boolean isStudent = false;
		String studentId = null;
		ColumnList<String> classData = cqlDAO.readByKey(ColumnFamilyConstants.USER_GROUP_ASSOCIATION, classId);
		if (classData != null) {
			studentId = classData.getColumnByName(userId).getName();
			LOG.debug("studentId : " + studentId);
		}
		if (studentId != null) {
			isStudent = true;
		}
		return isStudent;
	}
}
