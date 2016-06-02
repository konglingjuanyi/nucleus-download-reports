package org.gooru.nucleus.reports.infra.download.verticles;

import org.gooru.nucleus.reports.infra.constants.MessageConstants;
import org.gooru.nucleus.reports.infra.constants.MessagebusEndpoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;

public class AuthVerticle extends AbstractVerticle {

	 private static final Logger LOG = LoggerFactory.getLogger(AuthVerticle.class);
	    public static final String ACCESS_TOKEN_VALIDITY = "access_token_validity";

	    @Override
	    public void start(Future<Void> voidFuture) throws Exception {
	        EventBus eb = vertx.eventBus();
	        eb.localConsumer(MessagebusEndpoints.MBEP_AUTH, message -> {
	            LOG.debug("Received message: " + message.body());
	            vertx.executeBlocking(future -> {
	                JsonObject result = getAccessToken(message.headers().get(MessageConstants.MSG_HEADER_TOKEN));
	                future.complete(result);
	            }, res -> {
	                if (res.result() != null) {
	                    JsonObject result = (JsonObject) res.result();
	                    DeliveryOptions options = new DeliveryOptions()
	                        .addHeader(MessageConstants.MSG_OP_STATUS, MessageConstants.MSG_OP_STATUS_SUCCESS);
	                    message.reply(result, options);
	                } else {
	                    message.reply(null);
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
	    
	    private JsonObject getAccessToken(String token) {/*
	        JsonObject accessToken = RedisClient.instance().getJsonObject(token);
	        if (accessToken != null) {
	            int expireAtInSeconds = accessToken.getInteger(ACCESS_TOKEN_VALIDITY);
	            RedisClient.instance().expire(token, expireAtInSeconds);
	        }
	        return accessToken;
	    */
	    	if(token != null){
	    		JsonObject j = new JsonObject();
	    		j.put("sessionToken", token);
	    		j.put("user", "daniel");
	    		return j;
	    	}
	    	return new JsonObject();
	    	}
}
