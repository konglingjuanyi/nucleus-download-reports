package org.gooru.nucleus.reports.infra.responses.auth;

import org.gooru.nucleus.reports.infra.constants.MessageConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

public class AuthPrefsMessageBusJsonResponseHolder implements AuthResponseHolder {


    private static final Logger LOG = LoggerFactory.getLogger(AuthResponseHolder.class);
    private final Message<Object> message;
    private boolean isAuthorized = false;
    private String userId = null;
    private String userRole = null;
    
    public AuthPrefsMessageBusJsonResponseHolder(Message<Object> message) {
        this.message = message;
        if (message != null && message.body() != null) {
            LOG.debug("Received response from Auth End point : {}", message.body().toString());
            if (!(message.body() instanceof JsonObject)) {
                LOG.error("Message body is NOT JsonObject");
                throw new IllegalArgumentException("Message body should be initialized with JsonObject");
            }
            String result = message.headers().get(MessageConstants.MSG_OP_STATUS);
            LOG.debug("Received header from Auth response : {}", result);
            if (result != null && result.equalsIgnoreCase(MessageConstants.MSG_OP_STATUS_SUCCESS)) {
                isAuthorized = true;
                setUserRole();
            }
        }
    }

    @Override
    public boolean isAuthorized() {
        return isAuthorized;
    }

    @Override
    public String getUserId(){
    	return userId;
    }
    
    @Override
    public String getUserRole(){
    	return userRole;
    }
    
    @Override
    public boolean isAnonymous() {
        JsonObject jsonObject = (JsonObject) message.body();
        String userUId = jsonObject != null ? jsonObject.getString(MessageConstants.MSG_USER_ID) : null;
        this.userId = userUId;
        return !(userUId != null && !userUId.isEmpty() && !userUId.equalsIgnoreCase(MessageConstants.MSG_USER_ANONYMOUS));
    }
    private void setUserRole(){
    	JsonObject jsonObject = (JsonObject) message.body();
    	if(jsonObject != null){
    		if(jsonObject.containsKey(MessageConstants.MSG_IS_TEACHER) && jsonObject.getBoolean(MessageConstants.MSG_IS_TEACHER)){
    			userRole = MessageConstants.MSG_TEACHER;
    		}else if(jsonObject.containsKey(MessageConstants.MSG_IS_STUDENT) && jsonObject.getBoolean(MessageConstants.MSG_IS_STUDENT)){
    			userRole = MessageConstants.MSG_STUDENT;
    		}
    		
    	}
    }
}
