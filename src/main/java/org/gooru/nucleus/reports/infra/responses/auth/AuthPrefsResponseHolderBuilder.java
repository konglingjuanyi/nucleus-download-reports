package org.gooru.nucleus.reports.infra.responses.auth;

import io.vertx.core.eventbus.Message;

public class AuthPrefsResponseHolderBuilder {
	 	private final Message<Object> message;

	    public AuthPrefsResponseHolderBuilder(Message<Object> message) {
	        this.message = message;
	    }

	    public AuthResponseHolder build() {
	        return new AuthPrefsMessageBusJsonResponseHolder(message);
	    }
}
