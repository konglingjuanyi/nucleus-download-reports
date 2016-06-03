package org.gooru.nucleus.reports.infra.responses.auth;


public interface AuthResponseHolder {
		
		boolean isAuthorized();

	    boolean isAnonymous();
	    
	    String getUserId();
	    	 
}
