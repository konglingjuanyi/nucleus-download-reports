package org.gooru.nucleus.reports.infra.downlod.service;

import io.vertx.core.json.JsonObject;

public interface ClassExportService {

	static ClassExportService instance(){
		 return new ClassExportServiceImpl();
	}
	
	JsonObject exportCsv(String classId, String courseId, String userId, String userRole, String zipFileName);

}
