package org.gooru.nucleus.reports.downlod.service;

import java.io.IOException;
import java.text.ParseException;
import java.util.concurrent.ExecutionException;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import io.vertx.core.json.JsonObject;

public interface ClassExportService {

	static ClassExportService instance(){
		 return new ClassExportServiceImpl();
	}
	
	JsonObject exportCsv(String classId, String courseId, String userId, String userRole, String zipFileName) throws ParseException, IOException, ConnectionException, InterruptedException, ExecutionException;

}
