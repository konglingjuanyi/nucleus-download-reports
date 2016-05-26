package org.gooru.nucleus.reports.infra.component;

import java.util.HashMap;
import java.util.Map;

import org.gooru.nucleus.reports.infra.constants.ConfigConstants;
import org.gooru.nucleus.reports.infra.shutdown.Finalizer;
import org.gooru.nucleus.reports.infra.startup.Initializer;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * This is a manager class to initialize the utilities, Utilities initialized
 * may depend on the DB or application state. Thus their initialization sequence
 * may matter. It is advisable to keep the utility initialization for last.
 */
public final class UtilityManager implements Initializer, Finalizer {

    private static Map<String, Object> cacheMemery = null;
    private static String fileDownloadAppUrl = null;
    private static String fileSaveRealPath = null;
    
    public static UtilityManager getInstance() {
        return Holder.INSTANCE;
    }

    private UtilityManager() {
    }

    @Override
    public void finalizeComponent() {

    }

    @Override
    public void initializeComponent(Vertx vertx, JsonObject config) {
    	cacheMemery = new HashMap<String, Object>();
    	fileDownloadAppUrl = config.getString(ConfigConstants.FILE_DOWNLOAD_APP_URL);
    	fileSaveRealPath = config.getString(ConfigConstants.FILE_SAVE_REAL_PATH);
    }
    public Map<String, Object> getCacheMemory(){
    	return cacheMemery;
    }
    public String getDownloadAppUrl(){
    	return fileDownloadAppUrl;
    }
    public String getFileSaveRealPath(){
    	return fileSaveRealPath;
    }
    private static final class Holder {
		private static final UtilityManager INSTANCE = new UtilityManager();
	}
}
