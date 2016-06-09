package org.gooru.nucleus.reports.downlod.service;

import java.util.Map;

import com.netflix.astyanax.model.ColumnList;

public interface BaseService {

    static BaseService instance() {
        return new BaseServiceImpl();
    }

    String appendHyphen(String... texts);

    String appendSlash(String... texts);

    String appendTilda(String... texts);

    void setDefaultResourceUsage(String title, Map<String, Object> dataMap);

    void setDefaultCollectionUsage(String title, Map<String, Object> dataMap);

    void setDefaultUsage(String title, Map<String, Object> dataMap);

    Map<String, Object> getDataMap();

    void setResourceMetrics(ColumnList<String> usageDataSet, Map<String, Object> dataMap, String title, String leastId);

    void setMetrics(ColumnList<String> usageDataSet, Map<String, Object> dataMap, String title, String leastId);

    String getLeastTitle(String courseTitle, String unitTitle, String lessonTitle, String collectionTitle, String type);
}
