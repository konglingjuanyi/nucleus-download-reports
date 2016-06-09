package org.gooru.nucleus.reports.downlod.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.StringUtils;
import org.gooru.nucleus.reports.download.dao.CqlCassandraDao;
import org.gooru.nucleus.reports.infra.constants.ColumnFamilyConstants;
import org.gooru.nucleus.reports.infra.constants.ConfigConstants;
import org.gooru.nucleus.reports.infra.constants.ExportConstants;
import org.gooru.nucleus.reports.infra.constants.MessageConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

public class DataServiceImpl implements DataService {

    private final CqlCassandraDao cqlDAO = CqlCassandraDao.instance();

    private final BaseService baseService = BaseService.instance();

    private final Logger LOGGER = LoggerFactory.getLogger(DataServiceImpl.class);

    @Override
    public List<String> getClassMembersList(String classId, String userId, String userRole)
        throws InterruptedException, ExecutionException {
        List<String> classMembersList = null;
        if (StringUtils.isNotBlank(userRole) && userRole.equalsIgnoreCase(MessageConstants.MSG_TEACHER)) {
            classMembersList = getClassMembers(classId);
        } else if (StringUtils.isNotBlank(userRole) && userRole.equalsIgnoreCase(MessageConstants.MSG_STUDENT)) {
            classMembersList = new ArrayList<>();
            classMembersList.add(userId);
        }
        return classMembersList;
    }

    @Override
    public List<String> getCollectionItems(String contentId) throws ConnectionException {
        List<String> collectionItems = new ArrayList<>();
        ColumnList<String> collectionItemSet = cqlDAO.readByKey(ColumnFamilyConstants.COLLECTION_ITEM_ASSOC, contentId);
        for (Column<String> column : collectionItemSet) {
            collectionItems.add(column.getName());
        }
        return collectionItems;
    }

    @Override
    public List<String> getClassMembers(String classId) throws InterruptedException, ExecutionException {
        List<String> classMembersList = new ArrayList<>();
        ResultSet classMemberSet = cqlDAO.getArchievedClassMembers(classId);
        for (Row collectionItemRow : classMemberSet) {
            classMembersList.add(collectionItemRow.getString(ConfigConstants.COLUMN_1));
        }
        return classMembersList;
    }

    @Override
    public void setUserDetails(Map<String, Object> dataMap, String userId) throws ConnectionException {
        ColumnList<String> userDetailSet = cqlDAO.readByKey(ColumnFamilyConstants.USER, userId);
        dataMap
            .put(ExportConstants.FIRST_NAME, userDetailSet.getStringValue("firstname", ConfigConstants.STRING_EMPTY));
        dataMap.put(ExportConstants.LAST_NAME, userDetailSet.getStringValue("lastname", ConfigConstants.STRING_EMPTY));
    }

    @Override
    public String getContentTitle(String contentId) throws ConnectionException {
        Collection<String> resourceColumns = new ArrayList<>();
        resourceColumns.add(ConfigConstants.TITLE);
        ColumnList<String> contentDetails = cqlDAO.read(ColumnFamilyConstants.RESOURCE, contentId, resourceColumns);
        return contentDetails.getStringValue(ConfigConstants.TITLE, ConfigConstants.STRING_EMPTY);
    }

    @Override
    public String getSessionId(String rowKey) throws ConnectionException {
        String sessionId = null;
        ColumnList<String> recentSession = cqlDAO.readByKey(ColumnFamilyConstants.SESSIONS, rowKey);
        if (recentSession != null) {
            sessionId = recentSession.getStringValue(ConfigConstants._SESSION_ID, ConfigConstants.NA);
        }
        return sessionId;
    }

    @Override
    public void setUsageData(Map<String, Object> dataMap, String title, String rowKey, String collectionType)
        throws ConnectionException {
        ColumnList<String> usageDataSet =
            cqlDAO.readByKey(ColumnFamilyConstants.CLASS_ACTIVITY, baseService.appendTilda(rowKey, collectionType));
        long views = 0;
        long scoreInPercentage = 0;
        long timespent = 0;
        for (Column<String> metricsColumn : usageDataSet) {
            if (!ConfigConstants.IGNORE_COLUMNS.matcher(metricsColumn.getName()).matches()) {
                if (metricsColumn.getName().endsWith(ConfigConstants.VIEWS)) {
                    views += metricsColumn.getLongValue();
                } else if (metricsColumn.getName().endsWith(ConfigConstants.SCORE_IN_PERCENTAGE)) {
                    scoreInPercentage += metricsColumn.getLongValue();
                } else if (metricsColumn.getName().endsWith(ConfigConstants.TIME_SPENT)) {
                    timespent += metricsColumn.getLongValue();
                }
            }
        }
        scoreInPercentage = views != 0 ? (scoreInPercentage / views) : 0;
        dataMap.put(baseService.appendHyphen(collectionType, ExportConstants.VIEWS), views);
        dataMap.put(baseService.appendHyphen(collectionType, ExportConstants.TIME_SPENT), timespent);
        dataMap.put(baseService.appendHyphen(collectionType, ExportConstants.SCORE_IN_PERCENTAGE), scoreInPercentage);
    }

    @Override
    public String getUserRole(String classId, String userId) throws ConnectionException {
        String userRole = null;
        if (this.isTeacher(classId, userId)) {
            userRole = MessageConstants.MSG_TEACHER;
        } else if (this.isStudent(classId, userId)) {
            userRole = MessageConstants.MSG_TEACHER;
        }
        return userRole;
    }

    private boolean isTeacher(String classId, String userId) throws ConnectionException {
        boolean isTeacher = false;
        String teacherId = null;
        ColumnList<String> classData = cqlDAO.readByKey(ColumnFamilyConstants.CLASS, classId);
        if (classData != null) {
            teacherId = classData.getStringValue(ConfigConstants._CREATOR_UID, null);
            LOGGER.debug("teacherId : " + teacherId);
        }
        if (teacherId != null && userId != null && userId.equalsIgnoreCase(teacherId)) {
            isTeacher = true;
        }
        LOGGER.debug("isTeacher : " + isTeacher);
        return isTeacher;
    }

    private boolean isStudent(String classId, String userId) throws ConnectionException {
        boolean isStudent = false;
        ColumnList<String> classData = cqlDAO.readByKey(ColumnFamilyConstants.USER_GROUP_ASSOCIATION, classId);
        if (classData != null && classData.getColumnByName(userId) != null) {
            isStudent = true;
        }

        LOGGER.debug("isStudent : " + isStudent);
        return isStudent;
    }
}
