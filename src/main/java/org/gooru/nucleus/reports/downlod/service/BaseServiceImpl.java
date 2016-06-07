package org.gooru.nucleus.reports.downlod.service;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.gooru.nucleus.reports.infra.constants.ConfigConstants;
import org.gooru.nucleus.reports.infra.constants.ExportConstants;

import com.netflix.astyanax.model.ColumnList;

public class BaseServiceImpl implements BaseService{
	
	@Override
	public void setMetrics(ColumnList<String> usageDataSet, Map<String, Object> dataMap, String title,
			String leastId) {
		dataMap.put(ExportConstants.VIEWS, usageDataSet.getLongValue(appendTilda(leastId, ConfigConstants.VIEWS), 0L));
		dataMap.put(ExportConstants.TIME_SPENT,
				usageDataSet.getLongValue(appendTilda(leastId, ConfigConstants.TIME_SPENT), 0L));
		dataMap.put(ExportConstants.SCORE_IN_PERCENTAGE,
				usageDataSet.getLongValue(appendTilda(leastId, ConfigConstants.SCORE_IN_PERCENTAGE), 0L));
	}

	@Override
	public void setResourceMetrics(ColumnList<String> usageDataSet, Map<String, Object> dataMap, String title,
			String leastId) {
		dataMap.put(appendHyphen(title, ExportConstants.VIEWS),
				usageDataSet.getLongValue(appendTilda(leastId, ConfigConstants.VIEWS), 0L));
		dataMap.put(appendHyphen(title, ExportConstants.TIME_SPENT),
				usageDataSet.getLongValue(appendTilda(leastId, ConfigConstants.TIME_SPENT), 0L));
		dataMap.put(appendHyphen(title, ExportConstants.ANSWER_STATUS), usageDataSet
				.getStringValue(appendTilda(leastId, ConfigConstants._QUESTION_STATUS), ConfigConstants.NA));
		dataMap.put(appendHyphen(title, ExportConstants.TEXT),
				usageDataSet.getStringValue(appendTilda(leastId, ConfigConstants._TEXT), ConfigConstants.NA));
	}

	@Override
	public Map<String, Object> getDataMap() {
		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		dataMap.put(ExportConstants.FIRST_NAME, "");
		dataMap.put(ExportConstants.LAST_NAME, "");
		return dataMap;
	}

	@Override
	public void setDefaultUsage(String title, Map<String, Object> dataMap) {
		dataMap.put(appendHyphen(ConfigConstants.COLLECTION, ExportConstants.VIEWS), 0);
		dataMap.put(appendHyphen(ConfigConstants.COLLECTION, ExportConstants.TIME_SPENT), 0);
		dataMap.put(appendHyphen(ConfigConstants.COLLECTION, ExportConstants.SCORE_IN_PERCENTAGE), 0);
		dataMap.put(appendHyphen(ConfigConstants.ASSESSMENT, ExportConstants.VIEWS), 0);
		dataMap.put(appendHyphen(ConfigConstants.ASSESSMENT, ExportConstants.TIME_SPENT), 0);
		dataMap.put(appendHyphen(ConfigConstants.ASSESSMENT, ExportConstants.SCORE_IN_PERCENTAGE), 0);
	}

	@Override
	public void setDefaultCollectionUsage(String title, Map<String, Object> dataMap) {
		dataMap.put(ExportConstants.VIEWS, 0);
		dataMap.put(ExportConstants.TIME_SPENT, 0);
		dataMap.put(ExportConstants.SCORE_IN_PERCENTAGE, 0);
	}

	@Override
	public void setDefaultResourceUsage(String title, Map<String, Object> dataMap) {
		dataMap.put(appendHyphen(title, ExportConstants.VIEWS), 0);
		dataMap.put(appendHyphen(title, ExportConstants.TIME_SPENT), 0);
		dataMap.put(appendHyphen(title, ExportConstants.ANSWER_STATUS), ConfigConstants.NA);
		dataMap.put(appendHyphen(title, ExportConstants.TEXT), ConfigConstants.NA);
	}

	@Override
	public String appendTilda(String... texts) {
		StringBuffer sb = new StringBuffer();
		for (String text : texts) {
			if (StringUtils.isNotBlank(text)) {
				if (sb.length() > 0) {
					sb.append(ConfigConstants.TILDA);
				}
				sb.append(text);
			}
		}
		return sb.toString();
	}

	@Override
	public String appendSlash(String... texts) {
		StringBuffer sb = new StringBuffer();
		for (String text : texts) {
			if (StringUtils.isNotBlank(text)) {
				if (sb.length() > 0) {
					sb.append(ConfigConstants.SLASH);
				}
				sb.append(text);
			}
		}
		return sb.toString();
	}

	@Override
	public String appendHyphen(String... texts) {
		StringBuffer sb = new StringBuffer();
		for (String text : texts) {
			if (StringUtils.isNotBlank(text)) {
				if (sb.length() > 0) {
					sb.append(ConfigConstants.HYPHEN);
				}
				sb.append(text);
			}
		}
		return sb.toString();
	}
	
	@Override
	public String getLeastTitle(String courseTitle, String unitTitle, String lessonTitle, String collectionTitle,
			String type) {
		String title = courseTitle;
		if (type.equalsIgnoreCase(ConfigConstants.COURSE)) {
			title = courseTitle;
		} else if (type.equalsIgnoreCase(ConfigConstants.UNIT)) {
			title = unitTitle;
		} else if (type.equalsIgnoreCase(ConfigConstants.LESSON)) {
			title = lessonTitle;
		} else if (type.equalsIgnoreCase(ConfigConstants.COLLECTION)) {
			title = collectionTitle;
		}
		return title;
	}
}
