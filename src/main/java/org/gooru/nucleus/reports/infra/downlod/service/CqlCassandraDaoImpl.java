package org.gooru.nucleus.reports.infra.downlod.service;

import java.util.Collection;

import org.gooru.nucleus.reports.infra.component.CassandraClient;
import org.gooru.nucleus.reports.infra.constants.ColumnFamilyConstants;
import org.gooru.nucleus.reports.infra.constants.ConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.retry.ConstantBackoff;
import com.netflix.astyanax.serializers.StringSerializer;

public class CqlCassandraDaoImpl implements CqlCassandraDao {

	private final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
	private final com.netflix.astyanax.model.ConsistencyLevel CONSISTENCY_LEVEL = com.netflix.astyanax.model.ConsistencyLevel.CL_EACH_QUORUM;
	
	private static final Logger LOG = LoggerFactory.getLogger(CqlCassandraDaoImpl.class);		
	
	private CassandraClient cassandra = CassandraClient.getInstance();

	@Override
	public ResultSet getArchievedClassMembers(String classId) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(cassandra.getArchieveKeyspace(), ColumnFamilyConstants.USER_GROUP_ASSOCIATION)
					.where(QueryBuilder.eq(ConfigConstants.KEY, classId))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = cassandra.getArchieveCassandraSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}
	
	@Override
	public ResultSet getArchievedClassData(String rowKey) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(cassandra.getArchieveKeyspace(), ColumnFamilyConstants.CLASS_ACTIVITY)
					.where(QueryBuilder.eq(ConfigConstants.KEY, rowKey))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = cassandra.getArchieveCassandraSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}
	
	@Override
	public ResultSet getArchievedContentTitle(String contentId) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(cassandra.getArchieveKeyspace(), ColumnFamilyConstants.DIM_RESOURCE)
					.where(QueryBuilder.eq(ConfigConstants.KEY, "GLP~"+contentId))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = cassandra.getArchieveCassandraSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}

	@Override
	public ResultSet getArchievedUserDetails(String userId) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(cassandra.getArchieveKeyspace(), ColumnFamilyConstants.DIM_USER)
					.where(QueryBuilder.eq(ConfigConstants.KEY, userId))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = cassandra.getArchieveCassandraSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}
	@Override
	public ResultSet getArchievedCollectionItem(String contentId) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(cassandra.getArchieveKeyspace(), ColumnFamilyConstants.COLLECTION_ITEM_ASSOC)
					.where(QueryBuilder.eq(ConfigConstants.KEY, contentId))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = cassandra.getArchieveCassandraSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}
	@Override
	public ResultSet getArchievedCollectionRecentSessionId(String rowKey) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(cassandra.getArchieveKeyspace(), ColumnFamilyConstants.SESSIONS)
					.where(QueryBuilder.eq(ConfigConstants.KEY, rowKey))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = cassandra.getArchieveCassandraSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}
	@Override
	public ResultSet getArchievedSessionData(String sessionId) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(cassandra.getArchieveKeyspace(), ColumnFamilyConstants.SESSION_ACTIVITY)
					.where(QueryBuilder.eq(ConfigConstants.KEY, sessionId))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
			ResultSetFuture resultSetFuture = cassandra.getArchieveCassandraSession().executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Exception:", e);
		}
		return result;
	}
	@Override
	public ProtocolVersion getClusterProtocolVersion(){
		return cassandra.getProtocolVersion();
	}
	
	@Override
	public ColumnList<String> readByKey(String columnFamilyName, String key) {
		ColumnList<String> query = null;

		try {
			query = cassandra.getKeyspace().prepareQuery(this.accessColumnFamily(columnFamilyName)).setConsistencyLevel(CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key).execute().getResult();
		} catch (Exception e) {
			LOG.error("error while reading columnfamily "+columnFamilyName, e);
		}

		return query;
	}

	@Override
	public ColumnList<String> read(String columnFamilyName, String key, Collection<String> columnList) {
		ColumnList<String> queryResult = null;

		try {
			RowQuery<String, String> query = cassandra.getKeyspace().prepareQuery(this.accessColumnFamily(columnFamilyName)).setConsistencyLevel(CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key);
			if (!columnList.isEmpty()) {
				query.withColumnSlice(columnList);
			}
			queryResult = query.execute().getResult();
		} catch (Exception e) {
			LOG.error("error while reading columnfamily "+columnFamilyName+ "using columnslice", e);
		}

		return queryResult;
	}
	private ColumnFamily<String, String> accessColumnFamily(String columnFamilyName) {
		ColumnFamily<String, String> aggregateColumnFamily;
		aggregateColumnFamily = new ColumnFamily<String, String>(columnFamilyName, StringSerializer.get(), StringSerializer.get());
		return aggregateColumnFamily;
	}

}
