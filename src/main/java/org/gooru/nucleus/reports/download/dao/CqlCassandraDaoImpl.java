package org.gooru.nucleus.reports.download.dao;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

import org.gooru.nucleus.reports.infra.component.CassandraClient;
import org.gooru.nucleus.reports.infra.constants.ColumnFamilyConstants;
import org.gooru.nucleus.reports.infra.constants.ConfigConstants;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.retry.ConstantBackoff;
import com.netflix.astyanax.serializers.StringSerializer;

public class CqlCassandraDaoImpl implements CqlCassandraDao {

    private final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

    private final com.netflix.astyanax.model.ConsistencyLevel CONSISTENCY_LEVEL =
        com.netflix.astyanax.model.ConsistencyLevel.CL_QUORUM;

    private final CassandraClient cassandra = CassandraClient.getInstance();

    @Override
    public ResultSet getArchievedClassMembers(String classId) throws InterruptedException, ExecutionException {
        ResultSet result;
        Statement select = QueryBuilder.select().all()
            .from(cassandra.getArchieveKeyspace(), ColumnFamilyConstants.USER_GROUP_ASSOCIATION)
            .where(QueryBuilder.eq(ConfigConstants.KEY, classId)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
        ResultSetFuture resultSetFuture = cassandra.getArchieveCassandraSession().executeAsync(select);
        result = resultSetFuture.get();
        return result;
    }

    @Override
    public ColumnList<String> readByKey(String columnFamilyName, String key) throws ConnectionException {
        ColumnList<String> query;
        query = cassandra.getKeyspace().prepareQuery(CqlCassandraDaoImpl.accessColumnFamily(columnFamilyName))
            .setConsistencyLevel(CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key).execute()
            .getResult();

        return query;
    }

    @Override
    public ColumnList<String> read(String columnFamilyName, String key, Collection<String> columnList)
        throws ConnectionException {
        ColumnList<String> queryResult;

        RowQuery<String, String> query =
            cassandra.getKeyspace().prepareQuery(CqlCassandraDaoImpl.accessColumnFamily(columnFamilyName))
                .setConsistencyLevel(CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key);
        if (!columnList.isEmpty()) {
            query.withColumnSlice(columnList);
        }
        queryResult = query.execute().getResult();

        return queryResult;
    }

    private static ColumnFamily<String, String> accessColumnFamily(String columnFamilyName) {
        ColumnFamily<String, String> aggregateColumnFamily;
        aggregateColumnFamily = new ColumnFamily<>(columnFamilyName, StringSerializer.get(), StringSerializer.get());
        return aggregateColumnFamily;
    }

}
