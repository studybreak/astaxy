package com.hangtime.astaxy;

import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.CassandraOperationTracer;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.KeyspaceTracerFactory;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.shallows.EmptyKeyspaceTracerFactory;
import com.netflix.astyanax.thrift.AbstractKeyspaceOperationImpl;
import com.netflix.astyanax.thrift.ddl.ThriftKeyspaceDefinitionImpl;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftProxy implements Cassandra.Iface
{
    private static Logger logger = LoggerFactory.getLogger(ThriftProxy.class);

    private static final String UTF_8 = "UTF-8";
    private static final Charset charset = Charset.forName(UTF_8);

    private final AstyanaxConfiguration asConfig;
    private final Keyspace client;
    private final ConnectionPool<Cassandra.Client> connectionPool;
    private final AstyanaxContext<Keyspace> context;
    private final ConnectionPoolConfiguration cpConfig;
    private final KeyspaceTracerFactory tracerFactory;

    @SuppressWarnings("unchecked")
    public ThriftProxy(AstyanaxContext<Keyspace> context) {
        this.asConfig = context.getAstyanaxConfiguration();
        this.client = context.getEntity();
        this.connectionPool = (ConnectionPool<Cassandra.Client>)context.getConnectionPool();
        this.context = context;
        this.cpConfig = context.getConnectionPoolConfiguration();
        this.tracerFactory = EmptyKeyspaceTracerFactory.getInstance();
    }

    public void login(AuthenticationRequest auth_request)
    throws AuthenticationException, AuthorizationException, TException
    {
        throw new TException("Method is not implemented: login");
    }

    public void set_keyspace(String keyspace)
    throws InvalidRequestException, TException
    {
        log_result("set_keyspace", keyspace);
        if (!this.context.getKeyspaceName().equals(keyspace)) {
            throw new InvalidRequestException("Cannot operate on keyspace " + keyspace);
        }
    }

    public ColumnOrSuperColumn get(ByteBuffer key, ColumnPath column_path, ConsistencyLevel consistency_level)
    throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException, TException
    {
        throw new TException("Method is not implemented: get");
    }

    public List<ColumnOrSuperColumn> get_slice(ByteBuffer key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        throw new TException("Method is not implemented: get_slice");
    }

    public int get_count(ByteBuffer key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        throw new TException("Method is not implemented: get_count");
    }

    public Map<ByteBuffer,List<ColumnOrSuperColumn>> multiget_slice(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        throw new TException("Method is not implemented: multiget_slice");
    }

    public Map<ByteBuffer,Integer> multiget_count(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        throw new TException("Method is not implemented: multiget_count");
    }

    public List<KeySlice> get_range_slices(ColumnParent column_parent, SlicePredicate predicate, KeyRange range, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        throw new TException("Method is not implemented: get_range_slices");
    }

    public List<KeySlice> get_paged_slice(String column_family, KeyRange range, ByteBuffer start_column, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        throw new TException("Method is not implemented: get_paged_slice");
    }

    public List<KeySlice> get_indexed_slices(ColumnParent column_parent, IndexClause index_clause, SlicePredicate column_predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        throw new TException("Method is not implemented: get_indexed_slices");
    }

    public void insert(ByteBuffer key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        throw new TException("Method is not implemented: insert");
    }

    public void add(ByteBuffer key, ColumnParent column_parent, CounterColumn column, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        throw new TException("Method is not implemented: add");
    }

    public void remove(ByteBuffer key, ColumnPath column_path, long timestamp, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        throw new TException("Method is not implemented: remove");
    }

    public void remove_counter(ByteBuffer key, ColumnPath path, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        throw new TException("Method is not implemented: remove_counter");
    }

    public void batch_mutate(Map<ByteBuffer,Map<String,List<Mutation>>> mutation_map, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        throw new TException("Method is not implemented: batch_mutate");
    }

    public void truncate(String cfname)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        throw new TException("Method is not implemented: truncate");
    }

    public Map<String,List<String>> describe_schema_versions()
    throws InvalidRequestException, TException
    {
        throw new TException("Method is not implemented: describe_schema_versions");
    }

    public List<KsDef> describe_keyspaces()
    throws InvalidRequestException, TException
    {
        throw new TException("Method is not implemented: describe_keyspace");
    }

    public String describe_cluster_name()
    throws TException
    {
        throw new TException("Method is not implemented: describe_cluster_name");
    }

    public String describe_version()
    throws TException
    {
        throw new TException("Method is not implemented: describe_version");
    }

    public List<TokenRange> describe_ring(String keyspace)
    throws InvalidRequestException, TException
    {
        throw new TException("Method is not implemented: describe_ring");
    }

    public Map<String,String> describe_token_map()
    throws InvalidRequestException, TException
    {
        throw new TException("Method is not implemented: describe_token_map");
    }

    public String describe_partitioner()
    throws TException
    {
        throw new TException("Method is not implemented: describe_partitioner");
    }

    public String describe_snitch()
    throws TException
    {
        throw new TException("Method is not implemented: describe_snitch");
    }

    public KsDef describe_keyspace(String keyspace)
    throws NotFoundException, InvalidRequestException, TException
    {
        long start = System.nanoTime();

        if (!this.context.getKeyspaceName().equals(keyspace)) {
            throw new InvalidRequestException("Cannot operate on keyspace " + keyspace);
        }

        try
        {
            KsDef result = ((ThriftKeyspaceDefinitionImpl)client.describeKeyspace()).getThriftKeyspaceDefinition();
            log_result("describe_keyspace", keyspace, start);
            return result;
        }
        catch (ConnectionException e) {
            throw new TException("Connection error", e);
        }
    }

    public List<String> describe_splits(String cfName, String start_token, String end_token, int keys_per_split)
    throws InvalidRequestException, TException
    {
        throw new TException("Method is not implemented: describe_splits");
    }

    public String system_add_column_family(CfDef cf_def)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        throw new TException("Method is not implemented: system_add_column_family");
    }

    public String system_drop_column_family(String column_family)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        throw new TException("Method is not implemented: system_drop_column_family");
    }

    public String system_add_keyspace(KsDef ks_def)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        throw new TException("Method is not implemented: system_add_keyspace");
    }

    public String system_drop_keyspace(String keyspace)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        throw new TException("Method is not implemented: system_drop_keyspace");
    }

    public String system_update_keyspace(KsDef ks_def)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        throw new TException("Method is not implemented: system_update_keyspace");
    }

    public String system_update_column_family(CfDef cf_def)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        throw new TException("Method is not implemented: system_update_column_family");
    }

    public CqlResult execute_cql_query(ByteBuffer query, Compression compression)
    throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        long start = System.nanoTime();

        final ByteBuffer q = query;
        final Compression c = compression;

        try {
            OperationResult<CqlResult> result = connectionPool.executeWithFailover(
                    new AbstractCqlOperationImpl<CqlResult>(tracerFactory.newTracer(CassandraOperationType.CQL),
                                                            client.getKeyspaceName(), q) {
                        @Override
                        public CqlResult internalExecute(Cassandra.Client thriftClient) throws Exception {
                            return thriftClient.execute_cql_query(q, c);
                        }
                    }, asConfig.getRetryPolicy());

            log_result("execute_cql_query", query, result, start);
            return result.getResult();
        }
        catch (OperationException e) {
            log_result("exception_cql_query", query);
            throw new TException(e);
        }
        catch (ConnectionException e) {
            log_result("exception_cql_query", query);
            throw new TException(e);
        }
    }

    private abstract static class AbstractCqlOperationImpl<R> extends AbstractKeyspaceOperationImpl<R> {
        private ByteBuffer key = null;
        private static Pattern re = Pattern.compile("key[ ']*(?:in *|=)[ (']*([^,)']*)", Pattern.CASE_INSENSITIVE);

        public AbstractCqlOperationImpl(CassandraOperationTracer tracer, Host pinnedHost, String keyspaceName, ByteBuffer q) {
            super(tracer, pinnedHost, keyspaceName);

            String cql = charset.decode(q.duplicate()).toString();
            Matcher m = AbstractCqlOperationImpl.re.matcher(cql);
            if (m.find() && m.groupCount() > 0) {
                this.key = ByteBuffer.wrap(m.group(1).getBytes(charset));
            }
        }

        public AbstractCqlOperationImpl(CassandraOperationTracer tracer, String keyspaceName, ByteBuffer cql) {
            this(tracer, null, keyspaceName, cql);
        }

        @Override
        public ByteBuffer getRowKey() {
            return key;
        }
    }


    public CqlPreparedResult prepare_cql_query(ByteBuffer query, Compression compression)
    throws InvalidRequestException, TException
    {
        throw new TException("Method is not implemented: prepare_cql_query");
    }

    public CqlResult execute_prepared_cql_query(int itemId, List<ByteBuffer> values)
    throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        throw new TException("Method is not implemented: execute_prepared_cql_query");
    }

    public void set_cql_version(String version)
    throws InvalidRequestException, TException
    {
        throw new TException("Method is not implemented: set_cql_version");
    }

    protected void log_result(String method, String msg, OperationResult<?> result, long start) {
        long queryLatency = result != null ? result.getLatency(TimeUnit.MICROSECONDS) : -1;
        long fullLatency = start != 0 ? (System.nanoTime() - start)/1000 : -1;
        String host = "-";
        if (result != null) host = result.getHost().toString();
        logger.info(host + "\t" + queryLatency + "\t" + fullLatency + " \t" + method + ": " + msg.substring(0, Math.min(msg.length(), 32)));
    }

    protected void log_result(String method, String msg) {
        log_result(method, msg, null, 0);
    }

    protected void log_result(String method, ByteBuffer msg) {
        log_result(method, byteBufToString(msg), null, 0);
    }

    protected void log_result(String method, String msg, long start) {
        log_result(method, msg, null, start);
    }

    protected void log_result(String method, ByteBuffer msg, OperationResult<?> result, long start) {
        log_result(method, byteBufToString(msg), result, start);
    }

    protected String byteBufToString(ByteBuffer buf) {
        byte[] bytearray = new byte[buf.remaining()];
        buf.get(bytearray);
        return new String(bytearray);
    }
}
