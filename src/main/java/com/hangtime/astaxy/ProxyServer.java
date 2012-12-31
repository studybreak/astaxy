package com.hangtime.astaxy;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.retry.BoundedExponentialBackoff;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CustomTHsHaServer;
import org.apache.cassandra.thrift.TCustomNonblockingServerSocket;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyServer
{
    private static Logger logger = LoggerFactory.getLogger(ProxyServer.class);

    private final InetAddress address;
    private final int port;
    private final Cassandra.Iface handler;
    private volatile ThriftServerThread server;

    public static void main(String[] args)
    {
        String clusterName = "Test Cluster";
        String keyspaceName = "twohop";
        String localDatacenter = "Cassandra";
        String listenAddress = "0.0.0.0";
        String seeds = "127.0.0.1";
        int listenPort = 9170;

        AstyanaxContext.Builder builder = new AstyanaxContext.Builder()
            .forCluster(clusterName)
            .forKeyspace(keyspaceName)
            .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
                .setDiscoveryType(NodeDiscoveryType.TOKEN_AWARE)
                .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE)
                .setRetryPolicy(new BoundedExponentialBackoff(10, 100, 3))
            )
            .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(clusterName)
                .setSocketTimeout(1000)
                .setMaxTimeoutWhenExhausted(2000)
                .setInitConnsPerHost(10)
                .setMaxConnsPerHost(100)
                .setMaxConns(1000)
                .setLatencyAwareUpdateInterval(10000)
                .setLatencyAwareResetInterval(10000) 
                .setLatencyAwareBadnessThreshold(0.50f)
                .setLatencyAwareWindowSize(100)
                .setSeeds(seeds)
                .setLocalDatacenter(localDatacenter)
            )
            .withConnectionPoolMonitor(new CountingConnectionPoolMonitor());

        AstyanaxContext<Keyspace> context = builder.buildKeyspace(ThriftFamilyFactory.getInstance()); 
        context.start();

        Cassandra.Iface handler = new ThriftProxy(context);
        
        try {
            InetAddress address = InetAddress.getByName(listenAddress);
            ProxyServer proxy = new ProxyServer(address, listenPort, handler);
            proxy.start();
        }
        catch (UnknownHostException e) {
            logger.error("Unable to find local host.", e);            
        }
    }

    public ProxyServer(InetAddress address, int port, Cassandra.Iface handler)
    {
        this.address = address;
        this.port = port;
        this.handler = handler;
    }

    public void start()
    {
        if (server == null)
        {
            server = new ThriftServerThread(address, port, handler);
            server.start();
        }
    }

    public void stop()
    {
        if (server != null)
        {
            server.stopServer();
            try
            {
                server.join();
            }
            catch (InterruptedException e)
            {
                logger.error("Interrupted while waiting thrift server to stop", e);
            }
            server = null;
        }
    }

    public boolean isRunning()
    {
        return server != null;
    }

    private static class ThriftServerThread extends Thread
    {
        private TServer thriftServer;

        public ThriftServerThread(InetAddress listenAddr, int listenPort, Cassandra.Iface handler)
        {
            Cassandra.Processor processor = new Cassandra.Processor(handler);

            // Transport
            logger.info(String.format("Binding thrift service to %s:%s", listenAddr, listenPort));

            // Protocol factory
            TProtocolFactory tProtocolFactory = new TBinaryProtocol.Factory(true, true, 16*1024*1024);

            // Transport factory
            int tFramedTransportSize = 15*1024*1024;
            TTransportFactory inTransportFactory = new TFramedTransport.Factory(tFramedTransportSize);
            TTransportFactory outTransportFactory = new TFramedTransport.Factory(tFramedTransportSize);
            logger.info("Using TFastFramedTransport with a max frame size of {} bytes.", tFramedTransportSize);

            TNonblockingServerTransport serverTransport;
            try
            {
                serverTransport = new TCustomNonblockingServerSocket(new InetSocketAddress(listenAddr, listenPort),
                                                                     true, null, null);
            }
            catch (TTransportException e)
            {
                throw new RuntimeException(String.format("Unable to create thrift socket to %s:%s", listenAddr, listenPort), e);
            }

            // This is NIO selector service but the invocation will be Multi-Threaded with the Executor service.
            ExecutorService executorService = new JMXEnabledThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 4,
                                                                               Runtime.getRuntime().availableProcessors() * 4,
                                                                               60L,
                                                                               TimeUnit.SECONDS,
                                                                               new SynchronousQueue<Runnable>(),
                                                                               new NamedThreadFactory("RPC-Thread"), "RPC-THREAD-POOL");
            TNonblockingServer.Args serverArgs = new TNonblockingServer.Args(serverTransport).inputTransportFactory(inTransportFactory)
                                                                               .outputTransportFactory(outTransportFactory)
                                                                               .inputProtocolFactory(tProtocolFactory)
                                                                               .outputProtocolFactory(tProtocolFactory)
                                                                               .processor(processor);

            logger.info(String.format("Using custom half-sync/half-async thrift server on %s : %s", listenAddr, listenPort));
            
            // Check for available processors in the system which will be equal to the IO Threads.
            thriftServer = new CustomTHsHaServer(serverArgs, executorService, Runtime.getRuntime().availableProcessors());
        }

        public void run()
        {
            logger.info("Listening for thrift clients...");
            thriftServer.serve();
        }

        public void stopServer()
        {
            logger.info("Stop listening to thrift clients");
            thriftServer.stop();
        }
    }
}
