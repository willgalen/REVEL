package com.datastax.spark.connector.japi;

import com.datastax.driver.scala.core.ColumnSelector;
import com.datastax.driver.scala.core.CassandraConnector;
import com.datastax.spark.connector.streaming.DStreamFunctions;
import com.datastax.driver.scala.core.io.RowWriterFactory;
import com.datastax.driver.scala.core.conf.WriteConf;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.dstream.DStream;

/**
 * A Java API wrapper over {@link org.apache.spark.streaming.dstream.DStream} to provide Spark Cassandra Connector
 * functionality.
 *
 * <p>To obtain an instance of this wrapper, use one of the factory methods in {@link
 * com.datastax.spark.connector.japi.CassandraJavaUtil} class.</p>
 */
@SuppressWarnings("UnusedDeclaration")
public class DStreamJavaFunctions<T> extends RDDAndDStreamCommonJavaFunctions<T> {
    public final DStream<T> dstream;
    private final DStreamFunctions<T> dsf;

    DStreamJavaFunctions(DStream<T> dStream) {
        this.dstream = dStream;
        this.dsf = new DStreamFunctions<>(dStream);
    }

    @Override
    public CassandraConnector defaultConnector() {
        return dsf.connector();
    }

    @Override
    protected SparkConf getConf() {
        return dstream.ssc().conf();
    }

    @Override
    protected void saveToCassandra(String keyspace, String table, RowWriterFactory<T> rowWriterFactory,
                                   ColumnSelector columnNames, WriteConf conf, CassandraConnector connector) {
        dsf.saveToCassandra(keyspace, table, columnNames, conf, connector, rowWriterFactory);
    }
}
