package com.datastax.spark.connector.japi;

import com.datastax.driver.scala.core.*;
import com.datastax.driver.scala.core.conf.WriteConf;
import com.datastax.driver.scala.core.io.*;
import scala.Option;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.scala.core.conf.CassandraSettings.Write;
import com.datastax.driver.scala.core.BatchSize;
import org.apache.spark.SparkConf;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.allColumns;

/**
 * Java API wrapper over either {@code RDD} or {@code DStream} to provide Spark Cassandra Connector functionality.
 *
 * <p>To obtain an instance of the specific wrapper, use one of the factory methods in {@link
 * com.datastax.spark.connector.japi.CassandraJavaUtil} class.</p>
 */
@SuppressWarnings("UnusedDeclaration")
public abstract class RDDAndDStreamCommonJavaFunctions<T> {

    /**
     * Returns the default Cassandra connector instance for the wrapped RDD or DStream.
     *
     * @return an instance of {@link com.datastax.driver.scala.core.CassandraConnector}
     */
    public abstract CassandraConnector defaultConnector();

    protected abstract SparkConf getConf();

    /**
     * Returns the default write configuration instance for the wrapped RDD or DStream.
     *
     * @return an instance of {@link com.datastax.driver.scala.core.conf.WriteConf}
     */
    public WriteConf defaultWriteConf() {
        String ns = "spark.";
        SparkConf conf = getConf();

        Option bs = conf.getOption(ns + Write.BatchSizeInBytesProperty()).map(new AbstractFunction1<String, Integer>() {
          @Override public Integer apply(String v1) { return Integer.valueOf(v1); }});
        Option cs = conf.getOption(ns + Write.ConsistencyLevelProperty()).map(new AbstractFunction1<String, ConsistencyLevel>() {
          @Override public ConsistencyLevel apply(String v1) { return ConsistencyLevel.valueOf(v1); }});
        Option bsr = conf.getOption(ns + Write.BatchSizeInRowsProperty());
        Option pl = conf.getOption(ns + Write.ParallelismLevelProperty()).map(new AbstractFunction1<String, Integer>() {
          @Override public Integer apply(String v1) { return Integer.valueOf(v1); }});

        return WriteConf.apply(bs, cs, bsr, pl);
    }

    protected abstract void saveToCassandra(String keyspace, String table, RowWriterFactory<T> rowWriterFactory,
                                            ColumnSelector columnNames, WriteConf conf, CassandraConnector connector);

    /**
     * @deprecated this method will be removed in future release, please use {@link #writerBuilder(String, String,
     * com.datastax.driver.scala.core.io.RowWriterFactory)}
     */
    @Deprecated
    public void saveToCassandra(String keyspace, String table, RowWriterFactory<T> rowWriterFactory, ColumnSelector columnNames) {
        new WriterBuilder(keyspace, table, rowWriterFactory, columnNames, defaultConnector(), defaultWriteConf()).saveToCassandra();
    }

    /**
     * Creates a writer builder object with specified parameters.
     *
     * <p>Writer builder is used to configure parameters write operation and eventually save the data to Cassandra. By
     * default the builder is configured to save all the columns with default connector and Spark Cassandra Connector
     * default parameters.</p>
     *
     * <p>To obtain an instance of {@link com.datastax.driver.scala.core.io.RowWriterFactory} use one of utility
     * methods in {@link com.datastax.spark.connector.japi.CassandraJavaUtil}.</p>
     *
     * @param keyspaceName     the target keyspace name
     * @param tableName        the target table name
     * @param rowWriterFactory a row writer factory to be used to save objects from RDD or DStream
     *
     * @return an instance of {@link com.datastax.spark.connector.japi.RDDAndDStreamCommonJavaFunctions.WriterBuilder}
     *
     * @see com.datastax.spark.connector.japi.CassandraJavaUtil#mapToRow(com.datastax.driver.scala.mapping.ColumnMapper)
     * @see com.datastax.spark.connector.japi.CassandraJavaUtil#mapToRow(Class, java.util.Map)
     * @see com.datastax.spark.connector.japi.CassandraJavaUtil#mapToRow(Class, org.apache.commons.lang3.tuple.Pair[])
     */
    public WriterBuilder writerBuilder(String keyspaceName, String tableName, RowWriterFactory<T> rowWriterFactory) {
        return new WriterBuilder(keyspaceName, tableName, rowWriterFactory, allColumns, defaultConnector(), defaultWriteConf());
    }

    /**
     * Write builder is a container for various parameters which determine how the objects from RDD or DStream are to
     * be saved to Cassandra.
     */
    public class WriterBuilder implements Serializable {
        /** Target keyspace name, where the data are to be saved. */
        public final String keyspaceName;

        /** Target table name, where the data are to be saved. */
        public final String tableName;

        /** A factory for row writer which will be used to map objects from the RDD or DStream into Cassandra rows. */
        public final RowWriterFactory<T> rowWriterFactory;

        /** A column selector which defines which columns are to be saved. */
        public final ColumnSelector columnSelector;

        /** A Cassandra connector which defines a connection to Cassandra server. */
        public final CassandraConnector connector;

        /** A set of write operation parameters which are related mainly to performance and failure tolerance. */
        public final WriteConf writeConf;

        /**
         * Refer to particular fields description for more information.
         */
        public WriterBuilder(String keyspaceName, String tableName, RowWriterFactory<T> rowWriterFactory,
                             ColumnSelector columnSelector, CassandraConnector connector, WriteConf writeConf) {
            this.keyspaceName = keyspaceName;
            this.tableName = tableName;
            this.rowWriterFactory = rowWriterFactory;
            this.columnSelector = columnSelector;
            this.connector = connector;
            this.writeConf = writeConf;
        }

        /**
         * Returns a copy of this builder with {@link #connector} changed to a specified one.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withConnector(CassandraConnector connector) {
            if (connector != this.connector)
                return new WriterBuilder(keyspaceName, tableName, rowWriterFactory, columnSelector, connector, writeConf);
            else
                return this;
        }

        /**
         * Returns a copy of this builder with {@link #writeConf} changed to a specified one.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withWriteConf(WriteConf writeConf) {
            if (writeConf != this.writeConf)
                return new WriterBuilder(keyspaceName, tableName, rowWriterFactory, columnSelector, connector, writeConf);
            else
                return this;
        }

        /**
         * Returns a copy of this builder with {@link #rowWriterFactory} changed to a specified one.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withRowWriterFactory(RowWriterFactory<T> factory) {
            return new WriterBuilder(keyspaceName, tableName, factory, columnSelector, connector, writeConf);
        }

        /**
         * Returns a copy of this builder with {@link #columnSelector} changed to a specified one.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withColumnSelector(ColumnSelector columnSelector) {
            return new WriterBuilder(keyspaceName, tableName, rowWriterFactory, columnSelector, connector, writeConf);
        }

        /**
         * Returns a copy of this builder with the new write configuration which has batch size changed to a specified
         * one.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withBatchSize(BatchSize batchSize) {
            if (writeConf.batchSize() != batchSize)
                return withWriteConf(new WriteConf(batchSize, writeConf.consistencyLevel(),
                                writeConf.parallelismLevel(), writeConf.ttl(), writeConf.timestamp()));
            else
                return this;
        }

        /**
         * Returns a copy of this builder with the new write configuration which has consistency level changed to a
         * specified one.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withConsistencyLevel(ConsistencyLevel consistencyLevel) {
            if (writeConf.consistencyLevel() != consistencyLevel)
                return withWriteConf(new WriteConf(writeConf.batchSize(), consistencyLevel,
                                writeConf.parallelismLevel(), writeConf.ttl(), writeConf.timestamp()));
            else
                return this;
        }

        /**
         * Returns a copy of this builder with the new write configuration which has parallelism level changed to a
         * specified one.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withParallelismLevel(int parallelismLevel) {
            if (writeConf.parallelismLevel() != parallelismLevel)
                return withWriteConf(new WriteConf(writeConf.batchSize(), writeConf.consistencyLevel(),
                                parallelismLevel, writeConf.ttl(), writeConf.timestamp()));
            else
                return this;
        }


        private WriterBuilder withTimestamp(TimestampOption timestamp) {
            return Objects.equals(writeConf.timestamp(), timestamp)
                ? this
                : withWriteConf(new WriteConf(
                    writeConf.batchSize(),
                    writeConf.consistencyLevel(),
                    writeConf.parallelismLevel(),
                    writeConf.ttl(),
                    timestamp));
        }


        /**
         * Returns a copy of this builder with the new write configuration which has custom write timestamp
         * changed to a value specified in microseconds.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withConstantTimestamp(long timeInMicroseconds) {
            return withTimestamp(TimestampOption$.MODULE$.constant(timeInMicroseconds));
        }

        /**
         * Returns a copy of this builder with the new write configuration which has custom write timestamp
         * changed to a specified value.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withConstantTimestamp(Date timestamp) {
            long timeInMicroseconds = timestamp.getTime() * 1000L;
            return withConstantTimestamp(timeInMicroseconds);
        }

        /**
         * Returns a copy of this builder with the new write configuration which has custom write timestamp
         * changed to a specified value.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withConstantTimestamp(DateTime timestamp) {
            long timeInMicroseconds = timestamp.getMillis() * 1000L;
            return withConstantTimestamp(timeInMicroseconds);
        }

        /**
         * Returns a copy of this builder with the new write configuration which has write timestamp set
         * to use automatic value set by Cassandra.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withAutoTimestamp() {
            return withTimestamp(TimestampOption.auto$.MODULE$);
        }

        /**
         * Returns a copy of this builder with the new write configuration which has write timestamp set
         * to a placeholder which will be filled-in by mapper.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withPerRowTimestamp(String placeholder) {
            return withTimestamp(TimestampOption$.MODULE$.perRow(placeholder));
        }


        private WriterBuilder withTTL(TTLOption ttl) {
            return Objects.equals(writeConf.ttl(), ttl)
                ? this
                : withWriteConf(new WriteConf(
                    writeConf.batchSize(),
                    writeConf.consistencyLevel(),
                    writeConf.parallelismLevel(),
                    ttl,
                    writeConf.timestamp()));
        }

        /**
         * Returns a copy of this builder with the new write configuration which has TTL set to a given
         * number of seconds.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withConstantTTL(int ttlInSeconds) {
            return withTTL(TTLOption$.MODULE$.constant(ttlInSeconds));
        }

        /**
         * Returns a copy of this builder with the new write configuration which has TTL set to a given
         * duration.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withConstantTTL(Duration ttl) {
            int secs = (int) ttl.getStandardSeconds();
            return withConstantTTL(secs);
        }

        /**
         * Returns a copy of this builder with the new write configuration which has write TTL set
         * to use automatic value set by Cassandra.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withAutoTTL() {
            return withTTL(TTLOption.auto$.MODULE$);
        }

        /**
         * Returns a copy of this builder with the new write configuration which has TTL set to a placeholder
         * which will be filled-in by mapper.
         *
         * <p>If the same instance is passed as the one which is currently set, no copy of this builder is created.</p>
         *
         * @return this instance or copy to allow method invocation chaining
         */
        public WriterBuilder withPerRowTTL(String placeholder) {
            return withTTL(TTLOption$.MODULE$.perRow(placeholder));
        }

        /**
         * Sets the output operator for the RDD or DStream.
         *
         * <p>Calling this method cause the data to be written to the database using all the configuration
         * settings.</p>
         */
        public void saveToCassandra() {
            RDDAndDStreamCommonJavaFunctions.this.saveToCassandra(keyspaceName, tableName, rowWriterFactory,
                    columnSelector, writeConf, connector);
        }

    }
}
