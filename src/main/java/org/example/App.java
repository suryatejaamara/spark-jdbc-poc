package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
/**
 * Hello world!
 *
 */
public class App 
{
    public static void main(String[] args) {
        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("SparkCDCtoADLS")
                .getOrCreate();

        // Connect to SQL Server CDC tables
        String jdbcUrl = "jdbc:sqlserver://your-sql-server;databaseName=your-db";
        String cdcTable = "your_cdc_table";

        Dataset<Row> cdcDF = spark.read()
                .format("jdbc")
                .option("url", jdbcUrl)
                .option("dbtable", "cdc." + cdcTable + "_CT")
                .option("user", "your-username")
                .option("password", "your-password")
                .load();

        // Process and transform the CDC data
        Dataset<Row> processedDF = cdcDF.select(
                cdcDF.col("__$start_lsn").cast("string"),
                cdcDF.col("__$operation").cast("string"),
                cdcDF.col("column1"),
                cdcDF.col("column2"),
                // Add other columns as needed
                cdcDF.col("__$update_mask").cast("string"),
                cdcDF.col("__$seqval").cast("string"),
                cdcDF.col("__$command_id").cast("string"),
                cdcDF.col("__$commit_lsn").cast("string"),
                cdcDF.col("__$tran_seqno").cast("string")
        );//.withColumn("timestamp", functions.current_timestamp()); // Add a timestamp column

        // Write processed data to ADLS
        String adlsPath = "adl://your-adls-account.azuredatalakestore.net/path/to/output";
        processedDF.write()
                .mode("append")
                .parquet(adlsPath);

        // Stop the Spark session
        spark.stop();
    }
}

