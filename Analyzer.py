# Import the necessary libraries
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


class PaypayAnalyzer:
    def __init__(self, input_path: str):
        self.input_path = input_path

    def read_dataset_spark(self):
        """
        Load dataset to spark dataframe

        Returns:
            Spark dataframe
        """
        # Initialize the entry-point for dataset to spark
        sparkSession = SparkSession.builder.master('local') \
            .appName('paypay_project_assignment') \
            .getOrCreate()

        # Set the loglevel to "Warn" to check the application
        sparkSession.sparkContext.setLogLevel('WARN')

        # Define the schema based on the amazon site
        schema = T.StructType([
            T.StructField('create_time', T.TimestampType(), True),
            T.StructField('elb', T.StringType(), True),
            T.StructField('client_host_port', T.StringType(), True),
            T.StructField('backend_host_port', T.StringType(), True),
            T.StructField('request_processing_time', T.DoubleType(), True),
            T.StructField('backend_processing_time', T.DoubleType(), True),
            T.StructField('response_processing_time', T.DoubleType(), True),
            T.StructField('elb_status_code', T.IntegerType(), True),
            T.StructField('backend_status_code', T.IntegerType(), True),
            T.StructField('received_bytes', T.IntegerType(), True),
            T.StructField('sent_bytes', T.IntegerType(), True),
            T.StructField('request', T.StringType(), True),
            T.StructField('user_agent', T.StringType(), True),
            T.StructField('ssl_cipher', T.StringType(), True),
            T.StructField('ssl_protocol', T.StringType(), True)
        ])

        # Read the log.gz file into spark dataframe
        # Split the string in the file by space between words
        # Set the encoding of the file to "utf-8"
        # Set the defined schema structure for spark
        fileLogDF = sparkSession.read \
            .option("delimiter", " ") \
            .option("header", True) \
            .option("charset", 'utf-8') \
            .schema(schema) \
            .csv(self.input_path)
        return fileLogDF

    def show_goal_tasks(self):
        """
        4 goals:
            1. Show aggregate the total hits based on ip per session
            2. Show average session time
            3. Show unique url hits
            4. Show most engaged users based on descending order
        """

        # First task
        # Make a buckets of time windows for every 15 minutes for each session
        # Count the grouped values for ["interval", "client_host_port"] and renamed the column
        # Show the first 20 rows with no limit on contents
        fileLogDF = self.read_dataset_spark()
        processedDF = fileLogDF.withColumn("client_host",
                                           F.split("client_host_port", ":").getItem(0))\
                        .withColumn("url", F.split("request", " ").getItem(1))
        sessionizedWindowDF = processedDF.withColumn("interval",
                                                   F.window("create_time", "15 minutes"))
        sessionizedHitsIP = sessionizedWindowDF \
            .groupby("interval", "client_host") \
            .count() \
            .withColumnRenamed("count", "num_hits_ip")
        sessionizedHitsIP.show(truncate=False)

        # Second task
        # Create columns for the begin and ending hit in "create_time"
        # - based on grouped ["interval", "client_host_port"]
        # Get the time difference for each host in the session
        # Get the average for the session duration.
        sessionedTemporaryDF = sessionizedWindowDF.groupBy("interval", "client_host").agg(
            F.min("create_time").alias("beginning_hit_time"),
            F.max("create_time").alias("ending_hit_time")
        )

        sessionedTemporaryDF = sessionedTemporaryDF \
            .withColumn("session_duration",
                        F.unix_timestamp("ending_hit_time") - F.unix_timestamp("beginning_hit_time")) \
            .drop("beginning_hit_time") \
            .drop("ending_hit_time")
        sessionizedWindowDF = sessionizedWindowDF.join(sessionedTemporaryDF, ["interval", "client_host"])
        averageSession = sessionizedWindowDF.groupBy().agg({"session_duration": "avg"})
        averageSession.show()

        # Third task
        # Get the unique count for each url hits
        uniqueUrlHits = sessionizedWindowDF \
            .groupBy("client_host", "interval", "url") \
            .count() \
            .distinct() \
            .withColumnRenamed("count", "unique_url_hits")
        uniqueUrlHits.show(truncate=False)

        # Fourth task
        # Arrange the "session_duration" in descending to show the longest session time
        longestSession = sessionizedWindowDF.sort(F.desc("session_duration")).distinct()
        longestSession.show(truncate=False)