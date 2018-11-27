import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.sql.Timestamp;
import scala.Tuple2;


public final class SparkStructuredStreamingUsage {

    private static void runAggregation(SparkSession spark) throws Exception {
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9900)
                .load();

        Dataset<String> words = lines.as(Encoders.STRING()).flatMap(
                (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
                Encoders.STRING());

        Dataset<Row> wordCounts = words.groupBy("value").count();

        StreamingQuery query = wordCounts.writeStream()
                .outputMode("update")
                .format("console")
                .start();
        query.awaitTermination();
        // nc -lk 9900
    }

    private static void runWindowed(SparkSession spark) throws Exception {
        // Create DataFrame representing the stream of input lines from connection to host:port
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9900)
                .option("includeTimestamp", true)
                .load();
        // Split the lines into words, retaining timestamps
        Dataset<Row> words = lines
                .as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
                .flatMap((FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>) t -> {
                            List<Tuple2<String, Timestamp>> result = new ArrayList<>();
                            for (String word : t._1.split(" ")) {
                                result.add(new Tuple2<>(word, t._2));
                            }
                            return result.iterator();
                        },
                        Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())
                ).toDF("word", "timestamp");

        // Group the data by window and word and compute the count of each group
        Dataset<Row> windowedCounts = words.withWatermark("timestamp", "10 minutes")
                .groupBy(
                        functions.window(words.col("timestamp"), "30 seconds", "10 seconds"),
                        words.col("word")
        ).count();

        // Start running the query that prints the windowed word counts to the console
        StreamingQuery query = windowedCounts.writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", "false")
                .start();

        query.awaitTermination();
    }

    private static void runJoin(SparkSession spark) throws Exception {
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9900)
                .load();

        Dataset<String> words = lines.as(Encoders.STRING()).flatMap(
                (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
                Encoders.STRING());

        Dataset<Row> wordCounts = words.groupBy("value").count().toDF("name", "count");

        Dataset<Row> staticDf = spark.read().json("data/sparkSql/input/people.json");

        Dataset<Row> inner_joined_result = wordCounts.join(staticDf, "name");         // inner equi-join with a static DF

        StreamingQuery query1 = inner_joined_result.writeStream()
                .outputMode("complete")
                .format("console")
                .start();
        query1.awaitTermination();

        Dataset<Row> left_outer_result = wordCounts.join(staticDf, wordCounts.col("name").equalTo(staticDf.col("name")), "left_outer");  // left outer join with a static DF

        StreamingQuery query2 = left_outer_result.writeStream()
                .outputMode("complete")
                .format("console")
                .start();
        query2.awaitTermination();
    }

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SparkStructuredStreamingUsage")
                .getOrCreate();
        runAggregation(spark);
        runWindowed(spark);
        runJoin(spark);
    }
}

