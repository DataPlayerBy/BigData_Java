import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Collections;
import java.io.Serializable;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;


public class SparkSqlUsage {

    private static void runQuery(SparkSession spark) {
        Dataset<Row> df = spark.read().json("data/sparkSql/input/people.json");
        df.show();
        df.printSchema();
        df.select("name").show();
        df.select(col("name"), col("age").plus(1)).show();
        df.filter(col("age").gt(21)).show();
        df.groupBy("age").count().show();
    }

    public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    private static void runDatasetCreation(SparkSession spark) {
        // Encoders for most common types are provided in class Encoders
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map(
                (MapFunction<Integer, Integer>) value -> value + 1,
                integerEncoder);
        transformedDS.show();

        // Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);

        // Create an instance of a Bean class
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);
        Dataset<Person> javaBeanDS = spark.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        javaBeanDS.show();

        // DataFrames can be converted to a Dataset by providing a class. Mapping based on name
        Dataset<Person> peopleDS = spark.read().json("data/sparkSql/input/people.json").as(personEncoder);
        peopleDS.show();
    }

    private static void runDatasetCreation2(SparkSession spark) {
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        // Create an RDD of Person objects from a text file
        Dataset<Person> peopleDS = spark.read()
                .textFile("data/sparkSql/input/people.txt")
                .map((MapFunction<String, Person>) line -> {
                    String[] parts = line.split(",");
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(Integer.parseInt(parts[1].trim()));
                    return person;
                }, personEncoder);

        peopleDS.show();
    }

    public static class SelfDefinedAverageFunction extends UserDefinedAggregateFunction {
        // Whether this function always returns the same output on the identical input
        public boolean deterministic() {
            return true;
        }

        // Data types of input arguments of this aggregate function
        public StructType inputSchema() {
            List<StructField> inputFields = new ArrayList<>();
            inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.LongType, true));
            return DataTypes.createStructType(inputFields);
        }

        // Data types of values in the aggregation buffer
        public StructType bufferSchema() {
            List<StructField> bufferFields = new ArrayList<>();
            bufferFields.add(DataTypes.createStructField("sum", DataTypes.LongType, true));
            bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
            return DataTypes.createStructType(bufferFields);
        }

        // The data type of the returned value
        public DataType dataType() {
            return DataTypes.DoubleType;
        }

        // initialize the buffer
        public void initialize(MutableAggregationBuffer buffer) {
            buffer.update(0, 0L);
            buffer.update(1, 0L);
        }

        // Updates the given aggregation buffer `buffer` with new input data from `input`
        public void update(MutableAggregationBuffer buffer, Row input) {
            if (!input.isNullAt(0)) {
                long updatedSum = buffer.getLong(0) + input.getLong(0);
                long updatedCount = buffer.getLong(1) + 1;
                buffer.update(0, updatedSum);
                buffer.update(1, updatedCount);
            }
        }

        // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`,
        // This is called when we merge two partially aggregated data together.
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            long mergedSum = buffer1.getLong(0) + buffer2.getLong(0);
            long mergedCount = buffer1.getLong(1) + buffer2.getLong(1);
            buffer1.update(0, mergedSum);
            buffer1.update(1, mergedCount);
        }

        // Calculates the final result
        public Double evaluate(Row buffer) {
            return ((double) buffer.getLong(0)) / buffer.getLong(1);
        }
    }

    private static void runSelfDefinedAverageFunction(SparkSession spark) {
        spark.udf().register("selfAverage", new SelfDefinedAverageFunction());
        Dataset<Row> df = spark.read().json("data/sparkSql/input/people.json");
        df.createOrReplaceTempView("people");
        df.show();
        Dataset<Row> result = spark.sql("SELECT selfAverage(age) as average_age, max(age) as max_age FROM people");
        result.show();
    }

    private static void runSaveAsJson(SparkSession spark, String inPath, String outPath) {
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        // Create an RDD of Person objects from a text file
        Dataset<Person> peopleDS = spark.read()
                .textFile(inPath)
                .map((MapFunction<String, Person>) line -> {
                    String[] parts = line.split(",");
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(Integer.parseInt(parts[1].trim()));
                    return person;
                }, personEncoder);
        peopleDS.write().format("json").save(outPath);
    }

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SparkSqlUsage")
                //.config("spark.some.config.option", "some-value")
                .getOrCreate();

        runQuery(spark);
        runDatasetCreation(spark);
        runDatasetCreation2(spark);
        runSelfDefinedAverageFunction(spark);
        runSaveAsJson(spark, args[0], args[1]);
        spark.stop();
    }
}
