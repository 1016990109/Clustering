package nju.software.util;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class NewMongodbUtil {
    public void saveSortView(){
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri","mongodb://192.168.1.212/shop.shop_info")
                .config("spark.mongodb.output.uri","mongodb://192.168.1.212/shop.shop_info")
                .getOrCreate() ;
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext()) ;
        Dataset<Row> implicitDS = MongoSpark.load(jsc).toDF() ;
        implicitDS.printSchema();
        implicitDS.createOrReplaceTempView("shop_info");
        Dataset<Row> cetenarians = sparkSession.sql("SELECT * FROM shop_info") ;
        long count = cetenarians.count() ;
        System.out.println(count);
    }
}
