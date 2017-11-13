package nju.software.util;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Function1;
import scala.Tuple2$;

/**
 * Created by zzt on 10/23/17.
 *
 * <h3></h3>
 */
public class HdfsUtil {
    public static String userFile = "hdfs://master:192.168.1.211/user/*.csv" ;
    public static String userPayFile = "hdfs://master:192.168.1.211/count2/*.csv" ;
  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder().appName("Custering").master("192.168.1.211").getOrCreate();
    Dataset<Row> userPay = spark.read().csv(userFile).cache() ;
    Dataset<Row> u_s_count = userPay.groupBy("user_id","shop_id").count() ;
    u_s_count.rdd() ;
      KMeans kMeans = new KMeans().setK(2).setSeed(1L) ;
      KMeansModel model = kMeans.fit(u_s_count) ;
    spark.stop();
  }
//  public static JavaPairRDD<String,String> getUserPay(){
//    SparkSession spark = SparkSession.builder().appName("Custering").master("192.168.1.211").getOrCreate();
//    Dataset<Row> userPay = spark.read().csv(userFile).cache() ;
//    Dataset<Row> u_s_count = userPay.groupBy("user_id","shop_id").count() ;
//    return u_s_count.rdd().map(
//            new Function<Row, Tuple2<String,String>>(){
//              @Override
//              public Tuple2<String,String> call(Row arg) throws Exception {
//                return ro;
//              }
//            }) ;
//  }
}
