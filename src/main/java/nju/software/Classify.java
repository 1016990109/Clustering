package nju.software;

import nju.software.model.Shop;
import nju.software.model.UserPay;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

public class Classify {
    private static String hdfsIp = "hdfs://192.168.1.211:9000" ;
    public static void main(String[] args){
        Classify classify = new Classify() ;
                classify.createUserInfo();
    }
    public void createUserInfo(){
        SparkSession sparkSession = SparkSession.builder().appName("createUserInfo")
                .master("local").getOrCreate() ;
        Dataset<UserPay> userPay = sparkSession.read().option("header", "false")
                .csv(hdfsIp+"/ml/pay/*.csv").toDF("shop_id","user_id","time_stamp").as(Encoders.bean(UserPay.class));
        userPay.createOrReplaceTempView("userPay");
        Dataset<Shop> shopInfo = sparkSession.read().option("header", "false")
                .csv(hdfsIp+"/shop/*.csv")
                .toDF("id","city_name","location_id","per_pay","score","comment_cnt","shop_level","cate_1_name","cate_2_name","cate_3_name")
                .as(Encoders.bean(Shop.class));
//        shopInfo.show();
        Dataset<Row> join = userPay.join(shopInfo, userPay.col("shop_id").equalTo(shopInfo.col("id")));
        Dataset<Row> agg = join.groupBy("user_id").agg(count("id").name("times"), avg("per_pay").name("avg"));
        long count = agg.count() ;
//        System.out.println(count);
        agg.write().csv(hdfsIp+"/userInfo/");
    }

}
