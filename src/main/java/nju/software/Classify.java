package nju.software;

import nju.software.model.Shop;
import nju.software.model.UserPay;
import nju.software.util.FileUtil;
import nju.software.util.TimeStampUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class Classify implements Serializable {
    private static SparkSession sparkSession;
    private static String hdfsIp = "hdfs://192.168.1.211:9000";
    private static String shopPath = "/shop/*.csv" ;
    private static String payTimesPath = "/ml/pay/*.csv" ;
    private static String userInfoPath = "/userInfo/" ;
    private static String payPath = "/ml/pay/part-00001-4b04f654-7ff4-4812-8d53-5bf0c5db3ab3-c000.csv";
    private static String viewPath = "/ml/view/part-00000-1e45f0db-29a2-45b1-a234-8fecd1f0c7a9-c000.csv";
    private static String viewTimePath = "" ;
    private static String trainDataPath = "" ;
    private static String modelPath = "./model.txt" ;

    public static void main(String[] args) {
        Classify classify = new Classify();
//        classify.createUserInfo();
//        classify.getViewTimes();
    }

    public Classify() {
        sparkSession = SparkSession.builder().appName("classify")
                .master("local").getOrCreate();
    }

    public void createUserInfo() {
        Dataset<UserPay> userPay = sparkSession.read().option("header", "false")
                .csv(hdfsIp + payTimesPath).toDF("shop_id", "user_id", "time_stamp").as(Encoders.bean(UserPay.class));
        Dataset<Shop> shopInfo = sparkSession.read().option("header", "false")
                .csv(hdfsIp + shopPath)
                .toDF("id", "city_name", "location_id", "per_pay", "score", "comment_cnt", "shop_level", "cate_1_name", "cate_2_name", "cate_3_name")
                .as(Encoders.bean(Shop.class));
        Dataset<Row> join = userPay.join(shopInfo, userPay.col("shop_id").equalTo(shopInfo.col("id")));
        Dataset<Row> agg = join.groupBy("user_id").agg(count("id").name("times"), avg("per_pay").name("avg"));
        long count = agg.count();
        agg.write().csv(hdfsIp + userInfoPath);
    }

    public void getViewTimes() {
        Dataset<UserPay> userPay = sparkSession.read().option("header", "false")
                .csv(hdfsIp + payPath).
                        toDF("user_id", "shop_id", "time_stamp").as(Encoders.bean(UserPay.class));
        Dataset<UserPay> userView = sparkSession.read().option("header", "false")
                .csv(hdfsIp + viewPath).
                        toDF("user_id", "shop_id", "time_stamp").as(Encoders.bean(UserPay.class));
        Dataset<Row> viewJoinPay = userPay.join(userView, userPay.col("user_id").equalTo(userView.col("user_id"))
                .and(userPay.col("shop_id").equalTo(userView.col("shop_id"))), "right");


        viewJoinPay.filter((FilterFunction<Row>) row -> {
            if (row.getString(0) == null)
                return true;
            else
                return false;
        }).foreach((ForeachFunction<Row>) row -> FileUtil.writeViewTime(0 + "," + row.getString(3) + "," + row.getString(4) + "," + 1));

        viewJoinPay.filter((FilterFunction<Row>) row -> {
            String payTime = row.getString(2);
            if (payTime == null)
                return false;
            String viewTime = row.getString(5);
            if (TimeStampUtil.moreThanOnlyOneWeek(payTime, viewTime))
                return true;
            else
                return false;
        }).groupBy(userPay.col("user_id"), userPay.col("shop_id"), userPay.col("time_stamp"))
                .agg(userPay.col("user_id"), userPay.col("shop_id"), countDistinct(userView.col("time_stamp")).name("times"))
                .foreach((ForeachFunction<Row>) row -> FileUtil.writeViewTime(1 + "," + row.getString(0) + "," + row.getString(1) + "," + row.getLong(5)));
    }

    public void union(){
        Dataset<Row> userInfo = sparkSession.read().option("header", "false").csv(hdfsIp + userInfoPath)
                .toDF("user_id", "pay_times", "avg");
        Dataset<Row> shopInfo = sparkSession.read().option("header", "false")
                .csv(hdfsIp + shopPath)
                .toDF("id", "city_name", "location_id", "per_pay", "score", "comment_cnt", "shop_level", "cate_1_name", "cate_2_name", "cate_3_name")
                .as(Encoders.bean(Shop.class)).select("id","location_id","score","comment_cnt","per_pay");
        Dataset<Row> viewTimes = sparkSession.read().option("header", "false").csv(hdfsIp + viewTimePath)
                .toDF("lable", "user_id", "shop_id", "view_times");
        viewTimes.join(shopInfo,viewTimes.col("shop_id").equalTo(shopInfo.col("id")))
                .join(userInfo,userInfo.col("user_id").equalTo(viewTimes.col("user_id"))) ;
        viewTimes.write().csv(hdfsIp+trainDataPath);
    }

    public void train(){
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        JavaRDD<LabeledPoint> lBdata = sc.textFile(hdfsIp + trainDataPath).map(s -> {
            s.replaceFirst(",",":") ;
            String[] line = s.split(":") ;
            Vector dense = Vectors.dense(Arrays.stream(s.split(",")).mapToDouble(Double::parseDouble).toArray());
            return new LabeledPoint(Double.parseDouble(line[0]), dense);
        });
        LogisticRegressionModel model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(lBdata.rdd());
        model.save(sparkSession.sparkContext(),modelPath);

    }
}
