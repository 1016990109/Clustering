package nju.software;

import nju.software.model.Shop;
import nju.software.model.UserPay;
import nju.software.util.TimeStampUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
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
    private static String shopPath = "/shop/*.csv";
    private static String payTimesPath = "/ml/pay/06/*.csv";
    private static String userInfoPath = "/userInfo/";
    private static String payPath = "/ml/pay/07/*.csv";
    private static String viewPath = "/ml/view/07/*.csv";
    private static String viewTimePath = "/ml/viewTime/";
    private static String trainDataPath = "/ml/train/";
    private static String modelPath = "./model";

    public static void main(String[] args) {
        Classify classify = new Classify();
//        classify.createUserInfo();
//        classify.getViewTimes();
//        classify.union();
//        classify.train();
        classify.test();
    }

    public Classify() {
        sparkSession = SparkSession.builder().appName("classify")
                .master("local").getOrCreate();
    }

    public void createUserInfo() {
        Dataset<UserPay> userPay = sparkSession.read().option("header", "false")
                .csv(hdfsIp + payTimesPath).toDF("user_id", "shop_id", "time_stamp").as(Encoders.bean(UserPay.class));
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


//        viewJoinPay.filter((FilterFunction<Row>) row -> {
//            if (row.getString(0) == null)
//                return true;
//            else
//                return false;
//        }).foreach((ForeachFunction<Row>) row -> FileUtil.writeViewTime(0 + "," + row.getString(3) + "," + row.getString(4) + "," + 1));

        Dataset<String> viewNoPay = viewJoinPay.filter((FilterFunction<Row>) row -> {
            if (row.getString(0) == null)
                return true;
            else
                return false;
        }).map(new MapFunction<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return 0 + "," + row.getString(3) + "," + row.getString(4) + "," + 1;
            }
        }, Encoders.STRING());

        Dataset<String> viewPay = viewJoinPay.filter((FilterFunction<Row>) row -> {
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
                .map(new MapFunction<Row, String>() {
                    @Override
                    public String call(Row row) throws Exception {
                        return 1 + "," + row.getString(0) + "," + row.getString(1) + "," + row.getLong(5);
                    }
                }, Encoders.STRING());

        viewNoPay.union(viewPay).write().text(hdfsIp + viewTimePath);
    }

    public void union() {
        Dataset<Row> userInfo = sparkSession.read().option("header", "false").csv(hdfsIp + userInfoPath+"*.csv")
                .toDF("user_id", "pay_times", "avg");
        Dataset<Row> shopInfo = sparkSession.read().option("header", "false")
                .csv(hdfsIp + shopPath)
                .toDF("id", "city_name", "location_id", "per_pay", "score", "comment_cnt", "shop_level", "cate_1_name", "cate_2_name", "cate_3_name")
                .as(Encoders.bean(Shop.class)).select("id", "location_id", "per_pay", "score", "comment_cnt", "shop_level");
        Dataset<Row> viewTimes = sparkSession.read().option("header", "false").csv(hdfsIp + viewTimePath+"*.txt")
                .toDF("label", "user_id", "shop_id", "view_times");
        viewTimes.join(shopInfo, viewTimes.col("shop_id").equalTo(shopInfo.col("id")))
                .join(userInfo, userInfo.col("user_id").equalTo(viewTimes.col("user_id")), "left")
            .select(viewTimes.col("label"),viewTimes.col("view_times"),
                shopInfo.col("location_id"), shopInfo.col("per_pay"), shopInfo.col("score"), shopInfo.col("comment_cnt"), shopInfo.col("shop_level"),
                userInfo.col("pay_times"), userInfo.col("avg")).na().fill("0")
            .write().csv(hdfsIp + trainDataPath);
    }

    public void train() {
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        JavaRDD<LabeledPoint> lBdata = sc.textFile(hdfsIp + trainDataPath).map(s -> {
            s = s.replaceFirst(",", ":");
            String[] line = s.split(":");
            Vector dense = Vectors.dense(Arrays.stream(line[1].split(",")).mapToDouble(Double::parseDouble).toArray());
            return new LabeledPoint(Double.parseDouble(line[0]), dense);
        });
        LogisticRegressionModel model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(lBdata.rdd());
        model.save(sparkSession.sparkContext(), modelPath);
    }

    public void test() {
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        double[] splitArray = {0.9, 0.1};
        JavaRDD<LabeledPoint>[] lBdata = sc.textFile(hdfsIp + trainDataPath).map(s -> {
            s = s.replaceFirst(",", ":");
            String[] line = s.split(":");
            Vector dense = Vectors.dense(Arrays.stream(line[1].split(",")).mapToDouble(Double::parseDouble).toArray());
            return new LabeledPoint(Double.parseDouble(line[0]), dense);
        }).randomSplit(splitArray);
        JavaRDD<Vector> map = lBdata[1].map(new Function<LabeledPoint, Vector>() {
            @Override
            public Vector call(LabeledPoint labeledPoint) throws Exception {
                return labeledPoint.features();
            }
        });
        LogisticRegressionModel model = LogisticRegressionModel.load(sparkSession.sparkContext(), modelPath);
        JavaRDD<Double> predict = model.predict(map);
        predict.foreach(new VoidFunction<Double>() {
            @Override
            public void call(Double aDouble) throws Exception {
                System.out.println(aDouble);
            }
        });
    }
}
