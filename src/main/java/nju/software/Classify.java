package nju.software;

import nju.software.model.Shop;
import nju.software.model.UserPay;
import nju.software.util.TimeStampUtil;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.feature.StandardScaler;
import org.apache.spark.mllib.feature.StandardScalerModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.sql.*;
import scala.collection.immutable.HashMap;

import java.io.Serializable;

import static org.apache.spark.sql.functions.*;

public class Classify implements Serializable {
    private static SparkSession sparkSession;
    private static String hdfsIp = "hdfs://master:9000";

    private static String shopPath = "/newShop/";


    public static void main(String[] args) {


        String lastMonthUserPayPath = "/pay/06/";
        String shopInfoPath = "/newShop/";
        String userInfoPath = "/userInfo/";

        String trainPayPath = "/pay/07/";
        String trainViewPath = "/view/07/";
        String trainViewTimesPath = "/viewTime/train/";
        String trainDataPath = "/ml/train/";
        String modelPath = "/ml/logisticRegression/";

        String testPayPath = "/pay/08/";
        String testViewPath = "/view/08/";
        String testViewTimesPath = "/viewTime/test/";
        String testDataPath = "/ml/test/";

        String testResultPath = "/ml/LogisticRegressionResult/";

        Classify classify = new Classify();
//        classify.createUserInfo(lastMonthUserPayPath,shopInfoPath,userInfoPath);

//        classify.getViewTimes(trainPayPath,trainViewPath,trainViewTimesPath);
//        classify.union(userInfoPath,shopInfoPath,trainViewTimesPath,trainDataPath);
//        classify.train(trainDataPath, modelPath);
//
//        classify.getViewTimes(testPayPath,testViewPath,testViewTimesPath);
//        classify.union(userInfoPath,shopInfoPath,testViewTimesPath,testDataPath);
//
//        classify.test(testDataPath,modelPath,testResultPath);
        classify.ct();

    }

    public Classify() {
        sparkSession = SparkSession.builder().appName("classify")
                .master("local").getOrCreate();
    }

    public void ct() {
        Dataset<Row> rowDataset = sparkSession.read().option("header", "false").csv(hdfsIp + "/ml/test").toDF();
        rowDataset.select(col("_c0")).groupBy("_c0").count().show();
    }

    /**
     * get last month user'pay record from path1,and shopInfo from path2 ,calculate and save user'pay info into savePath
     *
     * @param path1
     * @param path2
     * @param savePath
     */
    public void createUserInfo(String path1, String path2, String savePath) {
        Dataset<UserPay> userPay = sparkSession.read().option("header", "false")
                .csv(hdfsIp + path1).toDF("user_id", "shop_id", "time_stamp").as(Encoders.bean(UserPay.class));
        Dataset<Shop> shopInfo = sparkSession.read().option("header", "false")
                .csv(hdfsIp + path2)
                .toDF("id", "city_number", "location_number", "per_pay", "score", "comment_cnt", "shop_level", "cate_1_name", "cate_2_name", "cate_3_name")
                .as(Encoders.bean(Shop.class));
        Dataset<Row> join = userPay.join(shopInfo, userPay.col("shop_id").equalTo(shopInfo.col("id")));
        Dataset<Row> agg = join.groupBy("user_id").agg(count("id").name("times"), avg("per_pay").name("avg"));
        agg.write().csv(hdfsIp + savePath);
    }

    /**
     * get pay record from path1 and view record from path2,calauter and save view times record into savePath
     *
     * @param path1
     * @param path2
     * @param savePath
     */
    public void getViewTimes(String path1, String path2, String savePath) {
        Dataset<UserPay> userPay = sparkSession.read().option("header", "false")
                .csv(hdfsIp + path1).
                        toDF("user_id", "shop_id", "time_stamp").as(Encoders.bean(UserPay.class));
        Dataset<UserPay> userView = sparkSession.read().option("header", "false")
                .csv(hdfsIp + path2).
                        toDF("user_id", "shop_id", "time_stamp").as(Encoders.bean(UserPay.class));
        Dataset<Row> viewJoinPay = userPay.join(userView, userPay.col("user_id").equalTo(userView.col("user_id"))
                .and(userPay.col("shop_id").equalTo(userView.col("shop_id"))), "right");

        Dataset<String> viewNoPay = viewJoinPay.filter((FilterFunction<Row>) row -> {
            if (row.getString(0) == null)
                return true;
            else
                return false;
        }).map((MapFunction<Row, String>) row -> 0 + "," + row.getString(3) + "," + row.getString(4) + "," + 1, Encoders.STRING())
                .randomSplit(new double[]{0.86, 0.14})[1];
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
                .map((MapFunction<Row, String>) row -> 1 + "," + row.getString(0) + "," + row.getString(1) + "," + row.getLong(5), Encoders.STRING());

        viewNoPay.union(viewPay).write().text(hdfsIp + savePath);
    }

    /**
     * get userInfo from path1,shopInfo from path2,and viewTimesInfo from path3,calculate and save into savePath
     *
     * @param path1
     * @param path2
     * @param path3
     * @param savePath
     */
    public void union(String path1, String path2, String path3, String savePath) {
        Dataset<Row> userInfo = sparkSession.read().option("header", "false").csv(hdfsIp + path1)
                .toDF("user_id", "pay_times", "avg");
        Dataset<Row> shopInfo = sparkSession.read().option("header", "false")
                .csv(hdfsIp + path2)
                .toDF("id", "city_number", "location_number", "per_pay", "score", "comment_cnt", "shop_level", "cate_1_name", "cate_2_name", "cate_3_name")
                .select("id", "city_number", "location_number", "per_pay", "score", "comment_cnt", "shop_level");
        Dataset<Row> viewTimes = sparkSession.read().option("header", "false").csv(hdfsIp + path3)
                .toDF("label", "user_id", "shop_id", "view_times");
        viewTimes.join(shopInfo, viewTimes.col("shop_id").equalTo(shopInfo.col("id")))
                .join(userInfo, userInfo.col("user_id").equalTo(viewTimes.col("user_id")), "left")
                .select(viewTimes.col("label"), viewTimes.col("view_times")
                        , shopInfo.col("city_number"), shopInfo.col("location_number"), shopInfo.col("per_pay"),
                        shopInfo.col("score"), shopInfo.col("comment_cnt"), shopInfo.col("shop_level"),
                        userInfo.col("pay_times"), userInfo.col("avg")).na().fill("0")
                .write().csv(hdfsIp + savePath);
    }

    /**
     * get data from path,and save model into savePath
     *
     * @param path
     * @param savePath
     */
    public void train(String path, String savePath) {
        Dataset<LabeledPoint> labeledPoint = sparkSession.read().option("header", "false").csv(hdfsIp + path)
                .toDF("label", "view_times", "city_number", "location_number"
                        , "per_pay", "score", "comment_cnt", "shop_level", "pay_times", "avg")
                .map((MapFunction<Row, LabeledPoint>) row
                        -> new LabeledPoint(Double.parseDouble(row.getString(0))
                        , Vectors.dense(new double[]{
                        Double.parseDouble(row.getString(1)),
                        Double.parseDouble(row.getString(4)),
                        Double.parseDouble(row.getString(5)),
                        Double.parseDouble(row.getString(6)),
                        Double.parseDouble(row.getString(7)),
                        Double.parseDouble(row.getString(8)),
                        Double.parseDouble(row.getString(9)),
                })), Encoders.bean(LabeledPoint.class));
        //        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
//        JavaRDD<LabeledPoint> labeledPoint = sc.textFile(hdfsIp + path).map(s -> {
//            String[] line = s.split(",");
//            Vector dense = Vectors.dense(Double.parseDouble(line[1]),
//                    Double.parseDouble(line[4]), Double.parseDouble(line[5]), Double.parseDouble(line[6]),
//                    Double.parseDouble(line[7]), Double.parseDouble(line[8]), Double.parseDouble(line[8]));
//            return new LabeledPoint(Double.parseDouble(line[0]), dense);
//        });

        Dataset<Vector> vector = labeledPoint.map(new MapFunction<LabeledPoint, Vector>() {
            @Override
            public Vector call(LabeledPoint labeledPoint) throws Exception {
                return labeledPoint.features();
            }
        }, Encoders.kryo(Vector.class));
        StandardScalerModel standardScalerModel = new StandardScaler().fit(vector.rdd()).setWithMean(true).setWithStd(true);
        Dataset<LabeledPoint> tranLabeledPoint = labeledPoint.map(s -> new LabeledPoint(s.label(), standardScalerModel.transform(s.features())), Encoders.bean(LabeledPoint.class));

        LogisticRegressionModel model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(tranLabeledPoint.rdd());
        model.save(sparkSession.sparkContext(), hdfsIp + savePath);
    }

    /**
     * get data from path1,and load model from path2,predict and save into savePath
     *
     * @param path1
     * @param path2
     * @param savePath
     */
    public void test(String path1, String path2, String savePath) {
//        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
//        JavaRDD<LabeledPoint> lBdata = sc.textFile(hdfsIp + path1).map(s -> {
//            s = s.replaceFirst(",", ":");
//            String[] line = s.split(":");
//            Vector dense = Vectors.dense(Arrays.stream(line[1].split(",")).mapToDouble(Double::parseDouble).toArray());
//            return new LabeledPoint(Double.parseDouble(line[0]), dense);
//        });

        Dataset<LabeledPoint> lBdata = sparkSession.read().option("header", "false").csv(hdfsIp + path1).toDF("label", "view_times", "city_number", "location_number"
                , "per_pay", "score", "comment_cnt", "shop_level", "pay_times", "avg").map((MapFunction<Row, LabeledPoint>) row
                -> new LabeledPoint(Double.parseDouble(row.getString(0))
                , Vectors.dense(new double[]{
                Double.parseDouble(row.getString(1)),
                Double.parseDouble(row.getString(4)),
                Double.parseDouble(row.getString(5)),
                Double.parseDouble(row.getString(6)),
                Double.parseDouble(row.getString(7)),
                Double.parseDouble(row.getString(8)),
                Double.parseDouble(row.getString(9)),
        })), Encoders.bean(LabeledPoint.class));
        LogisticRegressionModel model = LogisticRegressionModel.load(sparkSession.sparkContext(), path2);
        Dataset<String> result = lBdata.map(s -> s.label() + "," + model.predict(s.features()), Encoders.STRING());
        result.write().csv(hdfsIp + savePath);
    }

    //    public void union() {
//        Dataset<Row> userInfo = sparkSession.read().option("header", "false").csv(hdfsIp + userInfoPath+"")
//                .toDF("user_id", "pay_times", "avg");
//        Dataset<Row> shopInfo = sparkSession.read().option("header", "false")
//                .csv(hdfsIp + shopPath)
//                .toDF("id", "city_name", "location_id", "per_pay", "score", "comment_cnt", "shop_level", "cate_1_name", "cate_2_name", "cate_3_name")
//                .as(Encoders.bean(Shop.class)).select("id", "location_id", "per_pay", "score", "comment_cnt", "shop_level");
//        Dataset<Row> viewTimes = sparkSession.read().option("header", "false").csv(hdfsIp + viewTimePath+"*.txt")
//                .toDF("label", "user_id", "shop_id", "view_times");
//        viewTimes.join(shopInfo, viewTimes.col("shop_id").equalTo(shopInfo.col("id")))
//                .join(userInfo, userInfo.col("user_id").equalTo(viewTimes.col("user_id")), "left")
//            .select(viewTimes.col("label"),viewTimes.col("view_times"),
//                shopInfo.col("location_id"), shopInfo.col("per_pay"), shopInfo.col("score"), shopInfo.col("comment_cnt"), shopInfo.col("shop_level"),
//                userInfo.col("pay_times"), userInfo.col("avg")).na().fill("0")
//            .write().csv(hdfsIp + trainDataPath);
//    }

//    public void train() {
//        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
//
//        JavaRDD<LabeledPoint> lBdata = sc.textFile(hdfsIp + trainDataPath+"").map(s -> {
//            s = s.replaceFirst(",", ":");
//            String[] line = s.split(":");
//            Vector dense = Vectors.dense(Arrays.stream(line[1].split(",")).mapToDouble(Double::parseDouble).toArray());
//            return new LabeledPoint(Double.parseDouble(line[0]), dense);
//        });
//        new LogisticRegression().setFeaturesCol("")
//        LogisticRegressionModel model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(lBdata.rdd());
//        model.save(sparkSession.sparkContext(), modelPath);
//    }


    public void transformCityAndLocation(String oldShopPath, String newShopPath) {
        Dataset<Row> shopInfo = sparkSession.read().option("header", "false")
                .csv(hdfsIp + oldShopPath)
                .toDF("id", "city_name", "location_id", "per_pay", "score", "comment_cnt", "shop_level", "cate_1_name", "cate_2_name", "cate_3_name");
        Dataset<String> newShopInfo = shopInfo.map((MapFunction<Row, String>) row -> row.getString(0)
                + "," + row.getString(1).hashCode()
                + "," + row.getString(1).hashCode() + row.getString(2)
                + "," + (row.getString(3) == null ? "0" : row.getString(3))
                + "," + (row.getString(4) == null ? "0" : row.getString(4))
                + "," + (row.getString(5) == null ? "0" : row.getString(5))
                + "," + (row.getString(6) == null ? "0" : row.getString(6))
                + "," + (row.getString(7) == null ? "0" : row.getString(7))
                + "," + (row.getString(8) == null ? "0" : row.getString(8))
                + "," + (row.getString(9) == null ? "0" : row.getString(9)), Encoders.STRING());
        newShopInfo.write().text(hdfsIp + newShopPath);
    }


    public void descisionTree() {
        Dataset<LabeledPoint> rowDataset = sparkSession.read().option("header", "false").csv(hdfsIp + "/ml/result2")
                .toDF("predict", "label", "city_number")
                .map((MapFunction<Row, LabeledPoint>) s -> new LabeledPoint(Double.parseDouble(s.getString(1)), Vectors.dense(new double[]{Double.parseDouble(s.getString(0)), Double.parseDouble(s.getString(0))})), Encoders.bean(LabeledPoint.class));
//        VectorIndexerModel featureIndexer = new VectorIndexer()
//                .setInputCol("features")
//                .setOutputCol("indexedFeatures")
//                .fit(rowDataset) ;
//        DecisionTreeRegressor dt = new DecisionTreeRegressor()
//                .setFeaturesCol("indexedFeatures") ;
//        Pipeline pip = new Pipeline()
//                .setStages(new PipelineStage[]{featureIndexer,dt}) ;
//        PipelineModel model = pip.fit(rowDataset) ;
//
//        Dataset<Row>[] datasets = rowDataset.randomSplit(new double[]{0.9, 0.1});
//        model.transform(datasets[1]).show(100);
        DecisionTreeModel model = DecisionTree.trainClassifier(rowDataset.rdd(), 2, new HashMap<>(), "gini", 3, 32);
        Dataset<String> map = rowDataset.randomSplit(new double[]{0.9, 0.1})[1].map((MapFunction<LabeledPoint, String>) labeledPoint -> labeledPoint.label() + "  " + model.predict(labeledPoint.features()), Encoders.STRING());
        map.show(100);
//        model.predict()
    }
}
