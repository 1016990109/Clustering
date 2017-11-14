package nju.software;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import nju.software.model.Users;
import nju.software.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by SuperSY on 2017/11/8.
 */
public class Main implements Serializable {
    public static void main(String[] args) {
        Main m = new Main();
        m.train();
    }
    public void saveUser(){
        SparkConf sparkConf = new SparkConf().setAppName("Kmeans").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        Configuration config = new Configuration();



        //get shop infomation
        config.set("mongo.input.uri", "mongodb://192.168.1.212:27017/shop.shop_info");
        JavaPairRDD<Object, BSONObject> shopInfoRDD = sc.newAPIHadoopRDD(config, MongoInputFormat.class, Object.class, BSONObject.class);
        JavaRDD<String> shopIdRDD = shopInfoRDD.map(
                new Function<Tuple2<Object, BSONObject>, String>() {
                    @Override
                    public String call(Tuple2<Object, BSONObject> arg) throws Exception {
                        return arg._2.get("shop_id").toString();
                    }
                }
        );
        List<String> shopIdList = shopIdRDD.collect();


        //get user_pay
        config.set("mongo.input.uri", "mongodb://192.168.1.212:27017/shop.user_pay");
        JavaPairRDD<Object, BSONObject> user_pay_row = sc.newAPIHadoopRDD(config, MongoInputFormat.class, Object.class, BSONObject.class);

        //create users instance
        JavaPairRDD<String, String> user_shop = user_pay_row.mapToPair(
                new PairFunction<Tuple2<Object, BSONObject>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Object, BSONObject> arg) throws Exception {
                        String user_id = arg._2.get("user_id").toString();
                        String shop_id = arg._2.get("shop_id").toString();
                        return new Tuple2<>(user_id, shop_id);
                    }
                });
        Users u = user_shop.aggregate(new Users(), addUserView, addUsers);

        //save into user.txt
        u.save(shopIdList);

        sc.stop() ;
    }

    public void train(){
        SparkConf sparkConf = new SparkConf().setAppName("Kmeans").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        Configuration config = new Configuration();

        //create log file
        FileUtil.writeLog(new Date().toString() + ": begin\n");

        JavaRDD<Vector> data = sc.textFile("./user.txt").map(s -> {
            String[] line = s.split(":");
            return Vectors.dense(Arrays.stream(line[1].split(" ")).mapToDouble(Double::parseDouble).toArray());
        });


        //train get Cost
        int numIterations = 20 ;
        for (int k = 2; k < 21; k++)
        {
            KMeansModel clusters = KMeans.train(data.rdd(), k, numIterations);
            double wssse = clusters.computeCost(data.rdd());
            FileUtil.writeLog(new Date().toString() + ": k=" + k + ",误差: " + wssse + "\n");
        }
        FileUtil.closeLog();
        sc.stop();
    }

    public void clustering(int k){
        SparkConf sparkConf = new SparkConf().setAppName("Kmeans").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        Configuration config = new Configuration();

        //create data
        JavaRDD<LabeledPoint> lBdata = sc.textFile("./user.txt").map(s -> {
            String[] line = s.split(":");
            Vector dense = Vectors.dense(Arrays.stream(line[1].split(" ")).mapToDouble(Double::parseDouble).toArray());
            return new LabeledPoint(Double.parseDouble(line[0]), dense);
        });

        //clustering
        int numIterations = 20 ;
        KMeansModel clusters = KMeans.train(lBdata.map(s -> {return s.features();}).rdd(), k, numIterations);
        Broadcast<KMeansModel> broadcast = sc.broadcast(clusters);


        //get k_id-user_id map
        JavaPairRDD<String,String> k_user = lBdata.mapToPair(new PairFunction<LabeledPoint, String, String>() {
            @Override
            public Tuple2<String, String> call(LabeledPoint s) throws Exception {
                return new Tuple2<String, String>(broadcast.value().predict(s.features()) + "",s.label() + "");
            }
        }) ;

        //get k_id-user1_id,user2_id...
        JavaPairRDD<String, String> k_users = k_user.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s + "," + s2;
            }
        });

        //save into clusteringResult.txt
        k_users.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> s) throws Exception {
                FileUtil.writeClustering(s._1+":    "+s._2);
            }
        });

        FileUtil.closeClustering();
        sc.stop();
    }
    public void cal() {
//        SparkConf sparkConf = new SparkConf().setAppName("Kmeans").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        Configuration config = new Configuration();
//
//        //create log file
//        FileUtil.writeLog(new Date().toString() + ": begin\n");


//        //get shop infomation
//        config.set("mongo.input.uri", "mongodb://192.168.1.212:27017/shop.shop_info");
//        JavaPairRDD<Object, BSONObject> shopInfoRDD = sc.newAPIHadoopRDD(config, MongoInputFormat.class, Object.class, BSONObject.class);
//        JavaRDD<String> shopIdRDD = shopInfoRDD.map(
//                new Function<Tuple2<Object, BSONObject>, String>() {
//                    @Override
//                    public String call(Tuple2<Object, BSONObject> arg) throws Exception {
//                        return arg._2.get("shop_id").toString();
//                    }
//                }
//        );
//        List<String> shopIdList = shopIdRDD.collect();

//        List<String> shopIdList = new ArrayList<>();
//        shopIdList.add("1");
//        shopIdList.add("2");
        //get user_pay infomation
//        config.set("mongo.input.uri", "mongodb://192.168.1.212:27017/shop.user_pay");
//        config.set("mongo.output.uri", "mongodb://192.168.1.212:27017/shop.user_pay_total");
//        JavaPairRDD<Object, BSONObject> user_view = sc.newAPIHadoopRDD(config, MongoInputFormat.class, Object.class, BSONObject.class);
//
//
//        JavaPairRDD<String, String> view = user_view.mapToPair(
//                new PairFunction<Tuple2<Object, BSONObject>, String, String>() {
//            @Override
//            public Tuple2<String, String> call(Tuple2<Object, BSONObject> arg) throws Exception {
//                String user_id = arg._2.get("user_id").toString();
//                String shop_id = arg._2.get("shop_id").toString();
//                return new Tuple2<>(user_id, shop_id);
//            }
//        });
//        Users u = view.aggregate(new Users(), addUserView, addUsers);
//        u.save(shopIdList);

//        JavaRDD<Vector> data = sc.textFile("./user.txt").map(s -> {
//            String[] line = s.split(":");
//            return Vectors.dense(Arrays.stream(line[1].split(" ")).mapToDouble(Double::parseDouble).toArray());
//        }) ;
//        //train
//        JavaRDD<LabeledPoint> data = sc.textFile("./user.txt").map(s -> {
//            String[] line = s.split(":");
//            Vector dense = Vectors.dense(Arrays.stream(line[1].split(" ")).mapToDouble(Double::parseDouble).toArray());
//            return new LabeledPoint(Double.parseDouble(line[0]), dense);
//        });

//        JavaRDD<Vector> data = sc.parallelize(u.createV(shopIdList));
//        int numIterations = 20;
//        int run = 10;
//        FileUtil.writeLog(new Date().toString() + ": 构造数据完成\n");
//        KMeans kMeans = new KMeans() ;
//        KMeansModel train = KMeans.train(data.rdd(), 2, 20);
//        Broadcast<KMeansModel> broadcast = sc.broadcast(train);

        //        sc.parallelize(train.predict(data).) ;
//        for (int k = 2; k < 21; k++)

// {
////            data.foreach(new VoidFunction<Vector>() {
////                @Override
////                public void call(Vector vector) throws Exception {
////                    System.out.println(vector.toArray());
////                }
////            });
//            KMeansModel clusters = KMeans.train(data.rdd(), k, numIterations);
////            KMeans kMeans1= new KMeans();
////            kMeans1.setK(2);
////            kMeans1.setMaxIterations(20);
////            KMeansModel clusters = kMeans1.run(data.rdd());
//
//            double wssse = clusters.computeCost(data.rdd());
////            for (int i = 0; i < clusters.clusterCenters().length; i++) {
////                double[] centerArray = clusters.clusterCenters()[i].toArray();
////                for (int j = 0; j < centerArray.length; j++) {
////                    if (centerArray[j] != 0) {
////                        System.out.println(centerArray[j]);
////                    }
////                }
////            }
//            FileUtil.writeLog(new Date().toString() + ": k=" + k + ",误差: " + wssse + "\n");
//        }
//        FileUtil.close();
    }

    Function2<Users, Tuple2<String, String>, Users> addUserView =
            new Function2<Users, Tuple2<String, String>, Users>() {
                public Users call(Users users, Tuple2<String, String> userView) throws Exception {
                    Map<String, Integer> shops = users.user.get(userView._1);
                    if (shops == null)
                        shops = new HashMap<>();
                    Integer times = shops.get(userView._2);
                    if (times == null)
                        times = 0;
                    times++;
                    shops.put(userView._2, times);
                    users.user.put(userView._1, shops);
                    return users;
                }

            };
    Function2<Users, Users, Users> addUsers =
            new Function2<Users, Users, Users>() {
                @Override
                public Users call(Users users, Users users2) throws Exception {
                    users.user.putAll(users2.user);
                    return users;
                }
            };


    /**
     * save into mongodb
     * @param sc
     * @param u
     * @param shopIdList
     * @param config
     */
    public void saveToMongodb(JavaSparkContext sc,Users u,List<String> shopIdList,Configuration config){
        JavaRDD<LabeledPoint> user_pay_total = sc.parallelize(u.createLP(shopIdList));
        JavaPairRDD<Object, BSONObject> save = user_pay_total.mapToPair(new PairFunction<LabeledPoint, Object, BSONObject>() {
            @Override
            public Tuple2<Object, BSONObject> call(LabeledPoint labeledPoint) throws Exception {
                BSONObject bson = new BasicBSONObject();
                bson.put("user_id", labeledPoint.label());
                for (int i = 0; i < shopIdList.size(); i++) {
                    bson.put(shopIdList.get(i), labeledPoint.features().apply(i));
                }
                return new Tuple2<>(null, bson);
            }
        });
        save.saveAsNewAPIHadoopFile("file:///data", Object.class, Object.class, MongoOutputFormat.class, config);
    }

}
