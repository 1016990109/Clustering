//package nju.software;
//
//import com.mongodb.hadoop.MongoInputFormat;
//import com.mongodb.spark.MongoSpark;
//import nju.software.model.Users;
//import nju.software.util.FileUtil;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.*;
//import org.apache.spark.broadcast.Broadcast;
//import org.apache.spark.mllib.clustering.KMeans;
//import org.apache.spark.mllib.clustering.KMeansModel;
//import org.apache.spark.mllib.linalg.*;
//import org.apache.spark.mllib.linalg.Vector;
//import org.apache.spark.mllib.regression.LabeledPoint;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.bson.BSONObject;
//import scala.Tuple2;
//
//import java.io.Serializable;
//import java.math.BigDecimal;
//import java.util.*;
//
///**
// * Created by SuperSY on 2017/11/8.
// */
//public class Clustering implements Serializable {
//    public static void main(String[] args) {
//        Clustering m = new Clustering();
////        m.saveUser();
//        m.train();
////        m.clustering(100);
//        m.recommend();
//    }
//    public void saveUser(){
//        SparkConf sparkConf = new SparkConf().setAppName("Kmeans").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        Configuration config = new Configuration();
//
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
//
//        //get user_pay
//        config.set("mongo.input.uri", "mongodb://192.168.1.212:27017/shop.user_pay");
//        JavaPairRDD<Object, BSONObject> user_pay_row = sc.newAPIHadoopRDD(config, MongoInputFormat.class, Object.class, BSONObject.class);
//
//        //create users instance
//        JavaPairRDD<String, String> user_shop = user_pay_row.mapToPair(
//                new PairFunction<Tuple2<Object, BSONObject>, String, String>() {
//                    @Override
//                    public Tuple2<String, String> call(Tuple2<Object, BSONObject> arg) throws Exception {
//                        String user_id = arg._2.get("user_id").toString();
//                        String shop_id = arg._2.get("shop_id").toString();
//                        return new Tuple2<>(user_id, shop_id);
//                    }
//                });
//        Users u = user_shop.aggregate(new Users(), addUserView, addUsers);
//
//        //save into user.txt
//        u.save(shopIdList);
//
//        sc.stop() ;
//    }
//
//    public void train(){
//        SparkConf sparkConf = new SparkConf().setAppName("Kmeans").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        Configuration config = new Configuration();
//
//        //create log file
//        FileUtil.writeLog(new Date().toString() + ": begin\n");
//
//        JavaRDD<Vector> data = sc.textFile("./user.txt").map(s -> {
//            String[] line = s.split(":");
//            return Vectors.dense(Arrays.stream(line[1].split(" ")).mapToDouble(Double::parseDouble).toArray());
//        });
//
//        //train get Cost
//        int numIterations = 20 ;
//        for (int k = 70; k < 1000; k+=10) {
//            KMeansModel clusters = KMeans.train(data.rdd(), k, numIterations);
//            double wssse = clusters.computeCost(data.rdd());
//            FileUtil.writeLog(new Date().toString() + ": k=" + k + ",误差: " + wssse + "\n");
//        }
//        FileUtil.closeLog();
//        sc.stop();
//    }
//
//    public void clustering(int k){
//        SparkConf sparkConf = new SparkConf().setAppName("Kmeans").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        Configuration config = new Configuration();
//
//        //create data
//        JavaRDD<LabeledPoint> lBdata = sc.textFile("./user.txt").map(s -> {
//            String[] line = s.split(":");
//            Vector dense = Vectors.dense(Arrays.stream(line[1].split(" ")).mapToDouble(Double::parseDouble).toArray());
//            return new LabeledPoint(Double.parseDouble(line[0]), dense);
//        });
//        //clustering
//        int numIterations = 20 ;
//        KMeansModel clusters = KMeans.train(lBdata.map(s -> {return s.features();}).rdd(), k, numIterations);
//        Broadcast<KMeansModel> broadcast = sc.broadcast(clusters);
//        //get k_id-user_id map
//        JavaPairRDD<String,String> k_user = lBdata.mapToPair(new PairFunction<LabeledPoint, String, String>() {
//            @Override
//            public Tuple2<String, String> call(LabeledPoint s) throws Exception {
//                return new Tuple2<String, String>(broadcast.value().predict(s.features()) + "",s.label() + "");
//            }
//        }) ;
//        //get k_id-user1_id,user2_id...
//        JavaPairRDD<String, String> k_users = k_user.reduceByKey(new Function2<String, String, String>() {
//            @Override
//            public String call(String s, String s2) throws Exception {
//                return s + "," + s2;
//            }
//        });
//        //save into clusteringResult.txt
//        k_users.foreach(new VoidFunction<Tuple2<String, String>>() {
//            @Override
//            public void call(Tuple2<String, String> s) throws Exception {
//                FileUtil.writeClustering(s._1+":"+s._2);
//            }
//        });
//        FileUtil.closeClustering();
//        sc.stop();
//    }
//
//    public void recommend(){
//        final SparkSession spark = SparkSession.builder()
//                .master("local").appName("recommend")
//                .config("spark.mongodb.input.uri", "mongodb://192.168.1.212/shop.user_pay")
//                .config("spark.mongodb.output.uri", "mongodb://192.168.1.212/shop.out")
//                .getOrCreate() ;
//
//        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
//        Dataset<Row> rowDataset = MongoSpark.load(jsc).toDF();
//        rowDataset.createOrReplaceTempView("user_pay");
//        JavaRDD<String> clusteringData = jsc.textFile("./clusteringResult.txt");
//        clusteringData.foreach((VoidFunction<String>) s -> {
//            String[] line = s.split(":") ;
//            String[] user_ids = line[1].split(",") ;
//            List<int[]> user_shop_times = new ArrayList<>() ;
//            int[] shop_times = new int[2001] ;
//            for(String user_id:user_ids){
//                int[] per = new int[2001] ;
//                per[0] = new BigDecimal(user_id.trim()).intValue() ;
//                Dataset<Row> pays = spark.sql("SELECT shop_id From user_pay WHERE user_id = "+per[0]) ;
//                pays.foreach((ForeachFunction<Row>) row -> {
//                    int shop_id = row.getInt(0) ;
//                    per[shop_id] = 1 ;
//                    shop_times[shop_id]++ ;
//                });
//                if(user_shop_times.size()<=10)
//                    user_shop_times.add(per) ;
//            }
//            int max = getMax(shop_times);
//            StringBuilder sb = new StringBuilder() ;
//            sb.append("In cluster ").append(line[0]).append(",we can recommend top shop:").append(max).append("to users:");
//            for(int[] ints:user_shop_times){
//                if (ints[max]==0) {
//                    sb.append(ints[0]+ "    ") ;
//                }
//            }
//            System.out.println(sb.toString());
//        });
//    }
//    public int getMax(int[] shop_times){
//        int max = 0 ;
//        for(int i=0;i<shop_times.length;i++){
//            if(shop_times[i]>max)
//                max = i ;
//        }
//        return max ;
//    }
//
//    Function2<Users, Tuple2<String, String>, Users> addUserView =
//            new Function2<Users, Tuple2<String, String>, Users>() {
//                public Users call(Users users, Tuple2<String, String> userView) throws Exception {
//                    Map<String, Integer> shops = users.user.get(userView._1);
//                    if (shops == null)
//                        shops = new HashMap<>();
//                    Integer times = shops.get(userView._2);
//                    if (times == null)
//                        times = 0;
//                    times++;
//                    shops.put(userView._2, times);
//                    users.user.put(userView._1, shops);
//                    return users;
//                }
//
//            };
//    Function2<Users, Users, Users> addUsers =
//            new Function2<Users, Users, Users>() {
//                @Override
//                public Users call(Users users, Users users2) throws Exception {
//                    users.user.putAll(users2.user);
//                    return users;
//                }
//            };
//
//
//
//
//}
