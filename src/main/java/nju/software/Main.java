package nju.software;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import nju.software.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.util.*;

/**
 * Created by SuperSY on 2017/11/8.
 */
public class Main implements Serializable {
    public static void main(String[] args) {
        Main m = new Main();
        m.cal();
    }

    public void cal() {
        SparkConf sparkConf = new SparkConf().setAppName("Kmeans").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        Configuration config = new Configuration();
        FileUtil.writeLog(new Date().toString() + ": begin\n");

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

        config.set("mongo.input.uri", "mongodb://192.168.1.212:27017/shop.user_pay");
        config.set("mongo.output.uri", "mongodb://192.168.1.212:27017/shop.user_pay_total");
        JavaPairRDD<Object, BSONObject> user_view = sc.newAPIHadoopRDD(config, MongoInputFormat.class, Object.class, BSONObject.class);

        JavaPairRDD<String, String> view = user_view.mapToPair(new PairFunction<Tuple2<Object, BSONObject>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Object, BSONObject> arg) throws Exception {
                String user_id = arg._2.get("user_id").toString();
                String shop_id = arg._2.get("shop_id").toString();
                return new Tuple2<>(user_id, shop_id);
            }
        });
        Users user = new Users();
        Users u = view.aggregate(user, addUserView, addUsers);


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
        JavaRDD<Vector> data = sc.parallelize(u.createV(shopIdList));
        int numIterations = 20;
        int run = 10;
        FileUtil.writeLog(new Date().toString() + ": 构造数据完成\n");
        for (int k = 1; k < 2; k++) {
            KMeansModel clusters = KMeans.train(data.rdd(), k, numIterations, run);
            double wssse = clusters.computeCost(data.rdd());
            FileUtil.writeLog(new Date().toString() + ": k=" + k + ",误差: " + wssse + "\n");
        }


//        for(Vector center:clusters.clusterCenters()){
//            System.out.println("  "+center) ;
//        }
    }

    static Function2<Users, Tuple2<String, String>, Users> addUserView =
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
    static Function2<Users, Users, Users> addUsers =
            new Function2<Users, Users, Users>() {
                @Override
                public Users call(Users users, Users users2) throws Exception {
                    users.user.putAll(users2.user);
                    return users;
                }
            };

    class Users implements Serializable {
        Map<String, Map<String, Integer>> user;

        public Users() {
            user = new HashMap<>();
        }

        public Users(String user_id, String shop_id) {
            user = new HashMap<>();
            Map<String, Integer> shops = new HashMap<>();
            shops.put(shop_id, 1);
            user.put(user_id, shops);
        }

        public List<LabeledPoint> createLP(List<String> shopIdList) {
            List<LabeledPoint> list = new ArrayList<>();
            double[] values = new double[2000];
            int i = 0;
            for (String userId : user.keySet()) {

                i = 0;
                Map<String, Integer> shops = user.get(userId);
                for (String shopId : shopIdList) {
                    if (shops.get(shopId) != null)
                        values[i++] = shops.get(shopId) * 1.0;
                    else
                        values[i++] = 0;
                }

                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < 2000; j++) {
                    sb.append(values[j] + ",");
                }
                sb.append("\n");
                FileUtil.writeUser("userID:" + userId + ":    " + sb.toString());
                list.add(new LabeledPoint(Double.valueOf(userId) * 1.0, Vectors.dense(values)));
                shops = null;
            }
            return list;
        }

        public List<Vector> createV(List<String> shopIdList) {

            List<Vector> list = new ArrayList<>();

            double[] values = new double[2000];
            int i = 0;
            for (String userId : user.keySet()) {
                for (int j = 0; j < 2000; j++) {
                    values[j] = 0;
                }
                i = 0;
                Map<String, Integer> shops = user.get(userId);
                for (String shopId : shopIdList) {
                    if (shops.containsKey(shopId))
                        values[i++] = shops.get(shopId) * 1.0;
                    else
                        values[i++] = 0;
                }

                list.add(Vectors.dense(values));
                shops = null;
            }
            return list;
        }

    }
}
