//package nju.software.util;
//
//import com.mongodb.*;
////import com.mongodb.hadoop .MongoOutputFormat;
//import nju.software.model.Shop;
//import nju.software.model.Users;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.mllib.regression.LabeledPoint;
//import org.bson.BSONObject;
//import org.bson.BasicBSONObject;
//import scala.Tuple2;
//
//import java.net.UnknownHostException;
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * Created by SuperSY on 2017/11/8.
// */
//public class MongodbUtil {
//    private static String mongodbIp = "192.168.1.212" ;
//    private static int mongodbPort = 27017 ;
//    private static String database = "shop" ;
//    private static DB db ;
//    public static void init() throws UnknownHostException {
//        MongoClient mongoClient = new MongoClient(mongodbIp, mongodbPort);
//        db = mongoClient.getDB(database);
//    }
//    public static List<Shop> findAllShop(){
//        List<Shop> shops= new ArrayList<>() ;
//        DBCollection col = db.getCollection("shop_info") ;
//        DBCursor result = col.find() ;
//        while (result.hasNext()){
//            shops.add(new Shop(result.next())) ;
//        }
//        return shops ;
//    }
//    public static int findPayCountByUserIdAndShopId(String user_id,String shop_id){
//        DBCollection  col = db.getCollection("user_pay") ;
//        DBObject queryObject = new BasicDBObject() ;
//        queryObject.put("user_id",user_id) ;
//        queryObject.put("shop_id",shop_id) ;
//        DBCursor result = col.find(queryObject) ;
//
//        return result.count() ;
//    }
//    public void getUserPayRDD(){
//
//    }
//    public static void main(String[] args) throws UnknownHostException {
//        MongodbUtil.init();
//    }
//    /**
//     * save into mongodb
//     * @param sc
//     * @param u
//     * @param shopIdList
//     * @param config
//     */
//    public void saveToMongodb(JavaSparkContext sc, Users u, List<String> shopIdList, Configuration config){
//        JavaRDD<LabeledPoint> user_pay_total = sc.parallelize(u.createLP(shopIdList));
//        JavaPairRDD<Object, BSONObject> save = user_pay_total.mapToPair(new PairFunction<LabeledPoint, Object, BSONObject>() {
//            @Override
//            public Tuple2<Object, BSONObject> call(LabeledPoint labeledPoint) throws Exception {
//                BSONObject bson = new BasicBSONObject();
//                bson.put("user_id", labeledPoint.label());
//                for (int i = 0; i < shopIdList.size(); i++) {
//                    bson.put(shopIdList.get(i), labeledPoint.features().apply(i));
//                }
//                return new Tuple2<>(null, bson);
//            }
//        });
////        save.saveAsNewAPIHadoopFile("file:///data", Object.class, Object.class, MongoOutputFormat.class, config);
//    }
//}
