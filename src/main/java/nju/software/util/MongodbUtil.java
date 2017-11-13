package nju.software.util;

import com.mongodb.*;
import nju.software.model.Shop;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by SuperSY on 2017/11/8.
 */
public class MongodbUtil {
    private static String mongodbIp = "192.168.1.212" ;
    private static int mongodbPort = 27017 ;
    private static String database = "shop" ;
    private static DB db ;
    public static void init() throws UnknownHostException {
        MongoClient mongoClient = new MongoClient(mongodbIp, mongodbPort);
        db = mongoClient.getDB(database);
    }
    public static List<Shop> findAllShop(){
        List<Shop> shops= new ArrayList<>() ;
        DBCollection col = db.getCollection("shop_info") ;
        DBCursor result = col.find() ;
        while (result.hasNext()){
            shops.add(new Shop(result.next())) ;
        }
        return shops ;
    }
    public static int findPayCountByUserIdAndShopId(String user_id,String shop_id){
        DBCollection  col = db.getCollection("user_pay") ;
        DBObject queryObject = new BasicDBObject() ;
        queryObject.put("user_id",user_id) ;
        queryObject.put("shop_id",shop_id) ;
        DBCursor result = col.find(queryObject) ;

        return result.count() ;
    }
    public void getUserPayRDD(){

    }
    public static void main(String[] args) throws UnknownHostException {
        MongodbUtil.init();
    }
}
