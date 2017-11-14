package nju.software;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import nju.software.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by SuperSY on 2017/11/7.
 */
public class Test {
    public static void main(String[] args) {
        Test t = new Test();
   //     t.saveUserView();
        t.saveUserPay();
    }

    public static DB getDB() throws UnknownHostException {
        MongoClient mongoClient = new MongoClient("192.168.1.212", 27017);
        DB db = mongoClient.getDB("shop");
        return db;
    }

    public void saveShopInfo() {
        File shop_info = new File("/home/hadoop/shop_info.txt");
        try {
            List<String> lines = FileUtil.getStringFromFile(shop_info, "utf-8",-1);
            DB db = Test.getDB();
            DBCollection coll = db.getCollection("shop_info");
            List<String> keys = new ArrayList<String>() ;
            keys.add("shop_id") ;
            keys.add("city_name") ;
            keys.add("location_id") ;
            keys.add("per_pay") ;
            keys.add("score") ;
            keys.add("comment_cnt") ;
            keys.add("shop_level") ;
            keys.add("cate_1_name") ;
            keys.add("cate_2_name") ;
            keys.add("cate_3_name") ;
            int i = 0 ;

            for (String line : lines) {
                i = 0 ;
                String[] words = line.split(",");
                BasicDBObject doc = new BasicDBObject() ;
                for(String word:words){
                    doc.append(keys.get(i++),word) ;
                }
                coll.insert(doc);
                doc = null;
            }
        }catch (ArrayIndexOutOfBoundsException e){
            e.printStackTrace();
            System.out.println("OutOfBounds");
            System.exit(0);
        } catch (IOException e) {
            System.out.println("IOException");
            e.printStackTrace();
            System.exit(0);
        }
    }
    public void saveUserPay() {
        System.out.println("1");
        File shop_info = new File("/home/hadoop/user_pay.txt");
        try {
            List<String> lines = FileUtil.getStringFromFile(shop_info, "utf-8",58);
            DB db = Test.getDB();
            DBCollection coll = db.getCollection("user_pay");
            List<String> keys = new ArrayList<String>();
            keys.add("user_id");
            keys.add("shop_id");
    //                keys.add("time_stamp");
            int i = 0;
            System.out.println("3");
            for (String line : lines) {
                i = 0;
                String[] words = line.split(",");
                BasicDBObject doc = new BasicDBObject();
                for(String key:keys){
                    doc.append(key,words[i++]) ;
                }
                coll.insert(doc);
            }
        }catch (ArrayIndexOutOfBoundsException e){
            e.printStackTrace();
            System.out.println("OutOfBounds");
            System.exit(0);
        } catch (IOException e) {
            System.out.println("IOException");
            e.printStackTrace();
            System.exit(0);
        }
    }

    public void saveUserView() {
        File shop_info = new File("/home/hadoop/user_view.txt");
        try {
            List<String> lines = FileUtil.getStringFromFile(shop_info, "utf-8",-1);
            DB db = Test.getDB();
            DBCollection coll = db.getCollection("user_view");
            List<String> keys = new ArrayList<String>() ;
            keys.add("user_id") ;
            keys.add("shop_id") ;
//            keys.add("time_stamp") ;
            int i = 0 ;

            for (String line : lines) {
                i = 0 ;
                String[] words = line.split(",");
                BasicDBObject doc = new BasicDBObject() ;
                for(String word:words){
                    doc.append(keys.get(i++),word) ;
                }
                coll.insert(doc);
                doc = null;
            }
        }catch (ArrayIndexOutOfBoundsException e){
            e.printStackTrace();
            System.out.println("OutOfBounds");
            System.exit(0);
        } catch (IOException e) {
            System.out.println("IOException");
            e.printStackTrace();
            System.exit(0);
        }
    }
}
