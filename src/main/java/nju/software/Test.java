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
        MongoClient mongoClient = new MongoClient("192.168.172.1", 27017);
        DB db = mongoClient.getDB("shop");
        return db;
    }

    public void saveShopInfo() {
        File shop_info = new File("E://��һ/dataset/shop_info.txt");
        try {
            List<String> lines = FileUtil.getStringFromFile(shop_info, "utf-8");
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
            System.out.println("Խ��");
            System.exit(0);
        } catch (IOException e) {
            System.out.println("�ļ�shop_info.txt��ȡʧ��");
            e.printStackTrace();
            System.exit(0);
        }
    }
    public void saveUserPay() {
        System.out.println("1");
        File shop_info = new File("E://dataset/user_pay.txt");
        try {
            while(!FileUtil.isBreak) {
                List<String> lines = FileUtil.getStringFromFile(shop_info, "utf-8");
                DB db = Test.getDB();
                DBCollection coll = db.getCollection("user_pay");
                List<String> keys = new ArrayList<String>();
                keys.add("user_id");
                keys.add("shop_id");
                keys.add("time_stamp");
                int i = 0;
                System.out.println("3");
                for (String line : lines) {
                    i = 0;
                    String[] words = line.split(",");
                    BasicDBObject doc = new BasicDBObject();
                    for (String word : words) {
                        doc.append(keys.get(i++), word);
                    }
                    coll.insert(doc);
                    doc = null;
                }
            }
        }catch (ArrayIndexOutOfBoundsException e){
            e.printStackTrace();
            System.out.println("越界");
            System.exit(0);
        } catch (IOException e) {
            System.out.println("io失败");
            e.printStackTrace();
            System.exit(0);
        }
    }

    public void saveUserView() {
        File shop_info = new File("E://dataset/user_view.txt");
        try {
            List<String> lines = FileUtil.getStringFromFile(shop_info, "utf-8");
            DB db = Test.getDB();
            DBCollection coll = db.getCollection("user_view");
            List<String> keys = new ArrayList<String>() ;
            keys.add("user_id") ;
            keys.add("shop_id") ;
            keys.add("time_stamp") ;
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
            System.out.println("越界");
            System.exit(0);
        } catch (IOException e) {
            System.out.println("IO失败");
            e.printStackTrace();
            System.exit(0);
        }
    }
}
