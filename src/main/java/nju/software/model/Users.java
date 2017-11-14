package nju.software.model;

import nju.software.util.FileUtil;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class Users implements Serializable {
    public Map<String, Map<String, Integer>> user;

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

        int i = 0;
        for (String userId : user.keySet()) {
            double[] values = new double[2000];
            i = 0;
            Map<String, Integer> shops = user.get(userId);
            for (String shopId : shopIdList) {
                if (shops.get(shopId) != null)
                    values[i++] = shops.get(shopId) * 1.0;
                else
                    values[i++] = 0;
            }
            list.add(new LabeledPoint(Double.valueOf(userId) * 1.0, Vectors.dense(values)));
        }
        return list;
    }

    public List<Vector> createV(List<String> shopIdList) {
        List<Vector> list = new ArrayList<>();
        int i = 0;
        for (String userId : user.keySet()) {
            double[] values = new double[2000];
            i = 0 ;
            Map<String, Integer> shops = user.get(userId);
            for (String shopId : shopIdList) {
                if (shops.containsKey(shopId))
                    values[i++] = shops.get(shopId) * 1.0;
                else
                    values[i++] = 0;
            }

            list.add(Vectors.dense(values));
//            shops = null;
        }
        return list;
    }
    public void save(List<String> shopIdList){
        int i = 0;
        for (String userId : user.keySet()) {
            double[] values = new double[2000];
            i = 0 ;
            Map<String, Integer> shops = user.get(userId);
            for (String shopId : shopIdList) {
                if (shops.containsKey(shopId))
                    values[i++] = shops.get(shopId) * 1.0;
                else
                    values[i++] = 0;
            }
            FileUtil.writeUser(userId + ":" + Arrays.stream(values).boxed()
                    .map(d -> d+"").collect(Collectors.joining(" ")));
        }
        FileUtil.closeUser();
    }

}