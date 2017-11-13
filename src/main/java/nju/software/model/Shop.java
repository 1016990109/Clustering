package nju.software.model;

import com.mongodb.DBObject;

/**
 * Created by SuperSY on 2017/11/8.
 */
public class Shop {
    private String shop_id ;

    private String city_name ;

    private String  location_id ;

    private String  per_pay	;

    private String  score ;

    private String  comment_cnt ;

    private String shop_level ;

    private String cate_1_name ;

    private String cate_2_name;

    private String cate_3_name ;
    public Shop(DBObject object){
        this.shop_id = (String)object.get("shop_id") ;
        this.city_name = (String)object.get("city_name") ;
        this.location_id = (String)object.get("location_id") ;
        this.per_pay = (String)object.get("per_pay") ;
        this.score = (String)object.get("score") ;
        this.comment_cnt = (String)object.get("comment_cnt") ;
        this.shop_level = (String)object.get("shop_level") ;
        this.cate_1_name = (String)object.get("cate_1_name") ;
        this.cate_2_name = (String)object.get("cate_2_name") ;
        this.cate_3_name = (String)object.get("cate_3_name") ;
    }
}
