package nju.software.model;

import java.io.Serializable;

/**
 * Created by SuperSY on 2017/11/8.
 */
public class Shop implements Serializable{
    private String id;

    private String city_number ;

    private int  location_number ;

    private double  per_pay	;

    private double  score ;

    private int  comment_cnt ;

    private int shop_level ;

    private String cate_1_name ;

    private String cate_2_name;

    private String cate_3_name ;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCity_number() {
        return city_number;
    }

    public void setCity_number(String city_number) {
        this.city_number = city_number;
    }

    public int getLocation_number() {
        return location_number;
    }

    public void setLocation_number(int location_number) {
        this.location_number = location_number;
    }

    public double getPer_pay() {
        return per_pay;
    }

    public void setPer_pay(double per_pay) {
        this.per_pay = per_pay;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public int getComment_cnt() {
        return comment_cnt;
    }

    public void setComment_cnt(int comment_cnt) {
        this.comment_cnt = comment_cnt;
    }

    public int getShop_level() {
        return shop_level;
    }

    public void setShop_level(int shop_level) {
        this.shop_level = shop_level;
    }

    public String getCate_1_name() {
        return cate_1_name;
    }

    public void setCate_1_name(String cate_1_name) {
        this.cate_1_name = cate_1_name;
    }

    public String getCate_2_name() {
        return cate_2_name;
    }

    public void setCate_2_name(String cate_2_name) {
        this.cate_2_name = cate_2_name;
    }

    public String getCate_3_name() {
        return cate_3_name;
    }

    public void setCate_3_name(String cate_3_name) {
        this.cate_3_name = cate_3_name;
    }
}
