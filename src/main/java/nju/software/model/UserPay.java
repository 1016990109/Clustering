package nju.software.model;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;


public class UserPay implements Serializable{
    private int shop_id ;
    private int user_id ;
    private Timestamp time_stamp ;

    public int getShop_id() {
        return shop_id;
    }

    public void setShop_id(int shop_id) {
        this.shop_id = shop_id;
    }

    public int getUser_id() {
        return user_id;
    }

    public void setUser_id(int user_id) {
        this.user_id = user_id;
    }

    public Timestamp getTime_stamp() {
        return time_stamp;
    }

    public void setTime_stamp(Timestamp time_stamp) {
        this.time_stamp = time_stamp;
    }
}
