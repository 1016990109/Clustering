package nju.software.util;


import java.io.Serializable;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class TimeStampUtil implements Serializable{
    static DateFormat full = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss") ;
    public static Timestamp rollOneWeek(String current) {
        Calendar cc = Calendar.getInstance() ;
        try {
            cc.setTime(full.parse(current));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        cc.add(Calendar.DAY_OF_WEEK,-1);
        return new Timestamp(cc.getTimeInMillis()) ;
    }
    public static Timestamp addOneWeek(Timestamp current){
        Calendar cc = Calendar.getInstance() ;
        cc.setTime(current);
        cc.add(Calendar.DAY_OF_WEEK,1);
        return new Timestamp(cc.getTimeInMillis()) ;
    }
    public static boolean moreThanOnlyOneWeek(String t1,String t2){
        try {
            Calendar c1 = Calendar.getInstance();
            c1.setTime(full.parse(t1)) ;

            Calendar c2 = Calendar.getInstance();
            c2.setTime(full.parse(t2)) ;
            boolean b = c1.compareTo(c2) >= 0;
            c1.add(Calendar.DAY_OF_WEEK,-1);
            return b && c1.compareTo(c2)<0 ;
        } catch (ParseException e) {
            e.printStackTrace();
            return false ;
        }
    }
}
