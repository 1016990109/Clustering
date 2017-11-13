package nju.software.util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by SuperSY on 2017/11/7.
 */
public class FileUtil {
    public static boolean isBreak = false ;
    /**
     *
     * @param f
     * @param charset
     * @return
     * @throws IOException
     */
    public static List<String> getStringFromFile(File f, String charset) throws IOException {
        ArrayList<String> lines = new ArrayList<String>() ;
        FileInputStream fis = new FileInputStream(f);
        InputStreamReader isr = new InputStreamReader(fis, charset);
        BufferedReader br = new BufferedReader(isr);
        String line = null;
        while ((line = br.readLine()) != null) {
            lines.add(line) ;
        }
        br.close();
        isr.close();
        fis.close();
        return lines ;
    }
    public static void writwLog(File f,String content){
        try {
            FileWriter fw = new FileWriter(f,true) ;
            BufferedWriter bw = new BufferedWriter(fw) ;
            bw.write(content);
            bw.close();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
