package nju.software.util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by SuperSY on 2017/11/7.
 */
public class FileUtil {
    private static String fileLog = "clustering.log";
    private static PrintWriter logFw;
    private static String fileUser = "user.txt";
    private static PrintWriter userFw;
    private static String fileClustering = "clusteringResult.txt" ;
    private static PrintWriter clusteringFw ;

    /**
     * @param f
     * @param charset
     * @return
     * @throws IOException
     */
    public static List<String> getStringFromFile(File f, String charset,int skip) throws IOException {
        ArrayList<String> lines = new ArrayList<String>();
        FileInputStream fis = new FileInputStream(f);
        InputStreamReader isr = new InputStreamReader(fis, charset);
        BufferedReader br = new BufferedReader(isr);
        String line = null;
        int k = skip ;
        if(k!=-1)
            while ((line = br.readLine()) != null) {
                if(k-- == 0) {
                    lines.add(line);
                    k = skip;
                }
            }
        else
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }
        br.close();
        isr.close();
        fis.close();
        return lines;
    }

    public static void writeUser(String content) {
        try {
            if (userFw == null) {
                userFw = new PrintWriter(new BufferedWriter(new FileWriter(new File(fileUser))));
            }
            userFw.println(content);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void closeUser(){
        userFw.close();
    }

    public static void writeLog(String content) {
        try {
            if (logFw == null) {
                logFw = new PrintWriter(new FileWriter(new File(fileLog)));
            }
            logFw.println(content);
            logFw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void closeLog() {
            logFw.close();
    }

    public static void writeClustering(String content) {
        try {
            if (clusteringFw == null) {
                clusteringFw = new PrintWriter(new FileWriter(new File(fileClustering)));
            }
            clusteringFw.println(content);
            clusteringFw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void closeClustering() {
        clusteringFw.close();
    }
}
