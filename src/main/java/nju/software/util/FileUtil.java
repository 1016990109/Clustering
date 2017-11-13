package nju.software.util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by SuperSY on 2017/11/7.
 */
public class FileUtil {
    public static boolean isBreak = false;
    private static String fileLog = "/home/hong/clustering.log";
    private static FileWriter logFw;
    private static String fileUser = "/home/hong/user.txt";
    private static FileWriter userFw;

    /**
     * @param f
     * @param charset
     * @return
     * @throws IOException
     */
    public static List<String> getStringFromFile(File f, String charset) throws IOException {
        ArrayList<String> lines = new ArrayList<String>();
        FileInputStream fis = new FileInputStream(f);
        InputStreamReader isr = new InputStreamReader(fis, charset);
        BufferedReader br = new BufferedReader(isr);
        String line = null;
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
                userFw = new FileWriter(new File(fileUser));
            }
            userFw.write(content);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeLog(String content) {
        try {
            if (logFw == null) {
                logFw = new FileWriter(new File(fileLog));
            }
            logFw.write(content);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close() {
        try {
            logFw.close();
            userFw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
