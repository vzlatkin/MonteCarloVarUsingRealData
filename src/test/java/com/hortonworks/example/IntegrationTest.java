package com.hortonworks.example;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

/**
 * Created by Vlad on 1/4/2016.
 */
public class IntegrationTest {
    private static Main m;

    @BeforeClass
    public static void setUp() throws Exception {
        if (System.getProperty("os.name").startsWith("Windows")) {
            System.setProperty("hadoop.home.dir", new File("windows_libs\\2.4.0.0").getAbsolutePath());
        }
        System.setProperty("spark.master", "local");
        m = new Main();
    }


    @Test
    /*
    run with the data in HDFS
     */
    public void test_1() throws Exception {

        m.run(new String[]{new File("companies_list.txt").toURI().getPath(), "hdfs://sandbox.hortonworks.com/tmp/stockData/*.csv"});
    }

}