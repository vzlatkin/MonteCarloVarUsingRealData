package com.hortonworks.example;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import com.github.sakserv.propertyparser.PropertyParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by Vlad on 1/4/2016.
 */
public class UnitTest {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(UnitTest.class);
    private static HdfsLocalCluster dfsCluster;
    private static FileSystem hdfsFsHandle;
    private static Main m;

    // Setup the property parser
    private static PropertyParser propertyParser;

    static {
        try {
            propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
            propertyParser.parsePropsFile();
        } catch (IOException e) {
            LOG.error("Unable to load property file: {}", propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
        }
    }


    @BeforeClass
    public static void setUp() throws Exception {
        if (System.getProperty("os.name").startsWith("Windows")) {
            System.setProperty("hdp.release.version", "2.4.0.0");
        }
        System.setProperty("spark.master", "local");

        dfsCluster = new HdfsLocalCluster.Builder()
                .setHdfsNamenodePort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NAMENODE_PORT_KEY)))
                .setHdfsNamenodeHttpPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NAMENODE_HTTP_PORT_KEY)))
                .setHdfsTempDir(propertyParser.getProperty(ConfigVars.HDFS_TEMP_DIR_KEY))
                .setHdfsNumDatanodes(Integer.parseInt(propertyParser.getProperty(ConfigVars.HDFS_NUM_DATANODES_KEY)))
                .setHdfsEnablePermissions(
                        Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HDFS_ENABLE_PERMISSIONS_KEY)))
                .setHdfsFormat(Boolean.parseBoolean(propertyParser.getProperty(ConfigVars.HDFS_FORMAT_KEY)))
                .setHdfsEnableRunningUserAsProxyUser(Boolean.parseBoolean(
                        propertyParser.getProperty(ConfigVars.HDFS_ENABLE_RUNNING_USER_AS_PROXY_USER)))
                .setHdfsConfig(new Configuration())
                .build();
        dfsCluster.start();

        hdfsFsHandle = dfsCluster.getHdfsFileSystemHandle();
        m = new Main();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        hdfsFsHandle.close();
        dfsCluster.stop();
        m.close();
       }


    @Test
    /*
    tests var simulation with a single stock and a single trade
     */
    public void test_1() throws Exception {

        FSDataOutputStream writer = hdfsFsHandle.create(
                new Path("/tmp/stockData/BPL.csv"));
        writer.writeUTF("Date,Open,High,Low,Close,Volume,Adj Close,Symbol,Change_Pct\n" +
                "1990-01-02,x,x,x,x,x,x,BPL,0\n" +
                "1990-01-03,x,x,x,x,x,x,BPL,-1\n");
        writer.close();

        Object r = m.run(new String[]{UnitTest.class.getClassLoader().getResource("companies_list_test_1.txt").toURI().getPath(), "hdfs://localhost:20112/tmp/stockData/*.csv"});
        Assert.assertEquals("var simulation gave the wrong answer using single stock run", -1.0, (Float) r, 0.001);
    }

    @Test
    /*
    tests var simulation with a single stock and a multiple trades
     */
    public void test_2() throws Exception {

        FSDataOutputStream writer = hdfsFsHandle.create(
                new Path("/tmp/stockData/BPL.csv"));
        writer.writeUTF("Date,Open,High,Low,Close,Volume,Adj Close,Symbol,Change_Pct\n" +
                "1990-01-02,x,x,x,x,x,x,BPL,0\n" +
                "1990-01-03,x,x,x,x,x,x,BPL,-2\n" +
                "1990-01-04,x,x,x,x,x,x,BPL,-2\n" +
                "1990-01-05,x,x,x,x,x,x,BPL,-2\n" +
                "1990-01-06,x,x,x,x,x,x,BPL,-2\n");
        writer.close();

        Object r = m.run(new String[]{UnitTest.class.getClassLoader().getResource("companies_list_test_1.txt").toURI().getPath(), "hdfs://localhost:20112/tmp/stockData/*.csv"});
        Assert.assertEquals("var simulation gave the wrong answer", -2.0, (Float) r, 0.001);
    }

    @Test
    /*
    tests var simulation with multiple stocks and a multiple trades
     */
    public void test_3() throws Exception {

        FSDataOutputStream writer = hdfsFsHandle.create(new Path("/tmp/stockData/BPL.csv"));
        writer.writeUTF("Date,Open,High,Low,Close,Volume,Adj Close,Symbol,Change_Pct\n" +
                "1990-01-02,x,x,x,x,x,x,BPL,0\n" +
                "1990-01-03,x,x,x,x,x,x,BPL,-3\n" +
                "1990-01-04,x,x,x,x,x,x,BPL,-3\n" +
                "1990-01-05,x,x,x,x,x,x,BPL,-3\n" +
                "1990-01-06,x,x,x,x,x,x,BPL,-3\n");
        writer.close();

        writer = hdfsFsHandle.create(new Path("/tmp/stockData/AAPL.csv"));
        writer.writeUTF("Date,Open,High,Low,Close,Volume,Adj Close,Symbol,Change_Pct\n" +
                "1990-01-02,x,x,x,x,x,x,AAPL,0\n" +
                "1990-01-03,x,x,x,x,x,x,AAPL,-4\n" +
                "1990-01-04,x,x,x,x,x,x,AAPL,-4\n" +
                "1990-01-05,x,x,x,x,x,x,AAPL,-4\n" +
                "1990-01-06,x,x,x,x,x,x,AAPL,-4\n");
        writer.close();

        Object r = m.run(new String[]{UnitTest.class.getClassLoader().getResource("companies_list_test_3.txt").toURI().getPath(), "hdfs://localhost:20112/tmp/stockData/*.csv"});
        Assert.assertEquals("var simulation gave the wrong answer", -3.5, (Float) r, 0.001);
    }

    @Test
    /*
    tests var simulation where there are multiple stocks in a portfolio, but no trades that are common between them
     */
    public void test_4() throws Exception {

        FSDataOutputStream writer = hdfsFsHandle.create(new Path("/tmp/stockData/BPL.csv"));
        writer.writeUTF("Date,Open,High,Low,Close,Volume,Adj Close,Symbol,Change_Pct\n" +
                "1990-01-02,x,x,x,x,x,x,BPL,0\n" +
                "1990-01-03,x,x,x,x,x,x,BPL,-3\n");
        writer.close();

        writer = hdfsFsHandle.create(new Path("/tmp/stockData/AAPL.csv"));
        writer.writeUTF("Date,Open,High,Low,Close,Volume,Adj Close,Symbol,Change_Pct\n" +
                "1990-01-04,x,x,x,x,x,x,AAPL,0\n" +
                "1990-01-05,x,x,x,x,x,x,AAPL,-4\n");
        writer.close();

        Object r = m.run(new String[]{UnitTest.class.getClassLoader().getResource("companies_list_test_3.txt").toURI().getPath(), "hdfs://localhost:20112/tmp/stockData/*.csv"});
        Assert.assertEquals("var simulation gave the wrong answer", "No trade data", r);
    }

    @Test
    /*
    tests that $ amounts can be read from companies.txt instead of % weights
     */
    public void test_5() throws Exception {

        FSDataOutputStream writer = hdfsFsHandle.create(new Path("/tmp/stockData/BPL.csv"));
        writer.writeUTF("Date,Open,High,Low,Close,Volume,Adj Close,Symbol,Change_Pct\n" +
                "1990-01-02,x,x,x,x,x,x,BPL,1\n" +
                "1990-01-03,x,x,x,x,x,x,BPL,1\n" +
                "1990-01-04,x,x,x,x,x,x,BPL,1\n" +
                "1990-01-05,x,x,x,x,x,x,BPL,1\n" +
                "1990-01-06,x,x,x,x,x,x,BPL,1\n");
        writer.close();

        writer = hdfsFsHandle.create(new Path("/tmp/stockData/AAPL.csv"));
        writer.writeUTF("Date,Open,High,Low,Close,Volume,Adj Close,Symbol,Change_Pct\n" +
                "1990-01-02,x,x,x,x,x,x,AAPL,4\n" +
                "1990-01-03,x,x,x,x,x,x,AAPL,4\n" +
                "1990-01-04,x,x,x,x,x,x,AAPL,4\n" +
                "1990-01-05,x,x,x,x,x,x,AAPL,4\n" +
                "1990-01-06,x,x,x,x,x,x,AAPL,4\n");
        writer.close();

        Object r = m.run(new String[]{UnitTest.class.getClassLoader().getResource("companies_list_test_5.txt").toURI().getPath(), "hdfs://localhost:20112/tmp/stockData/*.csv"});
        Assert.assertEquals("var simulation gave the wrong answer", 1.6, (Float) r, 0.001);
    }
}