package com.lf.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Classname MyHBaseDemo
 * @Date 2021/9/3 4:41 下午
 * @Created by LiuFei
 */
public class MyHBaseDemo {

    public static Configuration conf;

    static {
        //使用HBaseConfiguration 的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "a.lf.bigdata,b.lf.bigdata,c.lf.bigdata");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
    }

    //创建表
    @Test
    public void testCreateTable() throws IOException {
        // Connection实现了java中的java.lang.AutoCloseable接口。所有实现了这个接口的类都可以在try-with-resources结构中使用。

        //创建Connection是一项繁重的操作。Connection线程安全的，因此客户端可以一次创建一个连接，并与其他线程共享。
        //另一方面，HBaseAdmin和HBaseAdmin实例是轻量级的，并且不是线程安全的。
        // 通常，每个客户端应用程序实例化单个Connection，每个线程都将获得其自己的Table实例。不建议对Table and Admin进行缓存或池化。

        try (Connection connection = ConnectionFactory.createConnection(conf);
             HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();) {
            TableName tableName = TableName.valueOf("students");
            if (admin.tableExists(tableName)) {
                System.out.println("table " + tableName.getNameAsString() + " exists");
            } else {
                ColumnFamilyDescriptor cfDesc = ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes("F"))
                        .setCompressionType(Compression.Algorithm.SNAPPY)
                        .setCompactionCompressionType(Compression.Algorithm.SNAPPY)
                        .setDataBlockEncoding(DataBlockEncoding.PREFIX)
                        .setBloomFilterType(BloomType.ROW)
                        .build();

                TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
                        .setColumnFamily(cfDesc)
                        .build();

                admin.createTable(tableDesc);
            }
        }
    }

    //向表中插入数据
    @Test
    public void testPut() throws IOException {
        TableName tableName = TableName.valueOf("students");
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(tableName);) {

            List<Put> puts = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                Put put = new Put(Bytes.toBytes(i+""));

                put.addColumn(Bytes.toBytes("F"), Bytes.toBytes("name"), Bytes.toBytes("name" + i));
                put.addColumn(Bytes.toBytes("F"), Bytes.toBytes("age"), Bytes.toBytes(20 + i+""));
                put.addColumn(Bytes.toBytes("F"), Bytes.toBytes("address"), Bytes.toBytes("djt" + i));

                puts.add(put);
            }

            table.put(puts);
        }

    }

    //获取指定行
    @Test
    public void testGet() throws IOException {
        TableName tableName = TableName.valueOf("students");
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(tableName);) {
            String rowkey = "2";
            Get get = new Get(Bytes.toBytes(rowkey));
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                System.out.println("rowkey = " + Bytes.toString(result.getRow()));
                System.out.println("列族 = " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列限定符 = " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值 = " + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("时间戳 = " + cell.getTimestamp());
            }
        }

    }

    //获取指定行，指定列
    @Test
    public void testGet1() throws IOException {
        TableName tableName = TableName.valueOf("students");
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(tableName);) {
            String rowkey = "2";
            Get get = new Get(Bytes.toBytes(rowkey));
            get.addColumn(Bytes.toBytes("F"), Bytes.toBytes("name"));
            get.addColumn(Bytes.toBytes("F"), Bytes.toBytes("age"));
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                System.out.println("rowkey = " + Bytes.toString(result.getRow()));
                System.out.println("列族 = " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列限定符 = " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值 = " + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("时间戳 = " + cell.getTimestamp());
            }
        }
    }

    //scan全表
    @Test
    public void testScan() throws IOException {
        TableName tableName = TableName.valueOf("students");
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(tableName);) {

            Scan scan = new Scan();

            ResultScanner resultScanner = table.getScanner(scan);

            for (Result result : resultScanner) {
                for (Cell cell : result.rawCells()) {
                    System.out.println("rowkey = " + Bytes.toString(result.getRow()));
                    System.out.println("列族 = " + Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.println("列限定符 = " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.println("值 = " + Bytes.toString(CellUtil.cloneValue(cell)));
                    System.out.println("时间戳 = " + cell.getTimestamp());
                }
            }

        }
    }

    @Test
    public void testScan1() throws IOException {
        TableName tableName = TableName.valueOf("students");
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(tableName);) {

            Scan scan = new Scan();

            scan.addColumn(Bytes.toBytes("F"), Bytes.toBytes("name"));
            scan.addColumn(Bytes.toBytes("F"), Bytes.toBytes("age"));

            scan.setCacheBlocks(true);
            scan.setCaching(100);
            scan.withStartRow(Bytes.toBytes(5 + ""));
            scan.withStopRow(Bytes.toBytes(8 + ""));

            ResultScanner resultScanner = table.getScanner(scan);

            for (Result result : resultScanner) {
                for (Cell cell : result.rawCells()) {
                    System.out.println("rowkey = " + Bytes.toString(result.getRow()));
                    System.out.println("列族 = " + Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.println("列限定符 = " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.println("值 = " + Bytes.toString(CellUtil.cloneValue(cell)));
                    System.out.println("时间戳 = " + cell.getTimestamp());
                }
            }
        }
    }

    //删除数据
    @Test
    public void testDelete() throws IOException {
        TableName tableName = TableName.valueOf("students");
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(tableName);) {

            List<Delete> deletes = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                Delete delete = new Delete(Bytes.toBytes(i + ""));
                deletes.add(delete);
            }

            table.delete(deletes);
        }
    }

    //删除表
    @Test
    public void testDeleteTable() throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf);
             HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();) {
            TableName tableName = TableName.valueOf("students");
            if (admin.tableExists(tableName)) {
                //先disable再delete
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println("table " + tableName.getNameAsString() + " delete success");
            } else {
                System.out.println("table " + tableName.getNameAsString() + " is not exists");
            }
        }
    }


    public static void main(String[] args) {
        System.out.println("hello hbase!");
    }
}
