package com.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseOperate {

    public static void main(String[] args) throws Exception {
        System.out.println(isTableExist("student"));
        createTable("student11", "info");
        createTable("student12", "base_info", "extra_info");
        addData("student11", "002", "info", "name", "liao");//注意增加数据，存在就是修改，不存在就是增加
        addData("student11", "002", "info", "age", "30");
        addData("student11", "004", "info", "school", "BUPT");
        getAllData("student11");
        getRowData("student11","001");
        getRowQualifierData("student11", "001", "info", "school");
        deleteRowsData("student11", "004");
        getAllData("student11");
        dropTable("student12");
        System.out.println(isTableExist("student11"));
        System.out.println(isTableExist("student12"));
    }

    public static Connection connection;
    public static HBaseAdmin admin;

    static {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "bigdata02:2181,bigdata03:2181,bigdata04:2181");
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = (HBaseAdmin) connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean isTableExist(String tableName) throws Exception {
        return admin.tableExists(tableName);
    }

    public static void createTable(String tableName, String... columnFamily) throws Exception {
        if (isTableExist(tableName)) {
            System.out.println("table " + tableName + " exists");
        } else {
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            admin.createTable(descriptor);
            System.out.println("table： " + tableName + "创建成功！");
        }
    }

    public static void addData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        //创建HTable对象
        HTable hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        //向Put对象中组装数据
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
        System.out.println("恭喜呀，插入数据成功啦");
    }

    public static void getAllData(String tableName) throws IOException{
        //HTable：封装了整个表的所有的信息（表名，列簇的信息），提供了操作该表数据所有的业务方法。
        HTable hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
        //得到用于扫描region的对象scan
        //Scan： 封装查询信息，很get有一点不同，Scan可以设置Filter
        Scan scan = new Scan();
        //使用HTable得到resultcanner实现类的对象
        ResultScanner resultScanner = hTable.getScanner(scan);
        for(Result result : resultScanner){
            //Cell：封装了Column的所有的信息：Rowkey、column qualifier、value、时间戳
            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                System.out.println("行键: " + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("列簇: " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值: " + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println();
            }
        }
    }

    //6、获取某行数据
    public static void getRowData(String tableName, String rowKey) throws IOException{
        HTable hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = hTable.get(get);
        //循环获取所有信息
        for(Cell cell : result.rawCells()){
            System.out.println("行键: " + Bytes.toString(result.getRow()));
            System.out.println("列簇: " + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值: " + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳: " + cell.getTimestamp());
        }
    }


    //7、获取某行指定的数据，比如指定某个列簇的某个列限定符
    public static void getRowQualifierData(String tableName, String rowKey, String family, String qualifier) throws IOException{
        HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = table.get(get);
        //循环获取所有信息,也可以单独打印自己需要的字段即可，这个一般根据业务需求修改。
        for(Cell cell : result.rawCells()){
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    //8、删除单行或多行数据
    public static void deleteRowsData(String tableName, String... rows) throws IOException{
        HTable hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
        List<Delete> deleteList = new ArrayList<Delete>();
        //循环
        for(String row : rows){
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        hTable.delete(deleteList);
        hTable.close();
    }

    //3、删除表 先disable 再drop（delete）
    public static void dropTable(String tableName) throws Exception{
        if(isTableExist(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("table: " + tableName + "删除成功");
        }else{
            System.out.println("table: " + tableName + "不存在");
        }
    }
}
