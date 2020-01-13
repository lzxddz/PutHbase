package cn.pidb.lzxtest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.io.IOException;
import java.util.UUID;


//import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor;
//import org.apache.hadoop.hbase.client.TableDescriptorBuilder.ModifyableTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;

//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.HColumnDescriptor;


public class MyHBaseTest {
    private static Configuration conf = null;
    private static Connection conn = null;
//    private static Table table = null;
    private static Admin admin = null;

    //public static final String hbaseZkQuorum = "10.0.88.53,10.0.88.54,10.0.88.55";
    public static final String hbaseZkQuorum = "10.0.82.235,10.0.82.236,10.0.82.237";
    public static final String zkZnodeParent = "/hbase-unsecure";

    static {
        // 设置连接信息
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", hbaseZkQuorum );
        //conf.set("hbase.zookeeper.property.clientPort", "2181");
        //conf.setInt("hbase.rpc.timeout", 2000);
        //conf.setInt("hbase.client.operation.timeout", 3000);
        //conf.setInt("hbase.client.scanner.timeout.period", 6000);
        conf.set("zookeeper.znode.parent",zkZnodeParent);
        try{
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception{
        // 创建表
        String[] colFams = {"blob"};
        String tblNameStr = "blobTable1";
        long colCount = 1000;
        int threadCount = 10; //线程数
        long rowCountPerThread = 1000000;//20000; //每个线程写入的记录条数

        long rowCountPerLog = 1000;  //每个线程，记录一次log的写入行数间隔

        String fileDir = "./test_files/1KB/";
        String logFileDir = "./test_logs/1KB/";
        // 处理命令行参数
        if(args.length > 0){
            if(args.length >= 1){
                if(args[0].equals("-help") ){
                    System.out.printf("args: <RowCountPerThread=%d> <ThreadCount=%d> <RowCountPerLog=%d>"+
                            "<TestFileDir=%s> <LogSaveDir=%s> <HTableColumnCount=%d> <HTableName=%s> ",
                            rowCountPerThread,threadCount,rowCountPerLog,fileDir,logFileDir,colCount,tblNameStr);
                    System.out.println();
                    System.exit(0);
                }
                try{
                    if(args.length > 0){
                        rowCountPerThread = Long.parseLong(args[0]);
                    }
                    if(args.length > 1){
                        threadCount = Integer.parseInt(args[1]);
                    }
                    if(args.length > 2){
                        rowCountPerLog = Long.parseLong(args[2]);
                    }
                    if(args.length > 3){
                        colCount = Long.parseLong(args[3]);
                    }
                    if(args.length > 4){
                        tblNameStr = args[4];
                    }
                }catch (Exception e){
                    e.printStackTrace();
                    System.exit(1);
                }

            }
        }

        if(!createTable(tblNameStr, colFams)){
            System.out.println("创建Hbase table 失败");
            System.exit(1);
        }
        System.out.println("Htable created");
       // String fileDir = "/home/bigdata/hbase_blob_test/files/"; // 10B txt
        logFileDir = logFileDir + new SimpleDateFormat("MMdd-HHmmss").format(new Date()) + "/";
        String mainLogFilePath = logFileDir +"main.log";
        FileWriter fw = null;
        try{
            File logDir = new File(logFileDir);
            if(logDir.exists()){
                throw new Exception("log file dir exists!");
            }
            else{
                logDir.mkdirs();
            }
            File logFile = new File(mainLogFilePath);
            logFile.createNewFile();

            logFile.setWritable(true);
            fw = new FileWriter(logFile);
        }catch (Exception e){
            e.printStackTrace();
            if(fw != null){
                fw.close();
            }
            System.exit(1);
        }

        String log_str = String.format("Dir:%s, ThreadCount:%d, ColumnCount:%d, RowCountPerThread:%d, AllBlobCount:%d  \n(rowCountPerLog:%d)\n",
                fileDir,threadCount,colCount,rowCountPerThread,rowCountPerThread*threadCount*colCount,rowCountPerLog);
        System.out.println(log_str);
        fw.write(log_str);
        fw.write("\n----hbase config: \n " );
        fw.write("hbase.zookeeper.quorum"+hbaseZkQuorum +" | " +
                "zookeeper.znode.parent"+zkZnodeParent+ " | "+
                "tablename: "+tblNameStr+"\n");

        log_str = new SimpleDateFormat("yyyy-MM-dd HH:mm::ss").format(new Date());
        System.out.println(log_str);
        fw.write(log_str);

        fw.write("\n#### begin ####\n");
        fw.flush();

        System.out.println("#### begin ####");
        System.out.println("create threads ...");

        long beginTime = System.currentTimeMillis();
        File logFile = new File(logFileDir);
        if(!logFile.exists()){
            logFile.mkdirs();
        }
        Thread[] threadArray = new Thread[threadCount];
        for(int i=0;i<threadCount;i++ ){
            //String testFilePath = fileDir + i + ".txt";// 10B txt files
            String testFilePath = fileDir + i + ".jpg";// 10B txt files
            String logFilePath = logFileDir + "thread_" + i +".log";
            Thread thread1 = new Thread(new MyTestThread(tblNameStr,colFams[0],
                    colCount,rowCountPerThread,testFilePath,logFilePath,rowCountPerLog));
            threadArray[i] = thread1;
            thread1.start();

            System.out.println("created thread : " + i);
            fw.write("created thread-" + i+", src file: "+testFilePath+"\n");
        }
        fw.flush();

        for(Thread t:threadArray){
            t.join();
        }

        long endTime = System.currentTimeMillis();
        long costSeconds = (endTime - beginTime) / 1000; // 花费总时间(S)
        String beginDateTime = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(beginTime));
        String endDateTime = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(endTime));



        System.out.println("#### end ####");
        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm::ss").format(new Date()));
        fw.write("\n#### end ####\n");
        fw.write(new SimpleDateFormat("yyyy-MM-dd HH:mm::ss").format(new Date())+"\n");


        System.out.println("---- Result ----");
        System.out.printf("Dir:%s, ThreadCount:%d, ColumnCount:%d, RowCountPerThread:%d, AllBlobCount:%d",
                fileDir,threadCount,colCount,rowCountPerThread,rowCountPerThread*threadCount*colCount);
        System.out.println();
        fw.write("---- Result ----\n");
        fw.write(String.format("Dir:%s, ThreadCount:%d, ColumnCount:%d, RowCountPerThread:%d, AllBlobCount:%d \n",
                fileDir,threadCount,colCount,rowCountPerThread,rowCountPerThread*threadCount*colCount));
        fw.flush();
        String costFmt = "";
        long tmp_cost = costSeconds;
        if(tmp_cost >= 86400){
            costFmt = String.valueOf(tmp_cost/86400) +"d";
            tmp_cost = tmp_cost % 86400;
        }
        if(tmp_cost >= 3600){
            costFmt = String.valueOf(tmp_cost/3600) +"h";
            tmp_cost = tmp_cost % 3600;
        }
        if(tmp_cost >= 60){
            costFmt = costFmt + String.valueOf(tmp_cost/60) +"m";
            tmp_cost = tmp_cost % 60;
        }
        costFmt = costFmt + String.valueOf(tmp_cost) +"s";


        System.out.printf(" begin: %s , end: %s , total cost: %s | %d seconds",
                beginDateTime,endDateTime,costFmt,costSeconds);
        System.out.println();
        fw.write(String.format(" begin: %s , end: %s , total cost: %s | %d seconds",
                beginDateTime,endDateTime,costFmt,costSeconds));
        fw.flush();
        fw.close();

    }



    /**
     * 创建表
     */
    public static Boolean createTable(String tableNameStr, String[] colFam) {
        try {
            TableName tableName = TableName.valueOf(tableNameStr);
            Table table = conn.getTable(tableName);

            if (admin.tableExists(tableName)) {
                //表已经存在
            } else {
                //表不存在
                // 创建表
                TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder.newBuilder(tableName);
                // 添加列族
                ArrayList<ColumnFamilyDescriptor> colFamDescs = new ArrayList<ColumnFamilyDescriptor>();
                for (String colStr : colFam) {
                    colFamDescs.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(colStr)).build());
                }

                tableDescBuilder.setColumnFamilies(colFamDescs);

                admin.createTable(tableDescBuilder.build());

                admin.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

}

class MyTestThread extends Thread{

    private String hTableName ; // 表名称
    private String columnFamilyName; // 列族
    private long columnCount; //每个列族的列数

    private long rowCount; // 写入记录数
    private String srcFilePath; // 源文件路径
    private String logPath; // 日志写入路径
    private long rowCountPerLog; // 日志记录间隔

    private static Configuration conf = null;
    private static Connection conn = null;
    //    private static Table table = null;
    private static Admin admin = null;


    static {
        // 设置连接信息
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", MyHBaseTest.hbaseZkQuorum);
        //conf.set("hbase.zookeeper.property.clientPort", "2181");
        //conf.setInt("hbase.rpc.timeout", 2000);
        //conf.setInt("hbase.client.operation.timeout", 3000);
        //conf.setInt("hbase.client.scanner.timeout.period", 6000);
        conf.set("zookeeper.znode.parent",MyHBaseTest.zkZnodeParent);
        try{
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public MyTestThread(String hTableName,String colFmName,long colCount,long rowCount,String srcFilePath, String logPath, long rowCountPerLog){
        this.hTableName = hTableName;
        this.columnFamilyName = colFmName;
        this.columnCount = colCount;
        this.rowCount = rowCount;
        this.srcFilePath = srcFilePath;
        this.logPath = logPath;
        this.rowCountPerLog = rowCountPerLog;
    }

    @Override
    public void run() {
        long tmp = rowCount;
        byte[] columnFamily = Bytes.toBytes(columnFamilyName) ;
        //读取文件数据
        byte[] fContents;
        try{
            fContents = getContent(srcFilePath);
        }catch (Exception e){
            e.printStackTrace();
            return;
        }
        FileWriter fw;
        try{
            File logFile = new File(logPath);
            if(!logFile.exists()){
                logFile.createNewFile();
            }
            logFile.setWritable(true);
            fw = new FileWriter(logFile);
            fw.write("file size: " + fContents.length + " \n");
            fw.write(System.currentTimeMillis() +"\n-------\n");
            fw.flush();
        }catch (Exception e){
            e.printStackTrace();
            return;
        }

        // 获取HTable
        Table table ;
        try{
            table = conn.getTable(TableName.valueOf(hTableName));
        }catch (Exception e){
            e.printStackTrace();
            return;
        }
        System.out.println("begin");
        long beginTime = System.currentTimeMillis();

        try{
            while (tmp>0){
                long tmp_logcount = rowCountPerLog;
                if(tmp<rowCountPerLog){
                    tmp_logcount = tmp;
                }
                for(long j=0;j<tmp_logcount;j++){
                    String key = UUID.randomUUID().toString().replaceAll("-","");
                    Put put = new Put(Bytes.toBytes(key));
                    for(int i=0;i<columnCount;i++){
                        put.addColumn(columnFamily, Bytes.toBytes(i), fContents);
                    }
                    table.put(put);
                }

                tmp -= tmp_logcount;

                System.out.println((String.format("%d,%d\n",System.currentTimeMillis(),rowCount-tmp)));
                fw.write(String.format("%d,%d\n",System.currentTimeMillis(),rowCount-tmp));
                fw.flush();

            }
            long endTime = System.currentTimeMillis();
            long costTime =( endTime - beginTime)/1000;
            String beginDateTime = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(beginTime));
            String endDateTime = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(endTime));
            String end_str = String.format("------\nbegin:%s;end:%s;cost:%d s",beginDateTime,endDateTime,costTime);
            fw.write(end_str);
            fw.flush();
            fw.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public byte[] getContent(String filePath) throws IOException {
        File file = new File(filePath);
        long fileSize = file.length();
        if (fileSize > Integer.MAX_VALUE) {
            System.out.println("file too big...");
            return null;
        }
        FileInputStream fi = new FileInputStream(file);
        byte[] buffer = new byte[(int) fileSize];
        int offset = 0;
        int numRead = 0;
        while (offset < buffer.length
                && (numRead = fi.read(buffer, offset, buffer.length - offset)) >= 0) {
            offset += numRead;
        }
        // 确保所有数据均被读取
        if (offset != buffer.length) {
            throw new IOException("Could not completely read file "
                    + file.getName());
        }
        fi.close();
        return buffer;
    }


}