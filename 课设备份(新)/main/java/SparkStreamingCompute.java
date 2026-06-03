import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SparkStreamingCompute {

    private static final String ZK_QUORUM = "qf01,qf02,qf03";
    private static final String ZK_PORT = "2181";
    private static final String SOURCE_TABLE = "student";
    private static final String RESULT_TABLE = "activity_stat";
    private static final String CF = "info";
    private static final int KEEP_LATEST_COUNT = 100;

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("SparkStreamingHBase")
                .setMaster("local[2]")
                .set("spark.ui.enabled", "false")
                .set("spark.driver.memory", "512m")
                .set("spark.testing.memory", "2147483648");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", ZK_QUORUM);
        hbaseConf.set("hbase.zookeeper.property.clientPort", ZK_PORT);

        System.out.println("========================================");
        System.out.println("   校园活动实时流计算 —— 已启动");
        System.out.println("========================================\n");

        jssc.socketTextStream("localhost", 9999).foreachRDD(rdd -> {

            try {
                Configuration readConf = HBaseConfiguration.create(hbaseConf);
                readConf.set(TableInputFormat.INPUT_TABLE, SOURCE_TABLE);

                JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD =
                        jssc.sparkContext().newAPIHadoopRDD(
                                readConf,
                                TableInputFormat.class,
                                ImmutableBytesWritable.class,
                                Result.class
                        );

                JavaRDD<Tuple2<Double, Integer>> dataRDD = hbaseRDD.map(tuple -> {
                    Result r = tuple._2();
                    try {
                        double score = Double.parseDouble(Bytes.toString(r.getValue(Bytes.toBytes(CF), Bytes.toBytes("score"))));
                        int checkin = Integer.parseInt(Bytes.toString(r.getValue(Bytes.toBytes(CF), Bytes.toBytes("checkin_num"))));
                        int sign = Integer.parseInt(Bytes.toString(r.getValue(Bytes.toBytes(CF), Bytes.toBytes("sign_num"))));
                        return new Tuple2<>(score, checkin * 100000 + sign);
                    } catch (Exception e) {
                        return null;
                    }
                }).filter(Objects::nonNull);

                long total = dataRDD.count();
                if (total <= 0) return;

                double totalScore = dataRDD.mapToDouble(t -> t._1()).sum();
                int totalCheckin = dataRDD.map(t -> t._2() / 100000).reduce(Integer::sum);
                int totalSign = dataRDD.map(t -> t._2() % 100000).reduce(Integer::sum);
                double avgScore = totalScore / total;
                double rate = (totalCheckin * 100.0) / totalSign;

                System.out.println("📊 实时统计 - 活动总数：" + total);
                System.out.println("📝 总报名：" + totalSign);
                System.out.println("✅ 总签到：" + totalCheckin);
                System.out.println("🎓 平均学分：" + String.format("%.2f", avgScore));
                System.out.println("📈 出勤率：" + String.format("%.2f", rate) + "%\n");

                // ===================== ✅ 图计算调用 =====================
                // 从 HBase 读取 activity_id 构建图关系
                JavaRDD<String> activityIdRDD = hbaseRDD.map(tuple -> {
                    Result result = tuple._2();
                    // 直接取列：info:activity_id
                    return Bytes.toString(result.getValue(Bytes.toBytes(CF), Bytes.toBytes("activity_id")));
                }).filter(Objects::nonNull).filter(id -> !id.isEmpty());

                // 调用 GraphX 方法
                Tuple2<Integer, Integer> graphRet = GraphXStudentAnalyzer.runGraph(activityIdRDD);
                int groupCount = graphRet._1();
                int maxGroupSize = graphRet._2();
                System.out.println(groupCount + maxGroupSize);
                // ==============================================================================

                writeToHBase(hbaseConf, total, totalSign, totalCheckin, avgScore, rate);
                cleanOldStatData(hbaseConf, KEEP_LATEST_COUNT);

            } catch (Exception ignored) {}
        });

        jssc.start();
        jssc.awaitTermination();
    }

    private static void writeToHBase(Configuration conf, long total, int totalSign, int totalCheckin, double avgScore, double rate) {
        try (Connection conn = ConnectionFactory.createConnection(conf);
             Table table = conn.getTable(TableName.valueOf(RESULT_TABLE))) {

            Put put = new Put(Bytes.toBytes("stat_" + System.currentTimeMillis()));
            put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("total_activity"), Bytes.toBytes(String.valueOf(total)));
            put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("total_sign"), Bytes.toBytes(String.valueOf(totalSign)));
            put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("total_checkin"), Bytes.toBytes(String.valueOf(totalCheckin)));
            put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("avg_score"), Bytes.toBytes(String.format("%.2f", avgScore)));
            put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("attendance_rate"), Bytes.toBytes(String.format("%.2f", rate)));
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void cleanOldStatData(Configuration conf, int keepCount) {
        try (Connection conn = ConnectionFactory.createConnection(conf);
             Table table = conn.getTable(TableName.valueOf(RESULT_TABLE));
             ResultScanner scanner = table.getScanner(new Scan())) {

            List<String> rowKeys = new ArrayList<>();
            for (Result res : scanner) {
                String rowKey = Bytes.toString(res.getRow());
                if (rowKey.startsWith("stat_")) rowKeys.add(rowKey);
            }

            if (rowKeys.size() > keepCount) {
                Collections.sort(rowKeys);
                int delCount = rowKeys.size() - keepCount;
                for (String k : rowKeys.subList(0, delCount)) {
                    table.delete(new Delete(Bytes.toBytes(k)));
                }
                System.out.println("🧹 已清理旧数据：" + delCount + "条");
            }
        } catch (Exception ignored) {}
    }
}