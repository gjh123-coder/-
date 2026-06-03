import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootApplication
@RestController
@RequestMapping("/api")
@CrossOrigin("*")
public class SpringBootHBaseApplication {

    private static final String QUORUM = "qf01,qf02,qf03";
    private static final String PORT = "2181";
    private static final String STUDENT_TABLE = "student";
    private static final String STAT_TABLE = "activity_stat";
    private static final String CF = "info";

    public static void main(String[] args) {
        SpringApplication.run(SpringBootHBaseApplication.class, args);
        System.out.println("======================================");
        System.out.println("✅ SpringBoot+HBase 启动成功");
        System.out.println("📗 活动列表：http://localhost:8080/api/student/list");
        System.out.println("📗 新增活动：http://localhost:8080/api/student/add");
        System.out.println("📗 修改状态：http://localhost:8080/api/student/updateStatus");
        System.out.println("📗 删除活动：http://localhost:8080/api/student/delete");
        System.out.println("📗 统计数据：http://localhost:8080/api/stat/latest");
        System.out.println("======================================");
    }

    // 查询所有活动数据
    @GetMapping("/student/list")
    public Map<String, Object> list() {
        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> dataList = new ArrayList<>();

        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", QUORUM);
            conf.set("hbase.zookeeper.property.clientPort", PORT);

            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Table table = conn.getTable(TableName.valueOf(STUDENT_TABLE));
                 ResultScanner scanner = table.getScanner(new Scan())) {

                for (Result res : scanner) {
                    Map<String, Object> data = new HashMap<>();
                    data.put("rowKey", Bytes.toString(res.getRow()));
                    data.put("activity_id", Bytes.toString(res.getValue(Bytes.toBytes(CF), Bytes.toBytes("activity_id"))));
                    data.put("activity_name", Bytes.toString(res.getValue(Bytes.toBytes(CF), Bytes.toBytes("activity_name"))));
                    data.put("activity_type", Bytes.toString(res.getValue(Bytes.toBytes(CF), Bytes.toBytes("activity_type"))));
                    data.put("publish_unit", Bytes.toString(res.getValue(Bytes.toBytes(CF), Bytes.toBytes("publish_unit"))));
                    data.put("activity_level", Bytes.toString(res.getValue(Bytes.toBytes(CF), Bytes.toBytes("activity_level"))));
                    data.put("activity_place", Bytes.toString(res.getValue(Bytes.toBytes(CF), Bytes.toBytes("activity_place"))));
                    data.put("start_time", Bytes.toString(res.getValue(Bytes.toBytes(CF), Bytes.toBytes("start_time"))));
                    data.put("end_time", Bytes.toString(res.getValue(Bytes.toBytes(CF), Bytes.toBytes("end_time"))));
                    data.put("score", Bytes.toString(res.getValue(Bytes.toBytes(CF), Bytes.toBytes("score"))));
                    data.put("sign_num", Bytes.toString(res.getValue(Bytes.toBytes(CF), Bytes.toBytes("sign_num"))));
                    data.put("checkin_num", Bytes.toString(res.getValue(Bytes.toBytes(CF), Bytes.toBytes("checkin_num"))));
                    data.put("audit_status", Bytes.toString(res.getValue(Bytes.toBytes(CF), Bytes.toBytes("audit_status"))));
                    data.put("activity_status", Bytes.toString(res.getValue(Bytes.toBytes(CF), Bytes.toBytes("activity_status"))));
                    dataList.add(data);
                }

                result.put("code", 200);
                result.put("msg", "success");
                result.put("total", dataList.size());
                result.put("data", dataList);
            }

        } catch (Exception e) {
            result.put("code", 500);
            result.put("msg", "查询失败");
            e.printStackTrace();
        }
        return result;
    }

    // 新增活动
    @PostMapping("/student/add")
    public Map<String, Object> add(@RequestBody Map<String, Object> data) {
        Map<String, Object> result = new HashMap<>();
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", QUORUM);
            conf.set("hbase.zookeeper.property.clientPort", PORT);

            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Table table = conn.getTable(TableName.valueOf(STUDENT_TABLE))) {

                String rowKey = data.get("activity_id").toString();
                Put put = new Put(Bytes.toBytes(rowKey));

                put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("activity_id"), Bytes.toBytes(data.get("activity_id").toString()));
                put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("activity_name"), Bytes.toBytes(data.get("activity_name").toString()));
                put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("activity_type"), Bytes.toBytes(data.get("activity_type").toString()));
                put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("publish_unit"), Bytes.toBytes(data.get("publish_unit").toString()));
                put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("activity_level"), Bytes.toBytes(data.get("activity_level").toString()));
                put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("activity_place"), Bytes.toBytes(data.get("activity_place").toString()));
                put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("start_time"), Bytes.toBytes(data.get("start_time").toString()));
                put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("end_time"), Bytes.toBytes(data.get("end_time").toString()));
                put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("score"), Bytes.toBytes(data.get("score").toString()));
                put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("sign_num"), Bytes.toBytes(data.get("sign_num").toString()));
                put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("checkin_num"), Bytes.toBytes(data.get("checkin_num").toString()));
                put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("audit_status"), Bytes.toBytes(data.get("audit_status").toString()));
                put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("activity_status"), Bytes.toBytes(data.get("activity_status").toString()));

                table.put(put);
                result.put("code", 200);
                result.put("msg", "添加成功");
            }
        } catch (Exception e) {
            result.put("code", 500);
            result.put("msg", "添加失败：" + e.getMessage());
            e.printStackTrace();
        }
        return result;
    }

    // 修改活动状态
    @PostMapping("/student/updateStatus")
    public Map<String, Object> updateStatus(@RequestBody Map<String, String> data) {
        Map<String, Object> result = new HashMap<>();
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", QUORUM);
            conf.set("hbase.zookeeper.property.clientPort", PORT);

            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Table table = conn.getTable(TableName.valueOf(STUDENT_TABLE))) {

                String rowKey = data.get("rowKey");
                String newStatus = data.get("activity_status");
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(CF), Bytes.toBytes("activity_status"), Bytes.toBytes(newStatus));
                table.put(put);

                result.put("code", 200);
                result.put("msg", "状态修改成功");
            }
        } catch (Exception e) {
            result.put("code", 500);
            result.put("msg", "修改失败：" + e.getMessage());
            e.printStackTrace();
        }
        return result;
    }

    // 删除活动数据
    @PostMapping("/student/delete")
    public Map<String, Object> delete(@RequestBody Map<String, String> data) {
        Map<String, Object> result = new HashMap<>();
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", QUORUM);
            conf.set("hbase.zookeeper.property.clientPort", PORT);

            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Table table = conn.getTable(TableName.valueOf(STUDENT_TABLE))) {

                String rowKey = data.get("rowKey");
                Delete delete = new Delete(Bytes.toBytes(rowKey));
                table.delete(delete);
                result.put("code", 200);
                result.put("msg", "删除成功");
            }
        } catch (Exception e) {
            result.put("code", 500);
            result.put("msg", "删除失败：" + e.getMessage());
            e.printStackTrace();
        }
        return result;
    }

    // ===================== 【修复版】获取最新统计数据 =====================
    @GetMapping("/stat/latest")
    public Map<String, Object> getLatestStatData() {
        Map<String, Object> result = new HashMap<>();
        Map<String, Object> statData = new HashMap<>();
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", QUORUM);
            conf.set("hbase.zookeeper.property.clientPort", PORT);

            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Table table = conn.getTable(TableName.valueOf(STAT_TABLE))) {

                Scan scan = new Scan();
                scan.setReversed(true);

                ResultScanner scanner = table.getScanner(scan);
                Result res = scanner.next(); // 只取第一条（最新）

                if (res != null && !res.isEmpty()) {
                    statData.put("total_activity", Bytes.toString(res.getValue(Bytes.toBytes(CF), Bytes.toBytes("total_activity"))));
                    statData.put("total_sign", Bytes.toString(res.getValue(Bytes.toBytes(CF), Bytes.toBytes("total_sign"))));
                    statData.put("total_checkin", Bytes.toString(res.getValue(Bytes.toBytes(CF), Bytes.toBytes("total_checkin"))));
                    statData.put("avg_score", Bytes.toString(res.getValue(Bytes.toBytes(CF), Bytes.toBytes("avg_score"))));
                    statData.put("attendance_rate", Bytes.toString(res.getValue(Bytes.toBytes(CF), Bytes.toBytes("attendance_rate"))));
                }

                result.put("code", 200);
                result.put("msg", "success");
                result.put("data", statData);
            }
        } catch (Exception e) {
            result.put("code", 500);
            result.put("msg", "获取统计数据失败");
            e.printStackTrace();
        }
        return result;
    }
}