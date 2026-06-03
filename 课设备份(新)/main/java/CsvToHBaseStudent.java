import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class CsvToHBaseStudent {

    public static void main(String[] args) {
        String csvFile = "D:\\网络媒体\\shuju.csv";
        String tableName = "student";
        String cf = "info";

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "qf01,qf02,qf03");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        List<Put> putList = new ArrayList<>();
        int batchSize = 1000;

        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin admin = connection.getAdmin();
             Table table = connection.getTable(TableName.valueOf(tableName));
             BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            // ===================== 老版本 HBase 创建表（兼容 1.x）=====================
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
                HColumnDescriptor hcd = new HColumnDescriptor(cf);
                htd.addFamily(hcd);
                admin.createTable(htd);
                System.out.println("✅ 表创建成功：" + tableName);
            }

            String line;
            boolean firstLine = true;
            int index = 1;

            while ((line = br.readLine()) != null) {
                if (firstLine) {
                    firstLine = false;
                    continue;
                }
                if (line.trim().isEmpty()) continue;

                // 切割 CSV（关键修复）
                String[] values = line.split(",", -1);

                String rowKey = "row_" + index;
                Put put = new Put(Bytes.toBytes(rowKey));

                // ===================== 你的 13 个活动字段  =====================
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("activity_id"),      Bytes.toBytes(values[0]));
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("activity_name"),    Bytes.toBytes(values[1]));
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("activity_type"),    Bytes.toBytes(values[2]));
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("publish_unit"),     Bytes.toBytes(values[3]));
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("activity_level"),   Bytes.toBytes(values[4]));
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("activity_place"),   Bytes.toBytes(values[5]));
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("start_time"),       Bytes.toBytes(values[6]));
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("end_time"),         Bytes.toBytes(values[7]));
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("score"),            Bytes.toBytes(values[8]));
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("sign_num"),         Bytes.toBytes(values[9]));
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("checkin_num"),      Bytes.toBytes(values[10]));
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("audit_status"),     Bytes.toBytes(values[11]));
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("activity_status"),  Bytes.toBytes(values[12]));

                putList.add(put);
                index++;

                // 批量提交，防止数据丢失
                if (putList.size() >= batchSize) {
                    table.put(putList);
                    putList.clear();
                    System.out.println("✅ 已插入：" + (index - 1) + " 条");
                }
            }

            // 最后一批
            if (!putList.isEmpty()) {
                table.put(putList);
            }

            System.out.println("🎉 全部数据写入完成！总条数：" + (index - 1));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}