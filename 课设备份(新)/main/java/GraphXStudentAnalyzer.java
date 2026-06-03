import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.lib.ConnectedComponents;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassTag$;

public class GraphXStudentAnalyzer {

    public static Tuple2<Integer, Integer> runGraph(JavaRDD<String> activityIdRDD) {
        try {
            // 1. 去重
            JavaRDD<String> distinctIds = activityIdRDD.distinct();

            // 2. 构建顶点
            JavaRDD<Tuple2<Long, String>> vertices = distinctIds.map(id -> {
                long vid = (long) id.hashCode();
                if (vid < 0) vid = -vid;
                return new Tuple2<>(vid, id);
            });

            long count = vertices.count();
            if (count <= 1) {
                return new Tuple2<>((int) count, (int) count);
            }

            // 3. 构建边
            JavaRDD<Edge<String>> edges = vertices.cartesian(vertices)
                    .filter(pair -> !pair._1()._1().equals(pair._2()._1()))
                    .map(pair -> new Edge<>(
                            pair._1()._1(),
                            pair._2()._1(),
                            "related"
                    ));

            // 4. 构建图
            Graph<String, String> graph = Graph.fromEdges(
                    edges.rdd(),
                    "unknown",
                    StorageLevel.MEMORY_ONLY(),
                    StorageLevel.MEMORY_ONLY(),
                    ClassTag$.MODULE$.apply(String.class),
                    ClassTag$.MODULE$.apply(String.class)
            );

            // 5. 连通分量
            Graph<Object, String> cc = ConnectedComponents.run(
                    graph,
                    ClassTag$.MODULE$.apply(String.class),
                    ClassTag$.MODULE$.apply(String.class)
            );

            // 6. 统计
            JavaPairRDD<Object, Integer> groupStats = cc.vertices().toJavaRDD()
                    .mapToPair(v -> new Tuple2<>(v._2(), 1))
                    .reduceByKey(Integer::sum);

            int groupCount = (int) groupStats.count();
            int maxGroupSize = 0;
            if (groupCount > 0) {
                maxGroupSize = groupStats.values().reduce(Math::max);
            }

            return new Tuple2<>(groupCount, maxGroupSize);

        } catch (Exception e) {
            e.printStackTrace();
            return new Tuple2<>(0, 0);
        }
    }
}