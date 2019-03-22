package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import models.Payload;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Exercise_3 {

    private static class VProg extends AbstractFunction3<Long, Payload,Payload,Payload> implements Serializable {
        @Override
        public Payload apply(Long vertexID, Payload vertexValue, Payload message) {
            if (message.getValue() == Integer.MAX_VALUE || message.getValue() < 0) { // superstep 0
                return vertexValue;
            } else if (message.getValue() <= vertexValue.getValue()) { // superstep > 0
                return message;
            } else {
                return vertexValue;
            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Payload,Integer>, Iterator<Tuple2<Object,Payload>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Payload>> apply(EdgeTriplet<Payload, Integer> triplet) {
            Integer val = triplet.attr();
            Tuple2<Object,Payload> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,Payload> dstVertex = triplet.toTuple()._2();

            if (sourceVertex._2.getValue()==Integer.MAX_VALUE || (sourceVertex._2.getValue()+val >= dstVertex._2.getValue())) {   // source vertex value is smaller than dst vertex?
                // do nothing
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Payload>>().iterator()).asScala();
            } else {
                // propagate source vertex value
                Payload newInfo = new Payload(sourceVertex._2.getValue()+val);
                newInfo.setVertices(sourceVertex._2.getVertices());
                newInfo.addVertex((Long)dstVertex._1);
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Payload>(triplet.dstId(),newInfo)).iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Payload,Payload,Payload> implements Serializable {
        @Override
        public Payload apply(Payload o, Payload o2) {
            if (o.getValue() <= o2.getValue()) {
                return o;
            } else {
                return o2;
            }
        }
    }
    public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();
        ArrayList<Long> initialPath = new ArrayList<>();
        initialPath.add(1l);
        List<Tuple2<Object,Payload>> vertices = Lists.newArrayList(
                new Tuple2<Object,Payload>(1l,new Payload(0,initialPath)),
                new Tuple2<Object,Payload>(2l,new Payload(Integer.MAX_VALUE)),
                new Tuple2<Object,Payload>(3l,new Payload(Integer.MAX_VALUE)),
                new Tuple2<Object,Payload>(4l,new Payload(Integer.MAX_VALUE)),
                new Tuple2<Object,Payload>(5l,new Payload(Integer.MAX_VALUE)),
                new Tuple2<Object,Payload>(6l,new Payload(Integer.MAX_VALUE))
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l,2l, 4), // A --> B (4)
                new Edge<Integer>(1l,3l, 2), // A --> C (2)
                new Edge<Integer>(2l,3l, 5), // B --> C (5)
                new Edge<Integer>(2l,4l, 10), // B --> D (10)
                new Edge<Integer>(3l,5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );

        JavaRDD<Tuple2<Object,Payload>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Payload, Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),null, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Payload.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Payload.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        ops.pregel(new Payload(Integer.MAX_VALUE),
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new VProg(),
                new sendMsg(),
                new merge(),
                ClassTag$.MODULE$.apply(Payload.class))
                .vertices()
                .toJavaRDD()
                .sortBy(f -> ((Tuple2<Object, Integer>) f)._1, true, 0)
                .foreach(v -> {
                    Tuple2<Object,Payload> vertex = (Tuple2<Object,Payload>)v;
                    System.out.print("Minimum path to get from "+labels.get(1l)+" to "+labels.get(vertex._1)+" is [");
                    String path="";
                    for(Object step : vertex._2.getVertices()){
                        path+=labels.get(step)+",";
                    }
                    path = path.substring(0,path.length()-1);
                    System.out.println(path+"]"+" with cost "+vertex._2.getValue());
                });
    }

}
