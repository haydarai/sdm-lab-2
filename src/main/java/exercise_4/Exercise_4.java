package exercise_4;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import utils.FileUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


public class Exercise_4 {

    public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) throws FileNotFoundException {
        // Open files
        String verticesFileName = "wiki-vertices.txt";
        String edgesFileName = "wiki-edges.txt";
        File verticesFile = FileUtil.loadFile(verticesFileName);
        File edgesFile = FileUtil.loadFile(edgesFileName);

        // Schemas
        StructType verticesSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
                new StructField("name", DataTypes.StringType, true, new MetadataBuilder().build()),
        });
        StructType edgesSchema = new StructType(new StructField[]{
                new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
                new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build()),
        });

        // Load vertices and edges
        List<Row> verticesList = new ArrayList<Row>();
        List<Row> edgesList = new ArrayList<Row>();

        Scanner sc = new Scanner(verticesFile);
        while (sc.hasNext()) {
            String line = sc.nextLine();
            if (line.indexOf('\t') != -1) {
                int index = line.indexOf("\t");
                String id = line.substring(0, index);
                String name = line.substring(index);
                verticesList.add(RowFactory.create(id, name));
            }
        }
        sc.close();

        sc = new Scanner(edgesFile);
        while (sc.hasNext()) {
            String line = sc.nextLine();
            if (line.indexOf('\t') != -1) {
                int index = line.indexOf("\t");
                String idFrom = line.substring(0, index);
                String idTo = line.substring(index);
                edgesList.add(RowFactory.create(idFrom, idTo));
            }
        }
        sc.close();

        // Create RDDs
        JavaRDD<Row> verticesRdd = ctx.parallelize(verticesList);
        Dataset<Row> vertices = sqlCtx.createDataFrame(verticesRdd, verticesSchema);
        JavaRDD<Row> edgesRdd = ctx.parallelize(edgesList);
        Dataset<Row> edges = sqlCtx.createDataFrame(edgesRdd, edgesSchema);

        // Create graph
        GraphFrame gf = GraphFrame.apply(vertices, edges);
        gf.edges().show();
        gf.vertices().show();

        // Apply pagerank
        gf = gf.pageRank().resetProbability(0.5).maxIter(10).run();
        gf.vertices().show();
    }
}
