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

import static org.apache.spark.sql.functions.desc;


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
                String name = line.substring(index + 1);
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
                String idTo = line.substring(index + 1);
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
//        gf.edges().show();
//        gf.vertices().show();

        // Apply pagerank
        for (double dampingFactor = 0.05; dampingFactor < 0.3; dampingFactor += 0.05) {
            long start = System.currentTimeMillis();
            GraphFrame prgf = gf.pageRank().resetProbability(dampingFactor).maxIter(10).run();
            prgf.vertices().orderBy(desc("pagerank")).limit(10).show();
            long end = System.currentTimeMillis();
            System.out.print((end - start) / 1000);
            System.out.println(" in seconds for dampingfactor = " + dampingFactor + " and iterations are 10.");
        }
        for (int maxIterations = 1; maxIterations <= 20; maxIterations += 2) {
            long start = System.currentTimeMillis();
            GraphFrame pggf = gf.pageRank().resetProbability(0.15).maxIter(maxIterations).run();
            pggf.vertices().orderBy(desc("pagerank")).limit(10).show();
            long end = System.currentTimeMillis();
            System.out.print((end - start) / 1000);
            System.out.println(" in seconds for dampingFactor is .15 and iterations is " + maxIterations);
        }
    }
}
