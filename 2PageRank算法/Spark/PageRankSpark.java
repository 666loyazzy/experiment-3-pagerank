import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class PageRankSpark {
    public static void main(String[] args) {
        if (args.length != 2) throw new IllegalArgumentException("PageRankSpark <input> <output>");
        try (JavaSparkContext context = new JavaSparkContext(new SparkConf().setAppName("PageRank 10 iterations"))) {
            JavaPairRDD<String, List<String>> links = context.textFile(args[0]).mapToPair(line -> {
                String[] fields = line.split("\t", -1);
                List<String> targets = fields[1].isEmpty() ? new ArrayList<>() : Arrays.asList(fields[1].split(","));
                return new Tuple2<>(fields[0], targets);
            }).cache();
            long pages = links.count();
            JavaPairRDD<String, Double> ranks = links.mapValues(targets -> 1.0).cache();
            for (int iteration = 1; iteration <= 10; iteration++) {
                JavaPairRDD<String, Tuple2<List<String>, Double>> joined = links.join(ranks).cache();
                double dangling = joined.values().filter(value -> value._1.isEmpty()).mapToDouble(value -> value._2).sum();
                JavaPairRDD<String, Double> contributions = joined.flatMapToPair(page -> {
                    List<Tuple2<String, Double>> result = new ArrayList<>();
                    List<String> targets = page._2._1;
                    for (String target : targets) result.add(new Tuple2<>(target, page._2._2 / targets.size()));
                    return result.iterator();
                }).reduceByKey(Double::sum);
                JavaPairRDD<String, Double> next = links.leftOuterJoin(contributions)
                        .mapValues(value -> 0.15 + 0.85 * (value._2.orElse(0.0) + dangling / pages)).cache();
                next.count();
                ranks.unpersist();
                joined.unpersist();
                ranks = next;
                System.out.println("Completed iteration " + iteration);
            }
            ranks.sortByKey(true, 1).map(page -> String.format(Locale.ROOT, "(%s,%.10f)", page._1, page._2))
                    .saveAsTextFile(args[1]);
        }
    }
}
