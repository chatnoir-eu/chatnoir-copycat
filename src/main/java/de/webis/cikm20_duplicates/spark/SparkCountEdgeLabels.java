package de.webis.cikm20_duplicates.spark;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.math.BigIntegerMath;

import lombok.SneakyThrows;
import scala.Tuple2;

public class SparkCountEdgeLabels {

	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			JavaRDD<String> exactDuplicates = context.textFile("cikm2020/exact-duplicates-simhash-cw09-cw12");
			JavaRDD<String> pairs = context.textFile("cikm2020/results/test-01");
		
			countEdgeLabels(exactDuplicates, pairs).saveAsTextFile("cikm2020/near-duplicate-graph/edge-count-cw09-cw12");
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/count-edges");

		return new JavaSparkContext(conf);
	}
	
	public static JavaRDD<String> countEdgeLabels(JavaRDD<String> exactDuplicates, JavaRDD<String> pairs) {
		JavaPairRDD<String, BigInteger> a = exactDuplicates.flatMapToPair(i -> flatten(i).iterator())
				.reduceByKey((i,j) -> i.add(j), 10)
				.filter(i -> !i._2.equals(BigInteger.ZERO));
		
		JavaPairRDD<String, BigInteger> b = pairs.mapToPair(i -> toTuple(i))
				.reduceByKey((i,j) -> i.add(j), 10)
				.filter(i -> !i._2.equals(BigInteger.ZERO));
		
		return a.union(b)
				.reduceByKey((i,j) -> i.add(j), 10)
				.filter(i -> !i._2.equals(BigInteger.ZERO))
				.map(i -> "{\"" + i._1() + "\":" + i._2() + "}");
	}

	private static Tuple2<String, BigInteger> toTuple(String a) {
		String[] parsed = a.split(",");
		if(parsed.length != 3) {
			throw new RuntimeException("Can not transform input to tuple: '" + a + "'");
		}
		
		String name;
		
		if((parsed[0].startsWith("clueweb09") && parsed[1].startsWith("clueweb12")) || (parsed[0].startsWith("clueweb12") && parsed[1].startsWith("clueweb09"))) {
			name = "clueweb09<->clueweb12";
		} else if (parsed[0].startsWith("clueweb09") && parsed[1].startsWith("clueweb09")) {
			name = "clueweb09<->clueweb09";
		} else if (parsed[0].startsWith("clueweb12") && parsed[1].startsWith("clueweb12")) {
			name = "clueweb12<->clueweb12";
		} else {
			throw new RuntimeException("Can not handle '" + a + "'.");
		}
		
		return new Tuple2<>(name +" (k=" + Integer.parseInt(parsed[2]) + ")", BigInteger.ONE);
	}
	
	@SneakyThrows
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static List<Tuple2<String, BigInteger>> flatten(String src) {
		Map<String, Object> s = new ObjectMapper().readValue(src, Map.class);
		List<String> docs = (List) s.get("equivalentDocuments");
		
		BigInteger all = edgesBetweenFullyConnectedGraph(docs.size());
		BigInteger clueWeb09 = edgesBetweenFullyConnectedGraph(docs.stream().filter(i -> i.startsWith("clueweb09")).count());
		BigInteger clueWeb12 = edgesBetweenFullyConnectedGraph(docs.stream().filter(i -> i.startsWith("clueweb12")).count());
		
		return Arrays.asList(
			new Tuple2<>("clueweb09<->clueweb09 (k=0)", clueWeb09),
			new Tuple2<>("clueweb12<->clueweb12 (k=0)", clueWeb12),
			new Tuple2<>("clueweb09<->clueweb12 (k=0)", all.subtract(clueWeb12).subtract(clueWeb09))
		);
	}
	
	private static BigInteger edgesBetweenFullyConnectedGraph(long count) {
		if(count <= 1) {
			return BigInteger.ZERO;
		}
		
		BigInteger nodeCount = BigIntegerMath.factorial((int) count);
		BigInteger denominator = BigInteger.valueOf(2l).multiply(BigIntegerMath.factorial(((int)count) -2));
		
		return nodeCount.divide(denominator);
	}
	
	
}
