package de.webis.copycat_spark.spark;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
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
import scala.Tuple3;

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
		
		JavaPairRDD<String, BigInteger> c = pairs.map(i -> toTupleWithJudgedInformations(i))
				.filter(i -> i != null)
				.mapToPair(i -> i)
				.reduceByKey((i,j) -> i.add(j), 10)
				.filter(i -> !i._2.equals(BigInteger.ZERO));
		
		JavaPairRDD<String, BigInteger> d = exactDuplicates.flatMap(i -> extractAllPairsWithJudgedDocuments(docs(i)))
				.filter(i -> i != null)
				.map(i -> i._1() +"," + i._2() +"," + i._3())
				.distinct()
				.map(i -> toTupleWithJudgedInformations(i))
				.filter(i -> i != null)
				.mapToPair(i -> i)
				.reduceByKey((i,j) -> i.add(j), 10)
				.filter(i -> !i._2.equals(BigInteger.ZERO));
		
		return a.union(b)
				.union(c)
				.union(d)
				.reduceByKey((i,j) -> i.add(j), 10)
				.filter(i -> !i._2.equals(BigInteger.ZERO))
				.map(i -> "{\"" + i._1() + "\":" + i._2() + "}");
	}

	private static Tuple2<String, BigInteger> toTupleWithJudgedInformations(String a) {
		String[] parsed = a.split(",");
		if(parsed.length != 3) {
			throw new RuntimeException("Can not transform input to tuple: '" + a + "'");
		}

		String left,right,
				leftId = parsed[0],
				rightId = parsed[1];
		
		if((leftId.startsWith("clueweb09") && rightId.startsWith("clueweb12")) || (leftId.startsWith("clueweb12") && rightId.startsWith("clueweb09"))) {
			left = "clueweb09";
			right = "clueweb12";
		} else if (leftId.startsWith("clueweb09") && rightId.startsWith("clueweb09")) {
			left = "clueweb09";
			right = "clueweb09";
		} else if (leftId.startsWith("clueweb12") && rightId.startsWith("clueweb12")) {
			left = "clueweb12";
			right = "clueweb12";
		} else {
			throw new RuntimeException("Can not handle '" + a + "'.");
		}
		
		boolean leftJudged = SparkCreateSourceDocuments.DOCS_TO_TOPIC.containsKey(leftId);
		boolean rightJudged = SparkCreateSourceDocuments.DOCS_TO_TOPIC.containsKey(rightId);
		
		if(!leftJudged && !rightJudged) {
			return null;
		}
		
		return new Tuple2<>(left+"(" + (leftJudged ? "" : "un") + "judged)<->" + right + "(" + (rightJudged ? "" : "un") + "judged)" +" (k=" + Integer.parseInt(parsed[2]) + ")", BigInteger.ONE);
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
	static List<String> docs(String src) {
		Map<String, Object> s = new ObjectMapper().readValue(src, Map.class);
		return new ArrayList<>((List) s.get("equivalentDocuments"));
	}
	
	private static List<Tuple2<String, BigInteger>> flatten(String src) {
		List<String> docs = docs(src);
		
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

	public static Iterator<Tuple3<String, String, Integer>> extractAllPairsWithJudgedDocuments(List<String> ids) {
		Collections.sort(ids);

		return ids.stream().filter(i -> SparkCreateSourceDocuments.DOCS_TO_TOPIC.containsKey(i))
				.flatMap(i -> ids.stream().map(j -> bla(i, j)))
				.filter(i -> i != null)
				.distinct()
				.iterator();
	}
	
	private static Tuple3<String, String, Integer> bla(String i, String j) {
		if(j.compareTo(i) > 0) {
			return new Tuple3<>(i, j, 0);
		} else if(j.compareTo(i) < 0) {
			return new Tuple3<>(j, i, 0);
		}
		
		return null;
	}
}
