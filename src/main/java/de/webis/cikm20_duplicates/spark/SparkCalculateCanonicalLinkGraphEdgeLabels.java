package de.webis.cikm20_duplicates.spark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.htrace.shaded.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.ImmutableList;

import de.webis.cikm20_duplicates.spark.SparkCanonicalLinkGraphExtraction.CanonicalLinkGraphEdge;
import de.webis.cikm20_duplicates.spark.eval.SparkAnalyzeCanonicalLinkGraph;
import lombok.SneakyThrows;
import scala.Tuple2;

public class SparkCalculateCanonicalLinkGraphEdgeLabels {

	private static final String DIR = "cikm2020/canonical-link-graph/";
	
	public static void main(String[] args) {
		String[] corpora = new String[] {/*"cw09" ,*/ "cw12"/*, "cc-2015-11"*/};
		
		try (JavaSparkContext context = context()) {
			for(String corpus : corpora) {
				JavaRDD<String> input = context.textFile(DIR + corpus);
				
				edgeLabels(input, new HashPartitioner(10000))
					.saveAsTextFile(DIR + corpus + "-calulated-edges");
			}
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/analyze-canonical-link-graph");
	
		return new JavaSparkContext(conf);
	}
	
	public static JavaRDD<String> edgeLabels(JavaRDD<String> input, Partitioner partitioner) {
		return input.mapToPair(i -> toPair(i))
			.filter(i -> i != null)
			.repartitionAndSortWithinPartitions(partitioner)
			.groupByKey()
			.flatMapToPair(i -> pairsForCalculation(i))
			.filter(i -> i != null && i._1() != null && i._2() != null && i._2()._1() != null && i._2()._2() != null)
			.repartitionAndSortWithinPartitions(partitioner)
			.map(i -> reportEdgeOrNull(i._2()._1(), i._2()._2()))
			.filter(i -> i != null);
	}

	private static Tuple2<String, CanonicalLinkGraphEdge> toPair(String src) {
		CanonicalLinkGraphEdge edge = CanonicalLinkGraphEdge.fromString(src);
		String url = edge.getCanonicalLink().toString();
		String host = SparkAnalyzeCanonicalLinkGraph.hostFromUrl(url);
		if(host == null || host.equalsIgnoreCase("ERROR-PARSING-URL")) {
			return null;
		}
		
		return new Tuple2<>(url, edge);
	}
	
	private static Iterator<Tuple2<String, Tuple2<CanonicalLinkGraphEdge, CanonicalLinkGraphEdge>>> pairsForCalculation(Tuple2<String, Iterable<CanonicalLinkGraphEdge>> bla) {
		List<CanonicalLinkGraphEdge> edges = new ArrayList<>(ImmutableList.copyOf(bla._2().iterator()));
		Collections.sort(edges, (a,b) -> a.getDoc().getId().compareTo(b.getDoc().getId()));
		
		Stream<Tuple2<Integer, Integer>> indizesToCompare = IntStream.range(0, edges.size()).mapToObj(i -> i)
				.flatMap(i -> IntStream.range(i+1, edges.size()).mapToObj(j -> new Tuple2<Integer, Integer>(i,j)));
	
		return indizesToCompare.map(i -> new Tuple2<>(
				edges.get(i._1()).getDoc().getId() +"-" +edges.get(i._2()).getDoc().getId(),
				new Tuple2<>(edges.get(i._1()), edges.get(i._2()))
		)).iterator();
	}

	@SneakyThrows
	private static String reportEdgeOrNull(CanonicalLinkGraphEdge a, CanonicalLinkGraphEdge b) {
		if(!a.getCanonicalLink().equals(b.getCanonicalLink()) || a.getDoc().getId().compareTo(b.getDoc().getId()) >= 0) {
			return null;
		}
		
		Map<String, Object> ret = new LinkedHashMap<>();
		ret.put("canonicalLink", a.getCanonicalLink());
		ret.put("firstDoc", a);
		ret.put("secondDoc", b);
		ret.put("s3score", SparkEnrichRelevanceTransferPairs.s3Score(a.getDoc(), b.getDoc()));
		
		return new ObjectMapper().writeValueAsString(ret);
	}
}
