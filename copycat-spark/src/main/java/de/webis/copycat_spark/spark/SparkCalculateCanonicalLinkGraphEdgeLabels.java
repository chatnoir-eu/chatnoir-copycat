package de.webis.copycat_spark.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.htrace.shaded.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import de.webis.copycat_spark.spark.SparkCanonicalLinkGraphExtraction.CanonicalLinkGraphEdge;
import de.webis.copycat_spark.spark.eval.SparkAnalyzeCanonicalLinkGraph;
import de.webis.copycat_spark.util.TakeRandom;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import scala.Tuple2;

public class SparkCalculateCanonicalLinkGraphEdgeLabels {

	private static final String DIR = "cikm2020/canonical-link-graph/";
	
//	public static void main(String[] args) {
//		String[] corpora = new String[] {/*"cw09",*/ "cw12"/*, "cc-2015-11"*/};
//		
//		try (JavaSparkContext context = context()) {
//			for(String corpus : corpora) {
//				JavaRDD<String> input = context.textFile(DIR + corpus);
//				
//				edgeLabels(input, new HashPartitioner(50000))
//					.saveAsTextFile(DIR + corpus + "-calulated-edges-sampled-large-groups");
//			}
//		}
//	}
	
//	public static void main(String[] args) {
//		String[] corpora = new String[] {/*"cw09", "cw12",*/ "cc-2015-11" /*, "cc-2017-04"*/};
//		
//		try (JavaSparkContext context = context()) {
//			for(String corpus : corpora) {
//				JavaRDD<String> input = context.textFile(DIR + corpus + "-sample-0.1-and-large-groups");
//				
//				edgeLabels(input, new HashPartitioner(50000))
//					.saveAsTextFile(DIR + corpus + "-calulated-edges-sampled-large-groups");
//			}
//		}
//	}
	
	public static void main(String[] args) {
		String[] corpora = new String[] {"cw09", "cw12"};
		
		try (JavaSparkContext context = context()) {
			for(String corpus : corpora) {
				JavaPairRDD<String, CanonicalLinkGraphEdge> input = context.textFile(DIR + corpus + "-calulated-edges-sampled-large-groups")
					.flatMapToPair(src -> {
						CanonicalLinkGraphEdge2 edge = CanonicalLinkGraphEdge2.fromString(src);
						List<CollectionDocument> ret = Arrays.asList(edge.getFirstDoc().getDoc(), edge.getSecondDoc().getDoc());

						return ret.stream()
								.map(doc -> new Tuple2<>(doc.getId(), new CanonicalLinkGraphEdge(doc, null, null)))
								.iterator();
					});
				
				input.groupByKey(new HashPartitioner(20000))
					.map(i -> keepOnlyFirst(i))
					.saveAsTextFile(DIR + corpus + "-sample-large-groups");
			}
		}
	}
	
	private static String keepOnlyFirst(Tuple2<String, Iterable<CanonicalLinkGraphEdge>> i) {
		return i._2().iterator().next().toString();
	}
	
	
//	public static void main(String[] args) {
//		String[] corpora = new String[] {/*"cw09", "cw12", "cc-2015-11",*/ "cc-2017-04"};
//		
//		try (JavaSparkContext context = context()) {
//			for(String corpus : corpora) {
//				JavaRDD<String> input = context.textFile(DIR + corpus + "-sample-0.1");
//				JavaPairRDD<String, CanonicalLinkGraphEdge> idToEdge = partitionedBla(input, new HashPartitioner(10000), SparkCalculateCanonicalLinkGraphEdgeLabels::randomStringForGroupSplitting);
//				
//				JavaRDD<CanonicalLinkGraphEdge> ret = idToEdge
//					.groupByKey()
//					.flatMap(i -> new ArrayList<>(TakeRandom.takeRandomElements(50, i._2())).iterator());
//				
//				input = ret.map(i -> i.toString());
//				
//				idToEdge = partitionedBla(input, new HashPartitioner(10000));
//				
//				ret = idToEdge
//					.groupByKey()
//					.flatMap(i -> new ArrayList<>(TakeRandom.takeRandomElements(50, i._2())).iterator());
//				
//				ret.map(i -> i.toString())
//					.saveAsTextFile(DIR + corpus + "-sample-0.1-and-large-groups");
//			}
//		}
//	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/analyze-canonical-link-graph");
	
		return new JavaSparkContext(conf);
	}
	

	private static JavaPairRDD<String, CanonicalLinkGraphEdge> partitionedBla(JavaRDD<String> input, Partitioner partitioner, SerializableSupplier<String> stringProducer) {
		return input.mapToPair(i -> toPair(i))
				.filter(i -> i != null)
				.mapToPair(i -> new Tuple2<>(i._1() + stringProducer.get(), i._2()))
				.repartitionAndSortWithinPartitions(partitioner)
				.persist(StorageLevel.DISK_ONLY());
	}
	
	private static JavaPairRDD<String, CanonicalLinkGraphEdge> partitionedBla(JavaRDD<String> input, Partitioner partitioner) {
		return partitionedBla(input, partitioner, () -> "");
	}

	public static JavaRDD<String> edgeLabels(JavaRDD<String> input, Partitioner partitioner) {
		JavaPairRDD<String, CanonicalLinkGraphEdge> idToEdge = partitionedBla(input, partitioner);
		
		return idToEdge
			.groupByKey()
			.flatMap(i -> group(i))
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
	
	private static Iterator<String> group(Tuple2<String, Iterable<CanonicalLinkGraphEdge>> bla) {
		List<CanonicalLinkGraphEdge> edges = new ArrayList<>(TakeRandom.takeRandomElements(50, bla._2));
		Collections.sort(edges, (a,b) -> a.getDoc().getId().compareTo(b.getDoc().getId()));
		
		Stream<Tuple2<Integer, Integer>> indizesToCompare = IntStream.range(0, edges.size()).mapToObj(i -> i)
				.flatMap(i -> IntStream.range(i+1, edges.size()).mapToObj(j -> new Tuple2<Integer, Integer>(i,j)));
		
		return indizesToCompare.map(i -> reportEdgeOrNull(edges.get(i._1()), edges.get(i._2())))
				.filter(i -> i != null)
				.iterator();
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
	
	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	public static class CanonicalLinkGraphEdge2 {
		private String canonicalLink;
		private CanonicalLinkGraphEdge firstDoc, secondDoc;
		private double s3score;
		
		@SneakyThrows
		public static CanonicalLinkGraphEdge2 fromString(String src) {
			return new ObjectMapper().readValue(src, CanonicalLinkGraphEdge2.class);
		}
	}
	
	public static String randomStringForGroupSplitting() {
		return "-" + new Random().nextInt(100);
	}
	
	public static interface SerializableSupplier<T> extends Supplier<T>, Serializable {}
}
