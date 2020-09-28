package de.webis.cikm20_duplicates.app;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.webis.cikm20_duplicates.app.CreateWebGraph.WebGraphAnchor;
import de.webis.cikm20_duplicates.app.CreateWebGraph.WebGraphNode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import scala.Tuple2;
import scala.Tuple3;

public class WebGraphToGraphX {
	public static void main(String[] args) {
		try(JavaSparkContext context = context()) {
			JavaRDD<String> nodeIds = context.textFile("web-archive-analysis/corpus-commoncrawl-main-2020-16-graphx-nodes.tsv/part-*");
			int part = 0;
			
			for(String grapNodePath: intermediateNodesParts()) {
				JavaRDD<WebGraphNode> nodes = context.textFile(grapNodePath)
						.map(i -> WebGraphNode.fromString(i));
			
				transformToGraphXNodesByJoins(nodes, nodeIds)
					.map(i -> i.toString())
					.saveAsTextFile("web-archive-analysis/corpus-commoncrawl-main-2020-16-graphx-part-" + (part++) + ".jsonl", BZip2Codec.class);
			}
		}
	}
	
	static String url(WebGraphNode node) {
		return normalizedUrl(node.getSourceURL());
	}
	
	private static String normalizedUrl(String url) {
		return StringUtils.replace(StringUtils.replace(url, "https://", ""), "http://", "");
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": CreateWebGraph");

		return new JavaSparkContext(conf);
	}
	
	public static List<String> nodesParts() {
		return IntStream.range(0, 100)
			.mapToObj(i -> "web-archive-analysis/corpus-commoncrawl-main-2020-16-graphx-nodes.tsv/part-*" + String.format("%02d", i) + ".bz2")
			.collect(Collectors.toList());
	}
	
	public static List<String> intermediateNodesParts() {
		return IntStream.range(0, 100)
			.mapToObj(i -> "web-archive-analysis/intermediate-corpus-commoncrawl-main-2020-16-part*/part-*" + String.format("%02d", i) + ".bz2")
			.collect(Collectors.toList());
	}
	

	public static JavaRDD<GraphxWebNode> transformToGraphXNodesByJoins(JavaRDD<WebGraphNode> nodes, JavaRDD<String> nodeIds) {
		JavaRDD<TmpGraphxWebNode> ret = tmpNodes(nodes);
		JavaPairRDD<String, Long> urlToNodeId = nodeIds.mapToPair(i -> new Tuple2<>(normalizedUrl(i.split("\t")[0]), Long.valueOf(i.split("\t")[1])));
		JavaPairRDD<String, TmpGraphxWebNode> sourceToNode = ret.mapToPair(i -> new Tuple2<>(i.getSourceURL(), i));
		
		ret = sourceToNode.join(urlToNodeId).map(i -> insertSourceNodeId(i));
		JavaPairRDD<String, TmpGraphxEdge> targetToNode = ret.flatMap(i -> toTmpEdge(i))
				.mapToPair(i -> new Tuple2<>(i.getTargeturl(), i));
		
		JavaRDD<Tuple3<Long, Long, Long>> flatRet = targetToNode.join(urlToNodeId).map(i -> triple(i));
		
		return flatRet.groupBy(i -> i._1()).map(i -> tripleToNode(i));
	}
	
	private static GraphxWebNode tripleToNode(Tuple2<Long, Iterable<Tuple3<Long, Long, Long>>> i) {
		Long sourceId = i._1();
		Set<Long> targetIds = new HashSet<>();
		Long unixCrawlingTimestamp = Long.MAX_VALUE;
		
		for(Tuple3<Long, Long, Long> edge: i._2) {
			targetIds.add(edge._2());
			unixCrawlingTimestamp = Math.min(unixCrawlingTimestamp, edge._3());
		}
		
		targetIds.removeAll(Arrays.asList(sourceId));
		
		return new GraphxWebNode(sourceId, targetIds, unixCrawlingTimestamp);
	}

	private static Tuple3<Long, Long, Long> triple(Tuple2<String, Tuple2<TmpGraphxEdge, Long>> i) {
		return new Tuple3<>(i._2()._1().getSourceId(), i._2._2, i._2()._1().getUnixCrawlingTimestamp());
	}

	private static Iterator<TmpGraphxEdge> toTmpEdge(TmpGraphxWebNode node) {
		Set<String> targetUrls = node.getTargetUrls();
		targetUrls.add(node.getSourceURL());
		
		return targetUrls.stream()
				.map(i -> new TmpGraphxEdge(node.getUnixCrawlingTimestamp(), node.getSourceId(), i)).iterator();
	}
	
	private static TmpGraphxWebNode insertSourceNodeId(Tuple2<String, Tuple2<TmpGraphxWebNode, Long>> i) {
		i._2()._1().setSourceId(i._2()._2());
		
		return i._2()._1();
	}

	public static JavaRDD<GraphxWebNode> transformToGraphXNodes(JavaRDD<WebGraphNode> nodes, Iterator<JavaRDD<String>> nodeIds) {
		JavaRDD<TmpGraphxWebNode> ret = tmpNodes(nodes);
		
		while(nodeIds.hasNext()) {
			Map<String, Long> urlToId = urlToNodeId(nodeIds.next());
			ret = ret.map(i -> transformAvailableIds(i, urlToId));
		}
		
		return ret.unpersist(false).map(i -> new GraphxWebNode(i.getSourceId(), i.getTargetIds(), i.getUnixCrawlingTimestamp()));
	}
	
	private static JavaRDD<TmpGraphxWebNode> tmpNodes(JavaRDD<WebGraphNode> nodes) {
		return  nodes.map(i -> new TmpGraphxWebNode(
				normalizedUrl(i.getSourceURL()),
				unixTime(i),
				normalizedUrls(i.getAnchors()))
		);
	}
	
	private static Set<String> normalizedUrls(List<WebGraphAnchor> anchors) {
		return anchors.stream().map(j -> normalizedUrl(j.getTargetURL())).collect(Collectors.toSet());
	}
	
	@SneakyThrows
	private static long unixTime(WebGraphNode node) {
		Date ret = sdf().parse(node.getCrawlingTimestamp());
		
		return ret.getTime() / 1000;
	}
	
	private static SimpleDateFormat sdf() {
		SimpleDateFormat ret = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		ret.setTimeZone(TimeZone.getTimeZone("Europe/Berlin"));
		
		return ret;
	}
	
	private static TmpGraphxWebNode transformAvailableIds(TmpGraphxWebNode ret, Map<String, Long> urlToId) {
		if(urlToId.containsKey(normalizedUrl(ret.getSourceURL()))) {
			ret.setSourceId(urlToId.get(normalizedUrl(ret.getSourceURL())));
		}
		
		if(ret.getTargetIds() == null) {
			ret.setTargetIds(new HashSet<>());
		}
		
		for(String url: ret.getTargetUrls()) {
			if(urlToId.containsKey(url)) {
				ret.getTargetIds().add(urlToId.get(url));
			}
		}
		
		return ret;
	}

	private static Map<String, Long> urlToNodeId(JavaRDD<String> nodeIds) {
		return nodeIds.collect().stream().collect(Collectors.toMap(i -> normalizedUrl(i.split("\t")[0]), i -> Long.valueOf(i.split("\t")[1])));
	}
	
	
	
	@Data
	@SuppressWarnings("serial")
	public static class TmpGraphxWebNode implements Serializable {
		private final String sourceURL;
		private final Long unixCrawlingTimestamp;
		private final Set<String> targetUrls;
		private Long sourceId;
		private Set<Long> targetIds;
	}
	
	@Data
	@SuppressWarnings("serial")
	public static class TmpGraphxEdge implements Serializable {
		private final Long unixCrawlingTimestamp;
		private final Long sourceId;
		private Long targetId;
		private final String targeturl;
	}
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	@SuppressWarnings("serial")
	public static class GraphxWebNode implements Serializable {
		private Long sourceId;
		private Set<Long> targetIds;
		private long unixCrawlingTimestamp;
		
		@Override
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}

		@SneakyThrows
		public static GraphxWebNode fromString(String src) {
			return new ObjectMapper().readValue(src, GraphxWebNode.class);
		}
	}
}
