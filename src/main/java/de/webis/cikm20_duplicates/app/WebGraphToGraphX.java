package de.webis.cikm20_duplicates.app;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.webis.cikm20_duplicates.app.CreateWebGraph.WebGraphNode;
import de.webis.cikm20_duplicates.app.WebGraphToGraphX.GraphxWebNode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

public class WebGraphToGraphX {
//	public static void main(String[] args) {
//		try(JavaSparkContext context = context()) {
//			JavaRDD<WebGraphNode> nodes = context.textFile("web-archive-analysis/intermediate-corpus-commoncrawl-main-2020-16-part*/*")
//					.map(i -> WebGraphNode.fromString(i));
//			JavaRDD<String> urls = nodes.map(i -> url(i));
//			urls = urls.distinct();
//			
//			urls.zipWithIndex().map(i -> i._1() + "\t" + i._2())
//				.saveAsTextFile("web-archive-analysis/corpus-commoncrawl-main-2020-16-graphx-nodes.tsv", BZip2Codec.class);
//		}
//	}
	
	public static void main(String[] args) {
		try(JavaSparkContext context = context()) {
			Iterator<JavaRDD<String>> nodeIds = nodesParts().stream().map(i -> context.textFile(i)).iterator();
			JavaRDD<WebGraphNode> nodes = context.textFile("web-archive-analysis/intermediate-corpus-commoncrawl-main-2020-16-part*/*")
					.map(i -> WebGraphNode.fromString(i));
			
			JavaRDD<GraphxWebNode> actual = transformToGraphXNodes(nodes, nodeIds);
			actual.map(i -> i.toString())
				.repartition(5000)
				.saveAsTextFile("web-archive-analysis/corpus-commoncrawl-main-2020-16-graphx.jsonl", BZip2Codec.class);
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

	public static JavaRDD<GraphxWebNode> transformToGraphXNodes(JavaRDD<WebGraphNode> nodes, Iterator<JavaRDD<String>> nodeIds) {
		JavaRDD<TmpGraphxWebNode> ret = nodes.map(i -> new TmpGraphxWebNode(i)).cache();
		
		while(nodeIds.hasNext()) {
			Map<String, Long> urlToId = urlToNodeId(nodeIds.next());
			ret = ret.map(i -> transformAvailableIds(i, urlToId)).cache();
		}
		
		return ret.map(i -> new GraphxWebNode(i.getSourceId(), i.getTargetIds(), i.getOriginalNode().getCrawlingTimestamp()));
	}
	
	private static TmpGraphxWebNode transformAvailableIds(TmpGraphxWebNode ret, Map<String, Long> urlToId) {
		if(urlToId.containsKey(normalizedUrl(ret.originalNode.getSourceURL()))) {
			ret.setSourceId(urlToId.get(normalizedUrl(ret.originalNode.getSourceURL())));
		}
		
		if(ret.getTargetIds() == null) {
			ret.setTargetIds(new HashSet<>());
		}
		
		for(String url: ret.originalNode.getAnchors().stream().map(i -> normalizedUrl(i.getTargetURL())).collect(Collectors.toList())) {
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
		private final WebGraphNode originalNode;
		private Long sourceId;
		private Set<Long> targetIds;
	}
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	@SuppressWarnings("serial")
	public static class GraphxWebNode implements Serializable {
		private Long sourceId;
		private Set<Long> targetIds;
		private String crawlingTimestamp;
		
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
