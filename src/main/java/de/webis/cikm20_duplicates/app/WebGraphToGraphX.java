package de.webis.cikm20_duplicates.app;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.cikm20_duplicates.app.CreateWebGraph.WebGraphNode;

public class WebGraphToGraphX {
	public static void main(String[] args) {
		try(JavaSparkContext context = context()) {
			JavaRDD<WebGraphNode> nodes = context.textFile("web-archive-analysis/intermediate-corpus-commoncrawl-main-2020-16-part*/*")
					.map(i -> WebGraphNode.fromString(i));
			JavaRDD<String> urls = nodes.map(i -> url(i));
			urls = urls.distinct();
			
			urls.zipWithIndex().map(i -> i._1() + "\t" + i._2())
				.saveAsTextFile("web-archive-analysis/corpus-commoncrawl-main-2020-16-graphx-nodes.tsv", BZip2Codec.class);
		}
	}
	
	private static String url(WebGraphNode node) {
		return StringUtils.replace(StringUtils.replace(node.getSourceURL(), "https://", ""), "http://", "");
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": CreateWebGraph");

		return new JavaSparkContext(conf);
	}
	
	
}
