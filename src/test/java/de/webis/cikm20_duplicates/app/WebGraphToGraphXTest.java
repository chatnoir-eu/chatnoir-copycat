package de.webis.cikm20_duplicates.app;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;

import de.webis.cikm20_duplicates.app.CreateWebGraph.WebGraphAnchor;
import de.webis.cikm20_duplicates.app.CreateWebGraph.WebGraphNode;
import de.webis.cikm20_duplicates.app.WebGraphToGraphX.GraphxWebNode;

public class WebGraphToGraphXTest extends SharedJavaSparkContext {
	@Test
	public void approveFiles() {
		List<String> actual = WebGraphToGraphX.nodesParts();
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void approveSmallSampleGraphWithSingleNodeIds() {
		// (3)<-(1)->(2)
		Iterator<JavaRDD<String>> nodeIds = Arrays.asList(jsc().parallelize(Arrays.asList("http://example.com\t2", "https://google.de\t1", "http://facebook.de\t3"))).iterator();
		JavaRDD<WebGraphNode> nodes = jsc().parallelize(Arrays.asList(
			new WebGraphNode("http://google.de", "timestamp-1", anchors("https://example.com", "http://facebook.de", "http://example.com")),
			new WebGraphNode("https://example.com", "timestamp-2", anchors()),
			new WebGraphNode("http://facebook.de", "timestamp-3", anchors())
		));
		
		List<GraphxWebNode> expected = Arrays.asList(
			new GraphxWebNode(1l, new HashSet<Long>(Arrays.asList(2l, 3l)), "timestamp-1"),
			new GraphxWebNode(2l, new HashSet<Long>(Arrays.asList()), "timestamp-2"),
			new GraphxWebNode(3l, new HashSet<Long>(Arrays.asList()), "timestamp-3")
		);
		List<GraphxWebNode> actual = transformToGraphXNodes(nodes, nodeIds);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void approveSmallSampleGraphWithMultipleNodeIds() {
		// (3)<-(1)->(2)
		Iterator<JavaRDD<String>> nodeIds = Arrays.asList(
			jsc().parallelize(Arrays.asList("http://example.com\t2")),
			jsc().parallelize(Arrays.asList("https://google.de\t1")),
			jsc().parallelize(Arrays.asList("http://facebook.de\t3"))
		).iterator();
		JavaRDD<WebGraphNode> nodes = jsc().parallelize(Arrays.asList(
			new WebGraphNode("http://google.de", "timestamp-1", anchors("https://example.com", "http://facebook.de", "http://example.com")),
			new WebGraphNode("https://example.com", "timestamp-2", anchors()),
			new WebGraphNode("http://facebook.de", "timestamp-3", anchors())
		));
		
		List<GraphxWebNode> expected = Arrays.asList(
			new GraphxWebNode(1l, new HashSet<Long>(Arrays.asList(2l, 3l)), "timestamp-1"),
			new GraphxWebNode(2l, new HashSet<Long>(Arrays.asList()), "timestamp-2"),
			new GraphxWebNode(3l, new HashSet<Long>(Arrays.asList()), "timestamp-3")
		);
		List<GraphxWebNode> actual = transformToGraphXNodes(nodes, nodeIds);
		
		Assert.assertEquals(expected, actual);
	}
	
	private static List<WebGraphAnchor> anchors(String...urls) {
		return Arrays.asList(urls).stream()
				.map(i -> new WebGraphAnchor(i, "anchorText"))
				.collect(Collectors.toList());
	}
	
	private List<GraphxWebNode> transformToGraphXNodes(JavaRDD<WebGraphNode> nodes, Iterator<JavaRDD<String>> nodeIds) {
		return  WebGraphToGraphX.transformToGraphXNodes(nodes, nodeIds).collect();
	}
}
