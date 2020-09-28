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

import de.webis.cikm20_duplicates.app.CreateWebGraph.WebGraphAnchor;
import de.webis.cikm20_duplicates.app.CreateWebGraph.WebGraphNode;
import de.webis.cikm20_duplicates.app.WebGraphToGraphX.GraphxWebNode;
import de.webis.cikm20_duplicates.spark.SparkIntegrationTestBase;

public class WebGraphToGraphXTest extends SparkIntegrationTestBase {
	@Test
	public void approveFiles() {
		List<String> actual = WebGraphToGraphX.nodesParts();
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void approveFiles2() {
		List<String> actual = WebGraphToGraphX.intermediateNodesParts();
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void approveSmallSampleGraphWithSingleNodeIds() {
		// (3)<-(1)->(2)
		Iterator<JavaRDD<String>> nodeIds = Arrays.asList(jsc().parallelize(Arrays.asList("http://example.com\t2", "https://google.de\t1", "http://facebook.de\t3"))).iterator();
		JavaRDD<WebGraphNode> nodes = jsc().parallelize(Arrays.asList(
			new WebGraphNode("http://google.de", "2020-03-28T14:46:29Z", anchors("https://example.com", "http://facebook.de", "http://example.com")),
			new WebGraphNode("https://example.com", "2020-03-28T15:46:29Z", anchors()),
			new WebGraphNode("http://facebook.de", "2020-03-28T16:46:29Z", anchors())
		));
		
		List<GraphxWebNode> expected = Arrays.asList(
			new GraphxWebNode(1l, new HashSet<Long>(Arrays.asList(2l, 3l)), 1585403189l /*"2020-03-28T14:46:29Z"*/),
			new GraphxWebNode(2l, new HashSet<Long>(Arrays.asList()), 1585406789l /*"2020-03-28T15:46:29Z"*/),
			new GraphxWebNode(3l, new HashSet<Long>(Arrays.asList()), 1585410389l /*"2020-03-28T16:46:29Z"*/)
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
			new WebGraphNode("http://google.de", "2020-03-28T14:46:29Z", anchors("https://example.com", "http://facebook.de", "http://example.com")),
			new WebGraphNode("https://example.com", "2020-03-28T15:46:29Z", anchors()),
			new WebGraphNode("http://facebook.de", "2020-03-28T16:46:29Z", anchors())
		));
		
		List<GraphxWebNode> expected = Arrays.asList(
			new GraphxWebNode(1l, new HashSet<Long>(Arrays.asList(2l, 3l)), 1585403189l /*"2020-03-28T14:46:29Z"*/),
			new GraphxWebNode(2l, new HashSet<Long>(Arrays.asList()), 1585406789l /*"2020-03-28T15:46:29Z"*/),
			new GraphxWebNode(3l, new HashSet<Long>(Arrays.asList()), 1585410389l /*"2020-03-28T16:46:29Z"*/)
		);
		List<GraphxWebNode> actual = transformToGraphXNodes(nodes, nodeIds);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void approveSmallSampleGraphWithJoinMethod() {
		// (3)<-(1)->(2)
		JavaRDD<String> nodeIds = jsc().parallelize(Arrays.asList("http://example.com\t2", "https://google.de\t1", "http://facebook.de\t3"));
		JavaRDD<WebGraphNode> nodes = jsc().parallelize(Arrays.asList(
			new WebGraphNode("http://google.de", "2020-03-28T14:46:29Z", anchors("https://example.com", "http://facebook.de", "http://example.com")),
			new WebGraphNode("https://example.com", "2020-03-28T15:46:29Z", anchors()),
			new WebGraphNode("http://facebook.de", "2020-03-28T16:46:29Z", anchors())
		));
		
		List<GraphxWebNode> expected = Arrays.asList(
			new GraphxWebNode(1l, new HashSet<Long>(Arrays.asList(2l, 3l)), 1585403189l /*"2020-03-28T14:46:29Z"*/),
			new GraphxWebNode(2l, new HashSet<Long>(Arrays.asList()), 1585406789l /*"2020-03-28T15:46:29Z"*/),
			new GraphxWebNode(3l, new HashSet<Long>(Arrays.asList()), 1585410389l /*"2020-03-28T16:46:29Z"*/)
		);
		List<GraphxWebNode> actual = WebGraphToGraphX.transformToGraphXNodesByJoins(nodes, nodeIds).collect();
		
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
