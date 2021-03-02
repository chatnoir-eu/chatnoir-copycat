package de.webis.sigir2021.spark;

import org.apache.commons.io.IOUtils;
import org.approvaltests.Approvals;
import org.junit.Test;

import de.webis.sigir2021.spark.LoadWarcSnapshotsFromWaybackMachine;
import lombok.SneakyThrows;

public class RemovalOfWaybackDataFromHtmlTest {
	@Test
	@SneakyThrows
	public void testRemovalOfHtmlForHuffingtonPostPage() {
		String input = IOUtils.toString(RemovalOfWaybackDataFromHtmlTest.class.getResourceAsStream("/wayback-huffingtonpost-obama.html"));
		String actual =  LoadWarcSnapshotsFromWaybackMachine.removeAllWaybackContent(input);
		
		Approvals.verify(actual);
	}
}
