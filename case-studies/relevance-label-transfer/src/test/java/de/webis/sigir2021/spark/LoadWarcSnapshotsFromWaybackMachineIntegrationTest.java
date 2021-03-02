package de.webis.sigir2021.spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import de.webis.sigir2021.spark.LoadWarcSnapshotsFromWaybackMachine;
import de.webis.sigir2021.spark.LoadWarcSnapshotsFromWaybackMachine.TmpContent;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import scala.Tuple2;

public class LoadWarcSnapshotsFromWaybackMachineIntegrationTest extends SparkIntegrationTestBase {
	@Test
	public void testParsingOfStuff() {
		String input = "src/test/resources/sample-input/snapshot-wayback-topic-02.warc.gz";
		JavaPairRDD<String, List<CollectionDocument>> records = LoadWarcSnapshotsFromWaybackMachine.topicToWaybackDocumentsAll(jsc(), input);

		records = records.filter(i -> Arrays.asList("clueweb09-en0000-40-26134", "clueweb09-en0003-39-09826", "clueweb09-enwp00-51-09934", "clueweb09-enwp00-78-20657").contains(i._1()));
		Approvals.verifyAsJson(records.collect());
	}
	
	@Test
	public void testParsingAndFiltering1() {
		String input = "src/test/resources/sample-input/snapshot-wayback-topic-02.warc.gz";
		JavaPairRDD<String, List<CollectionDocument>> records = LoadWarcSnapshotsFromWaybackMachine.topicToWaybackDocumentsInCW12CrawlingTime(jsc(), input);
		// ersten zwei fliegen raus, letzten zwei bleiben drin
		records = records.filter(i -> Arrays.asList("clueweb09-en0000-40-26134", "clueweb09-en0003-39-09826", "clueweb09-enwp00-51-09934", "clueweb09-enwp00-78-20657").contains(i._1()));
		Approvals.verifyAsJson(records.collect());
	}
	
	@Test
	public void testParsingOfStuff2() {
		String input = "src/test/resources/sample-input/snapshot-wayback-topic-02.warc.gz";
		JavaPairRDD<String, List<CollectionDocument>> records = LoadWarcSnapshotsFromWaybackMachine.topicToWaybackDocumentsAll(jsc(), input);

		records = records.filter(i -> Arrays.asList("clueweb09-en0006-06-41866", "clueweb09-enwp02-04-08736").contains(i._1()));
		Approvals.verifyAsJson(records.collect());
	}
	
	@Test
	public void testParsingAndFiltering2() {
		String input = "src/test/resources/sample-input/snapshot-wayback-topic-02.warc.gz";
		JavaPairRDD<String, List<CollectionDocument>> records = LoadWarcSnapshotsFromWaybackMachine.topicToWaybackDocumentsInCW12CrawlingTime(jsc(), input);
		// clueweb09-en0006-06-41866 raus, clueweb09-enwp02-04-08736 rein
		records = records.filter(i -> Arrays.asList("clueweb09-en0006-06-41866", "clueweb09-enwp02-04-08736").contains(i._1()));
		Approvals.verifyAsJson(records.collect());
	}
	
	@Test
	public void testParsingOfTimestamp() {
		CollectionDocument doc = doc("Mon, 10-Oct-2011 10:41:30 GMT");
		
		boolean actual = LoadWarcSnapshotsFromWaybackMachine.keepOnlyRecordsWithinCw12(doc);
		
		Assert.assertFalse(actual);
	}
	
	@Test
	public void testParsingOfTimestamp2() {
		CollectionDocument doc = doc("Sun Nov 08 19:45:59 GMT 2009");
		
		boolean actual = LoadWarcSnapshotsFromWaybackMachine.keepOnlyRecordsWithinCw12(doc);
		
		Assert.assertFalse(actual);
	}
	
//	
//	@Test
//	public void createBigJsonFile() {
//		String input = allWarcs();
//		JavaPairRDD<String, List<CollectionDocument>> records = LoadWarcSnapshotsFromWaybackMachine.topicToWaybackDocumentsInCW12CrawlingTime(jsc(), input);
//		
//		
//		Approvals.verifyAsJson(records.map(i -> new TmpContent(i)).take(1));
//		records.map(i -> new TmpContent(i)).map(i -> i.toString())
//			.repartition(10)
//			.saveAsTextFile("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/redirects-and-snapshots-as-collection-documents.jsonl", GzipCodec.class);
//	}
	
	private static CollectionDocument doc(String date) {
		CollectionDocument ret = new CollectionDocument();
		ret.setCrawlingTimestamp(date);
		
		return ret;
	}
	
	private static final String allWarcs() {
		return "/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/{redirects,snapshots}/*/*/*/*.warc.gz";
	}
}
