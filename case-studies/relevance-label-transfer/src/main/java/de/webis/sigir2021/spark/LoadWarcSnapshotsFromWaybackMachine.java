package de.webis.sigir2021.spark;

import java.io.Serializable;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.google.common.collect.ImmutableList;

import de.webis.chatnoir2.mapfile_generator.inputformats.CommonCrawlInputFormat;
import de.webis.chatnoir2.mapfile_generator.warc.WarcRecord;
import de.webis.sigir2021.wayback_machine.JudgedDocumentsWarcReader;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import scala.Tuple2;

public class LoadWarcSnapshotsFromWaybackMachine {

	public static void main(String[] args) {
		try(JavaSparkContext context = context()) {
			JavaPairRDD<String, List<CollectionDocument>> records = topicToWaybackDocumentsInCW12CrawlingTime(
				context,
				"file:///mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/{redirects,snapshots}/*/*/*/*.warc.gz"
			);
			
			records.map(i -> new TmpContent(i)).map(i -> i.toString())
				.repartition(10)
				.saveAsTextFile("sigir2021/trec-judgments-in-wayback-machine/redirects-and-snapshots-as-collection-documents.jsonl", GzipCodec.class);
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("LoadWarcSnapshotsFromWaybackMachine");

		return new JavaSparkContext(conf);
	}
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	@SuppressWarnings("serial")
	public static class TmpContent implements Serializable {
		private String trecDocumentId;
		private List<CollectionDocument> documents;
		
		@Override
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}

		public TmpContent(Tuple2<String, List<CollectionDocument>> i) {
			this(i._1(), i._2());
		}
	}
	
	public static JavaPairRDD<String, List<CollectionDocument>> topicToWaybackDocumentsInCW12CrawlingTime(JavaSparkContext jsc, String input) {
		JavaPairRDD<String, List<CollectionDocument>> ret = topicToWaybackDocumentsAll(jsc, input);
		ret = ret.mapToPair(i -> keepOnlyRecordsWithinCw12(i));
		
		return ret.filter(i -> i != null && i._1() != null && i._2() != null && !i._2().isEmpty());
	}

	private static Tuple2<String, List<CollectionDocument>> keepOnlyRecordsWithinCw12(Tuple2<String, List<CollectionDocument>> i) {
		List<CollectionDocument> docs = new ArrayList<>(i._2.stream().filter(doc -> keepOnlyRecordsWithinCw12(doc)).collect(Collectors.toList()));
		
		return new Tuple2<>(i._1, docs);
	}
	
	@SneakyThrows
	static boolean keepOnlyRecordsWithinCw12(CollectionDocument doc) {
		try {
			if(doc.getCrawlingTimestamp() == null) {
				return true;
			}
			
			if(doc.getCrawlingTimestamp().contains(" 2009") || doc.getCrawlingTimestamp().contains(" 2010")) {
				return false;
			}
			
			Date crawlingDate = parseDate(doc);
			return JudgedDocumentsWarcReader.responseInCw12CrawlingTime(crawlingDate);
		} catch(Exception e) {
			throw new RuntimeException("Could not parse: '" + doc.getCrawlingTimestamp() + "'.");
		}
	}
	
	@SneakyThrows
	private static Date parseDate(CollectionDocument doc) {
		SimpleDateFormat sdf = new SimpleDateFormat("dd MMM yyyy hh:mm:ss zzz");
		try {
			return sdf.parse(StringUtils.substringAfter(doc.getCrawlingTimestamp().replaceAll("\\s+", " "), ","));
		} catch (Exception e) {
			sdf = new SimpleDateFormat("dd-MMM-yyyy hh:mm:ss zzz");
			return sdf.parse(StringUtils.substringAfter(doc.getCrawlingTimestamp().replaceAll("\\s+", " "), ","));
		}
	}

	static JavaPairRDD<String, List<CollectionDocument>> topicToWaybackDocumentsAll(JavaSparkContext jsc, String input) {
		JavaPairRDD<LongWritable, WarcRecord> records = records(jsc, input);
		JavaPairRDD<String, CollectionDocument> topicToDoc = records.mapToPair(i -> transformToCollectionDocument(i))
				.filter(i -> i != null && i._1() != null && i._2() != null);
		
		return topicToDoc.groupByKey().mapToPair(i -> joinLists(i));
	}
	
	private static Tuple2<String, List<CollectionDocument>> joinLists(Tuple2<String, Iterable<CollectionDocument>> i) {
		return new Tuple2<>(i._1(), new ArrayList<>(ImmutableList.copyOf(i._2())));
	}

	private static Tuple2<String, CollectionDocument> transformToCollectionDocument(Tuple2<LongWritable, WarcRecord> i) {
		return transformToCollectionDocument(i._2());
	}
	
	@SneakyThrows
	public static Tuple2<String, CollectionDocument> transformToCollectionDocument(WarcRecord record) {
		if (record == null) {
			return null;
		}

		Map<String, String> header = lowercasedHeaders(record);
		String contentBody = record.getContent();

		if (contentBody.getBytes().length > 1024 * 1024 || !isWarcResponse(record)) {
			// ignore large files and non-responses
			return null;
		}
		
		try {
			return transformToCollectionDocument(header, Jsoup.parse(contentBody));
		} catch (Exception e) {
			return null;
		}
	}
	
	@SneakyThrows
	private static Tuple2<String, CollectionDocument> transformToCollectionDocument(Map<String, String> header, Document doc) {
		String id = header.get("warc-trec-id");
		if (id == null || id.isEmpty()) {
			id = header.get("warc-record-id");
		}

		String targetUri = header.get("warc-target-uri");

		removeAllWaybackContent(doc);
		CollectionDocument ret = CollectionDocument.collectionDocument(doc.text(), id);
		try {
			ret.setUrl(new URL(targetUri));
		} catch (Exception e) {}
		ret.setCrawlingTimestamp(header.get("x-archive-orig-date"));

		return new Tuple2<>(header.get("trec_target_id"), ret);
	}
	
    public static String removeAllWaybackContent(String html) {
        if (null == html || html.trim().isEmpty()) {
            return "";
        }
        
        Document doc = Jsoup.parse(html);
        removeAllWaybackContent(doc);
        
        return doc.toString();
    }
    
	private static void removeAllWaybackContent(Document doc) {
		for(Element e: doc.select("div[id=wm-ipp-base]")) {
			e.remove();
		}
	}
	
	public static Map<String, String> lowercasedHeaders(WarcRecord record) {
		return lowercasedHeaders(record.getHeader().getHeaderMetadata());
	}
	
	public static Map<String, String> lowercasedHeaders(Map<String, String> headers) {
		return new HashMap<>(headers.entrySet().stream()
				.filter(i -> i != null && i.getKey() != null && i.getValue() != null)
				.collect(Collectors.toMap(i -> i.getKey().trim().toLowerCase(), i -> i.getValue())));
	}
	
	public static boolean isWarcResponse(WarcRecord record) {
		return record != null && record.getRecordType() != null && "response".equalsIgnoreCase(record.getRecordType().trim());
	}

	private static JavaPairRDD<LongWritable, WarcRecord> records(JavaSparkContext sc, String path) {
		return sc.newAPIHadoopFile(path, CommonCrawlInputFormat.class, LongWritable.class, WarcRecord.class, sc.hadoopConfiguration());
	}
}
