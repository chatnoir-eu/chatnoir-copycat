package de.webis.copycat_spark.spark;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.webis.chatnoir2.mapfile_generator.inputformats.ClueWeb09InputFormat;
import de.webis.chatnoir2.mapfile_generator.warc.WarcRecord;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import io.anserini.index.transform.JsoupStringTransform;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

public class SparkCanonicalLinkGraphExtraction {

//	@SuppressWarnings("unchecked")
//	public static void main(String[] args) {
////		String corpus = "cc-2017-04";
////		String sampleSize = "0.1";
////		String path = "/corpora/corpus-commoncrawl/CC-MAIN-2017-04-mapfile/data-r-*/data";
//		String corpus = "cc-2015-11";
//		String sampleSize = "0.1";
//		String path = "/corpora/corpus-commoncrawl/CC-MAIN-2015-11-mapfile/data-r-*/data";
//		
//		try (JavaSparkContext context = context()) {
//			Set<String> urlsToKeep = canonicalLinksToKeep(context.textFile("cikm2020/canonical-link-graph/" + corpus + "-canonical-urls-to-count-sample-" + sampleSize));
//			JavaHadoopRDD<Text, Text> rdd = (JavaHadoopRDD<Text, Text>) context.hadoopFile(path, SequenceFileInputFormat.class, Text.class, Text.class);	
//		
//			canonicalLinkedges(rdd, urlsToKeep).saveAsTextFile("cikm2020/canonical-link-graph/" + corpus + "-sample-" + sampleSize);
////			canonicalLinks(rdd).saveAsTextFile("cikm2020/canonical-link-graph/cc-2017-04-canonical-urls");
//		}
//	}

	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			JavaRDD<String> tmp = context.textFile("s3a://corpus-copycat/document-representations/cw09");
			JavaPairRDD<LongWritable, WarcRecord> records = context.newAPIHadoopFile("", ClueWeb09InputFormat.class, LongWritable.class, WarcRecord.class, context.hadoopConfiguration());
			
			tmp.sample(false, 0.01).saveAsTextFile("tmp-maik-delete-me.jsonl");
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/canonical-link-graph");
	
		return new JavaSparkContext(conf);
	}
	
	public static JavaRDD<String> canonicalLinks(JavaPairRDD<Text, Text> input) {
		return input.map(i -> toCanonicalUrl(i._1().toString(), i._2().toString()))
				.filter(i -> i != null);
	}
	
	public static JavaRDD<String> canonicalLinkedges(JavaPairRDD<Text, Text> input, Set<String> urlsToKeep) {
		return input.map(i -> toVertex(i._1().toString(), i._2().toString(), urlsToKeep))
				.filter(i -> i != null)
				.map(i -> i.toString());
	}

	@SneakyThrows
	private static String toCanonicalUrl(String internalId, String json) {
		CanonicalLinkGraphEdge ret = toVertex(internalId, json, null);
		
		if(ret == null || ret.getCanonicalLink() == null) {
			return null;
		}
		
		return ret.getCanonicalLink().toString();
	}
	
	@SneakyThrows
	private static CanonicalLinkGraphEdge toVertex(String internalId, String json, Set<String> urlsToKeep) {
		// ignore large files
        if (json.getBytes().length > 1024 * 1024) {
            return null;
        }
        
        
        JSONObject inputJson  = new JSONObject(json);
        final JSONObject metadata = inputJson.getJSONObject("metadata");
        if (null == metadata) {
            throw new JSONException("Missing 'metadata'");
        }

        final JSONObject payload = inputJson.getJSONObject("payload");
        if (null == payload) {
            throw new JSONException("Missing 'payload'");
        }

        final String contentBody = payload.getString("body");
        final String contentEncoding    = payload.getString("encoding");

        if (null == contentEncoding || null == contentBody) {
            throw new JSONException("Missing one of 'payload/[encoding|body]'");
        }

        if (!contentEncoding.equals("plain")) {
            return null;
        }
        
        if(!"response".equals(metadata.getString("WARC-Type"))) {
        	return null;
        }

        String targetUri = metadata.getString("WARC-Target-URI");
        URL canonicalLink = extractCanonicalLinkOrNull(targetUri, contentBody);
        if(canonicalLink == null || (urlsToKeep != null && !urlsToKeep.contains(canonicalLink.toString()))) {
        	return null;
        }
        
        
        String id = internalId;
        if(metadata.has("WARC-TREC-ID")) {
        	id = metadata.getString("WARC-TREC-ID");
        }
        String timestamp = metadata.getString("WARC-Date");
        
        CollectionDocument ret = CollectionDocument.collectionDocument(new JsoupStringTransform().apply(contentBody), id);
        ret.setUrl(new URL(targetUri));
        
		return new CanonicalLinkGraphEdge(ret, canonicalLink, timestamp);
	}
	
	@SneakyThrows
	public static URL extractCanonicalLinkOrNull(String resolveFrom, String contentBody) {
		try {
			return extractCanonicalLinkOrNull(resolveFrom, Jsoup.parse(contentBody));
		} catch(Exception e) {
			return null;
		}
	}
	
	@SneakyThrows
	public static URL extractCanonicalLinkOrNull(String resolveFrom, Document doc) {
		try {
			Elements canonicals = doc.head().select("link[rel=\"canonical\"][href]");
			if(canonicals.size() == 0) {
				return null;
			}
	
			return new URL(new URL(resolveFrom), canonicals.get(0).attr("href"));
		} catch(Exception e) {
			return null;
		}
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	@SuppressWarnings("serial")
	public static class CanonicalLinkGraphEdge implements Serializable {
		private CollectionDocument doc;
		private URL canonicalLink;
		private String crawlingTimestamp;
		
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}
		
		@SneakyThrows
		public static CanonicalLinkGraphEdge fromString(String src) {
			return new ObjectMapper().readValue(src, CanonicalLinkGraphEdge.class);
		}
	}

	public static Set<String> canonicalLinksToKeep(JavaRDD<String> input) {
		List<String> ret = input.map(i -> urlFromUrlToCount(i))
				.filter(i -> i != null)
				.distinct().collect();
		
		return new HashSet<>(ret);
	}
	
	@SneakyThrows
	private static final String urlFromUrlToCount(String src) {
		return (String) new ObjectMapper().readValue(src, Map.class).get("url");
	}
}
