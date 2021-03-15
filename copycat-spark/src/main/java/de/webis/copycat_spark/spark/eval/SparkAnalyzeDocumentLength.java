package de.webis.copycat_spark.spark.eval;

import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaHadoopRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.JSONException;
import org.json.JSONObject;

import de.webis.trec_ndd.util.TextCanonicalization;
import io.anserini.index.transform.JsoupStringTransform;
import lombok.SneakyThrows;
import scala.Tuple2;

public class SparkAnalyzeDocumentLength {

	public static void main(String[] args) {
		String corpus = "cw09";
		try (JavaSparkContext context = context()) {
			JavaRDD<String> docs = docs(context, corpus);
			
			docs
				.saveAsTextFile("cikm2020/document-lengths/" + corpus +"-csv.bzip2", BZip2Codec.class);
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/source-documents");

		return new JavaSparkContext(conf);
	}
	
	private static JavaRDD<String> docs(JavaSparkContext context, String corpus) {
		if ("cw09".equals(corpus)) {
			return ccDocs(context, "/corpora/corpora-thirdparty/corpus-clueweb/09-mapfile/data-r-*/data");
		} else if ("cw12".equals(corpus)) {
			return ccDocs(context, "/corpora/corpora-thirdparty/corpus-clueweb/12-mapfile/data-r-*/data");
		} else if ("cc-2015-11".equals(corpus)) {
			return ccDocs(context, "/corpora/corpus-commoncrawl/CC-MAIN-2015-11-mapfile/data-r-*/data");
		} else if ("cc-2017-04".equals(corpus)) {
			return ccDocs(context, "/corpora/corpus-commoncrawl/CC-MAIN-2017-04-mapfile/data-r-*/data");
		}
		
		throw new RuntimeException("Add more corpora :" + corpus);
	}
	
	@SuppressWarnings("unchecked")
	static JavaRDD<String> ccDocs(JavaSparkContext context, String path) {
		return ((JavaHadoopRDD<Text, Text>) context.hadoopFile(path, SequenceFileInputFormat.class, Text.class, Text.class))
				.map(kv -> chatnoirMapFileDocumentToDocOrNullFailsave(kv._1().toString(), kv._2().toString()))
				.filter(i -> i != null)
				.map(i -> i._1() +"," + i._2());
	}
	
	private static Tuple2<String, Long> chatnoirMapFileDocumentToDocOrNullFailsave(String keyStr, String valueStr) {
		try {
			return chatnoirMapFileDocumentToDocOrNull(keyStr, valueStr);
		} catch (Throwable e) {
			return null;
		}
	}
	
	@SneakyThrows
	private static Tuple2<String, Long> chatnoirMapFileDocumentToDocOrNull(String keyStr, String valueStr) {
		// ignore large files
		if (valueStr.getBytes().length > 1024 * 1024) {
			return null;
		}

		JSONObject inputJson  = new JSONObject(valueStr);

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

		String recordId = keyStr;
		
		if(metadata.has("WARC-TREC-ID")) {
			recordId = metadata.getString("WARC-TREC-ID");
		}
		
		List<String> words = TextCanonicalization.fullCanonicalization(new JsoupStringTransform().apply(contentBody));
		return new Tuple2<>(recordId, (long) words.size());
	}
}
