package de.webis.cikm20_duplicates.app;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.JSONArray;
import org.json.JSONObject;

import de.webis.chatnoir2.webclient.search.DocumentRetriever;
import de.webis.chatnoir2.webclient.search.DocumentRetriever.Document;
import lombok.SneakyThrows;

public class InjectRawDocuments {

	public static void main(String[] args) {
		try(JavaSparkContext context= context()) {
			for(String file: Arrays.asList("query_uuid_index_pairs.jsonl", "random_uuid_index_pairs.jsonl")) {
				JavaRDD<String> input = rdd(context, file);
				
				injectRawDocuments(input).saveAsTextFile("wstud-thesis-waechtler/warc/" + file);
			}
		}
	}
	
	@SneakyThrows
	private static JavaRDD<String> rdd(JavaSparkContext context, String fileName) {
		List<String> ret = Files.readAllLines(Paths.get("/mnt/ceph/storage/data-in-progress/wstud-thesis-waechtler/warc").resolve(fileName));
		return context.parallelize(ret, ret.size());
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": InjectRawDocuments");

		return new JavaSparkContext(conf);
	}
	
	public static JavaRDD<String> injectRawDocuments(JavaRDD<String> input) {
		return input.map(i -> addCw09Documents(i));
	}

	private static String addCw09Documents(String src) {
		JSONObject ret = new JSONObject(src);
		
		JSONArray uuids = ret.getJSONArray("uuid");
		JSONArray index = ret.getJSONArray("index");
		JSONArray docs = new JSONArray();
		
		for(int i=0; i< uuids.length(); i++) {
			if(!index.getString(i).equals("cw09")) {
				throw new RuntimeException("Invalid Index");
			}
			
			JSONObject doc = new JSONObject();
			doc.put("index", index.getString(i));
			doc.put("uuid", uuids.getString(i));
			doc.put("rawHtml", rawHtmlCw09(uuids.getString(i)));
			
			docs.put(doc);
		}
		
		if(docs.length() > 0) {
			ret.put("docs", docs);
		}
		
		return ret.toString();
	}

	private static String rawHtmlCw09(String uuid) {
		DocumentRetriever documentRetriever = new DocumentRetriever();

		UUID docUUID = UUID.fromString(uuid);
		Document ret = documentRetriever.getByUUID("cw09", docUUID);

		return ret == null ? null : ret.getBody();
	}
}
