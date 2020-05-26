package de.webis.cikm20_duplicates.spark;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaHadoopRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.JSONException;
import org.json.JSONObject;

import de.webis.cikm20_duplicates.util.FingerPrintUtil.Fingerprinter;
import de.webis.cikm20_duplicates.util.FingerPrintUtil;
import de.webis.cikm20_duplicates.util.SourceDocuments;
import de.webis.cikm20_duplicates.util.SourceDocuments.CollectionDocumentWithTopics;
import de.webis.cikm20_duplicates.util.SourceDocuments.DocumentWithFingerprint;
import de.webis.trec_ndd.trec_collections.AnseriniCollectionReader;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import de.webis.trec_ndd.util.S3Files;
import io.anserini.index.transform.JsoupStringTransform;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import scala.Tuple2;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration.OtherCollections;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;

/**
 * 
 * @author Maik Fröbe
 *
 */
public class SparkCreateSourceDocuments {
	
	static final Map<String, Set<String>> DOCS_TO_TOPIC = docsToTopic();

//	private static final String[] CORPORA = new String[] {/*"cw09", "cw12",*/ "cc-2015-11", "cc-2017-04"/*, "cc-2015-11-small-sample", "cc-2017-04-small-sample"*/};
	
	private static final String[] CORPORA = new String[] {
			//30-40 + 90-100 + 70-80
			"cc-2017-04-part-00030", "cc-2017-04-part-00031", "cc-2017-04-part-00032",
			"cc-2017-04-part-00033", "cc-2017-04-part-00034", "cc-2017-04-part-00035",
			"cc-2017-04-part-00036", "cc-2017-04-part-00037", "cc-2017-04-part-00038", 
			"cc-2017-04-part-00039", "cc-2017-04-part-00070", "cc-2017-04-part-00071",
			"cc-2017-04-part-00072", "cc-2017-04-part-00073", "cc-2017-04-part-00074",
			"cc-2017-04-part-00075", "cc-2017-04-part-00076", "cc-2017-04-part-00077",
			"cc-2017-04-part-00078", "cc-2017-04-part-00079", "cc-2017-04-part-00090",
			"cc-2017-04-part-00091", "cc-2017-04-part-00092", "cc-2017-04-part-00093",
			"cc-2017-04-part-00094", "cc-2017-04-part-00095", "cc-2017-04-part-00096",
			"cc-2017-04-part-00097", "cc-2017-04-part-00098", "cc-2017-04-part-00099"
	};
	
//	//TODO :130-140 + 150-160
//	private static final String[] CORPORA = new String[] {
//			"cc-2017-04-part-00130", "cc-2017-04-part-00131", "cc-2017-04-part-00132",
//			"cc-2017-04-part-00133", "cc-2017-04-part-00134", "cc-2017-04-part-00135",
//			"cc-2017-04-part-00136", "cc-2017-04-part-00137", "cc-2017-04-part-00138", 
//			"cc-2017-04-part-00139",
//			
//			"cc-2017-04-part-00150", "cc-2017-04-part-00151", "cc-2017-04-part-00152",
//			"cc-2017-04-part-00153", "cc-2017-04-part-00154", "cc-2017-04-part-00155",
//			"cc-2017-04-part-00156", "cc-2017-04-part-00157", "cc-2017-04-part-00158",
//			"cc-2017-04-part-00159"
//	};

/*	private static final String[] CORPORA = new String[] {
//			"cc-2015-11-part-0", // SUCCESS-FILE is there: cikm2020/document-fingerprints-final/cc-2015-11-part-0-jsonl/_SUCCESS
//			"cc-2015-11-part-1", // SUCCESS-FILE is there: cikm2020/document-fingerprints-final/cc-2015-11-part-1-jsonl/_SUCCESS
//			"cc-2015-11-part-2", // SUCCESS-FILE is there: cikm2020/document-fingerprints-final/cc-2015-11-part-2-jsonl/_SUCCESS
//			"cc-2015-11-part-3", // SUCCESS-FILE is there: cikm2020/document-fingerprints-final/cc-2015-11-part-3-jsonl/_SUCCESS
//			"cc-2015-11-part-4", // SUCCESS-FILE is there: cikm2020/document-fingerprints-final/cc-2015-11-part-4-jsonl/_SUCCESS
//			"cc-2015-11-part-5", // SUCCESS-FILE is there: cikm2020/document-fingerprints-final/cc-2015-11-part-5-jsonl/_SUCCESS
//			"cc-2015-11-part-6", // SUCCeSS-FILE is there: cikm2020/document-fingerprints-final/cc-2015-11-part-6-jsonl/_SUCCESS
//			"cc-2015-11-part-7", // SUCCESS-FILE is there: cikm2020/document-fingerprints-final/cc-2015-11-part-7-jsonl/_SUCCESS
//			"cc-2015-11-part-8", // SUCCESS-FILE is there: cikm2020/document-fingerprints-final/cc-2015-11-part-8-jsonl/_SUCCESS
//			"cc-2015-11-part-9"  // SUCCESS-FILE is there: cikm2020/document-fingerprints-final/cc-2015-11-part-9-jsonl/_SUCCESS
	};*/

	private static final AnseriniCollectionReader<?>
			CLUEWEB09 = new AnseriniCollectionReader<>(TrecCollections.CLUEWEB09),
			CLUEWEB12 = new AnseriniCollectionReader<>(TrecCollections.CLUEWEB12);
	
	public static final List<Fingerprinter<Integer>> PRODUCTION_FINGERPRINTS = Arrays.asList(
		FingerPrintUtil.simHashFingerPrinting(64, 3),
		FingerPrintUtil.productionFingerpringint(64, 3)	
	);

//	public static void main(String[] args) {
//		try (JavaSparkContext context = context()) {
//			for(String corpus: CORPORA) {
//				JavaRDD<CollectionDocument> docs = docs(context, corpus);
//				
//				fingerprintAllDocuments(context, docs, PRODUCTION_FINGERPRINTS)
//					.saveAsTextFile("cikm2020/document-fingerprints-final/" + corpus +"-jsonl.bzip2", BZip2Codec.class);
//			}
//		}
//	}
	
//	public static void main(String[] args) {
//		try (JavaSparkContext context = context()) {
//			for(String corpus: CORPORA) {
//				JavaRDD<CollectionDocument> docs = docs(context, corpus);
//				
//				fingerprintAllDocuments(context, docs, PRODUCTION_FINGERPRINTS)
//					.saveAsTextFile("cikm2020/document-fingerprints-final/" + corpus +"-jsonl");
//			}
//		}
//	}
	
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			for(String corpus: cc17Collections()) {
				if(dirExists(context, "cikm2020/document-fingerprints-final/" + corpus +"-jsonl")) {
					System.out.println("Exists: " + corpus);
				} else {
					System.out.println("Does not exist: " + corpus);
				}
				
//				JavaRDD<CollectionDocument> docs = docs(context, corpus);
//				
//				fingerprintAllDocuments(context, docs, PRODUCTION_FINGERPRINTS)
//					.saveAsTextFile("cikm2020/document-fingerprints-final/" + corpus +"-jsonl");
			}
		}
	}
	
	private static JavaRDD<CollectionDocument> docs(JavaSparkContext context, String corpus) {
		if ("cw09".equals(corpus)) {
			return docs(context, CLUEWEB09);
		} else if ("cw12".equals(corpus)) {
			return docs(context, CLUEWEB12);
		} else if ("cc-2015-11".equals(corpus)) {
			return ccDocs(context, "/corpora/corpus-commoncrawl/CC-MAIN-2015-11-mapfile/data-r-*/data");
		} else if ("cc-2017-04".equals(corpus)) {
			return ccDocs(context, "/corpora/corpus-commoncrawl/CC-MAIN-2017-04-mapfile/data-r-*/data");
		} else if ("cc-2015-11-small-sample".equals(corpus)) {
			return ccDocs(context, "/corpora/corpus-commoncrawl/CC-MAIN-2015-11-mapfile/data-r-00001/data");
		} else if ("cc-2017-04-small-sample".equals(corpus)) {
			return ccDocs(context, "/corpora/corpus-commoncrawl/CC-MAIN-2017-04-mapfile/data-r-00001/data");
		} else if (corpus.startsWith("cc-2017-04-part-00")) {
			for(String p : cc17Collections()) {
				if(p.equals(corpus)) {
					return ccDocs(context, "/corpora/corpus-commoncrawl/CC-MAIN-2017-04-mapfile/data-r-" + StringUtils.substringAfterLast(p, "-") + "/data");
				}
			}
		} else if (corpus.startsWith("cc-2015-11-part-")) {
			for(int i=0; i<10; i++) {
				if(("cc-2015-11-part-" + i).equals(corpus)) {
					return ccDocs(context, "/corpora/corpus-commoncrawl/CC-MAIN-2015-11-mapfile/data-r-*" + i + "/data");
				}
			}
		}
		
		throw new RuntimeException("Add more corpora :" + corpus);
	}

	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/source-documents");

		return new JavaSparkContext(conf);
	}
	
	public static JavaRDD<CollectionDocumentWithTopics> transformAllImportantDocuments(JavaSparkContext context, AnseriniCollectionReader<?>...acrs) {
		return docs(context, acrs)
				.map(doc -> transformDocIfImportantOrNull(doc))
				.filter(i -> i != null); 
	}
	
	public static JavaRDD<DocumentWithFingerprint> fingerprintAllDocuments(JavaSparkContext context, JavaRDD<CollectionDocument> docs, List<Fingerprinter<Integer>> fingerprinters) {
		return docs.map(i -> new DocumentWithFingerprint(i.getId(), i.getUrl(), i.getCanonicalUrl(), fp(i, fingerprinters)));
	}
	
	private static LinkedHashMap<String, ArrayList<Integer>> fp(CollectionDocument doc, List<Fingerprinter<Integer>> fingerprinters) {
		LinkedHashMap<String, ArrayList<Integer>> ret = new LinkedHashMap<>();
		
		for(Fingerprinter<Integer> f: fingerprinters) {
			ret.put(f.fingerprinterName(), new ArrayList<>(f.fingerprint(doc)));
		}
		
		return ret;
	}
	
	@SuppressWarnings("rawtypes")
	public static JavaRDD<DocumentWithFingerprint> fingerprintAllDocuments(JavaSparkContext context, List<Fingerprinter<Integer>> fingerprinters, AnseriniCollectionReader...acr) {
		return fingerprintAllDocuments(context, docs(context, acr), fingerprinters);
	}
	
	@SuppressWarnings("unchecked")
	static JavaRDD<CollectionDocument> ccDocs(JavaSparkContext context, String path) {
		return ((JavaHadoopRDD<Text, Text>) context.hadoopFile(path, SequenceFileInputFormat.class, Text.class, Text.class))
				.map(kv -> chatnoirMapFileDocumentToDocOrNullFailsave(kv._1().toString(), kv._2().toString()))
				.filter(i -> i != null);
	}

	private static CollectionDocument chatnoirMapFileDocumentToDocOrNullFailsave(String keyStr, String valueStr) {
		try {
			return chatnoirMapFileDocumentToDocOrNull(keyStr, valueStr);
		} catch (Throwable e) {
			return null;
		}
	}
	
	@SneakyThrows
	private static CollectionDocument chatnoirMapFileDocumentToDocOrNull(String keyStr, String valueStr) {
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

		String targetUri = metadata.getString("WARC-Target-URI");
		String recordId = metadata.getString("WARC-Record-ID");

		CollectionDocument ret = CollectionDocument.collectionDocument(new JsoupStringTransform().apply(contentBody), keyStr);
		ret.setUrl(new URL(targetUri));
		ret.setCanonicalUrl(SparkCanonicalLinkGraphExtraction.extractCanonicalLinkOrNull(targetUri, contentBody));

		return ret;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static JavaRDD<CollectionDocument> docs(JavaSparkContext context, AnseriniCollectionReader...acrs) {
		List<Tuple2<AnseriniCollectionReader, String>> readerToSegment = new ArrayList<>();
		
		for(AnseriniCollectionReader acr: acrs) {
			List<String> segmentPaths = acr.segmentPaths();
			for(String segmentPath: segmentPaths) {
				readerToSegment.add(new Tuple2<>(acr, segmentPath));
			}
		}
		
		return context.parallelize(readerToSegment, readerToSegment.size())
				.flatMap(i -> i._1().collectionDocumentsInPath(i._2()));
	}
	
	private static CollectionDocumentWithTopics transformDocIfImportantOrNull(CollectionDocument doc) {
		if(!DOCS_TO_TOPIC.containsKey(doc.getId())) {
			return null;
		}
		
		return new CollectionDocumentWithTopics(doc, new ArrayList<>(DOCS_TO_TOPIC.get(doc.getId())));
	}
	
	private static Map<String, Set<String>> docsToTopic() {
		Map<String, Set<String>> ret = new HashMap<>();
		for(Map.Entry<String, Set<String>> topicToIds : SourceDocuments.topicsToDocumentIds().entrySet()) {
			String topic = topicToIds.getKey();
			for(String docId: topicToIds.getValue()) {
				if(!ret.containsKey(docId)) {
					ret.put(docId, new HashSet<>());
				}
				
				ret.get(docId).add(topic);
			}
		}
		
		return ret;
	}
	
	private static AnseriniCollectionReader<?> commonCrawl(String[] args) {
		Namespace parsedArgs = parseArguments(args);
		S3Files s3Files = new S3Files(
				parsedArgs.getString("accessKey"),
				parsedArgs.getString("secretKey"),
				"corpus-commoncrawl-main-2015-11"
		);
		
		OtherCollections config = OtherCollections.commonCrawl_2015_02(s3Files);
		return new AnseriniCollectionReader<>(config);
	}
	
	private static Namespace parseArguments(String[] args) {
		ArgumentParser parser = ArgumentParsers.newFor("SparkCreateSourceDocuments").build().defaultHelp(true);

		parser.addArgument("--accessKey")
			.required(Boolean.TRUE)
			.help("Specify the s3 access key.");
		parser.addArgument("--secretKey")
			.required(Boolean.TRUE)
			.help("Specify the s3 secret key.");
		
		return parser.parseArgsOrFail(args);
	}

	public static List<String> cc17Collections() {
		return IntStream.range(0, 200)
			.mapToObj(i -> "cc-2017-04-part-" + String.format("%05d", i))
			.collect(Collectors.toList());
	}
	
	@SneakyThrows
	public static boolean dirExists(JavaSparkContext sc, String path) {
		FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
		
		return fs.exists(new Path(path));
	}
}
