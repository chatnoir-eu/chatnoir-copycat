package de.webis.cikm20_duplicates.util;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.webis.cikm20_duplicates.spark.SparkCalculateCanonicalLinkGraphEdgeLabels.CanonicalLinkGraphEdge2;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import lombok.SneakyThrows;

public class CreateStuffToJudge {
	private static final String
		DIR = "src/main/resources/canonical-link-edges-human-judgments/",
		
		//CREATED BY: de.webis.cikm20_duplicates.spark.eval.SparkSampleS3EdgesPerBin
		INPUT = DIR + "cw12-sampled-edges.jsonl";
	
	@SneakyThrows
	public static void main(String[] args) {
		for(String line: Files.readAllLines(Paths.get(INPUT))) {
			persistExamples(line);
		}
	}
	
	@SneakyThrows
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static void persistExamples(String json) {
		Map<String, Object> m = new ObjectMapper().readValue(json, Map.class); 
		
		if(!m.containsKey("s3Bucket") || m.get("s3Bucket") == null) {
			return;
		} else {
			System.out.println("Bucket: " + m.get("s3Bucket"));
		}
		
		List edges = (List) m.get("sample-edges");
		edges = edges.subList(0, 10);
		
		for(Object e: edges) {
			String eStr = new ObjectMapper().writeValueAsString(e);
			CanonicalLinkGraphEdge2 eVal = CanonicalLinkGraphEdge2.fromString(eStr);
			
			persistExample(eVal);
		}
	}
	
	@SneakyThrows
	private static void persistExample(CanonicalLinkGraphEdge2 bla) {
		CollectionDocument src = bla.getFirstDoc().getDoc();
		CollectionDocument target = bla.getSecondDoc().getDoc();
		
		Path dir = Paths.get(DIR + "judgments-by-daniel/" + src.getId() + "__to__" + target.getId());
		dir.toFile().mkdirs();
		
		Files.write(dir.resolve("metadata"), metadata(src, target, bla.getCanonicalLink(), bla.getS3score()).getBytes(StandardCharsets.UTF_8));
		Files.write(dir.resolve("src-doc"), src.getFullyCanonicalizedContent().getBytes(StandardCharsets.UTF_8));
		Files.write(dir.resolve("target-doc"), target.getFullyCanonicalizedContent().getBytes(StandardCharsets.UTF_8));
	}
	
	private static String metadata(CollectionDocument a, CollectionDocument b, String canonicalUrl, double s3) {
		return "Canonical-Link: " + canonicalUrl +"\n" +
				"First: "+ CollectionDocumentUtil.chatNoirURL(a.getId()) +"&plain\n" + 
				"Second: "+ CollectionDocumentUtil.chatNoirURL(b.getId()) +"&plain\n" +
				"S3: " + s3 +"\n";
	}
}
