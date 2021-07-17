package de.webis.copycat_spark.app;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.io.Files;

import de.webis.copycat_spark.app.DeduplicateTrecRunFile.AllPairsSimilarities;
import de.webis.copycat_spark.app.DeduplicateTrecRunFile.DocumentPairSimilarity;
import de.webis.copycat_spark.util.CollectionDocumentUtil;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import lombok.Data;
import lombok.SneakyThrows;

@Data
public class ExtractDocumentsToJudge {
	
	private final DeduplicateTrecRunFile runDeduplication;
	
	@SneakyThrows
	public void run(String json, Path p) {
		AllPairsSimilarities parsed = new ObjectMapper().readValue(json, AllPairsSimilarities.class);
		System.out.println("Extract examples for Topic " + parsed.getTopic());
		
		Map<String, CollectionDocument> idToDoc = idToDoc(parsed);
		
		for(DocumentPairSimilarity sim: parsed.getSimilarities()) {
			Path outputDir = outputDir(p, parsed.getTopic(), sim);
			
			persist(sim, outputDir, idToDoc);
		}
	}

	@SneakyThrows
	private void persist(DocumentPairSimilarity sim, Path outputDir, Map<String, CollectionDocument> idToDoc) {
		persistDoc(idToDoc.get(sim.getFirstId()), outputDir);
		persistDoc(idToDoc.get(sim.getSecondId()), outputDir);
		
		Map<String, Object> s3Meta = new LinkedHashMap<>();
		s3Meta.put("firstId", sim.getFirstId());
		s3Meta.put("secondId", sim.getSecondId());
		s3Meta.put("firstUrl", CollectionDocumentUtil.chatNoirURL("clueweb12", sim.getFirstId(), "cw12"));
		s3Meta.put("secondUrl", CollectionDocumentUtil.chatNoirURL("clueweb12", sim.getSecondId(), "cw12"));
		
		s3Meta.put("s3", sim.getSimilarities().get("s3score"));
		s3Meta.put("s3SampleReason", sim.getSimilarities().get("bin_key"));
		
		Files.write(new ObjectMapper().writeValueAsString(s3Meta).getBytes(), outputDir.resolve("s3score").toFile());
	}
	
	@SneakyThrows
	private void persistDoc(CollectionDocument doc, Path outputDir) {
		Files.write(doc.getFullyCanonicalizedContent().getBytes(), outputDir.resolve(doc.getId()).toFile());
		Files.write(doc.getFullyCanonicalizedContent().getBytes(), outputDir.resolve("full-text-" + doc.getId()).toFile());
	}

	Map<String, CollectionDocument> idToDoc(AllPairsSimilarities similarities) {
		Set<String> ret = new HashSet<>();
		
		for(DocumentPairSimilarity sim: similarities.getSimilarities()) {
			ret.add(sim.getFirstId());
			ret.add(sim.getSecondId());
		}
		
		return runDeduplication.docs(ret.stream().collect(Collectors.toList()));
	}
	
	private static Path outputDir(Path p, String topic, DocumentPairSimilarity sim) {
		Path ret = p.resolve("" + topic).resolve(sim.getFirstId() + "_vs_" + sim.getSecondId());
		ret.toFile().mkdirs();
		
		return ret;
	}
}
