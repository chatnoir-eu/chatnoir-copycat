package de.webis.sigir2021.trec;

import static io.anserini.index.generator.LuceneDocumentGenerator.FIELD_ID;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;

import de.webis.trec_ndd.spark.RunLine;
import de.webis.trec_ndd.trec_collections.QrelEqualWithoutScore;
import de.webis.trec_ndd.trec_collections.SharedTask;
import io.anserini.index.generator.LuceneDocumentGenerator;
import io.anserini.rerank.ScoredDocuments;
import io.anserini.rerank.lib.ScoreTiesAdjusterReranker;
import lombok.Data;
import lombok.SneakyThrows;

@Data
public class CombineRankings {
	private static final List<Float> WEIGHTS = Arrays.asList(0.0f, 0.2f, 0.4f, 0.6f, 0.8f, 1.0f);
	
	public static void main(String[] args) {
		Path baseDir = Paths.get("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/rankings/");
		
		createAllRuns(
			baseDir.resolve("topics-1-50-cw09"),
			"webis_warc_clueweb09_003",
			TransferredSharedTasks.WEB_2009_DUPLICATE_FREE,
			baseDir.resolve("topics-1-50-cw09").resolve("final-rankings")
		);
		
		createAllRuns(
			baseDir.resolve("topics-51-100-cw09"),
			"webis_warc_clueweb09_003",
			TransferredSharedTasks.WEB_2010_DUPLICATE_FREE,
			baseDir.resolve("topics-51-100-cw09").resolve("final-rankings")
		);
		
		createAllRuns(
			baseDir.resolve("topics-101-150-cw09"),
			"webis_warc_clueweb09_003",
			TransferredSharedTasks.WEB_2011_DUPLICATE_FREE,
			baseDir.resolve("topics-101-150-cw09").resolve("final-rankings")
		);
		
		createAllRuns(
			baseDir.resolve("topics-151-200-cw09"),
			"webis_warc_clueweb09_003",
			TransferredSharedTasks.WEB_2012_DUPLICATE_FREE,
			baseDir.resolve("topics-151-200-cw09").resolve("final-rankings")
		);
		
		createAllRuns(
			baseDir.resolve("topics-1-50-cw12"),
			"webis_warc_clueweb12_011",
			TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_2012,
			baseDir.resolve("topics-1-50-cw12").resolve("final-rankings")
		);
		
		createAllRuns(
			baseDir.resolve("topics-51-100-cw12"),
			"webis_warc_clueweb12_011",
			TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CW_2012,
			baseDir.resolve("topics-51-100-cw12").resolve("final-rankings")
		);
		
		createAllRuns(
			baseDir.resolve("topics-101-150-cw12"),
			"webis_warc_clueweb12_011",
			TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CW_2012,
			baseDir.resolve("topics-101-150-cw12").resolve("final-rankings")
		);
		
		createAllRuns(
			baseDir.resolve("topics-151-200-cw12"),
			"webis_warc_clueweb12_011",
			TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CW_2012,
			baseDir.resolve("topics-151-200-cw12").resolve("final-rankings")
		);
		
		createAllRuns(
			baseDir.resolve("topics-1-50-cw12wb12"),
			"clueweb12-and-wayback12",
			TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CW_WB_2012,
			baseDir.resolve("topics-1-50-cw12wb12").resolve("final-rankings")
		);
			
		createAllRuns(
			baseDir.resolve("topics-51-100-cw12wb12"),
			"clueweb12-and-wayback12",
			TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CW_WB_2012,
			baseDir.resolve("topics-51-100-cw12wb12").resolve("final-rankings")
		);
			
		createAllRuns(
			baseDir.resolve("topics-101-150-cw12wb12"),
			"clueweb12-and-wayback12",
			TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CW_WB_2012,
			baseDir.resolve("topics-101-150-cw12wb12").resolve("final-rankings")
		);
			
		createAllRuns(
			baseDir.resolve("topics-151-200-cw12wb12"),
			"clueweb12-and-wayback12",
			TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CW_WB_2012,
			baseDir.resolve("topics-151-200-cw12wb12").resolve("final-rankings")
		);
		
		createAllRuns(
			baseDir.resolve("topics-201-250-cw12"),
			"webis_warc_clueweb12_011",
			TransferredSharedTasks.WEB_2013_DUPLICATE_FREE,
			baseDir.resolve("topics-201-250-cw12").resolve("final-rankings")
		);
		
		createAllRuns(
			baseDir.resolve("topics-251-300-cw12"),
			"webis_warc_clueweb12_011",
			TransferredSharedTasks.WEB_2014_DUPLICATE_FREE,
			baseDir.resolve("topics-251-300-cw12").resolve("final-rankings")
		);
		
		createAllRuns(
			baseDir.resolve("topics-1-50-cc15"),
			"webis_warc_commoncrawl15_002",
			TransferredSharedTasks.TRANSFER_FROM_WEB_2009_TO_CC_2015,
			baseDir.resolve("topics-1-50-cc15").resolve("final-rankings")
		);
		
		createAllRuns(
			baseDir.resolve("topics-51-100-cc15"),
			"webis_warc_commoncrawl15_002",
			TransferredSharedTasks.TRANSFER_FROM_WEB_2010_TO_CC_2015,
			baseDir.resolve("topics-51-100-cc15").resolve("final-rankings")
		);
		
		createAllRuns(
			baseDir.resolve("topics-101-150-cc15"),
			"webis_warc_commoncrawl15_002",
			TransferredSharedTasks.TRANSFER_FROM_WEB_2011_TO_CC_2015,
			baseDir.resolve("topics-101-150-cc15").resolve("final-rankings")
		);
		
		createAllRuns(
			baseDir.resolve("topics-151-200-cc15"),
			"webis_warc_commoncrawl15_002",
			TransferredSharedTasks.TRANSFER_FROM_WEB_2012_TO_CC_2015,
			baseDir.resolve("topics-151-200-cc15").resolve("final-rankings")
		);
		
		createAllRuns(
			baseDir.resolve("topics-201-250-cc15"),
			"webis_warc_commoncrawl15_002",
			TransferredSharedTasks.TRANSFER_FROM_WEB_2013_TO_CC_2015,
			baseDir.resolve("topics-201-250-cc15").resolve("final-rankings")
		);

		createAllRuns(
			baseDir.resolve("topics-251-300-cc15"),
			"webis_warc_commoncrawl15_002",
			TransferredSharedTasks.TRANSFER_FROM_WEB_2014_TO_CC_2015,
			baseDir.resolve("topics-251-300-cc15").resolve("final-rankings")
		);
	}
	
	@SneakyThrows
	private static void createAllRuns(Path baseRunDir, String prefix, SharedTask task, Path outputDir) {
		CombineRankings combiner = new CombineRankings(baseRunDir.toFile(), prefix, task);
		
		for(float bodyWeight: WEIGHTS) {
			for(float titleWeight: WEIGHTS) {
				for(float metaWeight: WEIGHTS) {
					String output = combiner.combine(bodyWeight, titleWeight, metaWeight);
					Path outputFile = outputDir.resolve(CombineRankings.runTag(prefix, bodyWeight, titleWeight, metaWeight) +".txt");
					System.out.println("Write " + outputFile.getFileName().toString());
					
					Files.write(outputFile, output.getBytes());
				}
			}
		}
	}
	
	private final File runDir;
	private final String prefix;
	private final SharedTask task;
	private final Map<Integer, Map<String, Double>> topicToBodyScores;
	private final Map<Integer, Map<String, Double>> topicToTitleScores;
	private final Map<Integer, Map<String, Double>> topicToMetaScores;
	
	public CombineRankings(File runDir, String prefix, SharedTask task) {
		this.runDir = runDir;
		this.prefix = prefix;
		this.task = task;
		this.topicToBodyScores = loadScoresFromRunFile(runDir.toPath().resolve(prefix + "-body_lang.en^1-title_lang.en^0-meta_desc_lang.en^0"));
		this.topicToTitleScores = loadScoresFromRunFile(runDir.toPath().resolve(prefix + "-body_lang.en^0-title_lang.en^1-meta_desc_lang.en^0"));
		this.topicToMetaScores = loadScoresFromRunFile(runDir.toPath().resolve(prefix + "-body_lang.en^0-title_lang.en^0-meta_desc_lang.en^1"));
	}

	public String combine(float bodyWeight, float titleWeight, float metaWeight) {
		String ret = "";
		String runTag = runTag(prefix, bodyWeight, titleWeight, metaWeight);

		Map<String, List<RunLine>> topicToRunFiles = topicToRunFiles(task, bodyWeight, titleWeight, metaWeight);
		for (int qid : topics(topicToRunFiles)) {
			ScoredDocuments docs = adjustScoreTies(topicToRunFiles.get("" + qid));
			for (int i = 0; i < docs.documents.length; i++) {
				ret += "\n" + String.format(Locale.US, "%s Q0 %s %d %f %s", qid,
						docs.documents[i].getField(FIELD_ID).stringValue(), (i + 1), docs.scores[i], runTag);
			}
		}

		return ret.trim();
	}

	private Map<Integer, Map<String, Double>> loadScoresFromRunFile(Path runFile) {
		List<RunLine> lines = RunLine.parseRunlines(runFile);
		Map<Integer, Map<String, Double>> ret = new HashMap<>();
		
		for(RunLine line: lines) {
			if(!ret.containsKey(line.getTopic())) {
				ret.put(line.getTopic(), new LinkedHashMap<>());
			}

			ret.get(line.getTopic()).put(line.getDoucmentID(), score(line));
		}
		
		return ret;
	}
	
	private static String runTag(String prefix, float bodyWeight, float titleWeight, float metaWeight) {
		return prefix + "-body_lang.en^" + String.format("%.1f", bodyWeight) + "-title_lang.en^" 
				+ String.format("%.1f", titleWeight) + "-meta_desc_lang.en^" + String.format("%.1f", metaWeight);
	}

	private static List<Integer> topics(Map<String, List<RunLine>> topicToRunFiles) {
		return topicToRunFiles.keySet().stream().map(i -> Integer.parseInt(i)).sorted().collect(Collectors.toList());
	}

	private Map<String, List<RunLine>> topicToRunFiles(SharedTask task, float bodyWeight, float titleWeight, float metaWeight) {
		Map<String, List<RunLine>> ret = new LinkedHashMap<>();
		Set<QrelEqualWithoutScore> qrels = task.getQrelResourcesWithoutScore();
		
		for(QrelEqualWithoutScore qrel: qrels) {
			if(!ret.containsKey(qrel.getTopicNumber() + "")) {
				ret.put(qrel.getTopicNumber() + "", new ArrayList<>());
			}
			
			ret.get(qrel.getTopicNumber() + "").add(new RunLine(String.format(Locale.US, "%s Q0 %s %d %f %s",
					qrel.getTopicNumber(), qrel.getDocumentID(), 0, score(qrel.getTopicNumber(), qrel.getDocumentID(), bodyWeight, titleWeight, metaWeight), "tag")));
		}
		
		return ret;
	}

	private float score(int topicNumber, String documentID, float bodyWeight, float titleWeight, float metaWeight) {
		return (float) (topicToBodyScores.get(topicNumber).get(documentID) * bodyWeight
				+ topicToTitleScores.get(topicNumber).get(documentID) * titleWeight
				+ topicToMetaScores.get(topicNumber).get(documentID) * metaWeight);
	}

	private static ScoredDocuments adjustScoreTies(List<RunLine> runLines) {
		ScoredDocuments scoredDocs = docs(runLines);
		return new ScoreTiesAdjusterReranker().rerank(scoredDocs, null);
	}

	private static ScoredDocuments docs(List<RunLine> runLines) {
		runLines = new ArrayList<>(runLines);
		Collections.sort(runLines, (a, b) -> compare(b,a));
		ScoredDocuments scoredDocs = new ScoredDocuments();
		scoredDocs.documents = new Document[runLines.size()];
		scoredDocs.scores = new float[runLines.size()];

		for (int i = 0; i < runLines.size(); i++) {
			scoredDocs.documents[i] = doc(runLines.get(i));
			scoredDocs.scores[i] = score(runLines.get(i)).floatValue();
		}

		return scoredDocs;
	}
	
	private static int compare(RunLine a, RunLine b) {
		int ret = score(a).compareTo(score(b));
		
		if (ret != 0) {
			return ret;
		} else {
			return a.getDoucmentID().compareTo(b.getDoucmentID());
		}
	}

	private static Document doc(RunLine runLine) {
		Document ret = new Document();
		ret.add(new StringField(LuceneDocumentGenerator.FIELD_ID, runLine.getDoucmentID(), Store.YES));
		return ret;
	}

	private static Double score(RunLine runLine) {
		return Double.parseDouble(runLine.toString().split("[\\s\\t]+")[4]);
	}
}
