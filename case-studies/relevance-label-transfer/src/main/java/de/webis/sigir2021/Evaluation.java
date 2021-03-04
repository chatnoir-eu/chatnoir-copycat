package de.webis.sigir2021;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import org.json.JSONArray;
import org.json.JSONObject;

import de.webis.sigir2021.trec.CanonicalDocuments;
import de.webis.sigir2021.wayback_machine.JudgedDocumentsWarcReader;
import de.webis.trec_ndd.trec_collections.SharedTask;
import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;
import lombok.SneakyThrows;

public class Evaluation {

	private static Map<String, Integer> CW09_TO_COUNT_OF_CW12_WITH_SAME_URL;

	private static Map<String, Double> CW09_TO_MAX_S3SCORE;

	private static Map<String, Double> CW09_TO_MAX_S3SCORE_DUPLICATE_AWARE;

	private static Map<String, Double> CW09_TO_MAX_NEAR_DUPLICATE_S3SCORE;

	private static Map<String, Double> CW09_TO_MAX_NEAR_DUPLICATE_S3SCORE_DUPLICATE_AWARE;

	private static Map<String, Double> CW09_TO_WAYBACK_MAX;

	private static Map<String, Double> CW09_TO_WAYBACK_MAX_DUPLICATE_AWARE;

	public static final String WAYBACK_SIMILARITIES_FILE = "/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/wayback-similarities.jsonl";

	private Evaluation() {
		if (CW09_TO_COUNT_OF_CW12_WITH_SAME_URL == null) {

			CW09_TO_WAYBACK_MAX = waybackMaxSimilarity(i -> i);
			CW09_TO_WAYBACK_MAX_DUPLICATE_AWARE = waybackMaxSimilarity(i -> CanonicalDocuments.canonicalDocument(i));

			CW09_TO_COUNT_OF_CW12_WITH_SAME_URL = tmp();
			CW09_TO_MAX_S3SCORE = leftDocToMaxS3Score(i -> i);
			CW09_TO_MAX_S3SCORE_DUPLICATE_AWARE = leftDocToMaxS3Score(i -> CanonicalDocuments.canonicalDocument(i));
			CW09_TO_MAX_NEAR_DUPLICATE_S3SCORE = cw09ToMaxNearDuplicateS3Score(i -> i);
			CW09_TO_MAX_NEAR_DUPLICATE_S3SCORE_DUPLICATE_AWARE = cw09ToMaxNearDuplicateS3Score(
					i -> CanonicalDocuments.canonicalDocument(i));
		}
	}

	public static void main(String[] args) {
		for (String year : new String[] { "2009", "2010", "2011", "2012" }) {
			new Evaluation().runEvaluation(year);
		}
	}

	@SneakyThrows
	public void runEvaluation(String year) {
		SharedTask task = webTask(year);
		String ret = "";
		for (String topicStr : task.topicNumberToTopic().keySet()) {
			int topic = Integer.parseInt(topicStr);

			for (String relevantDoc : task.documentJudgments().getRelevantDocuments(topicStr)) {
				ret += report(topic, relevantDoc, true, year) + "\n";
			}

			for (String irrelevantDoc : task.documentJudgments().getIrrelevantDocuments(topicStr)) {
				ret += report(topic, irrelevantDoc, false, year) + "\n";
			}
		}

		Files.write(new File("web-" + year + ".jsonl").toPath(), ret.getBytes(StandardCharsets.UTF_8));
	}

	private static SharedTask webTask(String year) {
		return TrecSharedTask.valueOf("WEB_" + year);
	}

	@SneakyThrows
	private static Map<String, Integer> tmp() {
		Map<String, Integer> ret = new LinkedHashMap<>();
		try (Stream<String> lines = Files.lines(
				new File("/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/url-judgments-from-cw09-to-cw12.jsonl")
						.toPath(),
				StandardCharsets.UTF_8)) {

			lines.forEach(line -> {
				JSONObject json = new JSONObject(line);
				String cw09Id = json.getString("sourceId");
				JSONArray cw12Ids = json.getJSONArray("targetIds");

				ret.put(cw09Id, cw12Ids.length());
			});
		}

		return ret;
	}

	private String report(int topic, String doc, boolean relevant, String year) {
		String topicStr = "topic-" + String.format("%02d", topic);
		String content = new JudgedDocumentsWarcReader(
				"/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/web-" + year
						+ "/" + topicStr).entries(topic, doc);
		int matches = 0, non200Matches = 0, cw12Matches = CW09_TO_COUNT_OF_CW12_WITH_SAME_URL.getOrDefault(doc, 0);
		double maxS3Score = 0;
		double maxNearDuplicateS3Score = CW09_TO_MAX_NEAR_DUPLICATE_S3SCORE.getOrDefault(doc, 0d);
		if (content != null) {
			List<String> lines = Arrays.asList(content.split("\n"));
			lines = JudgedDocumentsWarcReader.keepResponsesInCw12CrawlingTime(lines);

			for (String line : lines) {
				if (200 == JudgedDocumentsWarcReader.parseResponseCode(line)) {
					matches++;
				} else {
					non200Matches++;
				}
			}
		}

		if (cw12Matches > 0) {
			maxS3Score = CW09_TO_MAX_S3SCORE.get(doc);
		}

		String ret = "{\"topic\":" + topic + ",\"document\":\"" + doc + "\",\"relevant\":" + relevant
				+ ",\"duplicate\":" + CanonicalDocuments.isDuplicate(doc) + ",\"matches\":" + matches
				+ ",\"non200Matches\":" + non200Matches + ",\"cw12Matches\":" + cw12Matches + ",\"cw12UrlMaxS3Score\":"
				+ String.format("%.4f", maxS3Score) + ",\"cw12MaxNearDuplicateS3Score\":"
				+ String.format("%.4f", maxNearDuplicateS3Score) + ",\"cw12UrlMaxS3ScoreDuplicateAware\":"
				+ String.format("%.4f",
						CW09_TO_MAX_S3SCORE_DUPLICATE_AWARE.getOrDefault(CanonicalDocuments.canonicalDocument(doc), 0d))
				+ ",\"cw12MaxNearDuplicateS3ScoreDuplicateAware\":"
				+ String.format("%.4f",
						CW09_TO_MAX_NEAR_DUPLICATE_S3SCORE_DUPLICATE_AWARE
								.getOrDefault(CanonicalDocuments.canonicalDocument(doc), 0d))

				+ ",\"waybackMachineS3Score\":" + String.format("%.4f", CW09_TO_WAYBACK_MAX.getOrDefault(doc, 0d))
				+ ",\"waybackMachineS3ScoreDuplicateAware\":"
				+ String.format("%.4f",
						CW09_TO_WAYBACK_MAX_DUPLICATE_AWARE.getOrDefault(CanonicalDocuments.canonicalDocument(doc), 0d))

				+ "}";
		System.out.println(ret);

		return ret;
	}

	// FIXME add url + domain.
	// FIXME: make this ready for eval against cc15, currently only to cw12 is
	// supported

	@SneakyThrows
	private static Map<String, Double> leftDocToMaxS3Score(Function<String, String> idMapper) {
		Map<String, Double> ret = new LinkedHashMap<>();
		try (Stream<String> lines = Files.lines(new File(
				"/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-05-10-2020/url-transfer-from-cw09-to-cw12-with-similarity.jsonl")
						.toPath(),
				StandardCharsets.UTF_8)) {
			lines.forEach(line -> {
				JSONObject json = new JSONObject(line);
				String cw09Id = json.getString("firstId");
				cw09Id = idMapper.apply(cw09Id);

				Double s3Score = Math.max(json.getDouble("s3Score"), ret.getOrDefault(cw09Id, 0.0d));
				ret.put(cw09Id, s3Score);
			});
		}

		return ret;
	}

	@SneakyThrows
	private static Map<String, Double> waybackMaxSimilarity(Function<String, String> idMapper) {
		Map<String, Double> ret = new LinkedHashMap<>();
		try (Stream<String> lines = Files.lines(new File(WAYBACK_SIMILARITIES_FILE).toPath(), StandardCharsets.UTF_8)) {
			lines.forEach(line -> {
				JSONObject json = new JSONObject(line);
				String cw09Id = json.getString("firstId");
				cw09Id = idMapper.apply(cw09Id);

				Double s3Score = Math.max(json.getDouble("s3Score"), ret.getOrDefault(cw09Id, 0.0d));
				ret.put(cw09Id, s3Score);
			});
		}

		return ret;
	}

	@SneakyThrows
	private static Map<String, Double> cw09ToMaxNearDuplicateS3Score(Function<String, String> idMapper) {
		System.out.println("Start cw09ToMaxNearDuplicateS3Score :" + new Date());
		Map<String, Double> ret = new LinkedHashMap<>();
		Stream<String> lines = Files.lines(new File(
				"/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-09-10-2020/relevance-transfer-only-near-duplicates.jsonl")
						.toPath(),
				StandardCharsets.UTF_8);

		lines.forEach(line -> {
			String cw09Id = cw09Id(line);
			String cw12Id = cw12Id(line);

			if (cw09Id != null && cw12Id != null) {
				cw09Id = idMapper.apply(cw09Id);

				Double s3Score = Math.max(s3Score(line), ret.getOrDefault(cw09Id, 0.0d));
				ret.put(cw09Id, s3Score);
			}
		});

		lines = Files.lines(new File(
				"/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-09-10-2020/relevance-transfer-exact-duplicates.jsonl")
						.toPath(),
				StandardCharsets.UTF_8);

		lines.forEach(line -> {
			String cw09Id = cw09Id(line);
			String cw12Id = cw12Id(line);

			if (cw09Id != null && cw12Id != null) {
				cw09Id = idMapper.apply(cw09Id);

				Double s3Score = Math.max(s3Score(line), ret.getOrDefault(cw09Id, 0.0d));
				ret.put(cw09Id, s3Score);
			}
		});

		System.out.println("Finish cw09ToMaxNearDuplicateS3Score :" + new Date());

		return ret;
	}

	public static String cw12Id(String json) {
		JSONObject parsed = new JSONObject(json);

		if (parsed.getString("firstId").startsWith("clueweb12")) {
			return parsed.getString("firstId");
		} else if (parsed.getString("secondId").startsWith("clueweb12")) {
			return parsed.getString("secondId");
		}

		return null;
	}

	public static String cw09Id(String json) {
		JSONObject parsed = new JSONObject(json);

		if (parsed.getString("firstId").startsWith("clueweb09")) {
			return parsed.getString("firstId");
		} else if (parsed.getString("secondId").startsWith("clueweb09")) {
			return parsed.getString("secondId");
		}

		return null;
	}

	public static String cw09OrCw12(String json) {
		String ret = cw09Id(json);
		if (ret != null) {
			return ret;
		}

		return cw12Id(json);
	}

	public static String cc15Id(String json) {
		JSONObject parsed = new JSONObject(json);

		if (!parsed.getString("firstId").startsWith("clueweb09")
				&& !parsed.getString("firstId").startsWith("clueweb12")) {
			return parsed.getString("firstId");
		} else if (!parsed.getString("secondId").startsWith("clueweb09")
				&& !parsed.getString("secondId").startsWith("clueweb12")) {
			return parsed.getString("secondId");
		}

		return null;
	}

	public static double s3Score(String json) {
		return new JSONObject(json).getDouble("s3Score");
	}
}
