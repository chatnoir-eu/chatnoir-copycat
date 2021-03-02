package de.webis.sigir2021.trec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import lombok.SneakyThrows;

public class CanonicalDocuments {
	
	private static Map<String, String> DOC_TO_CANONICAL = null;
	
	public static synchronized String canonicalDocument(String id) {
		if (DOC_TO_CANONICAL == null) {
			DOC_TO_CANONICAL = buildDocToCanonical();
		}
		
		return DOC_TO_CANONICAL.getOrDefault(id, id);
	}

	private static Map<String, String> buildDocToCanonical() {
		Map<String, String> ret = new LinkedHashMap<>();
		ret.putAll(buildDocToCanonical("/clueweb09-web-track-content-equivalent-groups.jsonl"));
		ret.putAll(buildDocToCanonical("/clueweb12-web-track-content-equivalent-groups.jsonl"));
		
		return ret;
	}
	
	@SneakyThrows
	private static Map<String, String> buildDocToCanonical(String resource) {
		List<String> jsonLines = IOUtils.readLines(CanonicalDocuments.class.getResourceAsStream(resource));
		Map<String, String> ret = new LinkedHashMap<>();
		
		for(String jsonLine: jsonLines) {
			List<String> equivalentDocs = equivalentDocs(jsonLine);
			
			for(int i=1; i<equivalentDocs.size(); i++) {
				ret.put(equivalentDocs.get(i), equivalentDocs.get(0));
			}
		}
		
		return ret;
	}

	private static List<String> equivalentDocs(String json) {
		List<String> ret = new ArrayList<>();
		JSONArray ids = new JSONObject(json).getJSONArray("ids");
		
		for(int i=0; i< ids.length(); i++) {
			ret.add(ids.getString(i));
		}
		
		Collections.sort(ret);
		
		return ret;
	}

	public static boolean isDuplicate(String id) {
		return id == null || !id.equals(canonicalDocument(id));
	}
}
