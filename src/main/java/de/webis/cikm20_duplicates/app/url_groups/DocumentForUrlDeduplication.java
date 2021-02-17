package de.webis.cikm20_duplicates.app.url_groups;

import java.io.Serializable;
import java.net.URL;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

import de.webis.cikm20_duplicates.util.SourceDocuments.DocumentWithFingerprint;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

@Data
@NoArgsConstructor
@AllArgsConstructor
@SuppressWarnings("serial")
public class DocumentForUrlDeduplication implements Serializable {
	private List<Integer> simHash64BitK3OneGramms;
	private String docId;
	private URL canonicalURL;
	
	public static DocumentForUrlDeduplication fromDocumentWithFingerprint(DocumentWithFingerprint src) {
		if(src == null || src.getCanonicalURL() == null || src.getDocId() == null || src.getFingerprints() == null || !src.getFingerprints().containsKey("64BitK3SimHashOneGramms")) {
			return null;
		}
		
		return new DocumentForUrlDeduplication(
			src.getFingerprints().get("64BitK3SimHashOneGramms"),
			src.getDocId(),
			src.getCanonicalURL()
		);
	}
	
	@Override
	@SneakyThrows
	public String toString() {
		return new ObjectMapper().writeValueAsString(this);
	}
	
	@SneakyThrows
	public static DocumentForUrlDeduplication fromString(String src) {
		return new ObjectMapper().readValue(src, DocumentForUrlDeduplication.class);
	}
}
