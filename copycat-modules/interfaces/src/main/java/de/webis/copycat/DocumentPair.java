package de.webis.copycat;

import java.util.Map;
import de.webis.trec_ndd.spark.DocumentHash;
import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3Score;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DocumentPair {

	private final S3Score s3Score;
	private final CollectionDocument first, second;
	private final DocumentHash firstHash, secondHash;
		
	public DocumentPair(S3Score s3Score, Map<String, CollectionDocument> docs, Map<String, DocumentHash> idToHash) {
		this.s3Score = s3Score;
		this.first  = docs.get(s3Score.getIdPair().getLeft());
		this.second  = docs.get(s3Score.getIdPair().getRight());
		this.firstHash = idToHash.get(s3Score.getIdPair().getLeft());
		this.secondHash = idToHash.get(s3Score.getIdPair().getRight());
	}
}
