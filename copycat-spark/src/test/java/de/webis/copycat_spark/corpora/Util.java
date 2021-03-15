package de.webis.copycat_spark.corpora;

import java.util.List;

import de.webis.trec_ndd.trec_collections.CollectionConfiguration;
import de.webis.trec_ndd.trec_collections.SharedTask;
import de.webis.trec_ndd.trec_collections.CollectionConfiguration.TrecCollections;
import lombok.experimental.UtilityClass;

@UtilityClass
class Util {
	static CollectionConfiguration collectionConfigurationWithSharedTasks(String pathToCollection, TrecCollections collection) {
		return collectionConfigurationWithSharedTasks(pathToCollection, null, collection);
	}
	
	@SuppressWarnings("serial")
	static CollectionConfiguration collectionConfigurationWithSharedTasks(String pathToCollection, List<SharedTask> sharedTasks, TrecCollections collection) {
		return new CollectionConfiguration() {
			@Override
			public String getPathToCollection() {
				return pathToCollection;
			}

			@Override
			public String getCollectionType() {
				return collection.getCollectionType();
			}

			@Override
			public String getDocumentGenerator() {
				return collection.getDocumentGenerator();
			}

			@Override
			public List<SharedTask> getSharedTasks() {
				return sharedTasks;
			}
		};
	}
}
