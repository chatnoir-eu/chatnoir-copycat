package de.webis.copycat_cli.doc_resolver;

import java.util.stream.Collectors;

import org.approvaltests.Approvals;
import org.junit.Test;

public class IrDatasetsDocumentResolverTest {
	@Test
	public void testJsonlInputForMsMarcoCreatedWithIrDatasets() {
		IrDatasetsDocumentResolver resolver = new IrDatasetsDocumentResolver("src/test/resources/example-ir-datasets/ms-marco-v1.jsonl");
		Approvals.verifyAsJson(resolver.allDocuments().collect(Collectors.toList()));
	}
}
