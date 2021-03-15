package de.webis.copycat_spark.app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import de.webis.copycat_spark.app.InjectRawDocuments;
import de.webis.copycat_spark.spark.SparkIntegrationTestBase;

public class InjectRawDocumentsIntegrationTest extends SparkIntegrationTestBase {
	@Test
	@Ignore
	public void testWithoutUUids() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(
			"{\"uuid\": [], \"index\": [], \"query\": \"What a great idea At Foursquare Venues The Mayor Eats For Free\", \"id\": 10943528}"
		));
		List<String> expected = Arrays.asList(
			"{\"query\":\"What a great idea At Foursquare Venues The Mayor Eats For Free\",\"index\":[],\"id\":10943528,\"uuid\":[]}"
		);
		List<String> actual = injectRawDocuments(input);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	@Ignore
	public void testWithUUids() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(
			"{\"uuid\": [\"a18bea8a-9ec0-536f-88fe-57da10a52737\", \"d58c97cf-c535-507b-9452-fca8e8cf89d6\", \"db12be9f-d4e2-5acc-9414-0682844e1786\", \"4a26168b-1235-5603-bf24-598f302bc51a\", \"22cfeecc-c33b-576f-bad7-558f45342752\"], \"index\": [\"cw09\", \"cw09\", \"cw09\", \"cw09\", \"cw09\"], \"query\": \"Great that books are being invested in but what about the primaries RT free books to sec schools\", \"id\": 10943770}"
		));
		List<String> actual = injectRawDocuments(input);
		
		Approvals.verifyAsJson(actual);
	}

	private List<String> injectRawDocuments(JavaRDD<String> input) {
		List<String> ret = new ArrayList<>(InjectRawDocuments.injectRawDocuments(input).collect());
		Collections.sort(ret);
		
		return ret;
	}
}
