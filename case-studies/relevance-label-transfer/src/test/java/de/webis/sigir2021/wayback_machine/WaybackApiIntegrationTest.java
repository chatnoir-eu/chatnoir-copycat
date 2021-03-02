package de.webis.sigir2021.wayback_machine;

//import java.io.File;
//import java.io.FileInputStream;
//import java.io.InputStream;
//import java.net.URI;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Map;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.zip.GZIPInputStream;
//
//import org.apache.commons.io.FileUtils;
//import org.apache.commons.io.IOUtils;
//import org.apache.commons.lang.StringUtils;
//import org.approvaltests.Approvals;
//import org.archive.io.warc.WARCRecordInfo;
//import org.archive.io.warc.WARCWriter;
//import org.archive.io.warc.WARCWriterPoolSettings;
//import org.archive.io.warc.WARCWriterPoolSettingsData;
//import org.archive.uid.RecordIDGenerator;
//import org.junit.Test;
//
//import com.google.common.io.Files;
//
//import lombok.SneakyThrows;

public class WaybackApiIntegrationTest {
//	@Test
//	@SneakyThrows
//	public void testCreationOfWarcFileFromSingleURL() {
//		String docId = "my-doc-id";
//		String recordToCrawl = "org,wikipedia,en)/wiki/ruth_ndesandjo 20120315144017 http://en.wikipedia.org:80/wiki/Ruth_Ndesandjo text/html 200 6GYMDNUDRFTIPSHSPIYIOPJAGODHSZXA 71883";
//		WARCRecordInfo record = WaybackApi.warcRecord(docId, recordToCrawl);
//		File dataFile = writeHeaderToRecord(record);
//		String actual = readWarcContent(dataFile);
//		
//		FileUtils.deleteDirectory(dataFile);
//		
//		Approvals.verify(actual);
//	}
//	
//	@Test
//	@SneakyThrows
//	public void testCreationOfWarcFileFromSingleURL2() {
//		String docId = "my-doc-id";
//		String recordToCrawl = "org,wikipedia,en)/wiki/biloxi,%20mississippi 20120111011258 http://en.wikipedia.org/wiki/Biloxi,_Mississippi text/html 301 3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ 395";
//		WARCRecordInfo record = WaybackApi.warcRecord(docId, recordToCrawl);
//		File dataFile = writeHeaderToRecord(record);
//		String actual = readWarcContent(dataFile);
//		
//		FileUtils.deleteDirectory(dataFile);
//		
//		Approvals.verify(actual);
//	}
//	
//	@SneakyThrows
//	private static String readWarcContent(File f) {
//		InputStream is = new FileInputStream(f.toPath().resolve("test.warc.gz").toFile());
//		is = new GZIPInputStream(is);
//		
//		String ret = IOUtils.toString(is);
//		return StringUtils.substringAfter(StringUtils.substringBefore(ret, "playback timings (ms):"), "DOCTYPE");
//	}
//	
//	@SneakyThrows
//	private static File writeHeaderToRecord(WARCRecordInfo record) {
//		File ret = Files.createTempDir();
//		ret.mkdirs();
//		
//		WARCWriter writer = writer(ret);
//		writer.writeRecord(record);
//		writer.close();
//		
//		return ret;
//	}
//	
//	private static WARCWriter writer(File f) {
//		AtomicInteger serialNo = new AtomicInteger(0);
//		WARCWriterPoolSettings settings = warcSettings(f);
//		
//		return new WARCWriter(serialNo, settings);
//	}
//	
//	private static WARCWriterPoolSettings warcSettings(File f) {
//		return new WARCWriterPoolSettingsData(
//			"",
//			"test",
//			1024*1024*100,
//			true,
//			Arrays.asList(f),
//			new ArrayList<>(),
//			new RecordIDGenerator() {
//
//				@Override
//				@SneakyThrows
//				public URI getRecordID() {
//					return new URI("http://google.de/1");
//				}
//
//				@Override
//				public URI getQualifiedRecordID(Map<String, String> qualifiers) {
//					return getRecordID();
//				}
//
//				@Override
//				public URI getQualifiedRecordID(String key, String value) {
//					return getRecordID();
//				}
//
//				@Override
//				public URI qualifyRecordID(URI recordId, Map<String, String> qualifiers) {
//					return getRecordID();
//				}
//				
//			}
//		);
//	}
}
