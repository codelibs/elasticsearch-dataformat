package org.codelibs.elasticsearch.df;

import org.apache.commons.codec.Charsets;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.runner.net.Curl;
import org.codelibs.elasticsearch.runner.net.CurlException;
import org.codelibs.elasticsearch.runner.net.CurlRequest;
import org.codelibs.elasticsearch.runner.net.CurlResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;
import static org.junit.Assert.*;

@RunWith(BlockJUnit4ClassRunner.class)
public class DataFormatPluginTest {

    private static ElasticsearchClusterRunner runner;

    private static String clusterName;

    private static int docNumber;

    private static Node node;

    private static final File csvTempFile;
    private static final File xlsTempFile;
    private static final File jsonTempFile;
    private static final String path;
    private static final String path_multitypes;

    private final Map<String, String> paramsCsv = new HashMap<>();
    private final Map<String, String> paramsXls = new HashMap<>();
    private final Map<String, String> paramsJson = new HashMap<>();

    static {
        docNumber = 20;

        csvTempFile = createTempFile("csvtest", ".csv");
        xlsTempFile = createTempFile("xlstest", ".xls");
        jsonTempFile = createTempFile("jsontest", ".json");
        path = "/dataset0/item0/_data";
        path_multitypes = "/dataset0/item0,item1/_data";
    }

    @BeforeClass
    public static void setUp() throws IOException {
        clusterName = "es-dataformat-" + System.currentTimeMillis();
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("http.cors.enabled", true);
                settingsBuilder.put("http.cors.allow-origin", "*");
                settingsBuilder.putArray("discovery.zen.ping.unicast.hosts", "localhost:9301-9310");
            }
        }).build(newConfigs().clusterName(clusterName).numOfNode(1)
                .pluginTypes("org.codelibs.elasticsearch.df.DataFormatPlugin"));

        // wait for yellow status
        runner.ensureYellow();

        indexing();

        node = runner.node();
    }

    @AfterClass
    public static void tearDown() throws IOException {
        // close runner
        runner.close();
        // delete all files
        runner.clean();
    }

    @Before
    public void prepareParams() {
        paramsCsv.put("format", "csv");
        paramsXls.put("format", "xls");
        paramsJson.put("format", "json");
    }

    @Test
    public void dumpCsv() throws IOException {

        // Download All as CSV
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("format", "csv").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(docNumber + 1, lines.length);
            System.out.println(lines[0]);
            assertTrue(lines[0].contains("\"aaa\""));
            assertTrue(lines[0].contains("\"bbb\""));
            assertTrue(lines[0].contains("\"ccc\""));
            assertTrue(lines[0].contains("\"eee.fff\""));
            assertTrue(lines[0].contains("\"eee.ggg\""));
            assertTrue(lines[0].contains("\"eee.hhh\""));
        }

        // Download All as CSV with Fields
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("format", "csv").param("fl", "aaa,eee.ggg").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(docNumber + 1, lines.length);
            assertTrue(lines[0].contains("\"aaa\""));
            assertFalse(lines[0].contains("\"bbb\""));
            assertFalse(lines[0].contains("\"ccc\""));
            assertFalse(lines[0].contains("\"eee.fff\""));
            assertTrue(lines[0].contains("\"eee.ggg\""));
            assertFalse(lines[0].contains("\"eee.hhh\""));
        }

        final String query = "{\"query\":{\"bool\":{\"must\":[{\"range\":{\"bbb\":{\"from\":\"1\",\"to\":\"10\"}}}],\"must_not\":[],\"should\":[]}},\"sort\":[\"bbb\"]}";

        // Download 10 docs as CSV with Query
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("format", "csv").param("search_type", "query_then_fetch").body(query).execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(11, lines.length);
            assertTrue(lines[0].contains("\"aaa\""));
            assertTrue(lines[0].contains("\"bbb\""));
            assertTrue(lines[0].contains("\"ccc\""));
            assertTrue(lines[0].contains("\"eee.fff\""));
            assertTrue(lines[0].contains("\"eee.ggg\""));
            assertTrue(lines[1].contains("\"1\""));
        }

        // Download 10 docs as CSV
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("q", "*:*").param("format", "csv").param("from", "5")
                .execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(11, lines.length);
        }

        // Download all the docs from the 5th as CSV
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("q", "*:*").param("format", "csv").param("from", "5")
                .param("size", String.valueOf(docNumber)).execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(docNumber - 5 + 1, lines.length);
        }

        final String queryWithFrom = "{\"query\":{\"match_all\":{}},\"from\":10,\"size\":" + String.valueOf(docNumber) + ",\"sort\":[\"bbb\"]}";

        // Download All as CSV with Query and from
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("format", "csv").body(queryWithFrom).execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(docNumber - 10 + 1, lines.length);
        }

        // Download All as CSV with Query and from
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("format", "csv").param("source", queryWithFrom)
                .execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(docNumber - 10 + 1, lines.length);
        }

        // Download All as CSV with search_type
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("search_type", "query_then_fetch")
                .param("format", "csv").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(docNumber + 1, lines.length);
            assertTrue(lines[0].contains("\"aaa\""));
            assertTrue(lines[0].contains("\"bbb\""));
            assertTrue(lines[0].contains("\"ccc\""));
            assertTrue(lines[0].contains("\"eee.fff\""));
            assertTrue(lines[0].contains("\"eee.ggg\""));
            assertTrue(lines[0].contains("\"eee.hhh\""));
        }
    }

    @Test
    public void dumpCsvWithFile() throws IOException {
        paramsCsv.put("file", csvTempFile.getAbsolutePath());

        // try-with-resources: java 7, ensure closing resources after try
        try (CurlResponse curlResponse = sendRequest(node, path, paramsCsv)) {
            assertAcknowledged(curlResponse, csvTempFile);
            final List<String> lines = Files.readAllLines(csvTempFile.toPath(), Charsets.UTF_8);
            assertEquals(docNumber + 1, lines.size());
            final String line = lines.get(0);
            assertLineContains(line, "\"aaa\"", "\"bbb\"", "\"ccc\"", "\"eee.fff\"", "\"eee.ggg\"", "\"eee.hhh\"");
        }
    }

    @Test
    public void dumpExcel() throws IOException {

        // Download All as Excel
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("format", "xls").execute()) {
            try (InputStream is = curlResponse.getContentAsStream()) {
                final POIFSFileSystem fs = new POIFSFileSystem(is);
                final HSSFWorkbook book = new HSSFWorkbook(fs);
                final HSSFSheet sheet = book.getSheetAt(0);
                assertEquals(docNumber, sheet.getLastRowNum());
            }
        }

        // Download All as Excel with Fields
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("format", "xls").param("fl", "aaa,eee.ggg").execute()) {
            try (InputStream is = curlResponse.getContentAsStream()) {
                final POIFSFileSystem fs = new POIFSFileSystem(is);
                final HSSFWorkbook book = new HSSFWorkbook(fs);
                final HSSFSheet sheet = book.getSheetAt(0);
                assertEquals(docNumber, sheet.getLastRowNum());
                final HSSFRow row = sheet.getRow(0);
                assertEquals("aaa", row.getCell(0).toString());
                assertEquals("eee.ggg", row.getCell(1).toString());
            }
        }

        final String query = "{\"query\":{\"bool\":{\"must\":[{\"range\":{\"bbb\":{\"from\":\"1\",\"to\":\"10\"}}}],\"must_not\":[],\"should\":[]}},\"sort\":[\"bbb\"]}";

        // Download 10 docs as Excel with Query
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("format", "xls").param("search_type", "query_then_fetch").body(query).execute()) {
            try (InputStream is = curlResponse.getContentAsStream()) {
                final POIFSFileSystem fs = new POIFSFileSystem(is);
                final HSSFWorkbook book = new HSSFWorkbook(fs);
                final HSSFSheet sheet = book.getSheetAt(0);
                assertEquals(10, sheet.getLastRowNum());
                assertEquals(6, sheet.getRow(0).getLastCellNum());
                assertEquals(6, sheet.getRow(1).getLastCellNum());
            }
        }

        // Download All as Excel with search_type
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("search_type", "query_then_fetch")
                .param("format", "xls").execute()) {
            try (InputStream is = curlResponse.getContentAsStream()) {
                final POIFSFileSystem fs = new POIFSFileSystem(is);
                final HSSFWorkbook book = new HSSFWorkbook(fs);
                final HSSFSheet sheet = book.getSheetAt(0);
                assertEquals(docNumber, sheet.getLastRowNum());
            }
        }
    }

    @Test
    public void dumpCsvMultitypesWithFile() throws IOException {
        paramsCsv.put("file", csvTempFile.getAbsolutePath());

        try (CurlResponse response = sendRequest(node, path_multitypes, paramsCsv)) {
            assertAcknowledged(response, csvTempFile);
            final List<String> lines = Files.readAllLines(csvTempFile.toPath());
            assertEquals(docNumber * 2 + 1, lines.size());
            final String header = lines.get(0);
            assertLineContains(header, "\"aaa\"", "\"bbb\"", "\"ccc\"", "\"eee.fff\"", "\"eee.ggg\"", "\"eee.hhh\"", "\"nnn\"");
            final String firstLine = lines.get(1);
            assertEquals(6, firstLine.split(",").length);
        }
    }

    @Test
    public void dumpXlsWithFile() throws IOException {
        paramsXls.put("file", xlsTempFile.getAbsolutePath());

        try (CurlResponse curlResponse = sendRequest(node, path, paramsXls)) {
            assertAcknowledged(curlResponse, xlsTempFile);
            try (InputStream is = new FileInputStream(xlsTempFile)) {
                final POIFSFileSystem fs = new POIFSFileSystem(is);
                final HSSFWorkbook book = new HSSFWorkbook(fs);
                final HSSFSheet sheet = book.getSheetAt(0);
                assertEquals(docNumber, sheet.getLastRowNum());
            }
        }
    }

    @Test
    public void dumpJson() throws IOException {

        // Download All as JSON
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("format", "json").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(docNumber * 2, lines.length);
            assertTrue(lines[0].startsWith("{\"index\":{\"_index\":\"dataset0\",\"_type\":\"item0\","));
            assertTrue(lines[1].startsWith("{\"aaa\""));
        }

        // Download All as JSON with index/type
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("format", "json").param("bulk.index", "dataset02")
                .param("bulk.type", "item02").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(docNumber * 2, lines.length);
            assertTrue(lines[0].startsWith("{\"index\":{\"_index\":\"dataset02\",\"_type\":\"item02\","));
            assertTrue(lines[1].startsWith("{\"aaa\""));
        }

        final String query = "{\"query\":{\"bool\":{\"must\":[{\"range\":{\"bbb\":{\"from\":\"1\",\"to\":\"10\"}}}],\"must_not\":[],\"should\":[]}},\"sort\":[\"bbb\"]}";

        // Download 10 docs as JSON with Query
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("format", "json").param("search_type", "query_then_fetch").body(query).execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(20, lines.length);
            assertTrue(lines[0].startsWith("{\"index\""));
            assertTrue(lines[1].startsWith("{\"aaa\":\"test 1\","));
        }

        // Download 10 docs as JSON
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("q", "*:*").param("format", "json").param("from", "5")
                .execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(20, lines.length);
        }

        // Download all the docs from the 5th as JSON
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("q", "*:*").param("format", "json").param("from", "5")
                .param("size", String.valueOf(docNumber)).execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals((docNumber - 5) * 2, lines.length);
        }

        final String queryWithFrom = "{\"query\":{\"match_all\":{}},\"from\":5,\"size\":" + String.valueOf(docNumber) + ",\"sort\":[\"bbb\"]}";

        // Download All as JSON with Query and from
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("format", "json").body(queryWithFrom).execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals((docNumber - 5) * 2, lines.length);
        }

        // Download All as JSON with Query and from
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("format", "json").param("source", queryWithFrom)
                .execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals((docNumber - 5) * 2, lines.length);
        }

        // Download All as JSON with search_type
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("search_type", "query_then_fetch")
                .param("format", "json").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(docNumber * 2, lines.length);
            assertTrue(lines[0].startsWith("{\"index\""));
            assertTrue(lines[1].startsWith("{\"aaa\""));
        }
    }

    @Test
    public void dumpJsonWithFile() throws IOException {
        paramsJson.put("file", jsonTempFile.getAbsolutePath());

        try (CurlResponse curlResponse = sendRequest(node, path, paramsJson)) {
            assertAcknowledged(curlResponse, jsonTempFile);
            final List<String> lines = Files.readAllLines(jsonTempFile.toPath(), Charsets.UTF_8);
            assertEquals(docNumber * 2, lines.size());
            assertTrue(lines.get(0).startsWith(
                    "{\"index\":{\"_index\":\"dataset0\",\"_type\":\"item0\","));
            assertTrue(lines.get(1).startsWith("{\"aaa\""));
        }
    }

    @Test
    public void dumpSizeLimit() throws IOException {

        // Default
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("format", "csv").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(docNumber + 1, lines.length);
        }

        // 50%
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("format", "csv").param("limit", "50%").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(docNumber + 1, lines.length);
        }

        //0%
        try (CurlResponse curlResponse = Curl.get(node, "/dataset0/item0/_data")
                .param("format", "csv").param("limit", "0").execute()) {
            curlResponse.getContentAsString();
            Assert.fail();
        } catch (CurlException e) {
            assertTrue(true);
        }
    }

    private static void indexing() {
        final String index0 = "dataset0";
        final String type0 = "item0";
        final String type1 = "item1";

        // create an index
        runner.createIndex(index0, (Settings) null);

        if (!runner.indexExists(index0)) {
            Assert.fail();
        }

        // create documents
        for (int i = 1; i <= docNumber; i++) {
            final IndexResponse indexResponse0 = runner.insert(index0, type0, String.valueOf(i),
                    "{" +
                            "\"aaa\":\"test " + i + "\"," +
                            "\"bbb\":" + i + "," +
                            "\"ccc\":\"2012-01-01:00:00.000Z\"," +
                            "\"eee\":{\"fff\":\"TEST " + i + "\", \"ggg\":" + i + ", \"hhh\":\"2013-01-01:00:00.000Z\"}" +
                            "}");
            final IndexResponse indexResponse1 = runner.insert(index0, type1, String.valueOf(i),
                    "{\"nnn\":" + i + "}");
            assertEquals(DocWriteResponse.Result.CREATED, indexResponse1.getResult());
            assertEquals(DocWriteResponse.Result.CREATED, indexResponse0.getResult());
        }
        runner.refresh();

        // search documents to verify
        SearchResponse searchResponse = runner.search(index0, type0, null, null, 0, 10);
        assertEquals(docNumber, searchResponse.getHits().getTotalHits());
        searchResponse = runner.search(index0, type1, null, null, 0, 10);
        assertEquals(docNumber, searchResponse.getHits().getTotalHits());
    }

    private static File createTempFile(String prefix, String suffix) {
        File file = null;
        try {
            file = Files.createTempFile(prefix, suffix).toFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // request deletion of created file when jvm terminates.
        file.deleteOnExit();
        return file;
    }

    private CurlResponse sendRequest(Node node, String path, Map<String, String> params) {
        CurlRequest request = Curl.get(node, path);
        for (final Map.Entry<String, String> entry : params.entrySet()) {
            request.param(entry.getKey(), entry.getValue());
        }
        return request.execute();
    }

    private void assertAcknowledged(CurlResponse response, File file) {
        Map<String, Object> contentAsMap = response.getContentAsMap();
        assertEquals("true", contentAsMap.get("acknowledged").toString());
        assertEquals(file.getName(),
                new File(contentAsMap.get("file").toString()).getName());
    }

    private void assertLineContains(String line, String... words) {
        for (String word : words) {
            assertTrue(line.contains(word));
        }
    }
}
