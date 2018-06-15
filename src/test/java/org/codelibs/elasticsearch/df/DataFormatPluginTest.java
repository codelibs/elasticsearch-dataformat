package org.codelibs.elasticsearch.df;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.Charsets;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.codelibs.curl.CurlException;
import org.codelibs.curl.CurlRequest;
import org.codelibs.curl.CurlResponse;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.runner.net.EcrCurl;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

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

    private final Map<String, String> paramsCsv = new HashMap<>();
    private final Map<String, String> paramsXls = new HashMap<>();
    private final Map<String, String> paramsJson = new HashMap<>();

    static {
        docNumber = 20;

        csvTempFile = createTempFile("csvtest", ".csv");
        xlsTempFile = createTempFile("xlstest", ".xls");
        jsonTempFile = createTempFile("jsontest", ".json");
        path = "/dataset0/item0/_data";
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
                settingsBuilder.putList("discovery.zen.ping.unicast.hosts", "localhost:9301-9310");
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

    @After
    public void clearParams() {
        paramsCsv.clear();
        paramsXls.clear();
        paramsJson.clear();
    }

    @Test
    public void dumpCsvSimple() throws IOException {
        try (CurlResponse curlResponse = createRequest(node, path, paramsCsv).execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(docNumber + 1, lines.length);
            assertLineContains(lines[0], "\"aaa\"", "\"bbb\"", "\"ccc\"", "\"eee.fff\"", "\"eee.ggg\"", "\"eee.hhh\"");
        }
    }

    @Test
    public void dumpCsvWithDenotedFields() throws IOException {
        dumpCsvWithDenotedFields("fields_name");
    }

    @Test
    public void dumpCsvWithDenotedFieldsCompatible() throws IOException {
        dumpCsvWithDenotedFields("fl");
    }

    @Test
    public void dumpCsvWithoutHeader() throws IOException {
        paramsCsv.put("append.header", "false");
        try (CurlResponse response = createRequest(node, path, paramsCsv).execute()) {
            assertEquals(docNumber, response.getContentAsString().split("\n").length);
        }
    }

    @Test
    public void dumpCsvWithQuery() throws IOException {

        // Download 10 docs as CSV with Query
        final String query = "{\"query\":{\"bool\":{\"must\":[{\"range\":{\"bbb\":{\"from\":\"1\",\"to\":\"10\"}}}],\"must_not\":[],\"should\":[]}},\"sort\":[\"bbb\"]}";
        paramsCsv.put("search_type", "query_then_fetch");
        try (CurlResponse response = createRequest(node, path, paramsCsv).body(query).execute()) {
            String[] lines = response.getContentAsString().split("\n");
            assertEquals(11, lines.length);
            assertLineContains(lines[0], "\"aaa\"", "\"bbb\"", "\"ccc\"", "\"eee.fff\"", "\"eee.ggg\"");
            assertLineContains(lines[1], "\"1\"");
        }

        // Download 10 docs as CSV
        clearParams();
        prepareParams();
        paramsCsv.put("q", "*:*");
        paramsCsv.put("from", "5");
        try (CurlResponse response = createRequest(node, path, paramsCsv).execute()) {
            assertEquals(16, response.getContentAsString().split("\n").length);
        }

        // Download all the docs from the 5th as CSV
        try (CurlResponse curlResponse = EcrCurl.get(node, "/dataset0/item0/_data")
                .header("Content-Type", "application/json").param("q", "*:*")
                .param("format", "csv").param("from", "5")
                .param("size", String.valueOf(docNumber)).execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(docNumber - 5 + 1, lines.length);
        }

        final String queryWithFrom = "{\"query\":{\"match_all\":{}},\"from\":10,\"size\":" + String.valueOf(docNumber) + ",\"sort\":[\"bbb\"]}";

        // Download All as CSV with Query and from
        try (CurlResponse curlResponse = EcrCurl.get(node, "/dataset0/item0/_data")
                .header("Content-Type", "application/json")
                .param("format", "csv").body(queryWithFrom).execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(docNumber - 10 + 1, lines.length);
        }

        // Download All as CSV with Query and from
        try (CurlResponse curlResponse = EcrCurl.get(node, "/dataset0/item0/_data")
                .header("Content-Type", "application/json")
                .param("format", "csv").param("source", queryWithFrom)
                .param("source_content_type", "application/json")
                .execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(docNumber - 10 + 1, lines.length);
        }

        // Download All as CSV with search_type
        try (CurlResponse curlResponse = EcrCurl.get(node, "/dataset0/item0/_data")
                .header("Content-Type", "application/json")
                .param("search_type", "query_then_fetch").param("format", "csv")
                .execute()) {
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
    public void dumpCsvInFile() throws IOException {
        paramsCsv.put("file", csvTempFile.getAbsolutePath());

        // try-with-resources: java 7, ensure closing resources after try
        try (CurlResponse curlResponse = createRequest(node, path, paramsCsv).execute()) {
            assertAcknowledged(curlResponse, csvTempFile);
            final List<String> lines = Files.readAllLines(csvTempFile.toPath(), Charsets.UTF_8);
            assertEquals(docNumber + 1, lines.size());
            final String line = lines.get(0);
            assertLineContains(line, "\"aaa\"", "\"bbb\"", "\"ccc\"", "\"eee.fff\"", "\"eee.ggg\"", "\"eee.hhh\"");
        }
    }

    @Test
    public void dumpExcelSimple() throws IOException {
        try (CurlResponse response = createRequest(node, path, paramsXls).execute()) {
            try (InputStream is = response.getContentAsStream()) {
                final POIFSFileSystem fs = new POIFSFileSystem(is);
                final HSSFWorkbook book = new HSSFWorkbook(fs);
                final HSSFSheet sheet = book.getSheetAt(0);
                assertEquals(docNumber, sheet.getLastRowNum());
            }
        }
    }

    @Test
    public void dumpExcelWithDenotedFields() throws IOException {
        dumpExcelWithDenotedFields("fields_name");
    }

    @Test
    public void dumpExcelWithDenotedFieldsCompatible() throws IOException {
        dumpExcelWithDenotedFields("fl");
    }

    @Test
    public void dumpExcelWithoutHeader() throws IOException {
        paramsXls.put("append.header", "false");
        try (CurlResponse response = createRequest(node, path, paramsXls).execute()) {
            try (InputStream is = response.getContentAsStream()) {
                final POIFSFileSystem fs = new POIFSFileSystem(is);
                final HSSFWorkbook book = new HSSFWorkbook(fs);
                final HSSFSheet sheet = book.getSheetAt(0);
                assertEquals(docNumber, sheet.getLastRowNum() + 1); // row number begins at 0
            }
        }
    }

    @Test
    public void dumpExcel() throws IOException {

        final String query = "{\"query\":{\"bool\":{\"must\":[{\"range\":{\"bbb\":{\"from\":\"1\",\"to\":\"10\"}}}],\"must_not\":[],\"should\":[]}},\"sort\":[\"bbb\"]}";

        // Download 10 docs as Excel with Query
        try (CurlResponse curlResponse = EcrCurl.get(node, "/dataset0/item0/_data")
                .header("Content-Type", "application/json")
                .param("format", "xls").param("search_type", "query_then_fetch")
                .body(query).execute()) {
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
        try (CurlResponse curlResponse = EcrCurl.get(node, "/dataset0/item0/_data")
                .header("Content-Type", "application/json")
                .param("search_type", "query_then_fetch").param("format", "xls")
                .execute()) {
            try (InputStream is = curlResponse.getContentAsStream()) {
                final POIFSFileSystem fs = new POIFSFileSystem(is);
                final HSSFWorkbook book = new HSSFWorkbook(fs);
                final HSSFSheet sheet = book.getSheetAt(0);
                assertEquals(docNumber, sheet.getLastRowNum());
            }
        }
    }

    @Test
    public void dumpXlsInFile() throws IOException {
        paramsXls.put("file", xlsTempFile.getAbsolutePath());

        try (CurlResponse curlResponse = createRequest(node, path, paramsXls).execute()) {
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
        try (CurlResponse curlResponse = EcrCurl.get(node, "/dataset0/item0/_data")
                .header("Content-Type", "application/json")
                .param("format", "json").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(docNumber * 2, lines.length);
            assertTrue(lines[0].startsWith("{\"index\":{\"_index\":\"dataset0\",\"_type\":\"item0\","));
            assertTrue(lines[1].startsWith("{\"aaa\""));
        }

        // Download All as JSON with index/type
        try (CurlResponse curlResponse = EcrCurl.get(node, "/dataset0/item0/_data")
                .header("Content-Type", "application/json")
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
        try (CurlResponse curlResponse = EcrCurl.get(node, "/dataset0/item0/_data")
                .header("Content-Type", "application/json")
                .param("format", "json")
                .param("search_type", "query_then_fetch").body(query)
                .execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(20, lines.length);
            assertTrue(lines[0].startsWith("{\"index\""));
            assertTrue(lines[1].startsWith("{\"aaa\":\"test 1\","));
        }

        // Download 10 docs as JSON
        try (CurlResponse curlResponse = EcrCurl.get(node, "/dataset0/item0/_data")
                .header("Content-Type", "application/json").param("q", "*:*")
                .param("format", "json").param("from", "5").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(30, lines.length);
        }

        // Download all the docs from the 5th as JSON
        try (CurlResponse curlResponse = EcrCurl.get(node, "/dataset0/item0/_data")
                .header("Content-Type", "application/json").param("q", "*:*")
                .param("format", "json").param("from", "5")
                .param("size", String.valueOf(docNumber)).execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals((docNumber - 5) * 2, lines.length);
        }

        final String queryWithFrom = "{\"query\":{\"match_all\":{}},\"from\":5,\"size\":" + String.valueOf(docNumber) + ",\"sort\":[\"bbb\"]}";

        // Download All as JSON with Query and from
        try (CurlResponse curlResponse = EcrCurl.get(node, "/dataset0/item0/_data")
                .header("Content-Type", "application/json")
                .param("format", "json").body(queryWithFrom).execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals((docNumber - 5) * 2, lines.length);
        }

        // Download All as JSON with Query and from
        try (CurlResponse curlResponse = EcrCurl.get(node, "/dataset0/item0/_data")
                .header("Content-Type", "application/json")
                .param("format", "json").param("source", queryWithFrom)
                .param("source_content_type", "application/json")
                .execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals((docNumber - 5) * 2, lines.length);
        }

        // Download All as JSON with search_type
        try (CurlResponse curlResponse = EcrCurl.get(node, "/dataset0/item0/_data")
                .header("Content-Type", "application/json")
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
    public void dumpJsonInFile() throws IOException {
        paramsJson.put("file", jsonTempFile.getAbsolutePath());

        try (CurlResponse curlResponse = createRequest(node, path, paramsJson).execute()) {
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
        try (CurlResponse curlResponse = EcrCurl.get(node, "/dataset0/item0/_data")
                .header("Content-Type", "application/json")
                .param("format", "csv").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(docNumber + 1, lines.length);
        }

        // 50%
        try (CurlResponse curlResponse = EcrCurl.get(node, "/dataset0/item0/_data")
                .header("Content-Type", "application/json")
                .param("format", "csv").param("limit", "50%").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(docNumber + 1, lines.length);
        }

        //0%
        try (CurlResponse curlResponse = EcrCurl.get(node, "/dataset0/item0/_data")
                .header("Content-Type", "application/json")
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
            assertEquals(DocWriteResponse.Result.CREATED, indexResponse0.getResult());
        }
        runner.refresh();

        // search documents to verify
        SearchResponse searchResponse = runner.search(index0, type0, null, null, 0, 10);
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

    private CurlRequest createRequest(Node node, String path, Map<String, String> params) {
        CurlRequest request = EcrCurl.get(node, path).header("Content-Type", "application/json");
        for (final Map.Entry<String, String> entry : params.entrySet()) {
            request.param(entry.getKey(), entry.getValue());
        }
        return request;
    }

    private void assertAcknowledged(CurlResponse response, File file) {
        Map<String, Object> contentAsMap = response.getContent(EcrCurl.jsonParser);
        assertEquals("true", contentAsMap.get("acknowledged").toString());
        assertEquals(file.getName(),
                new File(contentAsMap.get("file").toString()).getName());
    }

    private void assertLineContains(String line, String... words) {
        for (String word : words) {
            assertTrue(line.contains(word));
        }
    }

    private void assertLineNotContains(String line, String... words) {
        for (String word : words) {
            assertFalse(line.contains(word));
        }
    }

    private void dumpCsvWithDenotedFields(String fields) throws IOException {
        paramsCsv.put(fields, "aaa, eee.ggg");
        try (CurlResponse response = createRequest(node, path, paramsCsv).execute()) {
            final String content = response.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(docNumber + 1, lines.length);
            assertLineContains(lines[0], "\"aaa\"", "\"eee.ggg\"");
            assertLineNotContains(lines[0], "\"bbb\"", "\"ccc\"", "\"eee.fff\"", "\"eee.hhh\"");
        }
    }

    private void dumpExcelWithDenotedFields(String fields) throws IOException {
        paramsXls.put(fields, "aaa, eee.ggg");
        try (CurlResponse response = createRequest(node, path, paramsXls).execute()) {
            try (InputStream is = response.getContentAsStream()) {
                final POIFSFileSystem fs = new POIFSFileSystem(is);
                final HSSFWorkbook book = new HSSFWorkbook(fs);
                final HSSFSheet sheet = book.getSheetAt(0);
                assertEquals(docNumber, sheet.getLastRowNum());
                final HSSFRow row = sheet.getRow(0);
                assertEquals("aaa", row.getCell(0).toString());
                assertEquals("eee.ggg", row.getCell(1).toString());
            }
        }
    }

}
