package org.codelibs.elasticsearch.df;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.runner.net.Curl;
import org.codelibs.elasticsearch.runner.net.CurlResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;

import com.google.common.base.Charsets;

import junit.framework.TestCase;

public class DataFormatPluginTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    private String clusterName;

    @Override
    protected void setUp() throws Exception {
        clusterName = "es-dataformat-" + System.currentTimeMillis();
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("http.cors.enabled", true);
                settingsBuilder.put("http.cors.allow-origin", "*");
                settingsBuilder.put("index.number_of_shards", 3);
                settingsBuilder.put("index.number_of_replicas", 0);
                settingsBuilder.putArray("discovery.zen.ping.unicast.hosts", "localhost:9301-9310");
                settingsBuilder.put("plugin.types", "org.codelibs.elasticsearch.df.DataFormatPlugin");
                settingsBuilder.put("index.unassigned.node_left.delayed_timeout","0");
            }
        }).build(newConfigs().clusterName(clusterName).numOfNode(1));

        // wait for yellow status
        runner.ensureYellow();
    }

    @Override
    protected void tearDown() throws Exception {
        // close runner
        runner.close();
        // delete all files
        runner.clean();
    }

    public void test_runCluster() throws Exception {

        final String index = "dataset";
        final String type = "item";

        // create an index
        runner.createIndex(index, null);

        if (!runner.indexExists(index)) {
            fail();
        }

        // create 1000 documents
        for (int i = 1; i <= 1000; i++) {
            final IndexResponse indexResponse1 = runner
                    .insert(index,
                            type,
                            String.valueOf(i),
                            "{\"aaa\":\"test "
                                    + i
                                    + "\", \"bbb\":"
                                    + i
                                    + ", \"ccc\":\"2012-01-01:00:00.000Z\", \"eee\":{\"fff\":\"TEST "
                                    + i + "\", \"ggg\":" + i
                                    + ", \"hhh\":\"2013-01-01:00:00.000Z\"}}");
            assertTrue(indexResponse1.isCreated());
        }
        runner.refresh();

        // search 1000 documents
        {
            final SearchResponse searchResponse = runner.search(index, type,
                    null, null, 0, 10);
            assertEquals(1000, searchResponse.getHits().getTotalHits());
        }

        assertFile();
        assertCsvDownload();
        assertJsonDownload();
        assertExcelDownload();
    }

    private void assertFile() throws IOException {
        final Node node = runner.node();

        File csvTempFile = File.createTempFile("dftest", ".csv");
        csvTempFile.deleteOnExit();
        // Download All as CSV to file
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("format", "csv")
                .param("file", csvTempFile.getAbsolutePath()).execute()) {
            Map<String, Object> contentAsMap = curlResponse.getContentAsMap();
            assertEquals("true", contentAsMap.get("acknowledged").toString());
            assertEquals(csvTempFile.getName(),
                    new File(contentAsMap.get("file").toString()).getName());
            final List<String> lines = Files.readAllLines(csvTempFile.toPath(), Charsets.UTF_8);
            assertEquals(1001, lines.size());
            final String line = lines.get(0);
            assertTrue(line.contains("\"aaa\""));
            assertTrue(line.contains("\"bbb\""));
            assertTrue(line.contains("\"ccc\""));
            assertTrue(line.contains("\"eee.fff\""));
            assertTrue(line.contains("\"eee.ggg\""));
            assertTrue(line.contains("\"eee.hhh\""));
        }

        File xlsTempFile = File.createTempFile("dftest", ".xls");
        xlsTempFile.deleteOnExit();
        // Download All as Excel to file
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("format", "xls")
                .param("file", xlsTempFile.getAbsolutePath()).execute()) {
            Map<String, Object> contentAsMap = curlResponse.getContentAsMap();
            assertEquals("true", contentAsMap.get("acknowledged").toString());
            assertEquals(xlsTempFile.getName(),
                    new File(contentAsMap.get("file").toString()).getName());
            try (InputStream is = new FileInputStream(xlsTempFile)) {
                final POIFSFileSystem fs = new POIFSFileSystem(is);
                final HSSFWorkbook book = new HSSFWorkbook(fs);
                final HSSFSheet sheet = book.getSheetAt(0);
                assertEquals(1000, sheet.getLastRowNum());
            }
        }

        File jsonTempFile = File.createTempFile("dftest", ".json");
        jsonTempFile.deleteOnExit();
        // Download All as JSON to file
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("format", "json")
                .param("file", jsonTempFile.getAbsolutePath()).execute()) {
            Map<String, Object> contentAsMap = curlResponse.getContentAsMap();
            assertEquals("true", contentAsMap.get("acknowledged").toString());
            assertEquals(jsonTempFile.getName(),
                    new File(contentAsMap.get("file").toString()).getName());
            final List<String> lines = Files
                    .readAllLines(jsonTempFile.toPath(), Charsets.UTF_8);
            assertEquals(2000, lines.size());
            assertTrue(lines.get(0).startsWith(
                    "{\"index\":{\"_index\":\"dataset\",\"_type\":\"item\","));
            assertTrue(lines.get(1).startsWith("{\"aaa\""));
        }
    }

    private void assertCsvDownload() throws IOException {
        final Node node = runner.node();

        // Download All as CSV
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("format", "csv").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(1001, lines.length);
            assertTrue(lines[0].contains("\"aaa\""));
            assertTrue(lines[0].contains("\"bbb\""));
            assertTrue(lines[0].contains("\"ccc\""));
            assertTrue(lines[0].contains("\"eee.fff\""));
            assertTrue(lines[0].contains("\"eee.ggg\""));
            assertTrue(lines[0].contains("\"eee.hhh\""));
        }

        // Download All as CSV with Fields
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("format", "csv").param("fl", "aaa,eee.ggg").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(1001, lines.length);
            assertTrue(lines[0].contains("\"aaa\""));
            assertFalse(lines[0].contains("\"bbb\""));
            assertFalse(lines[0].contains("\"ccc\""));
            assertFalse(lines[0].contains("\"eee.fff\""));
            assertTrue(lines[0].contains("\"eee.ggg\""));
            assertFalse(lines[0].contains("\"eee.hhh\""));
        }

        final String query = "{\"query\":{\"bool\":{\"must\":[{\"range\":{\"bbb\":{\"from\":\"100\",\"to\":\"199\"}}}],\"must_not\":[],\"should\":[]}},\"sort\":[\"bbb\"]}";

        // Download 100 docs as CSV with Query
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("format", "csv").param("search_type", "query_then_fetch").body(query).execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(101, lines.length);
            assertTrue(lines[0].contains("\"aaa\""));
            assertTrue(lines[0].contains("\"bbb\""));
            assertTrue(lines[0].contains("\"ccc\""));
            assertTrue(lines[0].contains("\"eee.fff\""));
            assertTrue(lines[0].contains("\"eee.ggg\""));
            assertTrue(lines[1].contains("\"100\""));
        }

        // Download 10 docs as CSV
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("q", "*:*").param("format", "csv").param("from", "500")
                .execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(11, lines.length);
        }

        // Download 500 docs as CSV
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("q", "*:*").param("format", "csv").param("from", "500")
                .param("size", "1000").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(501, lines.length);
        }

        final String queryWithFrom = "{\"query\":{\"match_all\":{}},\"from\":900,\"size\":500,\"sort\":[\"bbb\"]}";

        // Download All as CSV with Query and from
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("format", "csv").body(queryWithFrom).execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(101, lines.length);
        }

        // Download All as CSV with Query and from
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("format", "csv").param("source", queryWithFrom)
                .execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(101, lines.length);
        }

        // Download All as CSV with search_type
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("search_type", "query_then_fetch")
                .param("format", "csv").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(1001, lines.length);
            assertTrue(lines[0].contains("\"aaa\""));
            assertTrue(lines[0].contains("\"bbb\""));
            assertTrue(lines[0].contains("\"ccc\""));
            assertTrue(lines[0].contains("\"eee.fff\""));
            assertTrue(lines[0].contains("\"eee.ggg\""));
            assertTrue(lines[0].contains("\"eee.hhh\""));
        }
    }

    private void assertExcelDownload() throws IOException {
        final Node node = runner.node();

        // Download All as Excel
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("format", "xls").execute()) {
            try (InputStream is = curlResponse.getContentAsStream()) {
                final POIFSFileSystem fs = new POIFSFileSystem(is);
                final HSSFWorkbook book = new HSSFWorkbook(fs);
                final HSSFSheet sheet = book.getSheetAt(0);
                assertEquals(1000, sheet.getLastRowNum());
            }
        }

        // Download All as Excel with Fields
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("format", "xls").param("fl", "aaa,eee.ggg").execute()) {
            try (InputStream is = curlResponse.getContentAsStream()) {
                final POIFSFileSystem fs = new POIFSFileSystem(is);
                final HSSFWorkbook book = new HSSFWorkbook(fs);
                final HSSFSheet sheet = book.getSheetAt(0);
                assertEquals(1000, sheet.getLastRowNum());
                final HSSFRow row = sheet.getRow(0);
                assertEquals("aaa", row.getCell(0).toString());
                assertEquals("eee.ggg", row.getCell(1).toString());
            }
        }

        final String query = "{\"query\":{\"bool\":{\"must\":[{\"range\":{\"bbb\":{\"from\":\"100\",\"to\":\"199\"}}}],\"must_not\":[],\"should\":[]}},\"sort\":[\"bbb\"]}";

        // Download 100 docs as Excel with Query
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("format", "xls").param("search_type", "query_then_fetch").body(query).execute()) {
            try (InputStream is = curlResponse.getContentAsStream()) {
                final POIFSFileSystem fs = new POIFSFileSystem(is);
                final HSSFWorkbook book = new HSSFWorkbook(fs);
                final HSSFSheet sheet = book.getSheetAt(0);
                assertEquals(100, sheet.getLastRowNum());
                assertEquals(6, sheet.getRow(0).getLastCellNum());
                assertEquals(6, sheet.getRow(1).getLastCellNum());
            }
        }

        // Download All as Excel with search_type
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("search_type", "query_then_fetch")
                .param("format", "xls").execute()) {
            try (InputStream is = curlResponse.getContentAsStream()) {
                final POIFSFileSystem fs = new POIFSFileSystem(is);
                final HSSFWorkbook book = new HSSFWorkbook(fs);
                final HSSFSheet sheet = book.getSheetAt(0);
                assertEquals(1000, sheet.getLastRowNum());
            }
        }
    }

    private void assertJsonDownload() throws IOException {
        final Node node = runner.node();

        // Download All as JSON
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("format", "json").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(2000, lines.length);
            assertTrue(lines[0].startsWith("{\"index\":{\"_index\":\"dataset\",\"_type\":\"item\","));
            assertTrue(lines[1].startsWith("{\"aaa\""));
        }

        // Download All as JSON with index/type
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("format", "json").param("bulk.index", "dataset2")
                .param("bulk.type", "item2").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(2000, lines.length);
            assertTrue(lines[0].startsWith("{\"index\":{\"_index\":\"dataset2\",\"_type\":\"item2\","));
            assertTrue(lines[1].startsWith("{\"aaa\""));
        }

        // Download All as JSON with Fields
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("format", "json").param("fl", "aaa,eee.ggg").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(2000, lines.length);
            assertTrue(lines[0].startsWith("{\"index\"")); // TODO
            assertTrue(lines[1].startsWith("{\"aaa\"")); // TODO
        }

        final String query = "{\"query\":{\"bool\":{\"must\":[{\"range\":{\"bbb\":{\"from\":\"100\",\"to\":\"199\"}}}],\"must_not\":[],\"should\":[]}},\"sort\":[\"bbb\"]}";

        // Download 100 docs as JSON with Query
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("format", "json").param("search_type", "query_then_fetch").body(query).execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(200, lines.length);
            assertTrue(lines[0].startsWith("{\"index\""));
            assertTrue(lines[1].startsWith("{\"aaa\":\"test 100\","));
        }

        // Download 10 docs as JSON
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("q", "*:*").param("format", "json").param("from", "500")
                .execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(20, lines.length);
        }

        // Download 500 docs as JSON
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("q", "*:*").param("format", "json").param("from", "500")
                .param("size", "1000").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(1000, lines.length);
        }

        final String queryWithFrom = "{\"query\":{\"match_all\":{}},\"from\":900,\"size\":500,\"sort\":[\"bbb\"]}";

        // Download All as JSON with Query and from
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("format", "json").body(queryWithFrom).execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(200, lines.length);
        }

        // Download All as JSON with Query and from
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("format", "json").param("source", queryWithFrom)
                .execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(200, lines.length);
        }

        // Download All as JSON with search_type
        try (CurlResponse curlResponse = Curl.get(node, "/dataset/item/_data")
                .param("search_type", "query_then_fetch")
                .param("format", "json").execute()) {
            final String content = curlResponse.getContentAsString();
            final String[] lines = content.split("\n");
            assertEquals(2000, lines.length);
            assertTrue(lines[0].startsWith("{\"index\""));
            assertTrue(lines[1].startsWith("{\"aaa\""));
        }
    }

}
