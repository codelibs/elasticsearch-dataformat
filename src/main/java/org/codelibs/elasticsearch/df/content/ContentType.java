package org.codelibs.elasticsearch.df.content;

import org.codelibs.elasticsearch.df.content.csv.CsvContent;
import org.codelibs.elasticsearch.df.content.json.JsonContent;
import org.codelibs.elasticsearch.df.content.xls.XlsContent;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;

public enum ContentType {
    CSV(10) {
        @Override
        public String contentType() {
            return "text/csv";
        }

        @Override
        public DataContent dataContent(final Client client,
                final RestRequest request, final RestChannel channel) {
            return new CsvContent(client, request, channel);
        }

        @Override
        public String fileName(final RestRequest request) {        	
        	final String index = request.param("index");
            if (index == null) {
                return "_all.csv";
            }
            final String type = request.param("type");
            if (type == null) {
                return index + ".csv";
            }
            return index + "_" + type + ".csv";
        }
    },
    EXCEL(20) {
        @Override
        public String contentType() {
            return "application/vnd.ms-excel";
        }

        @Override
        public DataContent dataContent(final Client client,
                final RestRequest request, final RestChannel channel) {
            return new XlsContent(client, request, channel);
        }

        @Override
        public String fileName(final RestRequest request) {     	
        	final String index = request.param("index");
            if (index == null) {
                return "_all.xls";
            }
            final String type = request.param("type");
            if (type == null) {
                return index + ".xls";
            }
            return index + "_" + type + ".xls";
        }
    },
    JSON(30) {
        @Override
        public String contentType() {
            return "application/json";
        }

        @Override
        public DataContent dataContent(final Client client,
                final RestRequest request, final RestChannel channel) {
            return new JsonContent(client, request, channel);
        }

        @Override
        public String fileName(final RestRequest request) {          
        	final String index = request.param("index");
            if (index == null) {
                return "_all.json";
            }
            final String type = request.param("type");
            if (type == null) {
                return index + ".json";
            }
            return index + "_" + type + ".json";
        }
    },
    EXCEL2007(40) {
        @Override
        public String contentType() {
            return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
        }

        @Override
        public DataContent dataContent(final Client client,
                final RestRequest request, final RestChannel channel) {
            return new XlsContent(client, request, channel, true);
        }

        @Override
        public String fileName(final RestRequest request) {  	
        	final String index = request.param("index");
            if (index == null) {
                return "_all.xlsx";
            }
            final String type = request.param("type");
            if (type == null) {
                return index + ".xlsx";
            }
            return index + "_" + type + ".xlsx";
        }
    };

    private int index;

    ContentType(final int index) {
        this.index = index;
    }

    public int index() {
        return index;
    }

    public abstract String contentType();

    public abstract String fileName(RestRequest request);

    public abstract DataContent dataContent(Client client, RestRequest request,
            RestChannel channel);
}
