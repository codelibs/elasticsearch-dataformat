package org.codelibs.elasticsearch.df.content;

import org.codelibs.elasticsearch.df.content.csv.CsvContent;
import org.codelibs.elasticsearch.df.content.json.JsonContent;
import org.codelibs.elasticsearch.df.content.xls.XlsContent;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.RestRequest;

/**
 * ContentType is a factory interface with 4 enumerated values
 */
public enum ContentType {

    // these variables are value of this enum type ContentType
    // they are by default public static final
    CSV(10) {
        @Override
        public String contentType() {
            return "text/csv";
        }

        @Override
        public DataContent dataContent(final Client client, final RestRequest request) {
            return new CsvContent(client, request, this);
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
                final RestRequest request) {
            return new XlsContent(client, request, this, false);
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
                final RestRequest request) {
            return new JsonContent(client, request, this);
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
                final RestRequest request) {
            return new XlsContent(client, request, this, true);
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

    // pass one argument into their values
    ContentType(final int index) {
        this.index = index;
    }

    public int index() {
        return index;
    }

    // every enum value should implement all the abstract method in their enum type
    public abstract String contentType();

    /**
     * Create a {@link DataContent} object associated with this {@link ContentType}
     * to do the dump operations.
     * @param client
     * @param request
     * @return
     */
    public abstract DataContent dataContent(Client client, RestRequest request);

    public abstract String fileName(RestRequest request);
}
