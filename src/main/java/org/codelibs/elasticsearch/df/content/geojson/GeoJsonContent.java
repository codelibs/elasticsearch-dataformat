package org.codelibs.elasticsearch.df.content.geojson;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.elasticsearch.df.content.ContentType;
import org.codelibs.elasticsearch.df.content.DataContent;
import org.codelibs.elasticsearch.df.util.JsonUtils;
import org.codelibs.elasticsearch.df.util.RequestUtil;
import org.codelibs.elasticsearch.df.util.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class GeoJsonContent extends DataContent {
    private static final Logger logger = LogManager.getLogger(GeoJsonContent.class);

    private final String geometryCoordinatesLonField;
    private final String geometryCoordinatesLatField;
    private final String geometryCoordinatesAltField;
    private final String geometryTypeField;
    private final String geometryCoordinatesField;
    private final boolean geometryKeepGeoInfo;
    private final List<String> excludeFields;

    public GeoJsonContent(final Client client, final RestRequest request, final ContentType contentType) {
        super(client, request, contentType);

        geometryCoordinatesLonField = request.param("geometry.lon_field",StringUtils.EMPTY_STRING);
        geometryCoordinatesLatField = request.param("geometry.lat_field",StringUtils.EMPTY_STRING);
        geometryCoordinatesAltField = request.param("geometry.alt_field",StringUtils.EMPTY_STRING);
        geometryTypeField = request.param("geometry.type_field",StringUtils.EMPTY_STRING);
        geometryCoordinatesField = request.param("geometry.coord_field",StringUtils.EMPTY_STRING);
        geometryKeepGeoInfo = request.paramAsBoolean("keep_geometry_info",false);

        final String[] fields = request.paramAsStringArray("exclude_fields", StringUtils.EMPTY_STRINGS);
        if (fields.length == 0) {
            excludeFields = new ArrayList<>();
        } else {
            final List<String> fieldList = new ArrayList<>();
            for (final String field : fields) {
                fieldList.add(field.trim());
            }
            excludeFields = Collections.unmodifiableList(fieldList);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("geometryTypeField: {}, geometryCoordinatesField: {}, geometryCoordinatesLonField: {}, " +
                         "geometryCoordinatesLatField: {}, geometryCoordinatesAltField: {}, geometryKeepGeoInfo: {}, excludeFields: {}",
                         geometryTypeField, geometryCoordinatesField, geometryCoordinatesLonField, 
                         geometryCoordinatesLatField, geometryCoordinatesAltField, geometryKeepGeoInfo, excludeFields);
        }
    }

    @Override
    public void write(final File outputFile, final SearchResponse response, final RestChannel channel,
            final ActionListener<Void> listener) {
        try {
            final OnLoadListener onLoadListener = new OnLoadListener(
                    outputFile, listener);
            onLoadListener.onResponse(response);
        } catch (final Exception e) {
            listener.onFailure(new ElasticsearchException("Failed to write data.",
                    e));
        }
    }

    protected class OnLoadListener implements ActionListener<SearchResponse> {
        protected ActionListener<Void> listener;

        protected Writer writer;

        protected File outputFile;

        private long currentCount = 0;

        private boolean firstLine = true;

        protected OnLoadListener(final File outputFile, final ActionListener<Void> listener) {
            this.outputFile = outputFile;
            this.listener = listener;
            try {
                writer = new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream(outputFile), "UTF-8"));
            } catch (final Exception e) {
                throw new ElasticsearchException("Could not open "
                        + outputFile.getAbsolutePath(), e);
            }
            try {
                writer.append("{\"type\": \"FeatureCollection\", \"features\": [");
            }catch (final Exception e) {
                onFailure(e);
            }
        }

        @Override
        public void onResponse(final SearchResponse response) {
            final Gson gsonWriter = new GsonBuilder().create();
            final String scrollId = response.getScrollId();
            final SearchHits hits = response.getHits();
            final int size = hits.getHits().length;
            currentCount += size;
            if (logger.isDebugEnabled()) {
                logger.debug("scrollId: {}, totalHits: {}, hits: {}, current: {}",
                        scrollId, hits.getTotalHits(), size, currentCount);
            }
            try {
                for (final SearchHit hit : hits) {
                    final String source = XContentHelper.convertToJson(
                            hit.getSourceRef(), true, false, XContentType.JSON);
                    if (!firstLine){
                        writer.append(',');
                    }else{
                        firstLine = false;
                    }

                    final JsonElement propertiesJson = JsonParser.parseString(source);
                    String geometryType = "";

                    JsonArray geometryCoordinates = new JsonArray();
                    if (!geometryCoordinatesField.isEmpty()){
                        JsonElement jsonEltCoord = JsonUtils.getJsonElement(propertiesJson,geometryCoordinatesField);
                        if (jsonEltCoord !=null && !jsonEltCoord.isJsonNull()){
                            geometryCoordinates = jsonEltCoord.getAsJsonArray​();
                            if (!geometryKeepGeoInfo){
                                JsonUtils.removeJsonElement(propertiesJson,geometryCoordinatesField);
                            }
                        }
                        if (!geometryTypeField.isEmpty()){
                            JsonElement jsonEltType = JsonUtils.getJsonElement(propertiesJson,geometryTypeField);
                            if (jsonEltType !=null && !jsonEltType.isJsonNull()){
                                geometryType = jsonEltType.getAsString();
                                if (!geometryKeepGeoInfo){
                                    JsonUtils.removeJsonElement(propertiesJson,geometryTypeField);
                                }
                            }
                        }
                    }else{
                        if (!geometryCoordinatesLonField.isEmpty() && !geometryCoordinatesLatField.isEmpty()){
                            JsonElement jsonEltLon = JsonUtils.getJsonElement(propertiesJson,geometryCoordinatesLonField);
                            JsonElement jsonEltLat = JsonUtils.getJsonElement(propertiesJson,geometryCoordinatesLatField);
                            if (jsonEltLon !=null && !jsonEltLon.isJsonNull() && jsonEltLat !=null && !jsonEltLat.isJsonNull()){
                                geometryCoordinates.add(jsonEltLon.getAsNumber());
                                geometryCoordinates.add(jsonEltLat.getAsNumber());
                                if (!geometryKeepGeoInfo) {
                                    JsonUtils.removeJsonElement(propertiesJson,geometryCoordinatesLonField);
                                    JsonUtils.removeJsonElement(propertiesJson,geometryCoordinatesLatField);
                                }
                            }
                        }
                        if (!geometryCoordinatesAltField.isEmpty()){
                            JsonElement jsonElt = JsonUtils.getJsonElement(propertiesJson,geometryCoordinatesAltField);
                            if (jsonElt !=null && !jsonElt.isJsonNull()){
                                geometryCoordinates.add(jsonElt.getAsNumber());
                                if (!geometryKeepGeoInfo) {
                                    JsonUtils.removeJsonElement(propertiesJson,geometryCoordinatesAltField);
                                }
                            }
                        }
                        geometryType = "Point";
                    }

                    for (String excludeField : excludeFields) {
                        JsonUtils.removeJsonElement(propertiesJson,excludeField);
                    }
                    
                    JsonObject geometryObject = new JsonObject();
                    geometryObject.addProperty("type", geometryType);
                    geometryObject.add("coordinates", geometryCoordinates);

                    JsonObject featureObject = new JsonObject();
                    featureObject.addProperty("type", "Feature");
                    featureObject.add("geometry", geometryObject);
                    featureObject.add("properties", propertiesJson.getAsJsonObject());

                    writer.append('\n').append(gsonWriter.toJson(featureObject));
                }

                if (size == 0 || scrollId == null) {
                    // end
                    writer.append('\n').append("]}");
                    writer.flush();
                    close();
                    listener.onResponse(null);
                } else {
                    client.prepareSearchScroll(scrollId)
                            .setScroll(RequestUtil.getScroll(request))
                            .execute(this);
                }
            } catch (final Exception e) {
                onFailure(e);
            }
        }

        @Override
        public void onFailure(final Exception e) {
            try {
                close();
            } catch (final Exception e1) {
                // ignore
            }
            listener.onFailure(new ElasticsearchException("Failed to write data.",
                    e));
        }

        private void close() {
            if (writer != null) {
                try {
                    writer.close();
                } catch (final IOException e) {
                    throw new ElasticsearchException("Could not close "
                            + outputFile.getAbsolutePath(), e);
                }
            }
        }
    }
}
