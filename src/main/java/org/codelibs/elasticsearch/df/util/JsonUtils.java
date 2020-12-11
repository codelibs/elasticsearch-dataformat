package org.codelibs.elasticsearch.df.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonNull;

public class JsonUtils {
    private JsonUtils() {
    }

    /**
     * Returns a JSON sub-element from the given JsonElement and the given path
     *
     * @param json - a Gson JsonElement
     * @param path - a JSON path, e.g. a.b.c[2].d
     * @return - a sub-element of json according to the given path
     */
    public static JsonElement getJsonElement(JsonElement json, String path){

        String[] parts = path.split("\\.|\\[|\\]");
        JsonElement result = json;

        for (String key : parts) {

            key = key.trim();
            if (key.isEmpty())
                continue;

            if (result == null){
                result = JsonNull.INSTANCE;
                break;
            }

            if (result.isJsonObject()){
                result = result.getAsJsonObject().get(key);
            }
            else if (result.isJsonArray()){
                int ix = Integer.valueOf(key);
                result = (ix < result.getAsJsonArray().size())?result.getAsJsonArray().get(ix):null;
            }
            else{
                break;
            }
        }

        return result;
    }

    /**
     * Returns a removed JSON sub-element from the given JsonElement and the given path
     *
     * @param json - a Gson JsonElement
     * @param path - a JSON path, e.g. a.b.c[2].d
     * @return - a removed sub-element of json according to the given path
     */
    public static JsonElement removeJsonElement(JsonElement json, String path){

        String[] parts = path.split("\\.|\\[|\\]");
        JsonElement result = json;

        for (int i = 0; i < parts.length; i++) {

            String key = parts[i].trim();
            if (key.isEmpty())
                continue;

            if (result == null){
                result = JsonNull.INSTANCE;
                break;
            }

            boolean lastPart = (i == parts.length-1);
            if (result.isJsonObject()){
                if (lastPart){
                    result = result.getAsJsonObject().remove(key);
                }else{
                    result = result.getAsJsonObject().get(key);
                }
            }
            else if (result.isJsonArray()){
                int ix = Integer.valueOf(key);
                if (lastPart){
                    result = result.getAsJsonArray().remove(ix);
                }else{
                    result = result.getAsJsonArray().get(ix);
                }
            }
            else{
                break;
            }
        }

        return result;
    }
}
