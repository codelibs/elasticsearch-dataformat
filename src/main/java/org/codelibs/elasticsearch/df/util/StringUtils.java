package org.codelibs.elasticsearch.df.util;

import java.util.Collection;

public final class StringUtils {

    public static final String EMPTY_STRING = "";

    public static final String[] EMPTY_STRINGS = new String[0];

    public static final String UTF_8 = "UTF-8";

    private StringUtils() {
    }

    /**
     * Check if a collection of a string is empty.
     *
     * @param c
     * @return
     */
    public static boolean isEmpty(final Collection<String> c) {
        if (c == null || c.isEmpty()) {
            return false;
        }
        for (final String text : c) {
            if (isNotEmpty(text)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check if a collection of a string is not empty.
     *
     * @param c
     * @return
     */
    public static boolean isNotEmpty(final Collection<String> c) {
        return !isEmpty(c);
    }

    /**
     * Check if a collection of a string is blank.
     *
     * @param c
     * @return
     */
    public static boolean isBlank(final Collection<String> c) {
        if (c == null || c.isEmpty()) {
            return false;
        }
        for (final String text : c) {
            if (isNotBlank(text)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check if a collection of a string is not blank.
     *
     * @param c
     * @return
     */
    public static boolean isNotBlank(final Collection<String> c) {
        return !isBlank(c);
    }

    /**
     * Check if a string is empty.
     *
     * @param text
     * @return
     */
    public static final boolean isEmpty(final String text) {
        return text == null || text.length() == 0;
    }

    /**
     * Check if a string is not empty.
     *
     * @param text
     * @return
     */
    public static final boolean isNotEmpty(final String text) {
        return !isEmpty(text);
    }

    /**
     * Check if a string is blank.
     *
     * @param str
     * @return
     */
    public static boolean isBlank(final String str) {
        if (str == null || str.length() == 0) {
            return true;
        }
        for (int i = 0; i < str.length(); i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check if a string is not blank.
     *
     * @param str
     * @return
     */
    public static boolean isNotBlank(final String str) {
        return !isBlank(str);
    }
}
