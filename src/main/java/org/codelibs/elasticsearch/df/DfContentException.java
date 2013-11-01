package org.codelibs.elasticsearch.df;

public class DfContentException extends DfSystemException {

    private static final long serialVersionUID = 1L;

    public DfContentException(final String message) {
        super(message);
    }

    public DfContentException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
