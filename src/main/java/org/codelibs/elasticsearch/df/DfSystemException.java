package org.codelibs.elasticsearch.df;

public class DfSystemException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public DfSystemException(final String message) {
        super(message);
    }

    public DfSystemException(final Throwable cause) {
        super(cause);
    }

    public DfSystemException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
