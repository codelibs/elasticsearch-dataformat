package org.codelibs.elasticsearch.df.netty;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

public interface NettyHttpProvider {
    void writeResponse(final Map<String, Object> headers, final FileInputStream fis) throws IOException;

    boolean isOpenConnection();
}
