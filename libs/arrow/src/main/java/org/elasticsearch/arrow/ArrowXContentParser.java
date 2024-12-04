/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow;

import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;

/**
 * An XContent parser that reads and Arrow stream.
 *
 * @see <a href="https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc">Arrow IPC</a>
 */
public class ArrowXContentParser extends ArrowJsonXContentParser {

    public ArrowXContentParser(XContentParserConfiguration config, InputStream in) throws IOException {
        super(config, new ArrowJsonParser(in));
    }

    @Override
    public XContentType contentType() {
        throw new UnsupportedOperationException("Arrow XContent is not a registered type");
    }

    @Override
    public void allowDuplicateKeys(boolean allowDuplicateKeys) {
        throw new UnsupportedOperationException("Allowing duplicate keys after the parser has been created is not possible for CBOR");
    }
}

