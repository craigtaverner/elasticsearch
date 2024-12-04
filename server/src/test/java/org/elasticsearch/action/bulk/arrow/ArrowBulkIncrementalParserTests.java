/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk.arrow;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.test.ESTestCase;

import java.io.InputStream;

public class ArrowBulkIncrementalParserTests extends ESTestCase {


    public void testParse() throws Exception {

        InputStream in = this.getClass().getResourceAsStream("employee.arrow");
        BytesReference bytes = Streams.readFully(in);

        ArrowBulkIncrementalParser parser = new ArrowBulkIncrementalParser();

        parser.parse(bytes, true);
    }
}
