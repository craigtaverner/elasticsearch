/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow;

import org.apache.arrow.memory.RootAllocator;

public class Arrow {

    private static final RootAllocator ROOT_ALLOCATOR = new RootAllocator();

    /**
     * Returns the global root allocator. Should be used to create child allocators to have
     * fine-grained memory allocation tracking and to enforce local limits.
     */
    public static RootAllocator rootAllocator() {
        return new RootAllocator();
    }
}
