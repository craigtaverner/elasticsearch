/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageMetadataResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * An incremental reader for Arrow dataframes.
 */
public class ArrowIncrementalParser implements Closeable {

    public interface Listener {
        /**
         * Start of the Arrow stream. It's the responsibility of the listener to close this vector root,
         * as it may need to live longer than the parser.
         */
        void startStream(VectorSchemaRoot schemaRoot) throws IOException;

        /**
         * A new {@code RecordBatch} was read. Its vectors are available in the {@code VectorSchemaRoot} that
         * was passed to {@link #startStream(VectorSchemaRoot)}.
         */
        void nextBatch() throws IOException;

        /**
         * Reached the end of the Arrow stream.
         */
        void endStream() throws IOException;
    }

    private final Listener listener;
    private BytesReferenceChannel channel;
    private long expectedDataLength;
    private ArrowStreamReader reader;

    private static final int PREFIX_LEN = 8;

    public ArrowIncrementalParser(BufferAllocator allocator, Listener listener) {
        this.listener = listener;
        this.expectedDataLength = PREFIX_LEN;
        this.channel = new BytesReferenceChannel();
        this.reader = new ArrowStreamReader(channel, allocator);
    }

    /**
     * When {@link #parse(BytesReference, boolean)} returns zero, provides the number of bytes
     * that are needed to continue parsing the Arrow stream. Note that {@code parse()} can
     * return zero multiple times with an increasing expected data length.
     */
    public long expectedDataLength() {
        return this.expectedDataLength;
    }

    @Override
    public void close() throws IOException {
        if (this.reader != null) {
            this.reader.close(); // Will also close channel.
            this.channel = null;
            this.reader = null;
        }
    }

    public int parse(BytesReference data, boolean lastData) throws IOException {
        int total = 0;
        int consumed;
        while ((consumed = doParse(data, lastData)) > 0) {
            total += consumed;
            data = data.slice(consumed, data.length() - consumed);
            // Start a new message
            expectedDataLength = PREFIX_LEN;
        }
        return total;
    }

    /**
     * Parse an Arrow message (metadata + body). If there aren't enough bytes available, return zero.
     */
    private int doParse(BytesReference data, boolean lastData) throws IOException {

        if (data.length() < expectedDataLength) {
            return 0;
        }

        // See https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc

        var continuation = data.getIntLE(0);
        if (continuation != 0xFFFFFFFF) {
            throw new IOException("Bad Arrow continuation prefix [" + Integer.toHexString(continuation) + "] prefix");
        }

        var metadataSize = data.getIntLE(4);

        if (metadataSize == 0) {
            // End of stream
            return PREFIX_LEN;
        }

        // FIXME: enforce a hard limit on metadata size?
        int trailing = metadataSize % 8;
        if (trailing % 8 != 0) {
            // padded to 8 bytes
            metadataSize += (8 - trailing);
        }

        expectedDataLength = PREFIX_LEN + metadataSize;
        if (data.length() < expectedDataLength) {
            return 0;
        }

        // We may expect some data after the metadata, read metadata to find body length.
        // The Arrow library doesn't make it easy to read metadata and then the body, so we read
        // the metadata once to get the body length (overhead is low since flatbuffers is zero-copy)
        ReadChannel ch = new ReadChannel(new BytesReferenceChannel(data));
        MessageMetadataResult metadata = MessageSerializer.readMessage(ch);
        // FIXME: enforce a hard limit on body length?
        expectedDataLength += metadata.getMessageBodyLength();
        if (data.length() < expectedDataLength) {
            return 0;
        }

        // We now have enough data to read a batch (message + data)
        channel.setData(data, lastData);
        long initialBytesRead = reader.bytesRead();

        if (reader.bytesRead() == 0) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            listener.startStream(root);

        } else {
            if (reader.loadNextBatch()) {
                listener.nextBatch();
            } else {
                expectedDataLength = 0;
                listener.endStream();
                close();
            }
        }

        return (int)(reader.bytesRead() - initialBytesRead);
    }

    /**
     * A {@code ReadableByteChannel} that reads from {@ByteReference} data. That data
     * can be updated, allowing incremental parsing from a single channel.
     */
    private static class BytesReferenceChannel implements ReadableByteChannel {

        private BytesRefIterator iterator;
        private BytesRef current;
        private int currentOffset;
        private int lastOffset;
        private boolean lastData = false;

        BytesReferenceChannel() {
        }

        BytesReferenceChannel(BytesReference data) throws IOException {
            setData(data, true);
        }

        void setData(BytesReference data, boolean lastData) throws IOException {
            this.lastData = lastData;
            this.iterator = data.iterator();
            nextBytesRef();
        }

        private void nextBytesRef() throws IOException {
            this.current = iterator.next();
            if (this.current == null) {
                this.currentOffset = 0;
                this.lastOffset = 0;
            } else {
                this.currentOffset = this.current.offset;
                this.lastOffset = this.currentOffset + this.current.length;
            }
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            int written = 0;
            int remaining;
            while ((remaining = dst.remaining()) > 0 && this.current != null) {
                int len = Math.min(remaining, this.lastOffset - this.currentOffset);
                dst.put(this.current.bytes, this.currentOffset, len);
                this.currentOffset += len;
                written += len;

                if (this.currentOffset == this.lastOffset) {
                    nextBytesRef();
                }
            }

            // FIXME: check if returning zero can cause an infinite loop as no additional data will come
            // even if new data may arrive later in the enclosing incremental parser.
            return written == 0 && lastData ? -1 : written;
        }

        @Override
        public boolean isOpen() {
            return iterator != null;
        }

        @Override
        public void close() {
            iterator = null;
            current = null;
        }
    }
}
