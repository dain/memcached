/*
 * Copyright 2010 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.memcached.protocol;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;


@ChannelPipelineCoverage("one")
public class TextFrameDecoder extends DelimiterBasedFrameDecoder
{
    private SessionStatus status;

    private ChannelBuffer delimiter;
    private boolean discardingTooLongFrame;
    private long tooLongFrameLength;
    private long maxFrameLength;

    /**
     * Creates a new instance.
     *
     * @param status session status instance for holding state of the session
     * @param maxFrameLength the maximum length of the decoded frame. A {@link
     * org.jboss.netty.handler.codec.frame.TooLongFrameException} is thrown if
     * frame length is exceeded
     */
    public TextFrameDecoder(SessionStatus status, int maxFrameLength)
    {
        super(maxFrameLength, ChannelBuffers.wrappedBuffer(new byte[]{'\r', '\n'}));
        if (status == null) {
            throw new NullPointerException("status is null");
        }

        this.status = status;
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, org.jboss.netty.channel.Channel channel, ChannelBuffer buffer)
            throws Exception
    {
        // check the state. if we're WAITING_FOR_DATA that means instead of breaking into lines, we need N bytes
        // otherwise, we're waiting for input
        if (status.state != SessionStatus.State.WAITING_FOR_DATA) {
            return readLine(buffer);
        }
        else {
            if (buffer.readableBytes() < status.bytesNeeded + delimiter.capacity()) {
                return null;
            }

            // verify delimiter matches at the right location
            ChannelBuffer dest = ChannelBuffers.buffer(delimiter.capacity());
            buffer.getBytes(status.bytesNeeded + buffer.readerIndex(), dest);

            if (!dest.equals(delimiter)) {
                // before we throw error... we're ready for the next command
                status.ready();

                // error, no delimiter at end of payload
                throw new IncorrectlyTerminatedPayloadException("payload not terminated correctly");
            }
            else {

                status.processingMultiline();

                // There's enough bytes in the buffer and the delimiter is at the end. Read it.

                // Successfully decoded a frame.  Return the decoded frame.
                ChannelBuffer result = ChannelBuffers.buffer(status.bytesNeeded);
                buffer.readBytes(result);

                // Consume
                buffer.skipBytes(delimiter.capacity());

                return result;
            }

        }
    }

    private Object readLine(ChannelBuffer buffer)
            throws TooLongFrameException
    {
        int minFrameLength = Integer.MAX_VALUE;
        ChannelBuffer foundDelimiter = null;
        {
            int frameLength = indexOf(buffer, delimiter);
            if (frameLength >= 0 && frameLength < minFrameLength) {
                minFrameLength = frameLength;
                foundDelimiter = delimiter;
            }
        }

        if (foundDelimiter != null) {
            int minDelimLength = foundDelimiter.capacity();
            ChannelBuffer frame;

            if (discardingTooLongFrame) {
                // We've just finished discarding a very large frame.
                // Throw an exception and go back to the initial state.
                long tooLongFrameLength = this.tooLongFrameLength;
                this.tooLongFrameLength = 0L;
                discardingTooLongFrame = false;
                buffer.skipBytes(minFrameLength + minDelimLength);
                throw new TooLongFrameException("The frame length exceeds " + maxFrameLength + ": " + tooLongFrameLength + minFrameLength + minDelimLength);
            }

            if (minFrameLength > maxFrameLength) {
                // Discard read frame.
                buffer.skipBytes(minFrameLength + minDelimLength);
                throw new TooLongFrameException("The frame length exceeds " + maxFrameLength + ": " + (long) minFrameLength);
            }

            frame = buffer.readBytes(minFrameLength);
            buffer.skipBytes(minDelimLength);

            status.processing();

            return frame;
        }
        else {
            if (buffer.readableBytes() > maxFrameLength) {
                // Discard the content of the buffer until a delimiter is found.
                tooLongFrameLength = buffer.readableBytes();
                buffer.skipBytes(buffer.readableBytes());
                discardingTooLongFrame = true;
            }

            return null;
        }
    }

    /**
     * Returns the number of bytes between the readerIndex of the haystack and
     * the first needle found in the haystack.  -1 is returned if no needle is
     * found in the haystack.
     */
    private static int indexOf(ChannelBuffer haystack, ChannelBuffer needle)
    {
        for (int i = haystack.readerIndex(); i < haystack.writerIndex(); i++) {
            int haystackIndex = i;
            int needleIndex;
            for (needleIndex = 0; needleIndex < needle.capacity(); needleIndex++) {
                if (haystack.getByte(haystackIndex) != needle.getByte(needleIndex)) {
                    break;
                }
                else {
                    haystackIndex++;
                    if (haystackIndex == haystack.writerIndex() &&
                            needleIndex != needle.capacity() - 1) {
                        return -1;
                    }
                }
            }

            if (needleIndex == needle.capacity()) {
                // Found the needle from the haystack!
                return i - haystack.readerIndex();
            }
        }
        return -1;
    }
}
