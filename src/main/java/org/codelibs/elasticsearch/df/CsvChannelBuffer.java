package org.codelibs.elasticsearch.df;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

import org.elasticsearch.common.netty.buffer.AbstractChannelBuffer;
import org.elasticsearch.common.netty.buffer.ChannelBuffer;
import org.elasticsearch.common.netty.buffer.ChannelBufferFactory;

public class CsvChannelBuffer extends AbstractChannelBuffer {

    @Override
    public byte[] array() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int arrayOffset() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int capacity() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public ChannelBuffer copy(final int arg0, final int arg1) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ChannelBuffer duplicate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ChannelBufferFactory factory() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte getByte(final int arg0) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void getBytes(final int arg0, final ByteBuffer arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void getBytes(final int arg0, final OutputStream arg1, final int arg2)
            throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getBytes(final int arg0, final GatheringByteChannel arg1,
            final int arg2) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void getBytes(final int arg0, final ChannelBuffer arg1,
            final int arg2, final int arg3) {
        // TODO Auto-generated method stub

    }

    @Override
    public void getBytes(final int arg0, final byte[] arg1, final int arg2,
            final int arg3) {
        // TODO Auto-generated method stub

    }

    @Override
    public int getInt(final int arg0) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getLong(final int arg0) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public short getShort(final int arg0) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getUnsignedMedium(final int arg0) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean hasArray() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isDirect() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public ByteOrder order() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setByte(final int arg0, final int arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setBytes(final int arg0, final ByteBuffer arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public int setBytes(final int arg0, final InputStream arg1, final int arg2)
            throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int setBytes(final int arg0, final ScatteringByteChannel arg1,
            final int arg2) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setBytes(final int arg0, final ChannelBuffer arg1,
            final int arg2, final int arg3) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setBytes(final int arg0, final byte[] arg1, final int arg2,
            final int arg3) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setInt(final int arg0, final int arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setLong(final int arg0, final long arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setMedium(final int arg0, final int arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setShort(final int arg0, final int arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public ChannelBuffer slice(final int arg0, final int arg1) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ByteBuffer toByteBuffer(final int arg0, final int arg1) {
        // TODO Auto-generated method stub
        return null;
    }

}
