package fuse4j.hadoopfs;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

/**
 * class HdfsClientReal
 */
class HdfsClientReal implements HdfsClient {
    FileSystem dfs = null;
    private final UserCache userCache;

    /**
     * constructor
     */
    HdfsClientReal(UserCache userCache, String hdfsUrl) throws IOException {
        this.userCache = userCache;
        try {
            Configuration conf = new Configuration();
            dfs = FileSystem.get(new URI(hdfsUrl), conf);
        } catch(URISyntaxException e) {
            throw new IOException("URL Issue");
        }
    }

    /**
     * getFileInfo()
     */
    public HdfsFileAttr getFileInfo(String path) {

        try {
            FileStatus dfsStat = dfs.getFileStatus(new Path(path));

            final boolean directory = dfsStat.isDir();
            final int inode = 0;
            final int mode = dfsStat.getPermission().toShort();
            final int uid = userCache.getUid(dfsStat.getOwner());
            final int gid = 0;


            // TODO: per-file block-size can't be retrieved correctly,
            //       using default block size for now.
            final long size = dfsStat.getLen();
            final int blocks = (int) Math.ceil(((double) size) / dfs.getDefaultBlockSize());

            // modification/create-times are the same as access-time
            final int modificationTime = (int) (dfsStat.getModificationTime() / 1000);

            HdfsFileAttr hdfsFileAttr = new HdfsFileAttr(directory, inode, mode, uid, gid, 1);
            hdfsFileAttr.setSize(size, blocks);
            hdfsFileAttr.setTime(modificationTime);

            // TODO Hack to set inode;
            hdfsFileAttr.inode = hdfsFileAttr.hashCode();

            return hdfsFileAttr;
        } catch(IOException ioe) {
            // fall through to failure
        }

        // failed
        return null;
    }

    /**
     * listPaths()
     */
    public HdfsDirEntry[] listPaths(String path) {
        try {
            FileStatus[] dfsStatList = dfs.listStatus(new Path(path));
            HdfsDirEntry[] hdfsDirEntries = new HdfsDirEntry[dfsStatList.length + 2];

            // Add special directories.
            hdfsDirEntries[0] = new HdfsDirEntry(true, ".", 0777);
            hdfsDirEntries[1] = new HdfsDirEntry(true, "..", 0777);

            for(int i = 0; i < dfsStatList.length; i++) {
                hdfsDirEntries[i + 2] = newHdfsDirEntry(dfsStatList[i]);
            }

            return hdfsDirEntries;

        } catch(IOException ioe) {
            return null;
        }
    }

    private HdfsDirEntry newHdfsDirEntry(FileStatus fileStatus) {
        final boolean directory = fileStatus.isDir();
        final String name = fileStatus.getPath().getName();
        final FsPermission permission = fileStatus.getPermission();

        return new HdfsDirEntry(directory, name, permission.toShort());
    }

    /**
     * openForRead()
     */
    public Object openForRead(String path) {
        try {
            FSDataInputStream input = dfs.open(new Path(path));

            return new HdfsFileIoContext(input);
        } catch(IOException ioe) {
            // fall through to failure
        }

        return null;
    }

    public Object createForWrite(String path) {
        try {
            // don't overwrite by default
            FSDataOutputStream output = dfs.create(new Path(path), false);

            return new HdfsFileIoContext(output);
        } catch(IOException ioe) {
            // fall through to failure
        }

        return null;
    }

    public boolean close(Object hdfsFile) {
        HdfsFileIoContext file = (HdfsFileIoContext) hdfsFile;
        try {
            if(file.ioStream instanceof FSDataOutputStream) {
                FSDataOutputStream output = (FSDataOutputStream) file.ioStream;
                output.close();
                return true;
            }

            if(file.ioStream instanceof FSDataInputStream) {
                FSDataInputStream output = (FSDataInputStream) file.ioStream;
                output.close();
                return true;
            }
        } catch(IOException ioe) {
            // fall through to failure
        }

        return false;
    }

    /**
     * read()
     */
    public boolean read(Object hdfsFile, ByteBuffer buf, long offset) {
        HdfsFileIoContext file = (HdfsFileIoContext) hdfsFile;

        if(!(file.ioStream instanceof FSDataInputStream)) {
            return false;
        }

        FSDataInputStream input = (FSDataInputStream) file.ioStream;

        byte[] readBuf = new byte[buf.capacity()];

        int bytesRead = 0;
        try {
            bytesRead = input.read(offset, readBuf, 0, readBuf.length);
        } catch(IOException ioe) {
            return false;
        }

        // otherwise return how much we read
        // TODO: does this handle 0 bytes?
        buf.put(readBuf, 0, bytesRead);
        return true;
    }

    /**
     * write()
     */
    public boolean write(Object hdfsFile, ByteBuffer buf, long offset) {
        boolean status = false;
        HdfsFileIoContext file = (HdfsFileIoContext) hdfsFile;

        if(!(file.ioStream instanceof FSDataOutputStream)) {
            return false;
        }

        FSDataOutputStream output = (FSDataOutputStream) file.ioStream;

        // get the data to write
        byte[] writeBuf = new byte[buf.capacity()];
        buf.get(writeBuf, 0, writeBuf.length);

        // lock this file so we can update the 'write-offset'
        synchronized(file) {
            // we will only allow contiguous writes
            if(offset == file.offsetWritten) {
                try {
                    output.write(writeBuf, 0, writeBuf.length);

                    // increase our offset
                    file.offsetWritten += writeBuf.length;

                    // return how much we read
                    // TODO: does this handle 0 bytes?
                    buf.position(writeBuf.length);

                    // if we are here, then everything is good
                    status = true;
                } catch(IOException ioe) {
                    // return failure
                    status = false;
                }
            }
        }

        return status;
    }

    /**
     * mkdir()
     */
    public boolean mkdir(String path) {
        try {
            return dfs.mkdirs(new Path(path));
        } catch(IOException ioe) {
            // fall through to failure
        }
        return false;
    }

    /**
     * unlink()
     */
    public boolean unlink(String filePath) {
        try {
            return dfs.delete(new Path(filePath));
        } catch(IOException ioe) {
            // fall through to failure
        }
        return false;
    }

    /**
     * rmdir()
     */
    public boolean rmdir(String dirPath) {
        return unlink(dirPath);
    }

    /**
     * rename()
     */
    public boolean rename(String src, String dst) {
        try {
            return dfs.rename(new Path(src), new Path(dst));
        } catch(IOException ioe) {
            // fall through to failure
        }
        return false;
    }
}

//
// class HdfsFileIoContext

//
class HdfsFileIoContext {
    public Object ioStream = null;

    public long offsetWritten = 0;

    HdfsFileIoContext(Object ioStream) {
        this.ioStream = ioStream;
        offsetWritten = 0;
    }
}
