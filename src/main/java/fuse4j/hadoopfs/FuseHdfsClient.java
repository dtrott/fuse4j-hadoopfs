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

import fuse.Errno;
import fuse.Filesystem3;
import fuse.FilesystemConstants;
import fuse.FuseDirFiller;
import fuse.FuseException;
import fuse.FuseGetattrSetter;
import fuse.FuseMount;
import fuse.FuseOpenSetter;
import fuse.FuseSizeSetter;
import fuse.FuseStatfsSetter;
import fuse.LifecycleSupport;
import fuse.XattrLister;
import fuse.XattrSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

@SuppressWarnings({"OctalInteger"})
public class FuseHdfsClient implements Filesystem3, XattrSupport, LifecycleSupport, Runnable {
    private static final Log log = LogFactory.getLog(FuseHdfsClient.class);

    private static final int BLOCK_SIZE = 512;
    private static final int NAME_LENGTH = 1024;

    private HdfsClient hdfs = null;

    private HashMap<String, HdfsFileContext> hdfsFileCtxtMap = null;

    private Thread ctxtMapCleanerThread = null;


    public FuseHdfsClient() {
        hdfs = HdfsClientFactory.create();

        hdfsFileCtxtMap = new HashMap<String, HdfsFileContext>();

        ctxtMapCleanerThread = new Thread(this);
        ctxtMapCleanerThread.start();
        log.info("created");
    }

    public int getattr(String path, FuseGetattrSetter getattrSetter) throws FuseException {
        log.info("getattr(): " + path + "\n");
        HdfsFileAttr s = hdfs.getFileInfo(path);

        if(s == null) {
            return Errno.ENOENT;
        }

        long size;
        long blocks;
        if(s.directory) {
            size = 512;
            blocks = 2;
        } else {
            size = s.size;
            blocks = (s.size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        }

        getattrSetter.set(
            s.inode, s.getMode(), s.numberOfLinks,
            s.uid, s.gid, 0,
            size, blocks,
            s.accessTime, s.modifiedTime, s.createTime
        );

        return 0;

    }


    public int readlink(String path, CharBuffer link) throws FuseException {
        // Not Supported
        log.info("readlink(): " + path + "\n");
        return Errno.ENOENT;
    }

    public int getdir(String path, FuseDirFiller filler) throws FuseException {

        log.info("getdir(): " + path + "\n");

        HdfsDirEntry[] entries = hdfs.listPaths(path);

        if(entries == null) {
            return FuseException.ENOTDIR;
        }

        for(HdfsDirEntry entry : entries) {
            filler.add(entry.name, entry.hashCode(), entry.getMode());
        }

        return 0;
    }

    /**
     * mknod()
     */
    public int mknod(String path, int mode, int rdev) throws FuseException {
        log.info("mknod(): " + path + " mode: " + mode + "\n");

        // do a quick check to see if the file already exists
        HdfsFileContext ctxt = pinFileContext(path);

        if(ctxt != null) {
            unpinFileContext(path);
            return FuseException.EPERM;
        }

        // create the file
        Object hdfsFile = hdfs.createForWrite(path);

        //
        // track this newly opened file, for writing.
        //

        //
        // there will be no 'release' for this mknod, therefore we must not
        // pin the file, it will still live in the tree, but will have a
        // '0' pin-count.
        // TODO: eventually have a worker-thread that looks at all
        //       '0' pin-count objects, ensures that they were opened for
        //       write and releases() them if no writes have happened.
        //       (this hack is to support HDFS's write-once semantics).
        //

        if(!addFileContext(path, new HdfsFileContext(hdfsFile, true), false)) {
            // if we raced with another thread, then close off the open file
            // and fail this open().
            hdfs.close(hdfsFile);

            // TODO: don't fail this open() when racing with another
            //       thread, instead just use the
            //       already inserted 'context'...?
            return FuseException.EACCES;
        }
        return 0;
    }

    public int mkdir(String path, int mode) throws FuseException {
        log.info("mkdir(): " + path + " mode: " + mode + "\n");

        boolean status = hdfs.mkdir(path);

        if(!status) {
            return FuseException.EACCES;
        }
        return 0;
    }

    public int unlink(String path) throws FuseException {
        log.info("unlink(): " + path + "\n");

        boolean status = hdfs.unlink(path);

        if(!status) {
            return FuseException.EACCES;
        }
        return 0;
    }

    public int rmdir(String path) throws FuseException {
        log.info("rmdir(): " + path + "\n");

        boolean status = hdfs.rmdir(path);

        if(!status) {
            return FuseException.EACCES;
        }

        return 0;
    }

    public int symlink(String from, String to) throws FuseException {
        //symlink not supported")
        return FuseException.ENOSYS;
    }

    public int rename(String from, String to) throws FuseException {
        log.info("rename(): " + from + " to: " + to + "\n");

        boolean status = hdfs.rename(from, to);

        if(!status) {
            return FuseException.EPERM;
        }
        return 0;
    }

    public int link(String from, String to) throws FuseException {
        log.info("link(): " + from + "\n");
        //link not supported")
        return FuseException.ENOSYS;
    }

    public int chmod(String path, int mode) throws FuseException {
        log.info("chmod(): " + path + "\n");
        // chmod not supported
        // but silently ignore the requests.
        return 0;
    }

    public int chown(String path, int uid, int gid) throws FuseException {
        log.info("chown(): " + path + "\n");
        throw new FuseException("chown not supported")
            .initErrno(FuseException.ENOSYS);
    }

    public int truncate(String path, long size) throws FuseException {
        log.info("truncate(): " + path + " size: " + size + "\n");
        // Not supported
        return FuseException.EPERM;
    }

    public int utime(String path, int atime, int mtime) throws FuseException {
        // not supported right now (write-once files...)
        log.info("utime(): " + path + "\n");
        return FuseException.ENOSYS;
    }


    public int statfs(FuseStatfsSetter statfsSetter) throws FuseException {
        statfsSetter.set(
            BLOCK_SIZE,
            1000,
            200,
            180,
            1000000, // TODO get actual file count.
            0,
            NAME_LENGTH
        );

        return 0;
    }


    private void createFileHandle(String path, int flags, FuseOpenSetter openSetter) {
        openSetter.setFh(new Object());
    }

    // if open returns a filehandle by calling FuseOpenSetter.setFh() method, it will be passed to every method that supports 'fh' argument
    /**
     * open()
     */
    public int open(String path, int flags, FuseOpenSetter openSetter) throws FuseException {
        log.info("open(): " + path + " flags " + flags + "\n");

        HdfsFileContext ctxt = pinFileContext(path);

        //
        // if this context is alive due to a recent mknod() operation
        // then we will allow subsequent open()'s for write.
        // until the first release().
        //
        if(ctxt != null) {
            // from here on, if we fail, we will have to unpin the file-context
            if(ctxt.openedForWrite) {
                // return true only for write access
                if(isWriteOnlyAccess(flags)) {
                    createFileHandle(path, flags, openSetter);
                    return 0;
                }

                // we cannot support read/write at this time, since the
                // file has only been opened for 'writing' on HDFS
                unpinFileContext(path);
                //only writes allowed")
                return FuseException.EACCES;
            }

            // if context has been opened already for reading,
            // then return true only for read access
            if(isReadOnlyAccess(flags)) {
                createFileHandle(path, flags, openSetter);
                return 0;
            }

            // we cannot support write at this time, since the
            // file has only been opened for 'writing' on HDFS
            unpinFileContext(path);

            //only reads allowed
            return FuseException.EACCES;
        }

        //
        // if we are here, then the ctxt must be null, and we will open
        // the file for reading via HDFS.
        //

        // we will only support open() for 'read'
        if(!isReadOnlyAccess(flags)) {
            // only reads allowed")
            return FuseException.EACCES;
        }

        // open the file
        Object hdfsFile = hdfs.openForRead(path);

        if(hdfsFile == null) {
            // uknown error in open()")
            return FuseException.EACCES;
        }

        // track this newly opened file, for reading.
        if(!addFileContext(path, new HdfsFileContext(hdfsFile, false), true)) {
            // if we raced with another thread, then close off the open file
            // and fail this open().
            hdfs.close(hdfsFile);

            // TODO: don't fail this open() when racing with another
            //       thread, instead just use the
            //       already inserted 'context'...?
            // "collision").initErrno(

            return FuseException.EACCES;
        }

        createFileHandle(path, flags, openSetter);
        return 0;
    }

    // fh is filehandle passed from open
    /**
     * read()
     */
    public int read(String path, Object fh, ByteBuffer buf, long offset) throws FuseException {

        //return Errno.EBADF;
        log.info("read(): " + path + " offset: " + offset + " len: "
            + buf.capacity() + "\n");

        HdfsFileContext ctxt = pinFileContext(path);

        if(ctxt == null) {
            //file not opened").initErrno(
            return FuseException.EPERM;
        }

        if(ctxt.openedForWrite) {
            unpinFileContext(path);
            //file not opened for read")
            return FuseException.EPERM;
        }

        boolean status = hdfs.read(ctxt.hdfsFile, buf, offset);

        unpinFileContext(path);

        if(!status) {
            // read failed
            return FuseException.EACCES;
        }

        return 0;
    }

    // fh is filehandle passed from open,
    // isWritepage indicates that write was caused by a writepage
    /**
     * write()
     */
    public int write(String path, Object fh, boolean isWritepage, ByteBuffer buf, long offset) throws FuseException {
        log.info("write(): " + path + " offset: " + offset + " len: "
            + buf.capacity() + "\n");

        HdfsFileContext ctxt = pinFileContext(path);

        if(ctxt == null) {
            // file not opened
            return FuseException.EPERM;
        }

        if(!ctxt.openedForWrite) {
            unpinFileContext(path);

            // file not opened for write")
            return FuseException.EPERM;
        }

        boolean status = hdfs.write(ctxt.hdfsFile, buf, offset);

        unpinFileContext(path);

        if(!status) {
            // write failed
            return FuseException.EACCES;
        }

        return 0;
    }

    // (called when last filehandle is closed), fh is filehandle passed from open
    public int release(String path, Object fh, int flags) throws FuseException {
        log.info("release(): " + path + " flags: " + flags + "\n");
        unpinFileContext(path);
        System.runFinalization();

        return 0;
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public int flush(String path, Object fh) throws FuseException {
        return 0;

    }

    public int fsync(String path, Object fh, boolean isDatasync) throws FuseException {
        return 0;
    }


    public int getxattr(String path, String name, ByteBuffer dst, int position) throws FuseException, BufferOverflowException {
        return 0;
    }

    public int getxattrsize(String path, String name, FuseSizeSetter sizeSetter) throws FuseException {
        return Errno.ENOATTR;
    }

    public int listxattr(String path, XattrLister lister) throws FuseException {
        return 0;
    }

    public int removexattr(String path, String name) throws FuseException {
        return 0;
    }

    public int setxattr(String path, String name, ByteBuffer value, int flags, int position) throws FuseException {
        return 0;
    }

    // LifeCycleSupport
    public int init() {
        log.info("Initializing Filesystem");
        return 0;
    }

    public int destroy() {
        log.info("Destroying Filesystem");

        if(ctxtMapCleanerThread != null) {
            ctxtMapCleanerThread.interrupt();
        }

        return 0;
    }

    //
    // Java entry point
    public static void main(String[] args) {
        log.info("entering");

        try {
            FuseMount.mount(args, new FuseHdfsClient(), log);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        finally {
            log.info("exiting");
        }
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //
    // Private Methods
    //

    /**
     * isWriteOnlyAccess()
     */
    private boolean isWriteOnlyAccess(int flags) {
        return ((flags & 0x0FF) == FilesystemConstants.O_WRONLY);
    }

    /**
     * isReadOnlyAccess()
     */
    private boolean isReadOnlyAccess(int flags) {
        return ((flags & 0x0FF) == FilesystemConstants.O_RDONLY);
    }

    /**
     * addFileContext()
     *
     * @return false if the path is already tracked
     */
    private boolean addFileContext(String path, HdfsFileContext ctxt,
                                   boolean pinFile) {
        boolean addedCtxt = false;

        synchronized(hdfsFileCtxtMap) {
            Object o = hdfsFileCtxtMap.get(path);
            if(o == null) {
                if(pinFile) {
                    ctxt.pinCount = 1;
                }
                hdfsFileCtxtMap.put(path, ctxt);
                addedCtxt = true;
            }
        }

        return addedCtxt;
    }

    /**
     * pinFileContext()
     */
    private HdfsFileContext pinFileContext(String path) {
        HdfsFileContext ctxt = null;

        synchronized(hdfsFileCtxtMap) {
            Object o = hdfsFileCtxtMap.get(path);
            if(o != null) {
                ctxt = (HdfsFileContext) o;
                ctxt.pinCount++;
            }
        }

        return ctxt;
    }

    /**
     * unpinFileContext()
     */
    private void unpinFileContext(String path) {
        synchronized(hdfsFileCtxtMap) {
            Object o = hdfsFileCtxtMap.get(path);

            if(o != null) {
                HdfsFileContext ctxt = (HdfsFileContext) o;

                ctxt.pinCount--;

                if(ctxt.pinCount == 0) {
                    unprotectedCleanupFileContext(path, ctxt);
                    hdfsFileCtxtMap.remove(path);
                }
            }
        }
    }

    /**
     * cleanupExpiredFileContexts()
     */
    private void cleanupExpiredFileContexts() {
        synchronized(hdfsFileCtxtMap) {
            Set paths = hdfsFileCtxtMap.keySet();
            Iterator pathsIter = paths.iterator();

            while(pathsIter.hasNext()) {
                String path = (String) pathsIter.next();

                // remove expired file-contexts
                HdfsFileContext ctxt = hdfsFileCtxtMap.get(path);
                if(ctxt.expired()) {
                    // close this context, and remove from the map
                    unprotectedCleanupFileContext(path, ctxt);
                    pathsIter.remove();
                }
            }
        }
    }

    /**
     * unprotectedCleanupFileContext()
     */
    private void unprotectedCleanupFileContext(String path, HdfsFileContext ctxt) {
        log.info("closing...(): " + path + "\n");

        hdfs.close(ctxt.hdfsFile);
    }

    //
    // HdfsFileContext garbage collection
    //

    /**
     * run()
     * - this method is called on a thread, it sleepily trolls the
     * HdfsFileContext-map to see if any expired contexts need to
     * be shutdown (this can happen if a mknod() occurred, but the
     * associated open()/write()/release() did not happen in which
     * case we shutdown the file on HDFS after a set period of time,
     * and the file becomes read-only.
     */
    public void run() {
        // run once a minute...
        final long timeToSleep = 10000;

        try {
            while(true) {
                // wait for a bit, before checking for expired contexts
                Thread.sleep(timeToSleep);

                cleanupExpiredFileContexts();
            }
        } catch(InterruptedException inte) {
            log.info("run(): " + inte + "\n");
            return;
        }
    }
}

/**
 * class HdfsFileContext
 */
class HdfsFileContext {
    public Object hdfsFile = null;

    public boolean openedForWrite = false;

    public long pinCount = 0;

    private long creationTime = 0;

    // longest time this context can live with a pinCount of 0
    //  - 20 seconds (in ms)
    private static final long expiryTime = 20000;

    /**
     * @param hdfsFile
     * @param openedForWrite
     */
    public HdfsFileContext(Object hdfsFile, boolean openedForWrite) {
        this.hdfsFile = hdfsFile;
        this.openedForWrite = openedForWrite;
        this.creationTime = System.currentTimeMillis();
    }

    public boolean expired() {
        return ((pinCount == 0) && ((System.currentTimeMillis() - creationTime) > expiryTime));
    }
}
