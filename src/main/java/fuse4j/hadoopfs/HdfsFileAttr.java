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

public class HdfsFileAttr extends HdfsFileInfo {
    public int uid;
    public int gid;
    public int numberOfLinks;

    public long size;
    public int blocks;

    public int createTime;
    public int modifiedTime;
    public int accessTime;

    public HdfsFileAttr() {
        super();
    }

    public HdfsFileAttr(boolean directory, long inode, int mode, int uid, int gid, int numberOfLinks) {
        super(directory, inode, mode);
        this.uid = uid;
        this.gid = gid;
        this.numberOfLinks = numberOfLinks;
    }

    public void setSize(long size, int blocks) {
        this.size = size;
        this.blocks = blocks;
    }

    public void setTime(int time) {
        setTime(time, time, time);
    }

    public void setTime(int createTime, int modifiedTime, int accessTime) {
        this.createTime = createTime;
        this.modifiedTime = modifiedTime;
        this.accessTime = accessTime;
    }

}
