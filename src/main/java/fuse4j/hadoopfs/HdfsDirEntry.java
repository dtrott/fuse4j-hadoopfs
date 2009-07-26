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

public class HdfsDirEntry extends HdfsFileInfo {

    public String name;

    public HdfsDirEntry(boolean directory, String name, int mode) {
        this(directory, 0, name, mode);
        // TODO: Hack to set inode.
        this.inode = this.hashCode();
    }

    public HdfsDirEntry(boolean directory, int inode, String name, int mode) {
        super(directory, inode, mode);
        this.name = name;
    }
}
