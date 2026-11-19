// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"maps"
	"strconv"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
)

type overwriteTrackingIO struct {
	io.WriteFileIO
	onFileCreate func(string)
}

func (fs *overwriteTrackingIO) Create(path string) (io.FileWriter, error) {
	fs.onFileCreate(path)
	return fs.WriteFileIO.Create(path)
}

func (fs *overwriteTrackingIO) WriteFile(path string, data []byte) error {
	fs.onFileCreate(path)
	return fs.WriteFileIO.WriteFile(path, data)
}

type overwriteTrackingReadIO struct {
	*overwriteTrackingIO
	reader io.ReadFileIO
}

func (fs *overwriteTrackingReadIO) ReadFile(path string) ([]byte, error) {
	return fs.reader.ReadFile(path)
}

type overwriteWriteLimits struct {
	workers         int
	bufferBatches   int
	rowGroupRows    int
	targetFileBytes int64
}

func (limits overwriteWriteLimits) apply(meta *MetadataBuilder, args recordWritingArgs) (*MetadataBuilder, recordWritingArgs) {
	args.maxWriteWorkers = limits.workers
	args.recordBatchBufferSize = limits.bufferBatches
	if limits.rowGroupRows <= 0 && limits.targetFileBytes <= 0 {
		return meta, args
	}
	// Only write properties differ; all shared metadata is read-only here.
	writeMeta := *meta
	writeMeta.props = maps.Clone(meta.props)
	if writeMeta.props == nil {
		writeMeta.props = make(iceberg.Properties)
	}
	if limits.rowGroupRows > 0 {
		writeMeta.props[ParquetRowGroupLimitKey] = strconv.Itoa(limits.rowGroupRows)
	}
	if limits.targetFileBytes > 0 {
		writeMeta.props[WriteTargetFileSizeBytesKey] = strconv.FormatInt(limits.targetFileBytes, 10)
	}
	return &writeMeta, args
}
