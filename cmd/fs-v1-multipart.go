// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"fmt"
	"io"
	"path"
	pathutil "path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/pkg/mimedb"

	"github.com/minio/minio/cmd/logger"
)

func (fs *FSObjects) tmpGetUploadIDDir(bucket, object, uploadID string) string {
	return pathJoin(fs.tmpGetMultipartSHADir(bucket, object), uploadID)
}

func (fs *FSObjects) tmpGetMultipartSHADir(bucket, object string) string {
	return getSHA256Hash([]byte(pathJoin(bucket, object)))
}

// Returns EXPORT/.minio.sys/multipart/SHA256/UPLOADID
func (fs *FSObjects) getUploadIDDir(bucket, object, uploadID string) string {
	return pathJoin(fs.disk.String(), minioMetaMultipartBucket, getSHA256Hash([]byte(pathJoin(bucket, object))), uploadID)
}

// Returns EXPORT/.minio.sys/multipart/SHA256
func (fs *FSObjects) getMultipartSHADir(bucket, object string) string {
	return pathJoin(fs.disk.String(), minioMetaMultipartBucket, getSHA256Hash([]byte(pathJoin(bucket, object))))
}

// Returns partNumber.etag
func (fs *FSObjects) encodePartFile(partNumber int, etag string, actualSize int64) string {
	return fmt.Sprintf("%.5d.%s.%d", partNumber, etag, actualSize)
}

// Returns partNumber and etag
func (fs *FSObjects) decodePartFile(name string) (partNumber int, etag string, actualSize int64, err error) {
	result := strings.Split(name, ".")
	if len(result) != 3 {
		return 0, "", 0, errUnexpected
	}
	partNumber, err = strconv.Atoi(result[0])
	if err != nil {
		return 0, "", 0, errUnexpected
	}
	actualSize, err = strconv.ParseInt(result[2], 10, 64)
	if err != nil {
		return 0, "", 0, errUnexpected
	}
	return partNumber, result[1], actualSize, nil
}

// Appends parts to an appendFile sequentially.
func (fs *FSObjects) backgroundAppend(ctx context.Context, bucket, object, uploadID string, metaFi FileInfo) {
	fs.appendFileMapMu.Lock()
	logger.GetReqInfo(ctx).AppendTags("uploadID", uploadID)
	file := fs.appendFileMap[uploadID]
	if file == nil {
		file = &fsAppendFile{
			filePath: pathJoin(fs.fsUUID, fmt.Sprintf("%s.%s", uploadID, mustGetUUID())),
		}
		fs.appendFileMap[uploadID] = file
	}
	fs.appendFileMapMu.Unlock()

	file.Lock()
	defer file.Unlock()

	// Since we append sequentially nextPartNumber will always be len(file.parts)+1
	nextPartNumber := len(file.parts) + 1
	uploadIDPath := fs.tmpGetUploadIDDir(bucket, object, uploadID)

	sort.Slice(metaFi.Parts, func(i, j int) bool { return metaFi.Parts[i].Number < metaFi.Parts[j].Number })

	for _, part := range metaFi.Parts {
		partNumber, etag, actualSize := part.Number, part.ETag, part.ActualSize
		if partNumber < nextPartNumber {
			// Part already appended.
			continue
		}
		if partNumber > nextPartNumber {
			// Required part number is not yet uploaded.
			return
		}

		partPath := pathJoin(uploadIDPath, fs.encodePartFile(partNumber, etag, actualSize))
		entryBuf, err := fs.disk.ReadAll(ctx, minioMetaMultipartBucket, partPath)
		if err != nil {
			reqInfo := logger.GetReqInfo(ctx).AppendTags("partPath", partPath)
			reqInfo.AppendTags("filepath", file.filePath)
			logger.LogIf(ctx, err)
			return
		}
		err = fs.disk.AppendFile(ctx, minioMetaTmpBucket, file.filePath, entryBuf)
		if err != nil {
			reqInfo := logger.GetReqInfo(ctx).AppendTags("partPath", partPath)
			reqInfo.AppendTags("filepath", file.filePath)
			logger.LogIf(ctx, err)
			return
		}

		file.parts = append(file.parts, partNumber)
		nextPartNumber++
	}
}

// ListMultipartUploads - lists all the uploadIDs for the specified object.
// We do not support prefix based listing.
func (fs *FSObjects) ListMultipartUploads(ctx context.Context, bucket, object, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	var result ListMultipartsInfo
	var err error

	if err := checkListMultipartArgs(ctx, bucket, object, keyMarker, uploadIDMarker, delimiter, fs); err != nil {
		return ListMultipartsInfo{}, toObjectErr(err)
	}

	if _, err := fs.disk.StatVol(ctx, bucket); err != nil {
		return ListMultipartsInfo{}, toObjectErr(err, bucket)
	}

	result.MaxUploads = maxUploads
	result.KeyMarker = keyMarker
	result.Prefix = object
	result.Delimiter = delimiter

	var uploadIDs []string
	uploadIDs, err = fs.disk.ListDir(ctx, minioMetaMultipartBucket, fs.tmpGetMultipartSHADir(bucket, object), -1)
	if err != nil {
		if err == errFileNotFound {
			return result, nil
		}
		logger.LogIf(ctx, err)
		return ListMultipartsInfo{}, toObjectErr(err, bucket, object)
	}

	for i := range uploadIDs {
		uploadIDs[i] = strings.TrimSuffix(uploadIDs[i], SlashSeparator)
	}

	// S3 spec says uploadIDs should be sorted based on initiated time, we need
	// to read the metadata entry.
	var uploads []MultipartInfo

	populatedUploadIds := set.NewStringSet()

	for _, uploadID := range uploadIDs {
		if populatedUploadIds.Contains(uploadID) {
			continue
		}
		fi, err := fs.disk.ReadVersion(ctx, minioMetaMultipartBucket, pathJoin(fs.tmpGetUploadIDDir(bucket, object, uploadID)), "", false)
		if err != nil {
			return ListMultipartsInfo{}, toObjectErr(err, bucket, object)
		}
		populatedUploadIds.Add(uploadID)
		uploads = append(uploads, MultipartInfo{
			Object:    object,
			UploadID:  uploadID,
			Initiated: fi.ModTime,
		})
	}
	sort.Slice(uploads, func(i int, j int) bool {
		return uploads[i].Initiated.Before(uploads[j].Initiated)
	})

	uploadIndex := 0
	if uploadIDMarker != "" {
		for uploadIndex < len(uploads) {
			if uploads[uploadIndex].UploadID != uploadIDMarker {
				uploadIndex++
				continue
			}
			if uploads[uploadIndex].UploadID == uploadIDMarker {
				uploadIndex++
				break
			}
			uploadIndex++
		}
	}
	for uploadIndex < len(uploads) {
		result.Uploads = append(result.Uploads, uploads[uploadIndex])
		result.NextUploadIDMarker = uploads[uploadIndex].UploadID
		uploadIndex++
		if len(result.Uploads) == maxUploads {
			break
		}
	}

	result.IsTruncated = uploadIndex < len(uploads)

	if !result.IsTruncated {
		result.NextKeyMarker = ""
		result.NextUploadIDMarker = ""
	}

	return result, nil
}

// NewMultipartUpload - initialize a new multipart upload, returns a
// unique id. The unique id returned here is of UUID form, for each
// subsequent request each UUID is unique.
//
// Implements S3 compatible initiate multipart API.
func (fs *FSObjects) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (string, error) {
	if err := checkNewMultipartArgs(ctx, bucket, object, fs); err != nil {
		return "", toObjectErr(err, bucket)
	}

	if _, err := fs.disk.StatVol(ctx, bucket); err != nil {
		return "", toObjectErr(err, bucket)
	}

	// No metadata is set, allocate a new one.
	if opts.UserDefined == nil {
		opts.UserDefined = make(map[string]string)
	}

	if opts.UserDefined["content-type"] == "" {
		contentType := mimedb.TypeByExtension(pathutil.Ext(object))
		opts.UserDefined["content-type"] = contentType
	}

	fi := FileInfo{}

	// Calculate the version to be saved.
	if opts.Versioned {
		fi.VersionID = opts.VersionID
		if fi.VersionID == "" {
			fi.VersionID = mustGetUUID()
		}
	}

	fi.DataDir = mustGetUUID()
	fi.ModTime = UTCNow()
	fi.Metadata = cloneMSS(opts.UserDefined)

	uploadID := mustGetUUID()
	uploadIDPath := fs.tmpGetUploadIDDir(bucket, object, uploadID)
	tempUploadIDPath := pathJoin(fs.fsUUID, uploadID)

	defer func() {
		fs.deleteObject(context.Background(), minioMetaTmpBucket, tempUploadIDPath)
	}()

	if err := fs.disk.WriteMetadata(ctx, minioMetaTmpBucket, tempUploadIDPath, fi); err != nil {
		return "", toObjectErr(err, minioMetaTmpBucket, tempUploadIDPath)
	}

	if err := fs.disk.RenameFile(ctx, minioMetaTmpBucket, tempUploadIDPath, minioMetaMultipartBucket, uploadIDPath); err != nil {
		return "", toObjectErr(err, minioMetaMultipartBucket, uploadIDPath)
	}

	return uploadID, nil
}

// CopyObjectPart - similar to PutObjectPart but reads data from an existing
// object. Internally incoming data is written to '.minio.sys/tmp' location
// and safely renamed to '.minio.sys/multipart' for reach parts.
func (fs *FSObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int,
	startOffset int64, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (pi PartInfo, e error) {

	if srcOpts.VersionID != "" && srcOpts.VersionID != nullVersionID {
		return pi, VersionNotFound{
			Bucket:    srcBucket,
			Object:    srcObject,
			VersionID: srcOpts.VersionID,
		}
	}

	if err := checkNewMultipartArgs(ctx, srcBucket, srcObject, fs); err != nil {
		return pi, toObjectErr(err)
	}

	partInfo, err := fs.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID, srcInfo.PutObjReader, dstOpts)
	if err != nil {
		return pi, toObjectErr(err, dstBucket, dstObject)
	}

	return partInfo, nil
}

type CountingReader struct {
	bytesRead    int64
	targetReader *PutObjReader
}

func (c *CountingReader) Read(p []byte) (n int, err error) {
	n, err = c.targetReader.Read(p)
	if err == nil || err == io.EOF {
		c.bytesRead += int64(n)
	}
	return n, err
}

// PutObjectPart - reads incoming data until EOF for the part file on
// an ongoing multipart transaction. Internally incoming data is
// written to '.minio.sys/tmp' location and safely renamed to
// '.minio.sys/multipart' for reach parts.
func (fs *FSObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *PutObjReader, opts ObjectOptions) (pi PartInfo, e error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return pi, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}

	if err := checkPutObjectPartArgs(ctx, bucket, object, fs); err != nil {
		return pi, toObjectErr(err, bucket)
	}

	uploadIDLock := fs.NewNSLock(bucket, pathJoin(object, uploadID))
	ctx, err := uploadIDLock.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		return pi, err
	}
	readLocked := true
	defer func() {
		if readLocked {
			uploadIDLock.RUnlock()
		}
	}()

	cReader := &CountingReader{
		targetReader: r,
	}

	if _, err := fs.disk.StatVol(ctx, bucket); err != nil {
		return pi, toObjectErr(err, bucket)
	}

	// Validate input data size and it can never be less than -1.
	if r.Size() < -1 {
		logger.LogIf(ctx, errInvalidArgument, logger.Application)
		return pi, toObjectErr(errInvalidArgument)
	}

	uploadIDPath := fs.tmpGetUploadIDDir(bucket, object, uploadID)

	// Just check if the uploadID exists to avoid copy if it doesn't.
	fi, err := fs.disk.ReadVersion(ctx, minioMetaMultipartBucket, uploadIDPath, "", false)
	if err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return pi, InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
		}
		return pi, toObjectErr(err, bucket, object)
	}

	tmpPartPath := pathJoin(fs.fsUUID, uploadID+"."+mustGetUUID()+"."+strconv.Itoa(partID))

	defer func() {
		fs.deleteObject(context.Background(), minioMetaTmpBucket, tmpPartPath)
	}()

	err = fs.disk.CreateFile(ctx, minioMetaTmpBucket, tmpPartPath, r.Size(), cReader)
	if err != nil {
		// Should return IncompleteBody{} error when reader has fewer
		// bytes than specified in request header.
		if err == errLessData || err == errMoreData {
			return pi, IncompleteBody{Bucket: bucket, Object: object}
		}
		return pi, toObjectErr(err, minioMetaTmpBucket, object)
	}

	uploadIDLock.RUnlock()
	readLocked = false
	ctx, err = uploadIDLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return pi, err
	}
	defer uploadIDLock.Unlock()

	fi, err = fs.disk.ReadVersion(ctx, minioMetaMultipartBucket, uploadIDPath, "", false)
	if err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return pi, InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
		}
		return pi, toObjectErr(err, bucket, object)
	}

	etag := r.MD5CurrentHexString()
	if etag == "" {
		etag = GenETag()
	}

	// Rename temporary part file to its final location.
	partPath := pathJoin(uploadIDPath, fs.encodePartFile(partID, etag, r.ActualSize()))
	err = fs.disk.RenameFile(ctx, minioMetaTmpBucket, tmpPartPath, minioMetaMultipartBucket, partPath)
	if err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return pi, InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
		}
		return pi, toObjectErr(err, minioMetaMultipartBucket, partPath)
	}

	fi.ModTime = UTCNow()
	fi.AddObjectPart(partID, etag, cReader.bytesRead, r.ActualSize())

	if err = fs.disk.WriteMetadata(ctx, minioMetaMultipartBucket, uploadIDPath, fi); err != nil {
		return pi, toObjectErr(err, minioMetaMultipartBucket, uploadIDPath)
	}

	go fs.backgroundAppend(ctx, bucket, object, uploadID, fi)

	return PartInfo{
		PartNumber:   partID,
		LastModified: fi.ModTime,
		ETag:         etag,
		Size:         cReader.bytesRead,
		ActualSize:   r.ActualSize(),
	}, nil
}

// GetMultipartInfo returns multipart metadata uploaded during newMultipartUpload, used
// by callers to verify object states
// - encrypted
// - compressed
func (fs *FSObjects) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (MultipartInfo, error) {
	minfo := MultipartInfo{
		Bucket:   bucket,
		Object:   object,
		UploadID: uploadID,
	}

	if err := checkListPartsArgs(ctx, bucket, object, fs); err != nil {
		return minfo, toObjectErr(err)
	}

	var err error
	uploadIDLock := fs.NewNSLock(bucket, pathJoin(object, uploadID))
	ctx, err = uploadIDLock.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		return MultipartInfo{}, err
	}
	defer uploadIDLock.RUnlock()

	if _, err := fs.disk.StatVol(ctx, bucket); err != nil {
		return MultipartInfo{}, toObjectErr(err, bucket)
	}

	uploadIDPath := fs.tmpGetUploadIDDir(bucket, object, uploadID)
	// Just check if the uploadID exists to avoid copy if it doesn't.
	fi, err := fs.disk.ReadVersion(ctx, minioMetaMultipartBucket, uploadIDPath, "", false)
	if err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return MultipartInfo{}, InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
		}
		return MultipartInfo{}, toObjectErr(err, bucket, object)
	}

	minfo.UserDefined = cloneMSS(fi.Metadata)
	return minfo, nil
}

// ListObjectParts - lists all previously uploaded parts for a given
// object and uploadID.  Takes additional input of part-number-marker
// to indicate where the listing should begin from.
//
// Implements S3 compatible ListObjectParts API. The resulting
// ListPartsInfo structure is unmarshalled directly into XML and
// replied back to the client.
func (fs *FSObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker, maxParts int, opts ObjectOptions) (ListPartsInfo, error) {
	result := ListPartsInfo{}

	if err := checkListPartsArgs(ctx, bucket, object, fs); err != nil {
		return ListPartsInfo{}, toObjectErr(err)
	}

	uploadIDLock := fs.NewNSLock(bucket, pathJoin(object, uploadID))
	ctx, err := uploadIDLock.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		return ListPartsInfo{}, err
	}
	defer uploadIDLock.RUnlock()

	// Check if bucket exists
	if _, err := fs.disk.StatVol(ctx, bucket); err != nil {
		return ListPartsInfo{}, toObjectErr(err, bucket)
	}

	uploadIDPath := fs.tmpGetUploadIDDir(bucket, object, uploadID)

	// Just check if the uploadID exists to avoid copy if it doesn't.
	fi, err := fs.disk.ReadVersion(ctx, minioMetaMultipartBucket, uploadIDPath, "", false)
	if err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return ListPartsInfo{}, InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
		}
		return ListPartsInfo{}, toObjectErr(err, bucket, object)
	}

	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	result.MaxParts = maxParts
	result.PartNumberMarker = partNumberMarker
	result.UserDefined = cloneMSS(fi.Metadata)

	// For empty number of parts or maxParts as zero, return right here.
	if len(fi.Parts) == 0 || maxParts == 0 {
		return result, nil
	}

	// Limit output to maxPartsList.
	if maxParts > maxPartsList {
		maxParts = maxPartsList
	}

	// Only parts with higher part numbers will be listed.
	partIdx := objectPartIndex(fi.Parts, partNumberMarker)
	parts := fi.Parts
	if partIdx != -1 {
		parts = fi.Parts[partIdx+1:]
	}
	count := maxParts
	for _, part := range parts {
		result.Parts = append(result.Parts, PartInfo{
			PartNumber:   part.Number,
			ETag:         part.ETag,
			LastModified: fi.ModTime,
			Size:         part.Size,
		})
		count--
		if count == 0 {
			break
		}
	}
	// If listed entries are more than maxParts, we set IsTruncated as true.
	if len(parts) > len(result.Parts) {
		result.IsTruncated = true
		// Make sure to fill next part number marker if IsTruncated is
		// true for subsequent listing.
		nextPartNumberMarker := result.Parts[len(result.Parts)-1].PartNumber
		result.NextPartNumberMarker = nextPartNumberMarker
	}
	return result, nil
}

// CompleteMultipartUpload - completes an ongoing multipart
// transaction after receiving all the parts indicated by the client.
// Returns an md5sum calculated by concatenating all the individual
// md5sums of all the parts.
//
// Implements S3 compatible Complete multipart API.
func (fs *FSObjects) CompleteMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, parts []CompletePart, opts ObjectOptions) (ObjectInfo, error) {
	opts.ParentIsObject = fs.parentDirIsObject

	var err error
	if err = checkCompleteMultipartArgs(ctx, bucket, object, fs); err != nil {
		return ObjectInfo{}, toObjectErr(err)
	}

	// Hold read-locks to verify uploaded parts, also disallows
	// parallel part uploads as well.
	uploadIDLock := fs.NewNSLock(bucket, pathJoin(object, uploadID))
	ctx, err = uploadIDLock.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer uploadIDLock.RUnlock()

	if _, err := fs.disk.StatVol(ctx, bucket); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket)
	}

	uploadIDPath := fs.tmpGetUploadIDDir(bucket, object, uploadID)
	// Just check if the uploadID exists to avoid copy if it doesn't.
	fi, err := fs.disk.ReadVersion(ctx, minioMetaMultipartBucket, uploadIDPath, "", false)
	if err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return ObjectInfo{}, InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
		}
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Check if an object is present as one of the parent dir.
	// -- FIXME. (needs a new kind of lock).
	if opts.ParentIsObject != nil && opts.ParentIsObject(ctx, bucket, path.Dir(object)) {
		return ObjectInfo{}, toObjectErr(errFileParentIsFile, bucket, object)
	}

	defer ObjectPathUpdated(pathJoin(bucket, object))

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5 := getCompleteMultipartMD5(parts)

	// Calculate full object size.
	var objectSize int64

	// Calculate consolidated actual size.
	var objectActualSize int64

	// Save current erasure metadata for validation.
	var currentFI = fi

	// Allocate parts similar to incoming slice.
	fi.Parts = make([]ObjectPartInfo, len(parts))

	// Validate all parts and then commit to disk.
	for i, part := range parts {
		partIdx := objectPartIndex(currentFI.Parts, part.PartNumber)
		// All parts should have same part number.
		if partIdx == -1 {
			invp := InvalidPart{
				PartNumber: part.PartNumber,
				GotETag:    part.ETag,
			}
			return ObjectInfo{}, invp
		}

		// ensure that part ETag is canonicalized to strip off extraneous quotes
		part.ETag = canonicalizeETag(part.ETag)
		if currentFI.Parts[partIdx].ETag != part.ETag {
			invp := InvalidPart{
				PartNumber: part.PartNumber,
				ExpETag:    currentFI.Parts[partIdx].ETag,
				GotETag:    part.ETag,
			}
			return ObjectInfo{}, invp
		}

		// All parts except the last part has to be atleast 5MB.
		if (i < len(parts)-1) && !isMinAllowedPartSize(currentFI.Parts[partIdx].ActualSize) {
			return ObjectInfo{}, PartTooSmall{
				PartNumber: part.PartNumber,
				PartSize:   currentFI.Parts[partIdx].ActualSize,
				PartETag:   part.ETag,
			}
		}

		// Save for total object size.
		objectSize += currentFI.Parts[partIdx].Size

		// Save the consolidated actual size.
		objectActualSize += currentFI.Parts[partIdx].ActualSize

		// Add incoming parts.
		fi.Parts[i] = ObjectPartInfo{
			Number:     part.PartNumber,
			ETag:       currentFI.Parts[partIdx].ETag,
			Size:       currentFI.Parts[partIdx].Size,
			ActualSize: currentFI.Parts[partIdx].ActualSize,
		}
	}

	appendFallback := true // In case background-append did not append the required parts.
	appendFilePath := pathJoin(fs.fsUUID, fmt.Sprintf("%s.%s", uploadID, mustGetUUID()))

	// Most of the times appendFile would already be fully appended by now. We call fs.backgroundAppend()
	// to take care of the following corner case:
	// 1. The last PutObjectPart triggers go-routine fs.backgroundAppend, this go-routine has not started yet.
	// 2. Now CompleteMultipartUpload gets called which sees that lastPart is not appended and starts appending
	//    from the beginning
	fs.backgroundAppend(ctx, bucket, object, uploadID, currentFI)

	fs.appendFileMapMu.Lock()
	file := fs.appendFileMap[uploadID]
	delete(fs.appendFileMap, uploadID)
	fs.appendFileMapMu.Unlock()

	if file != nil {
		file.Lock()
		defer file.Unlock()
		// Verify that appendFile has all the parts.
		if len(file.parts) == len(parts) {
			for i := range parts {
				partIdx := objectPartIndex(currentFI.Parts, i)
				// All parts should have same part number.
				if partIdx == -1 {
					break
				}
				metaPart := currentFI.Parts[partIdx]
				if canonicalizeETag(parts[i].ETag) != metaPart.ETag {
					break
				}
				if parts[i].PartNumber != metaPart.Number {
					break
				}
				if i == len(parts)-1 {
					appendFilePath = file.filePath
					appendFallback = false
				}
			}
		}
	}

	if appendFallback {
		if file != nil {
			fs.disk.Delete(ctx, minioMetaTmpBucket, file.filePath, false)
		}
		for _, fiPart := range fi.Parts {
			partPath := pathJoin(uploadIDPath, fs.encodePartFile(fiPart.Number, fiPart.ETag, fiPart.ActualSize))
			entryBuf, err := fs.disk.ReadAll(ctx, minioMetaMultipartBucket, partPath)
			if err != nil {
				logger.LogIf(ctx, err)
				return ObjectInfo{}, toObjectErr(err, bucket, object)
			}
			err = fs.disk.AppendFile(ctx, minioMetaTmpBucket, appendFilePath, entryBuf)
			if err != nil {
				logger.LogIf(ctx, err)
				return ObjectInfo{}, toObjectErr(err, bucket, object)
			}
		}
	}

	// Save the final object size and modtime.
	fi.Size = objectSize
	fi.ModTime = opts.MTime
	if opts.MTime.IsZero() {
		fi.ModTime = UTCNow()
	}

	// Save successfully calculated md5sum.
	fi.Metadata["etag"] = s3MD5
	if opts.UserDefined["etag"] != "" { // preserve ETag if set
		fi.Metadata["etag"] = opts.UserDefined["etag"]
	}

	// Save the consolidated actual size.
	fi.Metadata[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(objectActualSize, 10)

	if err = fs.disk.WriteMetadata(ctx, minioMetaMultipartBucket, uploadIDPath, fi); err != nil {
		return ObjectInfo{}, toObjectErr(err, minioMetaMultipartBucket, uploadIDPath)
	}

	dataDir := mustGetUUID()
	tempObjFile := pathJoin(uploadIDPath, dataDir, "part.1")
	fs.disk.RenameFile(ctx, minioMetaTmpBucket, appendFilePath, minioMetaMultipartBucket, tempObjFile)

	lk := fs.NewNSLock(bucket, object)
	ctx, err = lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer lk.Unlock()

	// Rename the multipart object to final location.
	if err = fs.disk.RenameData(ctx, minioMetaMultipartBucket, uploadIDPath, dataDir, bucket, object); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	if err = fs.disk.Delete(ctx, minioMetaMultipartBucket, uploadIDPath, true); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Success, return object info.
	return fi.ToObjectInfo(bucket, object), nil
}

// AbortMultipartUpload - aborts an ongoing multipart operation
// signified by the input uploadID. This is an atomic operation
// doesn't require clients to initiate multiple such requests.
//
// All parts are purged from all disks and reference to the uploadID
// would be removed from the system, rollback is not possible on this
// operation.
//
// Implements S3 compatible Abort multipart API, slight difference is
// that this is an atomic idempotent operation. Subsequent calls have
// no affect and further requests to the same uploadID would not be
// honored.
func (fs *FSObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) error {
	if err := checkAbortMultipartArgs(ctx, bucket, object, fs); err != nil {
		return err
	}

	lk := fs.NewNSLock(bucket, pathJoin(object, uploadID))
	ctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return err
	}
	defer lk.Unlock()

	if _, err := fs.disk.StatVol(ctx, bucket); err != nil {
		return toObjectErr(err, bucket)
	}

	uploadIDPath := fs.tmpGetUploadIDDir(bucket, object, uploadID)

	// Just check if the uploadID exists to avoid copy if it doesn't.
	_, err = fs.disk.ReadVersion(ctx, minioMetaMultipartBucket, uploadIDPath, "", false)
	if err != nil {
		if err == errFileNotFound || err == errFileAccessDenied {
			return InvalidUploadID{Bucket: bucket, Object: object, UploadID: uploadID}
		}
		return toObjectErr(err, bucket, object)
	}

	fs.appendFileMapMu.Lock()
	// Remove file in tmp folder
	file := fs.appendFileMap[uploadID]
	if file != nil {
		fs.disk.Delete(ctx, minioMetaTmpBucket, file.filePath, false)
	}
	delete(fs.appendFileMap, uploadID)
	fs.appendFileMapMu.Unlock()

	if err = fs.disk.Delete(ctx, minioMetaMultipartBucket, uploadIDPath, true); err != nil {
		return toObjectErr(err, bucket, object)
	}

	return nil
}

// Removes multipart uploads if any older than `expiry` duration
// on all buckets for every `cleanupInterval`, this function is
// blocking and should be run in a go-routine.
func (fs *FSObjects) cleanupStaleUploads(ctx context.Context, cleanupInterval, expiry time.Duration) {
	timer := time.NewTimer(cleanupInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			// Reset for the next interval
			timer.Reset(cleanupInterval)

			now := time.Now()
			entries, err := readDir(pathJoin(fs.disk.String(), minioMetaMultipartBucket))
			if err != nil {
				continue
			}
			for _, entry := range entries {
				uploadIDs, err := readDir(pathJoin(fs.disk.String(), minioMetaMultipartBucket, entry))
				if err != nil {
					continue
				}

				// Remove the trailing slash separator
				for i := range uploadIDs {
					uploadIDs[i] = strings.TrimSuffix(uploadIDs[i], SlashSeparator)
				}

				for _, uploadID := range uploadIDs {
					fi, err := fsStatDir(ctx, pathJoin(fs.disk.String(), minioMetaMultipartBucket, entry, uploadID))
					if err != nil {
						continue
					}
					if now.Sub(fi.ModTime()) > expiry {
						fsRemoveAll(ctx, pathJoin(fs.disk.String(), minioMetaMultipartBucket, entry, uploadID))
						// It is safe to ignore any directory not empty error (in case there were multiple uploadIDs on the same object)
						fsRemoveDir(ctx, pathJoin(fs.disk.String(), minioMetaMultipartBucket, entry))

						// Remove uploadID from the append file map and its corresponding temporary file
						fs.appendFileMapMu.Lock()
						bgAppend, ok := fs.appendFileMap[uploadID]
						if ok {
							_ = fsRemoveFile(ctx, bgAppend.filePath)
							delete(fs.appendFileMap, uploadID)
						}
						fs.appendFileMapMu.Unlock()
					}
				}
			}
		}
	}
}
