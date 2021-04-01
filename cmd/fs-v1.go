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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/user"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio/pkg/bucket/lifecycle"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/cmd/config"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/color"
	xioutil "github.com/minio/minio/pkg/ioutil"
	"github.com/minio/minio/pkg/lock"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/minio/pkg/mimedb"
	"github.com/minio/minio/pkg/mountinfo"
)

// Default etag is used for pre-existing objects.
var defaultEtag = "00000000000000000000000000000000-1"

// FSObjects - Implements fs object layer.
type FSObjects struct {
	GatewayUnsupported

	// The storage backend behind this object layer
	disk StorageAPI

	// The count of concurrent calls on FSObjects API
	activeIOCount int64

	// Unique value to be used for all
	// temporary transactions.
	fsUUID string

	// This value shouldn't be touched, once initialized.
	fsFormatRlk *lock.RLockedFile // Is a read lock on `format.json`.

	// TODO: delete, has been moved to fsV1Storage
	// FS rw pool.
	rwPool *fsIOPool

	diskMount bool

	appendFileMap   map[string]*fsAppendFile
	appendFileMapMu sync.Mutex

	// To manage the appendRoutine go-routines
	nsMutex *nsLockMap
}

// Represents the background append file.
type fsAppendFile struct {
	sync.Mutex
	parts    []PartInfo // List of parts appended.
	filePath string     // Absolute path of the file in the temp location.
}

// NewFSObjectLayer - initialize new fs object layer.
func NewFSObjectLayer(fsPath string) (ObjectLayer, error) {
	ctx := GlobalContext

	disk, err := newLocalFSV1Storage(fsPath)
	if err != nil {
		if err == errMinDiskSize {
			return nil, config.ErrUnableToWriteInBackend(err).Hint(err.Error())
		}

		if os.IsPermission(err) {
			// Show a descriptive error with a hint about how to fix it.
			var username string
			if u, err := user.Current(); err == nil {
				username = u.Username
			} else {
				username = "<your-username>"
			}
			hint := fmt.Sprintf("Use 'sudo chown -R %s %s && sudo chmod u+rxw %s' to provide sufficient permissions.", username, fsPath, fsPath)
			return nil, config.ErrUnableToWriteInBackend(err).Hint(hint)
		}
		return nil, err
	}

	diskID, err := disk.GetDiskID()
	if err != nil {
		return nil, err
	}

	// Initialize fs objects.
	fs := &FSObjects{
		disk:   disk,
		fsUUID: diskID,
		rwPool: &fsIOPool{
			readersMap: make(map[string]*lock.RLockedFile),
		},
		nsMutex:       newNSLock(false),
		appendFileMap: make(map[string]*fsAppendFile),
		diskMount:     mountinfo.IsLikelyMountPoint(fsPath),
	}

	// Initialize `format.json`, this function also returns.
	rlk, err := initFormatFS(ctx, disk)
	if err != nil {
		return nil, err
	}

	// Once the filesystem has initialized hold the read lock for
	// the life time of the server. This is done to ensure that under
	// shared backend mode for FS, remote servers do not migrate
	// or cause changes on backend format.
	fs.fsFormatRlk = rlk

	go fs.cleanupStaleUploads(ctx, GlobalStaleUploadsCleanupInterval, GlobalStaleUploadsExpiry)
	go intDataUpdateTracker.start(ctx, fsPath)

	// Return successfully initialized object layer.
	return fs, nil
}

// NewNSLock - initialize a new namespace RWLocker instance.
func (fs *FSObjects) NewNSLock(bucket string, objects ...string) RWLocker {
	// lockers are explicitly 'nil' for FS mode since there are only local lockers
	return fs.nsMutex.NewNSLock(nil, bucket, objects...)
}

// SetDriveCounts no-op
func (fs *FSObjects) SetDriveCounts() []int {
	return nil
}

// Shutdown - should be called when process shuts down.
func (fs *FSObjects) Shutdown(ctx context.Context) error {
	fs.fsFormatRlk.Close()

	// Cleanup and delete tmp uuid.
	return fs.disk.DeleteVol(ctx, pathJoin(minioMetaTmpBucket, fs.fsUUID), true)
}

// BackendInfo - returns backend information
func (fs *FSObjects) BackendInfo() madmin.BackendInfo {
	return madmin.BackendInfo{Type: madmin.FS}
}

// LocalStorageInfo - returns underlying storage statistics.
func (fs *FSObjects) LocalStorageInfo(ctx context.Context) (StorageInfo, []error) {
	return fs.StorageInfo(ctx)
}

// StorageInfo - returns underlying storage statistics.
func (fs *FSObjects) StorageInfo(ctx context.Context) (StorageInfo, []error) {
	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	di, err := fs.disk.DiskInfo(ctx)
	if err != nil {
		return StorageInfo{}, []error{err}
	}
	storageInfo := StorageInfo{
		Disks: []madmin.Disk{
			{
				TotalSpace:     di.Total,
				UsedSpace:      di.Used,
				AvailableSpace: di.Free,
				DrivePath:      fs.disk.String(),
			},
		},
	}
	storageInfo.Backend.Type = madmin.FS
	return storageInfo, nil
}

// NSScanner returns data usage stats of the current FS deployment
func (fs *FSObjects) NSScanner(ctx context.Context, bf *bloomFilter, updates chan<- madmin.DataUsageInfo) error {
	// Load bucket totals
	var totalCache dataUsageCache
	err := totalCache.load(ctx, fs, dataUsageCacheName)
	if err != nil {
		return err
	}
	totalCache.Info.Name = dataUsageRoot
	buckets, err := fs.ListBuckets(ctx)
	if err != nil {
		return err
	}
	totalCache.Info.BloomFilter = bf.bytes()

	// Clear totals.
	var root dataUsageEntry
	if r := totalCache.root(); r != nil {
		root.Children = r.Children
	}
	totalCache.replace(dataUsageRoot, "", root)

	// Delete all buckets that does not exist anymore.
	totalCache.keepBuckets(buckets)

	for _, b := range buckets {
		// Load bucket cache.
		var bCache dataUsageCache
		err := bCache.load(ctx, fs, path.Join(b.Name, dataUsageCacheName))
		if err != nil {
			return err
		}
		if bCache.Info.Name == "" {
			bCache.Info.Name = b.Name
		}
		bCache.Info.BloomFilter = totalCache.Info.BloomFilter

		cache, err := fs.scanBucket(ctx, b.Name, bCache)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		logger.LogIf(ctx, err)
		cache.Info.BloomFilter = nil

		if cache.root() == nil {
			if intDataUpdateTracker.debug {
				logger.Info(color.Green("NSScanner:") + " No root added. Adding empty")
			}
			cache.replace(cache.Info.Name, dataUsageRoot, dataUsageEntry{})
		}
		if cache.Info.LastUpdate.After(bCache.Info.LastUpdate) {
			if intDataUpdateTracker.debug {
				logger.Info(color.Green("NSScanner:")+" Saving bucket %q cache with %d entries", b.Name, len(cache.Cache))
			}
			logger.LogIf(ctx, cache.save(ctx, fs, path.Join(b.Name, dataUsageCacheName)))
		}
		// Merge, save and send update.
		// We do it even if unchanged.
		cl := cache.clone()
		entry := cl.flatten(*cl.root())
		totalCache.replace(cl.Info.Name, dataUsageRoot, entry)
		if intDataUpdateTracker.debug {
			logger.Info(color.Green("NSScanner:")+" Saving totals cache with %d entries", len(totalCache.Cache))
		}
		totalCache.Info.LastUpdate = time.Now()
		logger.LogIf(ctx, totalCache.save(ctx, fs, dataUsageCacheName))
		cloned := totalCache.clone()
		updates <- cloned.dui(dataUsageRoot, buckets)
		enforceFIFOQuotaBucket(ctx, fs, b.Name, cloned.bucketUsageInfo(b.Name))
	}

	return nil
}

// scanBucket scans a single bucket in FS mode.
// The updated cache for the bucket is returned.
// A partially updated bucket may be returned.
func (fs *FSObjects) scanBucket(ctx context.Context, bucket string, cache dataUsageCache) (dataUsageCache, error) {
	// Get bucket policy
	// Check if the current bucket has a configured lifecycle policy
	lc, err := globalLifecycleSys.Get(bucket)
	if err == nil && lc.HasActiveRules("", true) {
		if intDataUpdateTracker.debug {
			logger.Info(color.Green("scanBucket:") + " lifecycle: Active rules found")
		}
		cache.Info.lifeCycle = lc
	}

	// Load bucket info.
	cache, err = scanDataFolder(ctx, fs.disk.String(), cache, func(item scannerItem) (sizeSummary, error) {
		bucket, object := item.bucket, item.objectPath()
		fsMetaBytes, err := xioutil.ReadFile(pathJoin(fs.disk.String(), minioMetaBucket, bucketMetaPrefix, bucket, object, fsMetaJSONFile))
		if err != nil && !osIsNotExist(err) {
			if intDataUpdateTracker.debug {
				logger.Info(color.Green("scanBucket:")+" object return unexpected error: %v/%v: %w", item.bucket, item.objectPath(), err)
			}
			return sizeSummary{}, errSkipFile
		}

		fsMeta := newFSMetaV1()
		metaOk := false
		if len(fsMetaBytes) > 0 {
			var json = jsoniter.ConfigCompatibleWithStandardLibrary
			if err = json.Unmarshal(fsMetaBytes, &fsMeta); err == nil {
				metaOk = true
			}
		}
		if !metaOk {
			fsMeta = defaultFsJSON(object)
		}

		// Stat the file.
		fi, fiErr := os.Stat(item.Path)
		if fiErr != nil {
			if intDataUpdateTracker.debug {
				logger.Info(color.Green("scanBucket:")+" object path missing: %v: %w", item.Path, fiErr)
			}
			return sizeSummary{}, errSkipFile
		}

		oi := fsMeta.ToObjectInfo(bucket, object, fi)
		sz := item.applyActions(ctx, fs, actionMeta{oi: oi})
		if sz >= 0 {
			return sizeSummary{totalSize: sz}, nil
		}

		return sizeSummary{totalSize: fi.Size()}, nil
	})

	return cache, err
}

/// Bucket operations

// MakeBucketWithLocation - create a new bucket, returns if it already exists.
func (fs *FSObjects) MakeBucketWithLocation(ctx context.Context, bucket string, opts BucketOptions) error {
	if opts.LockEnabled || opts.VersioningEnabled {
		return NotImplemented{}
	}

	// Verify if bucket is valid.
	if s3utils.CheckValidBucketNameStrict(bucket) != nil {
		return BucketNameInvalid{Bucket: bucket}
	}

	defer ObjectPathUpdated(bucket + slashSeparator)
	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	err := fs.disk.MakeVol(ctx, bucket)
	if err != nil {
		return toObjectErr(err, bucket)
	}

	meta := newBucketMetadata(bucket)
	if err := meta.Save(ctx, fs); err != nil {
		return toObjectErr(err, bucket)
	}

	globalBucketMetadataSys.Set(bucket, meta)

	return nil
}

// GetBucketPolicy - only needed for FS in NAS mode
func (fs *FSObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	meta, err := loadBucketMetadata(ctx, fs, bucket)
	if err != nil {
		return nil, BucketPolicyNotFound{Bucket: bucket}
	}
	if meta.policyConfig == nil {
		return nil, BucketPolicyNotFound{Bucket: bucket}
	}
	return meta.policyConfig, nil
}

// SetBucketPolicy - only needed for FS in NAS mode
func (fs *FSObjects) SetBucketPolicy(ctx context.Context, bucket string, p *policy.Policy) error {
	meta, err := loadBucketMetadata(ctx, fs, bucket)
	if err != nil {
		return err
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	configData, err := json.Marshal(p)
	if err != nil {
		return err
	}
	meta.PolicyConfigJSON = configData

	return meta.Save(ctx, fs)
}

// DeleteBucketPolicy - only needed for FS in NAS mode
func (fs *FSObjects) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	meta, err := loadBucketMetadata(ctx, fs, bucket)
	if err != nil {
		return err
	}
	meta.PolicyConfigJSON = nil
	return meta.Save(ctx, fs)
}

// GetBucketInfo - fetch bucket metadata info.
func (fs *FSObjects) GetBucketInfo(ctx context.Context, bucket string) (bi BucketInfo, e error) {
	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	st, err := fs.disk.StatVol(ctx, bucket)
	if err != nil {
		return bi, toObjectErr(err, bucket)
	}

	createdTime := st.Created
	meta, err := globalBucketMetadataSys.Get(bucket)
	if err == nil {
		createdTime = meta.Created
	}

	return BucketInfo{
		Name:    bucket,
		Created: createdTime,
	}, nil
}

// ListBuckets - list all s3 compatible buckets (directories) at fsPath.
func (fs *FSObjects) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	volsInfo, err := fs.disk.ListVols(ctx)
	if err != nil {
		return nil, err
	}
	bucketInfos := make([]BucketInfo, 0, len(volsInfo))
	for _, v := range volsInfo {
		// StorageAPI can send volume names which are
		// incompatible with buckets - these are
		// skipped, like the meta-bucket.
		if isReservedOrInvalidBucket(v.Name, false) {
			continue
		}
		bucketInfos = append(bucketInfos, BucketInfo(v))
	}

	// Sort bucket infos by bucket name.
	sort.Slice(bucketInfos, func(i, j int) bool {
		return bucketInfos[i].Name < bucketInfos[j].Name
	})

	// Succes.
	return bucketInfos, nil
}

// DeleteBucket - delete a bucket and all the metadata associated
// with the bucket including pending multipart, object metadata.
func (fs *FSObjects) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	defer ObjectPathUpdated(bucket + slashSeparator)

	// Attempt to delete regular bucket.
	if err := fs.disk.DeleteVol(ctx, bucket, forceDelete); err != nil {
		return toObjectErr(err, bucket)
	}

	// Cleanup all the bucket metadata.
	minioMetadataBucket := pathJoin(minioMetaBucket, bucketMetaPrefix, bucket)
	if err := fs.disk.DeleteVol(ctx, minioMetadataBucket, true); err != nil {
		return toObjectErr(err, bucket)
	}

	// Delete all bucket metadata.
	deleteBucketMetadata(ctx, fs, bucket)

	return nil
}

/// Object Operations

// CopyObject - copy object source object to destination object.
// if source object and destination object are same we only
// update metadata.
func (fs *FSObjects) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (oi ObjectInfo, err error) {
	if srcOpts.VersionID != "" && srcOpts.VersionID != nullVersionID {
		return oi, VersionNotFound{
			Bucket:    srcBucket,
			Object:    srcObject,
			VersionID: srcOpts.VersionID,
		}
	}

	cpSrcDstSame := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(dstBucket, dstObject))
	defer ObjectPathUpdated(path.Join(dstBucket, dstObject))

	if !cpSrcDstSame {
		objectDWLock := fs.NewNSLock(dstBucket, dstObject)
		ctx, err = objectDWLock.GetLock(ctx, globalOperationTimeout)
		if err != nil {
			return oi, err
		}
		defer objectDWLock.Unlock()
	}

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	if _, err := fs.disk.StatVol(ctx, srcBucket); err != nil {
		return oi, toObjectErr(err, srcBucket)
	}

	if cpSrcDstSame && srcInfo.metadataOnly {
		fsMetaPath := pathJoin(fs.disk.String(), minioMetaBucket, bucketMetaPrefix, srcBucket, srcObject, fsMetaJSONFile)
		wlk, err := fs.rwPool.Write(fsMetaPath)
		if err != nil {
			wlk, err = fs.rwPool.Create(fsMetaPath)
			if err != nil {
				logger.LogIf(ctx, err)
				return oi, toObjectErr(err, srcBucket, srcObject)
			}
		}
		// This close will allow for locks to be synchronized on `fs.json`.
		defer wlk.Close()

		// Save objects' metadata in `fs.json`.
		fsMeta := newFSMetaV1()
		if _, err = fsMeta.ReadFrom(ctx, wlk); err != nil {
			// For any error to read fsMeta, set default ETag and proceed.
			fsMeta = defaultFsJSON(srcObject)
		}

		fsMeta.Meta = cloneMSS(srcInfo.UserDefined)
		fsMeta.Meta["etag"] = srcInfo.ETag
		if _, err = fsMeta.WriteTo(wlk); err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}

		// Stat the file to get file size.
		fi, err := fsStatFile(ctx, pathJoin(fs.disk.String(), srcBucket, srcObject))
		if err != nil {
			return oi, toObjectErr(err, srcBucket, srcObject)
		}

		// Return the new object info.
		return fsMeta.ToObjectInfo(srcBucket, srcObject, fi), nil
	}

	if err := checkPutObjectArgs(ctx, dstBucket, dstObject, fs); err != nil {
		return ObjectInfo{}, err
	}

	objInfo, err := fs.putObject(ctx, dstBucket, dstObject, srcInfo.PutObjReader, ObjectOptions{ServerSideEncryption: dstOpts.ServerSideEncryption, UserDefined: srcInfo.UserDefined})
	if err != nil {
		return oi, toObjectErr(err, dstBucket, dstObject)
	}

	return objInfo, nil
}

// GetObjectNInfo - returns object info and a reader for object
// content.
func (fs *FSObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (gr *GetObjectReader, err error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return nil, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return nil, err
	}

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	if _, err = fs.disk.StatVol(ctx, bucket); err != nil {
		return nil, toObjectErr(err, bucket)
	}

	var nsUnlocker = func() {}

	if lockType != noLock {
		// Lock the object before reading.
		lock := fs.NewNSLock(bucket, object)
		switch lockType {
		case writeLock:
			ctx, err = lock.GetLock(ctx, globalOperationTimeout)
			if err != nil {
				return nil, err
			}
			nsUnlocker = lock.Unlock
		case readLock:
			ctx, err = lock.GetRLock(ctx, globalOperationTimeout)
			if err != nil {
				return nil, err
			}
			nsUnlocker = lock.RUnlock
		}
	}

	fi, err := fs.disk.ReadVersion(ctx, bucket, object, opts.VersionID, false)

	if err != nil {
		nsUnlocker()
		return nil, toObjectErr(err, bucket, object)
	}
	objInfo := fi.ToObjectInfo(bucket, object)

	// For a directory, we need to return a reader that returns no bytes.
	if HasSuffix(object, SlashSeparator) {
		// The lock taken above is released when
		// objReader.Close() is called by the caller.
		return NewGetObjectReaderFromReader(bytes.NewBuffer(nil), objInfo, opts, nsUnlocker)
	}

	if objInfo.DeleteMarker {
		nsUnlocker()
		if opts.VersionID == "" {
			return &GetObjectReader{
				ObjInfo: objInfo,
			}, toObjectErr(errFileNotFound, bucket, object)
		}

		// Make sure to return object info to provide extra information.
		return &GetObjectReader{
			ObjInfo: objInfo,
		}, toObjectErr(errMethodNotAllowed, bucket, object)
	}
	if objInfo.TransitionStatus == lifecycle.TransitionComplete {
		// If transitioned, stream from transition tier unless object is restored locally or restore date is past.
		restoreHdr, ok := caseInsensitiveMap(objInfo.UserDefined).Lookup(xhttp.AmzRestore)
		if !ok || !strings.HasPrefix(restoreHdr, "ongoing-request=false") || (!objInfo.RestoreExpires.IsZero() && time.Now().After(objInfo.RestoreExpires)) {
			transR, err := getTransitionedObjectReader(ctx, bucket, object, rs, h, objInfo, opts)
			nsUnlocker()
			return transR, err
		}
	}

	objReaderFn, off, length, err := NewGetObjectReader(rs, objInfo, opts, nsUnlocker)
	if err != nil {
		nsUnlocker()
		return nil, err
	}

	// Read the object, doesn't exist returns an s3 compatible error.
	ctxWithLockType := context.WithValue(ctx, lockTypeKey, lockType)
	readCloser, err := fs.disk.ReadFileStream(ctxWithLockType, bucket, object, off, length)
	if err != nil {
		nsUnlocker()
		return nil, toObjectErr(err, bucket, object)
	}

	closeFn := func() {
		readCloser.Close()
	}
	reader := io.LimitReader(readCloser, length)

	// Check if range is valid
	if off > objInfo.Size || off+length > objInfo.Size {
		err = InvalidRange{off, length, objInfo.Size}
		logger.LogIf(ctx, err, logger.Application)
		closeFn()
		nsUnlocker()
		return nil, err
	}

	return objReaderFn(reader, h, opts.CheckPrecondFn, closeFn)
}

// Create a new fs.json file, if the existing one is corrupt. Should happen very rarely.
func (fs *FSObjects) createFsJSON(object, fsMetaPath string) error {
	fsMeta := newFSMetaV1()
	fsMeta.Meta = map[string]string{
		"etag":         GenETag(),
		"content-type": mimedb.TypeByExtension(path.Ext(object)),
	}
	wlk, werr := fs.rwPool.Create(fsMetaPath)
	if werr == nil {
		_, err := fsMeta.WriteTo(wlk)
		wlk.Close()
		return err
	}
	return werr
}

// Used to return default etag values when a pre-existing object's meta data is queried.
func defaultFsJSON(object string) fsMetaV1 {
	fsMeta := newFSMetaV1()
	fsMeta.Meta = map[string]string{
		"etag":         defaultEtag,
		"content-type": mimedb.TypeByExtension(path.Ext(object)),
	}
	return fsMeta
}

// GetObjectInfo - reads object metadata and replies back ObjectInfo.
func (fs *FSObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (oi ObjectInfo, e error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return oi, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}

	var err error
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return oi, err
	}

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	if !opts.NoLock {
		// Lock the object before reading.
		lk := fs.NewNSLock(bucket, object)
		ctx, err = lk.GetRLock(ctx, globalOperationTimeout)
		if err != nil {
			return ObjectInfo{}, err
		}
		defer lk.RUnlock()
	}

	fi, err := fs.disk.ReadVersion(ctx, bucket, object, opts.VersionID, false)

	if err == errCorruptedFormat || err == io.EOF {
		// TODO: replace with fs.disk.WriteMetadata
		fsMetaPath := pathJoin(fs.disk.String(), minioMetaBucket, bucketMetaPrefix, bucket, object, fsMetaJSONFile)
		err = fs.createFsJSON(object, fsMetaPath)
		if err != nil {
			return oi, toObjectErr(err, bucket, object)
		}

		fi, err = fs.disk.ReadVersion(ctx, bucket, object, opts.VersionID, false)
	}

	if err != nil {
		return oi, toObjectErr(err, bucket, object)
	}

	oi = fi.ToObjectInfo(bucket, object)
	if oi.TransitionStatus == lifecycle.TransitionComplete {
		// overlay storage class for transitioned objects with transition tier SC Label
		if sc := transitionSC(ctx, bucket); sc != "" {
			oi.StorageClass = sc
		}
	}
	if !fi.VersionPurgeStatus.Empty() && opts.VersionID != "" {
		// Make sure to return object info to provide extra information.
		return oi, toObjectErr(errMethodNotAllowed, bucket, object)
	}

	if fi.Deleted {
		if opts.VersionID == "" || opts.DeleteMarker {
			return oi, toObjectErr(errFileNotFound, bucket, object)
		}
		// Make sure to return object info to provide extra information.
		return oi, toObjectErr(errMethodNotAllowed, bucket, object)
	}

	return oi, nil
}

// This function does the following check, suppose
// object is "a/b/c/d", stat makes sure that objects ""a/b/c""
// "a/b" and "a" do not exist.
func (fs *FSObjects) parentDirIsObject(ctx context.Context, bucket, parent string) bool {
	var isParentDirObject func(string) bool
	isParentDirObject = func(p string) bool {
		if p == "." || p == SlashSeparator {
			return false
		}
		if fsIsFile(ctx, pathJoin(fs.disk.String(), bucket, p)) {
			// If there is already a file at prefix "p", return true.
			return true
		}

		// Check if there is a file as one of the parent paths.
		return isParentDirObject(path.Dir(p))
	}
	return isParentDirObject(parent)
}

// PutObject - creates an object upon reading from the input stream
// until EOF, writes data directly to configured filesystem path.
// Additionally writes `fs.json` which carries the necessary metadata
// for future object operations.
func (fs *FSObjects) PutObject(ctx context.Context, bucket string, object string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if opts.Versioned {
		return objInfo, NotImplemented{}
	}

	if err := checkPutObjectArgs(ctx, bucket, object, fs); err != nil {
		return ObjectInfo{}, err
	}

	// Lock the object.
	lk := fs.NewNSLock(bucket, object)
	ctx, err = lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		logger.LogIf(ctx, err)
		return objInfo, err
	}
	defer lk.Unlock()
	defer ObjectPathUpdated(path.Join(bucket, object))

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	return fs.putObject(ctx, bucket, object, r, opts)
}

// putObject - wrapper for PutObject
func (fs *FSObjects) putObject(ctx context.Context, bucket string, object string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, retErr error) {
	data := r.Reader

	// No metadata is set, allocate a new one.
	meta := cloneMSS(opts.UserDefined)
	var err error

	// Validate if bucket name is valid and exists.
	if _, err = fs.disk.StatVol(ctx, bucket); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket)
	}

	fsMeta := newFSMetaV1()
	fsMeta.Meta = meta

	// This is a special case with size as '0' and object ends
	// with a slash separator, we treat it like a valid operation
	// and return success.
	if isObjectDir(object, data.Size()) {
		// Check if an object is present as one of the parent dir.
		if fs.parentDirIsObject(ctx, bucket, path.Dir(object)) {
			return ObjectInfo{}, toObjectErr(errFileParentIsFile, bucket, object)
		}
		if err = mkdirAll(pathJoin(fs.disk.String(), bucket, object), 0777); err != nil {
			logger.LogIf(ctx, err)
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		var fi os.FileInfo
		if fi, err = fsStatDir(ctx, pathJoin(fs.disk.String(), bucket, object)); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		return fsMeta.ToObjectInfo(bucket, object, fi), nil
	}

	// Check if an object is present as one of the parent dir.
	if fs.parentDirIsObject(ctx, bucket, path.Dir(object)) {
		return ObjectInfo{}, toObjectErr(errFileParentIsFile, bucket, object)
	}

	// Validate input data size and it can never be less than zero.
	if data.Size() < -1 {
		logger.LogIf(ctx, errInvalidArgument, logger.Application)
		return ObjectInfo{}, errInvalidArgument
	}

	var wlk *lock.LockedFile
	if bucket != minioMetaBucket {
		bucketMetaDir := pathJoin(fs.disk.String(), minioMetaBucket, bucketMetaPrefix)
		fsMetaPath := pathJoin(bucketMetaDir, bucket, object, fsMetaJSONFile)
		wlk, err = fs.rwPool.Write(fsMetaPath)
		var freshFile bool
		if err != nil {
			wlk, err = fs.rwPool.Create(fsMetaPath)
			if err != nil {
				logger.LogIf(ctx, err)
				return ObjectInfo{}, toObjectErr(err, bucket, object)
			}
			freshFile = true
		}
		// This close will allow for locks to be synchronized on `fs.json`.
		defer wlk.Close()
		defer func() {
			// Remove meta file when PutObject encounters
			// any error and it is a fresh file.
			//
			// We should preserve the `fs.json` of any
			// existing object
			if retErr != nil && freshFile {
				tmpDir := pathJoin(fs.disk.String(), minioMetaTmpBucket, fs.fsUUID)
				fsRemoveMeta(ctx, bucketMetaDir, fsMetaPath, tmpDir)
			}
		}()
	}

	// Uploaded object will first be written to the temporary location which will eventually
	// be renamed to the actual location. It is first written to the temporary location
	// so that cleaning it up will be easy if the server goes down.
	tempObj := mustGetUUID()

	fsTmpObjPath := pathJoin(fs.disk.String(), minioMetaTmpBucket, fs.fsUUID, tempObj)
	bytesWritten, err := fsCreateFile(ctx, fsTmpObjPath, data, data.Size())

	// Delete the temporary object in the case of a
	// failure. If PutObject succeeds, then there would be
	// nothing to delete.
	defer fsRemoveFile(ctx, fsTmpObjPath)

	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}
	fsMeta.Meta["etag"] = r.MD5CurrentHexString()

	// Should return IncompleteBody{} error when reader has fewer
	// bytes than specified in request header.
	if bytesWritten < data.Size() {
		return ObjectInfo{}, IncompleteBody{Bucket: bucket, Object: object}
	}

	// Entire object was written to the temp location, now it's safe to rename it to the actual location.
	fsNSObjPath := pathJoin(fs.disk.String(), bucket, object)
	if err = fsRenameFile(ctx, fsTmpObjPath, fsNSObjPath); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	if bucket != minioMetaBucket {
		// Write FS metadata after a successful namespace operation.
		if _, err = fsMeta.WriteTo(wlk); err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
	}

	// Stat the file to fetch timestamp, size.
	fi, err := fsStatFile(ctx, pathJoin(fs.disk.String(), bucket, object))
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Success.
	return fsMeta.ToObjectInfo(bucket, object, fi), nil
}

// DeleteObjects - deletes an object from a bucket, this operation is destructive
// and there are no rollbacks supported.
func (fs *FSObjects) DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete, opts ObjectOptions) ([]DeletedObject, []error) {
	errs := make([]error, len(objects))
	dobjects := make([]DeletedObject, len(objects))
	for idx, object := range objects {
		if object.VersionID != "" {
			errs[idx] = VersionNotFound{
				Bucket:    bucket,
				Object:    object.ObjectName,
				VersionID: object.VersionID,
			}
			continue
		}
		_, errs[idx] = fs.DeleteObject(ctx, bucket, object.ObjectName, opts)
		if errs[idx] == nil || isErrObjectNotFound(errs[idx]) {
			dobjects[idx] = DeletedObject{
				ObjectName: object.ObjectName,
			}
			errs[idx] = nil
		}
	}
	return dobjects, errs
}

// DeleteObject - deletes an object from a bucket, this operation is destructive
// and there are no rollbacks supported.
func (fs *FSObjects) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return objInfo, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}

	// Acquire a write lock before deleting the object.
	lk := fs.NewNSLock(bucket, object)
	ctx, err = lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return objInfo, err
	}
	defer lk.Unlock()

	if err = checkDelObjArgs(ctx, bucket, object); err != nil {
		return objInfo, err
	}

	defer ObjectPathUpdated(path.Join(bucket, object))

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	if _, err = fs.disk.StatVol(ctx, bucket); err != nil {
		return objInfo, toObjectErr(err, bucket)
	}

	var rwlk *lock.LockedFile

	minioMetaBucketDir := pathJoin(fs.disk.String(), minioMetaBucket)
	fsMetaPath := pathJoin(minioMetaBucketDir, bucketMetaPrefix, bucket, object, fsMetaJSONFile)
	if bucket != minioMetaBucket {
		rwlk, err = fs.rwPool.Write(fsMetaPath)
		if err != nil && err != errFileNotFound {
			logger.LogIf(ctx, err)
			return objInfo, toObjectErr(err, bucket, object)
		}
	}

	// Delete the object.
	if err = fsDeleteFile(ctx, pathJoin(fs.disk.String(), bucket), pathJoin(fs.disk.String(), bucket, object)); err != nil {
		if rwlk != nil {
			rwlk.Close()
		}
		return objInfo, toObjectErr(err, bucket, object)
	}

	// Close fsMetaPath before deletion
	if rwlk != nil {
		rwlk.Close()
	}

	if bucket != minioMetaBucket {
		// Delete the metadata object.
		err = fsDeleteFile(ctx, minioMetaBucketDir, fsMetaPath)
		if err != nil && err != errFileNotFound {
			return objInfo, toObjectErr(err, bucket, object)
		}
	}
	return ObjectInfo{Bucket: bucket, Name: object}, nil
}

// getObjectETag is a helper function, which returns only the md5sum
// of the file on the disk.
func (fs *FSObjects) getObjectETag(ctx context.Context, bucket, entry string, lock bool) (string, error) {
	fsMetaPath := pathJoin(fs.disk.String(), minioMetaBucket, bucketMetaPrefix, bucket, entry, fsMetaJSONFile)

	var reader io.Reader
	var fi os.FileInfo
	var size int64
	if lock {
		// Read `fs.json` to perhaps contend with
		// parallel Put() operations.
		rlk, err := fs.rwPool.Open(fsMetaPath)
		// Ignore if `fs.json` is not available, this is true for pre-existing data.
		if err != nil && err != errFileNotFound {
			logger.LogIf(ctx, err)
			return "", toObjectErr(err, bucket, entry)
		}

		// If file is not found, we don't need to proceed forward.
		if err == errFileNotFound {
			return "", nil
		}

		// Read from fs metadata only if it exists.
		defer fs.rwPool.Close(fsMetaPath)

		// Fetch the size of the underlying file.
		fi, err = rlk.LockedFile.Stat()
		if err != nil {
			logger.LogIf(ctx, err)
			return "", toObjectErr(err, bucket, entry)
		}

		size = fi.Size()
		reader = io.NewSectionReader(rlk.LockedFile, 0, fi.Size())
	} else {
		var err error
		reader, size, err = fsOpenFile(ctx, fsMetaPath, 0)
		if err != nil {
			return "", toObjectErr(err, bucket, entry)
		}
	}

	// `fs.json` can be empty due to previously failed
	// PutObject() transaction, if we arrive at such
	// a situation we just ignore and continue.
	if size == 0 {
		return "", nil
	}

	fsMetaBuf, err := ioutil.ReadAll(reader)
	if err != nil {
		logger.LogIf(ctx, err)
		return "", toObjectErr(err, bucket, entry)
	}

	var fsMeta fsMetaV1
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(fsMetaBuf, &fsMeta); err != nil {
		return "", err
	}

	// Check if FS metadata is valid, if not return error.
	if !isFSMetaValid(fsMeta.Version) {
		logger.LogIf(ctx, errCorruptedFormat)
		return "", toObjectErr(errCorruptedFormat, bucket, entry)
	}

	return extractETag(fsMeta.Meta), nil
}

// ListObjectVersions not implemented for FS mode.
func (fs *FSObjects) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (loi ListObjectVersionsInfo, e error) {
	return loi, NotImplemented{}
}

// ListObjects - list all objects at prefix upto maxKeys., optionally delimited by '/'. Maintains the list pool
// state for future re-entrant list requests.
func (fs *FSObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {
	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	// Default is recursive, if delimiter is set then list non recursive.
	recursive := true
	if delimiter == SlashSeparator {
		recursive = false
	}
	opts := listPathOptions{
		Bucket:             bucket,
		Prefix:             prefix,
		Separator:          delimiter,
		Limit:              maxKeys,
		Marker:             marker,
		Recursive:          recursive,
		InclDeleted:        true,
		IncludeDirectories: delimiter == SlashSeparator,
		AskDisks:           0,
	}

	return fs.fsListObjects(ctx, opts)
}

func (fs *FSObjects) fsListObjects(ctx context.Context, opts listPathOptions) (ListObjectsInfo, error) {
	var loi ListObjectsInfo

	// Cancel upstream if we finish before we expect.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := checkListObjsArgs(ctx, opts.Bucket, opts.Prefix, opts.Marker, fs); err != nil {
		return loi, err
	}

	// Marker is set validate pre-condition.
	if opts.Marker != "" {
		// Marker not common with prefix is not implemented. Send an empty response
		if !HasPrefix(opts.Marker, opts.Prefix) {
			return loi, nil
		}
	}

	// With max keys of zero we have reached eof, return right here.
	if opts.Limit == 0 {
		return loi, nil
	}

	// For delimiter and prefix as '/' we do not list anything at all
	// since according to s3 spec we stop at the 'delimiter'
	// along // with the prefix. On a flat namespace with 'prefix'
	// as '/' we don't have any entries, since all the keys are
	// of form 'keyName/...'
	if opts.Separator == SlashSeparator && opts.Prefix == SlashSeparator {
		return loi, nil
	}

	r, w := io.Pipe()
	metaR, err := newMetacacheReader(r)
	if err != nil {
		return loi, err
	}

	// Make sure we close the pipe so blocked writes doesn't stay around.
	// todo - move to a pool
	defer r.CloseWithError(context.Canceled)

	go func() {
		werr := fs.disk.WalkDir(ctx, WalkDirOptions{
			Bucket:         opts.Bucket,
			BaseDir:        baseDirFromPrefix(opts.Prefix),
			Recursive:      opts.Recursive,
			ReportNotFound: false,
			FilterPrefix:   opts.Prefix,
		}, w)
		w.CloseWithError(werr)
		if werr != io.EOF && werr != nil &&
			werr.Error() != errFileNotFound.Error() &&
			werr.Error() != errVolumeNotFound.Error() &&
			!errors.Is(werr, context.Canceled) {
			logger.LogIf(ctx, werr)
		}
	}()

	var eof bool
	result := ListObjectsInfo{}

	opts.Limit = maxKeysPlusOne(opts.Limit, opts.Marker != "")
	entries, err := metaR.filter(opts)
	if err != nil {
		if err == io.EOF {
			eof = true
		} else {
			return loi, err
		}
	}

	if opts.Limit > 0 && entries.len() > opts.Limit {
		entries.truncate(opts.Limit)
		eof = false
	}

	if opts.Marker != "" && len(entries.o) > 0 && entries.o[0].name == opts.Marker {
		entries.o = entries.o[1:]
	}

	objInfoFound := entries.objectInfos(opts.Bucket, opts.Prefix, opts.Separator, func(objectInfoBuf []byte) (ObjectInfo, error) {
		oi := ObjectInfo{}
		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		err := json.Unmarshal(objectInfoBuf, &oi)
		return oi, err
	})

	for _, objInfo := range objInfoFound {
		if objInfo.IsDir && opts.Separator != "" {
			result.Prefixes = append(result.Prefixes, objInfo.Name)
			continue
		}
		result.Objects = append(result.Objects, objInfo)
	}

	if !eof {
		result.IsTruncated = true
		if len(objInfoFound) > 0 {
			result.NextMarker = objInfoFound[len(objInfoFound)-1].Name
		}
	}

	// Success.
	return result, nil
}

// fileInfoVersionsFS is a copy of fileInfos converts the metadata to FileInfoVersions where possible.
// Metadata that cannot be decoded is skipped.
func (m *metaCacheEntriesSorted) objectInfos(bucket, prefix, delimiter string, objectInfo func(object []byte) (ObjectInfo, error)) (objects []ObjectInfo) {
	objects = make([]ObjectInfo, 0, m.len())
	prevPrefix := ""
	for _, entry := range m.o {
		if entry.isObject() {
			if delimiter != "" {
				idx := strings.Index(strings.TrimPrefix(entry.name, prefix), delimiter)
				if idx >= 0 {
					idx = len(prefix) + idx + len(delimiter)
					currPrefix := entry.name[:idx]
					if currPrefix == prevPrefix {
						continue
					}
					prevPrefix = currPrefix
					objects = append(objects, ObjectInfo{
						IsDir:  true,
						Bucket: bucket,
						Name:   currPrefix,
					})
					continue
				}
			}

			oi, err := objectInfo(entry.metadata)
			if err == nil {
				objects = append(objects, oi)
			}
			continue
		}
		if entry.isDir() {
			if delimiter == "" {
				continue
			}
			idx := strings.Index(strings.TrimPrefix(entry.name, prefix), delimiter)
			if idx < 0 {
				continue
			}
			idx = len(prefix) + idx + len(delimiter)
			currPrefix := entry.name[:idx]
			if currPrefix == prevPrefix {
				continue
			}
			prevPrefix = currPrefix
			objects = append(objects, ObjectInfo{
				IsDir:  true,
				Bucket: bucket,
				Name:   currPrefix,
			})
		}
	}

	return objects
}

// GetObjectTags - get object tags from an existing object
func (fs *FSObjects) GetObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (*tags.Tags, error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return nil, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}
	oi, err := fs.GetObjectInfo(ctx, bucket, object, ObjectOptions{})
	if err != nil {
		return nil, err
	}

	return tags.ParseObjectTags(oi.UserTags)
}

// PutObjectTags - replace or add tags to an existing object
func (fs *FSObjects) PutObjectTags(ctx context.Context, bucket, object string, tags string, opts ObjectOptions) (ObjectInfo, error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		return ObjectInfo{}, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}

	fsMetaPath := pathJoin(fs.disk.String(), minioMetaBucket, bucketMetaPrefix, bucket, object, fsMetaJSONFile)
	fsMeta := fsMetaV1{}
	wlk, err := fs.rwPool.Write(fsMetaPath)
	if err != nil {
		wlk, err = fs.rwPool.Create(fsMetaPath)
		if err != nil {
			logger.LogIf(ctx, err)
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
	}
	// This close will allow for locks to be synchronized on `fs.json`.
	defer wlk.Close()

	// Read objects' metadata in `fs.json`.
	if _, err = fsMeta.ReadFrom(ctx, wlk); err != nil {
		// For any error to read fsMeta, set default ETag and proceed.
		fsMeta = defaultFsJSON(object)
	}

	// clean fsMeta.Meta of tag key, before updating the new tags
	delete(fsMeta.Meta, xhttp.AmzObjectTagging)

	// Do not update for empty tags
	if tags != "" {
		fsMeta.Meta[xhttp.AmzObjectTagging] = tags
	}

	if _, err = fsMeta.WriteTo(wlk); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Stat the file to get file size.
	fi, err := fsStatFile(ctx, pathJoin(fs.disk.String(), bucket, object))
	if err != nil {
		return ObjectInfo{}, err
	}

	return fsMeta.ToObjectInfo(bucket, object, fi), nil
}

// DeleteObjectTags - delete object tags from an existing object
func (fs *FSObjects) DeleteObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	return fs.PutObjectTags(ctx, bucket, object, "", opts)
}

// HealFormat - no-op for fs, Valid only for Erasure.
func (fs *FSObjects) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, NotImplemented{}
}

// HealObject - no-op for fs. Valid only for Erasure.
func (fs *FSObjects) HealObject(ctx context.Context, bucket, object, versionID string, opts madmin.HealOpts) (
	res madmin.HealResultItem, err error) {
	return res, NotImplemented{}
}

// HealBucket - no-op for fs, Valid only for Erasure.
func (fs *FSObjects) HealBucket(ctx context.Context, bucket string, opts madmin.HealOpts) (madmin.HealResultItem,
	error) {
	return madmin.HealResultItem{}, NotImplemented{}
}

// Walk a bucket, optionally prefix recursively, until we have returned
// all the content to objectInfo channel, it is callers responsibility
// to allocate a receive channel for ObjectInfo, upon any unhandled
// error walker returns error. Optionally if context.Done() is received
// then Walk() stops the walker.
func (fs *FSObjects) Walk(ctx context.Context, bucket, prefix string, results chan<- ObjectInfo, opts ObjectOptions) error {
	if err := checkListObjsArgs(ctx, bucket, prefix, "", fs); err != nil {
		// Upon error close the channel.
		close(results)
		return err
	}

	if opts.WalkVersions {
		go func() {
			defer close(results)

			var marker, versionIDMarker string
			for {
				loi, err := fs.ListObjectVersions(ctx, bucket, prefix, marker, versionIDMarker, "", 1000)
				if err != nil {
					break
				}

				for _, obj := range loi.Objects {
					results <- obj
				}

				if !loi.IsTruncated {
					break
				}

				marker = loi.NextMarker
				versionIDMarker = loi.NextVersionIDMarker
			}
		}()
		return nil
	}

	go func() {
		defer close(results)

		var marker string
		for {
			loi, err := fs.ListObjects(ctx, bucket, prefix, marker, "", 1000)
			if err != nil {
				break
			}

			for _, obj := range loi.Objects {
				results <- obj
			}

			if !loi.IsTruncated {
				break
			}

			marker = loi.NextMarker
		}
	}()

	return nil
}

// HealObjects - no-op for fs. Valid only for Erasure.
func (fs *FSObjects) HealObjects(ctx context.Context, bucket, prefix string, opts madmin.HealOpts, fn HealObjectFn) (e error) {
	logger.LogIf(ctx, NotImplemented{})
	return NotImplemented{}
}

// GetMetrics - no op
func (fs *FSObjects) GetMetrics(ctx context.Context) (*BackendMetrics, error) {
	logger.LogIf(ctx, NotImplemented{})
	return &BackendMetrics{}, NotImplemented{}
}

// ListObjectsV2 lists all blobs in bucket filtered by prefix
func (fs *FSObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error) {
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}

	loi, err := fs.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return result, err
	}

	listObjectsV2Info := ListObjectsV2Info{
		IsTruncated:           loi.IsTruncated,
		ContinuationToken:     continuationToken,
		NextContinuationToken: loi.NextMarker,
		Objects:               loi.Objects,
		Prefixes:              loi.Prefixes,
	}
	return listObjectsV2Info, err
}

// IsNotificationSupported returns whether bucket notification is applicable for this layer.
func (fs *FSObjects) IsNotificationSupported() bool {
	return true
}

// IsListenSupported returns whether listen bucket notification is applicable for this layer.
func (fs *FSObjects) IsListenSupported() bool {
	return true
}

// IsEncryptionSupported returns whether server side encryption is implemented for this layer.
func (fs *FSObjects) IsEncryptionSupported() bool {
	return true
}

// IsCompressionSupported returns whether compression is applicable for this layer.
func (fs *FSObjects) IsCompressionSupported() bool {
	return true
}

// IsTaggingSupported returns true, object tagging is supported in fs object layer.
func (fs *FSObjects) IsTaggingSupported() bool {
	return true
}

// Health returns health of the object layer
func (fs *FSObjects) Health(ctx context.Context, opts HealthOptions) HealthResult {
	if _, err := os.Stat(fs.disk.String()); err != nil {
		return HealthResult{}
	}
	return HealthResult{
		Healthy: newObjectLayerFn() != nil,
	}
}

// ReadHealth returns "read" health of the object layer
func (fs *FSObjects) ReadHealth(ctx context.Context) bool {
	_, err := os.Stat(fs.disk.String())
	return err == nil
}

// TransitionObject - transition object content to target tier.
func (fs *FSObjects) TransitionObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	return NotImplemented{}
}

// RestoreTransitionedObject - restore transitioned object content locally on this cluster.
func (fs *FSObjects) RestoreTransitionedObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	return NotImplemented{}
}
