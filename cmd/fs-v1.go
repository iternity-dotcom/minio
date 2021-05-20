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
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/bucket/replication"
	"io"
	"net/http"
	"os"
	"os/user"
	"path"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	// "github.com/minio/minio-go/v7/pkg/set"
	// "github.com/minio/minio/pkg/bucket/replication"
	"github.com/minio/pkg/env"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/config"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/lock"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/bucket/policy"
	"github.com/minio/pkg/mimedb"
)

// Default etag is used for pre-existing objects.
var defaultEtag = "00000000000000000000000000000000-1"

// FSObjects - Implements fs object layer.
type FSObjects struct {
	GatewayUnsupported

	// The storage backend behind this object layer
	disk fsStorageAPI

	// The count of concurrent calls on FSObjects API
	activeIOCount int64

	// This value shouldn't be touched, once initialized.
	fsFormatRlk     *lock.RLockedFile // Is a read lock on `format.json`.
	appendFileMap   map[string]*fsAppendFile
	appendFileMapMu sync.Mutex

	// To manage the appendRoutine go-routines
	nsMutex *nsLockMap

	deletedCleanupSleeper *dynamicSleeper
}

// Represents the background append file.
type fsAppendFile struct {
	sync.Mutex
	parts    []int  // List of parts appended.
	filePath string // Absolute path of the file in the temp location.
}

type fsStorageAPI interface {
	StorageAPI
	MetaTmpBucket() string
	ContextWithMetaLock(ctx context.Context, lockType LockType, volume string, path ...string) (context.Context, func(err ...error), error)
	EncodeDirObject(object string) string
	DecodeDirObject(object string) string
	IsDirObject(object string) bool
	VersioningSupported() bool
}

// NewFSXLObjectLayer - initialize new fs object layer using the new xl storage meta format.
func NewFSXLObjectLayer(fsPath string) (ObjectLayer, error) {
	return newFSObjectLayer(fsPath, newLocalFSXLStorage)
}

// NewFSObjectLayer - initialize new fs object layer.
func NewFSObjectLayer(fsPath string) (ObjectLayer, error) {
	return newFSObjectLayer(fsPath, newLocalFSV1Storage)
}

func newFSObjectLayer(fsPath string, createStorageAPI func(string) (fsStorageAPI, error)) (ObjectLayer, error) {
	ctx := GlobalContext

	disk, err := createStorageAPI(fsPath)

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

	// Initialize fs objects.
	fs := &FSObjects{
		disk:                  disk,
		nsMutex:               newNSLock(false),
		appendFileMap:         make(map[string]*fsAppendFile),
		deletedCleanupSleeper: newDynamicSleeper(10, 2*time.Second),
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

	// cleanup ".trash/" folder every 5m minutes with sufficient sleep cycles, between each
	// deletes a dynamic sleeper is used with a factor of 10 ratio with max delay between
	// deletes to be 2 seconds.
	deletedObjectsCleanupInterval, err := time.ParseDuration(env.Get(envMinioDeleteCleanupInterval, "5m"))
	if err != nil {
		return nil, err
	}
	// start cleanup of deleted objects.
	go fs.cleanupDeletedObjects(ctx, deletedObjectsCleanupInterval)
	go intDataUpdateTracker.start(ctx, fsPath)

	// Return successfully initialized object layer.
	return fs, nil
}

func (fs *FSObjects) cleanupDeletedObjects(ctx context.Context, cleanupInterval time.Duration) {
	timer := time.NewTimer(cleanupInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			// Reset for the next interval
			timer.Reset(cleanupInterval)

			trashDirs, err := fs.disk.ListDir(ctx, minioMetaTmpDeletedBucket, "", -1)
			if err == nil {
				for _, trashDir := range trashDirs {
					wait := fs.deletedCleanupSleeper.Timer(ctx)
					fs.disk.DeleteVol(ctx, pathJoin(minioMetaTmpDeletedBucket, trashDir), true)
					wait()
				}
			}
		}
	}
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
	fs.disk.DeleteVol(ctx, fs.disk.MetaTmpBucket(), true)

	return fs.disk.Close()
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
	defer close(updates)
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
		upds := make(chan dataUsageEntry, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for update := range upds {
				totalCache.replace(b.Name, dataUsageRoot, update)
				if intDataUpdateTracker.debug {
					logger.Info(color.Green("NSScanner:")+" Got update:", len(totalCache.Cache))
				}
				cloned := totalCache.clone()
				updates <- cloned.dui(dataUsageRoot, buckets)
			}
		}()
		cache, err := fs.disk.NSScanner(ctx, bCache, upds)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		logger.LogIf(ctx, err)
		cache.Info.BloomFilter = nil
		wg.Wait()

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

// GetDisksID will return disk.
func (fs *FSObjects) GetDisksID(_ ...string) []StorageAPI {
	return []StorageAPI{fs.disk}
}

// MakeBucketWithLocation - create a new bucket, returns if it already exists.
func (fs *FSObjects) MakeBucketWithLocation(ctx context.Context, bucket string, opts BucketOptions) error {
	if (opts.LockEnabled || opts.VersioningEnabled) && !fs.disk.VersioningSupported() {
		return NotImplemented{}
	}

	// Verify if bucket is valid.
	if s3utils.CheckValidBucketNameStrict(bucket) != nil {
		return BucketNameInvalid{Bucket: bucket}
	}

	defer NSUpdated(bucket, slashSeparator)

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	err := fs.disk.MakeVol(ctx, bucket)
	if err != nil {
		return toObjectErr(err, bucket)
	}

	meta := newBucketMetadata(bucket)
	if opts.LockEnabled {
		meta.VersioningConfigXML = enabledBucketVersioningConfig
		meta.ObjectLockConfigXML = enabledBucketObjectLockConfig
	}

	if err := meta.Save(ctx, fs); err != nil {
		return toObjectErr(err, bucket)
	}

	globalBucketMetadataSys.Set(bucket, meta)

	return nil
}

// PutObjectMetadata - updated the object metadata
func (fs *FSObjects) PutObjectMetadata(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	object = fs.disk.EncodeDirObject(object)

	var err error
	// Lock the object before updating tags.
	lk := fs.NewNSLock(bucket, object)
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		logger.LogIf(ctx, err)
		return ObjectInfo{}, err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx.Cancel)

	ctx, cleanup, err := fs.disk.ContextWithMetaLock(ctx, writeLock, bucket, object)
	if err != nil {
		return ObjectInfo{}, err
	}

	defer func() {
		cleanup(err)
	}()

	fi, err := fs.disk.ReadVersion(ctx, bucket, object, opts.VersionID, false)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	if fi.Deleted {
		if opts.VersionID == "" {
			return ObjectInfo{}, toObjectErr(errFileNotFound, bucket, object)
		}
		return ObjectInfo{}, toObjectErr(errMethodNotAllowed, bucket, object)
	}

	for k, v := range opts.UserDefined {
		fi.Metadata[k] = v
	}
	fi.ModTime = opts.MTime
	fi.VersionID = opts.VersionID

	if err = fs.disk.UpdateMetadata(ctx, bucket, object, fi); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	objInfo := fi.ToObjectInfo(bucket, object)
	return objInfo, nil
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

	for idx, bInfo := range bucketInfos {
		meta, err := globalBucketMetadataSys.Get(bInfo.Name)
		if err == nil {
			bucketInfos[idx].Created = meta.Created
		}
	}

	// Succes.
	return bucketInfos, nil
}

// DeleteBucket - delete a bucket and all the metadata associated
// with the bucket including pending multipart, object metadata.
func (fs *FSObjects) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	defer NSUpdated(bucket, slashSeparator)

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

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
func (fs *FSObjects) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (ObjectInfo, error) {
	if srcOpts.VersionID != "" && srcOpts.VersionID != nullVersionID && !fs.disk.VersioningSupported() {
		return ObjectInfo{}, VersionNotFound{
			Bucket:    srcBucket,
			Object:    srcObject,
			VersionID: srcOpts.VersionID,
		}
	}

	srcObject = fs.disk.EncodeDirObject(srcObject)
	dstObject = fs.disk.EncodeDirObject(dstObject)

	cpSrcDstSame := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(dstBucket, dstObject))

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	if _, err := fs.disk.StatVol(ctx, srcBucket); err != nil {
		return ObjectInfo{}, toObjectErr(err, srcBucket)
	}

	if cpSrcDstSame && srcInfo.metadataOnly {
		// Version ID is set for the destination and source == destination version ID.
		// perform an in-place update.
		if dstOpts.VersionID != "" && srcOpts.VersionID == dstOpts.VersionID {
			return fs.updateMetaObject(ctx, srcBucket, srcObject, srcInfo, srcOpts, dstOpts)
		}
		// Destination is not versioned and source version ID is empty
		// perform an in-place update.
		if !dstOpts.Versioned && srcOpts.VersionID == "" {
			return fs.updateMetaObject(ctx, srcBucket, srcObject, srcInfo, srcOpts, dstOpts)
		}
		// CopyObject optimization where we don't create an entire copy
		// of the content, instead we add a reference, we disallow legacy
		// objects to be self referenced in this manner so make sure
		// that we actually create a new dataDir for legacy objects.
		if dstOpts.Versioned && srcOpts.VersionID != dstOpts.VersionID && !srcInfo.Legacy {
			srcInfo.versionOnly = true
			return fs.updateMetaObject(ctx, srcBucket, srcObject, srcInfo, srcOpts, dstOpts)
		}
	}

	putOpts := ObjectOptions{
		ServerSideEncryption: dstOpts.ServerSideEncryption,
		UserDefined:          srcInfo.UserDefined,
		Versioned:            dstOpts.Versioned,
		VersionID:            dstOpts.VersionID,
		MTime:                dstOpts.MTime,
	}

	return fs.PutObject(ctx, dstBucket, dstObject, srcInfo.PutObjReader, putOpts)
}

func (fs *FSObjects) updateMetaObject(ctx context.Context, bucket, object string, info ObjectInfo, srcOpts, dstOpts ObjectOptions) (ObjectInfo, error) {
	// This call shouldn't be used for anything other than metadata updates or adding self referential versions.
	if !info.metadataOnly {
		return ObjectInfo{}, NotImplemented{}
	}

	defer NSUpdated(bucket, object)

	lk := fs.NewNSLock(bucket, object)
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer lk.Unlock(lkctx.Cancel)
	ctx = lkctx.Context()

	ctx, cleanup, err := fs.disk.ContextWithMetaLock(ctx, writeLock, bucket, object)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	defer func() {
		cleanup(err)
	}()
	var fi FileInfo
	fi, err = fs.disk.ReadVersion(ctx, bucket, object, srcOpts.VersionID, false)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	if fi.Deleted {
		if srcOpts.VersionID == "" {
			err = errFileNotFound
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		err = errMethodNotAllowed
		return fi.ToObjectInfo(bucket, object), toObjectErr(err, bucket, object)
	}

	versionID := info.VersionID
	if info.versionOnly {
		versionID = dstOpts.VersionID
		// preserve destination versionId if specified.
		if versionID == "" {
			versionID = mustGetUUID()
		}
		fi.ModTime = UTCNow()
	}

	fi.VersionID = versionID // set any new versionID we might have created
	if !dstOpts.MTime.IsZero() {
		fi.ModTime = dstOpts.MTime
	}
	fi.Metadata = info.UserDefined
	info.UserDefined["etag"] = info.ETag

	if err = fs.disk.WriteMetadata(ctx, bucket, object, fi); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	return fi.ToObjectInfo(bucket, object), nil
}

// GetObjectNInfo - returns object info and a reader for object
// content.
func (fs *FSObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions) (*GetObjectReader, error) {
	var cleanup func(...error)
	var err error
	if opts.VersionID != "" && opts.VersionID != nullVersionID && !fs.disk.VersioningSupported() {
		return nil, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return nil, err
	}

	object = fs.disk.EncodeDirObject(object)

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
			lkctx, err := lock.GetLock(ctx, globalOperationTimeout)
			if err != nil {
				return nil, err
			}
			ctx = lkctx.Context()
			nsUnlocker = func() { lock.Unlock(lkctx.Cancel) }
		case readLock:
			lkctx, err := lock.GetRLock(ctx, globalOperationTimeout)
			if err != nil {
				return nil, err
			}
			ctx = lkctx.Context()
			nsUnlocker = func() { lock.RUnlock(lkctx.Cancel) }
		}
	}

	ctx, cleanup, err = fs.disk.ContextWithMetaLock(ctx, lockType, bucket, object)
	if err != nil {
		return nil, toObjectErr(err, bucket, object)
	}

	cleanUpFn := func() {
		cleanup(err)
	}

	fi, err := fs.disk.ReadVersion(ctx, bucket, object, opts.VersionID, false)

	if err != nil {
		cleanUpFn()
		nsUnlocker()
		return nil, toObjectErr(err, bucket, object)
	}
	objInfo := fi.ToObjectInfo(bucket, object)

	if objInfo.DeleteMarker {
		if opts.VersionID == "" {
			err = errFileNotFound
		} else {

			err = errMethodNotAllowed
		}
		cleanUpFn()
		nsUnlocker()
		return &GetObjectReader{
			ObjInfo: objInfo,
		}, toObjectErr(err, bucket, object)
	}

	// For a directory or an empty file, we need to return a reader that returns no bytes.
	if fs.disk.IsDirObject(object) || fi.Size == 0 {
		// The lock taken above is released when
		// objReader.Close() is called by the caller.
		return NewGetObjectReaderFromReader(bytes.NewBuffer(nil), objInfo, opts, nsUnlocker, cleanUpFn)
	}

	// If transitioned, stream from transition tier unless object is restored locally or restore date is past.
	if objInfo.IsRemote() {
		var transR *GetObjectReader
		transR, err = getTransitionedObjectReader(ctx, bucket, object, rs, h, objInfo, opts)
		cleanUpFn()
		nsUnlocker()
		return transR, err
	}

	objReaderFn, off, length, err := NewGetObjectReader(rs, objInfo, opts)
	if err != nil {
		cleanUpFn()
		nsUnlocker()
		return nil, err
	}

	objectPath := object
	if fi.DataDir != "" {
		objectPath = pathJoin(object, fi.DataDir, "part.1")
	}
	// Read the object, doesn't exist returns an s3 compatible error.
	readCloser, err := fs.disk.ReadFileStream(ctx, bucket, objectPath, off, length)
	if err != nil {
		cleanUpFn()
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
		cleanUpFn()
		nsUnlocker()
		return nil, err
	}

	return objReaderFn(reader, h, opts.CheckPrecondFn, closeFn, cleanUpFn, nsUnlocker)
}

// GetObjectInfo - reads object metadata and replies back ObjectInfo.
func (fs *FSObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {

	var cleanup func(...error)
	if opts.VersionID != "" && opts.VersionID != nullVersionID && !fs.disk.VersioningSupported() {
		return ObjectInfo{}, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}

	var err error
	if err = checkGetObjArgs(ctx, bucket, object); err != nil {
		return ObjectInfo{}, err
	}

	object = fs.disk.EncodeDirObject(object)

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	var lk RWLocker
	var lkctx LockContext
	lockType := noLock
	if !opts.NoLock {
		// Lock the object before reading.
		lk = fs.NewNSLock(bucket, object)
		lkctx, err = lk.GetRLock(ctx, globalOperationTimeout)
		if err != nil {
			return ObjectInfo{}, err
		}
		ctx = lkctx.Context()

		lockType = readLock
	}

	// save the old context in case we need to re-lock using write lock
	// this happens when we need to write a new meta file,
	// if the old is corrupted/missing
	oldCtx := ctx
	ctx, cleanup, err = fs.disk.ContextWithMetaLock(oldCtx, lockType, bucket, object)
	if err != nil {
		if lk != nil {
			lk.RUnlock(lkctx.Cancel)
		}
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	fi, err := fs.disk.ReadVersion(ctx, bucket, object, opts.VersionID, false)

	if err == errCorruptedFormat || err == io.EOF {
		// release the old locks and acquire write locks
		cleanup(err)
		if lk != nil {
			lk.RUnlock(lkctx.Cancel)
		}

		lk = fs.NewNSLock(bucket, object)
		lkctx, err = lk.GetLock(oldCtx, globalOperationTimeout)
		if err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		defer lk.Unlock(lkctx.Cancel)
		ctx = lkctx.Context()

		ctx, cleanup, err = fs.disk.ContextWithMetaLock(ctx, writeLock, bucket, object)
		if err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		defer func() {
			cleanup(err)
		}()
		fi = FileInfo{
			Metadata: map[string]string{
				"etag":         defaultEtag,
				"content-type": mimedb.TypeByExtension(path.Ext(object)),
			},
		}
		err = fs.disk.WriteMetadata(ctx, bucket, object, fi)
		if err != nil {
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}

		fi, err = fs.disk.ReadVersion(ctx, bucket, object, opts.VersionID, false)
	} else {
		defer func() {
			cleanup(err)
		}()
		if lk != nil {
			defer lk.RUnlock(lkctx.Cancel)
		}
	}

	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	oi := fi.ToObjectInfo(bucket, object)
	if !fi.VersionPurgeStatus.Empty() {
		// Make sure to return object info to provide extra information.
		err = errMethodNotAllowed
		return oi, toObjectErr(err, bucket, object)
	}

	if fi.Deleted {
		if opts.VersionID == "" || opts.DeleteMarker {
			err = errFileNotFound
			return oi, toObjectErr(err, bucket, object)
		}
		// Make sure to return object info to provide extra information.
		err = errMethodNotAllowed
		return oi, toObjectErr(err, bucket, object)
	}

	return oi, nil
}

// This function does the following check, suppose
// object is "a/b/c/d", stat makes sure that objects ""a/b/c""
// "a/b" and "a" do not exist.
func (fs *FSObjects) parentDirIsObject(ctx context.Context, bucket, parent string) bool {
	return fs.disk.CheckFile(ctx, bucket, parent) == nil
}

// PutObject - creates an object upon reading from the input stream
// until EOF, writes data directly to configured filesystem path.
// Additionally writes `fs.json` which carries the necessary metadata
// for future object operations.
func (fs *FSObjects) PutObject(ctx context.Context, bucket string, object string, r *PutObjReader, opts ObjectOptions) (ObjectInfo, error) {
	if opts.Versioned && !fs.disk.VersioningSupported() {
		return ObjectInfo{}, NotImplemented{}
	}
	var err error
	if err = checkPutObjectArgs(ctx, bucket, object, fs); err != nil {
		return ObjectInfo{}, err
	}

	object = fs.disk.EncodeDirObject(object)

	di, err := fs.disk.DiskInfo(ctx)
	if err != nil {
		return ObjectInfo{}, err
	}
	if !isMinioMetaBucketName(bucket) && !hasSpaceFor([]*DiskInfo{&di}, r.Size()) {
		return ObjectInfo{}, toObjectErr(errDiskFull)
	}

	// Lock the object.
	lk := fs.NewNSLock(bucket, object)
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		logger.LogIf(ctx, err)
		return ObjectInfo{}, err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx.Cancel)

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	cleanup := func(err ...error) {}
	if bucket != minioMetaBucket {
		ctx, cleanup, err = fs.disk.ContextWithMetaLock(ctx, writeLock, bucket, object)
	}

	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	defer func() {
		cleanup(err)
	}()

	// Validate if bucket name is valid and exists.
	if _, err = fs.disk.StatVol(ctx, bucket); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket)
	}

	cReader := newCountingReader(r)

	uniqueID := mustGetUUID()
	tempObjFolder := uniqueID

	// No metadata is set, allocate a new one.
	if opts.UserDefined == nil {
		opts.UserDefined = make(map[string]string)
	}

	// Validate input data size and it can never be less than zero.
	if cReader.Size() < -1 {
		logger.LogIf(ctx, errInvalidArgument, logger.Application)
		return ObjectInfo{}, toObjectErr(errInvalidArgument)
	}

	fi := FileInfo{
		Metadata: cloneMSS(opts.UserDefined),
	}

	// Check if an object is present as one of the parent dir.
	if fs.parentDirIsObject(ctx, bucket, path.Dir(object)) {
		return ObjectInfo{}, toObjectErr(errFileParentIsFile, bucket, object)
	}

	fi.VersionID = opts.VersionID
	if opts.Versioned && fi.VersionID == "" {
		fi.VersionID = mustGetUUID()
	}

	fi.DataDir = mustGetUUID()
	partName := "part.1"

	// Delete the temporary object in the case of a
	// failure. If PutObject succeeds, then there would be
	// nothing to delete.
	defer func() {
		fs.deleteObject(context.Background(), fs.disk.MetaTmpBucket(), tempObjFolder)
	}()

	if !fs.disk.IsDirObject(object) {
		// Uploaded object will first be written to the temporary location which will eventually
		// be renamed to the actual location. It is first written to the temporary location
		// so that cleaning it up will be easy if the server goes down.
		err = fs.disk.CreateFile(ctx, fs.disk.MetaTmpBucket(), pathJoin(tempObjFolder, fi.DataDir, partName), cReader.Size(), cReader)
		if err != nil {
			// Should return IncompleteBody{} error when reader has fewer
			// bytes than specified in request header.
			if err == errLessData || err == errMoreData {
				return ObjectInfo{}, IncompleteBody{Bucket: bucket, Object: object}
			}
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
	}

	fi.AddObjectPart(1, "", cReader.bytesRead, cReader.ActualSize())

	if opts.UserDefined["etag"] == "" {
		fi.Metadata["etag"] = cReader.MD5CurrentHexString()
	}

	// Guess content-type from the extension if possible.
	if opts.UserDefined["content-type"] == "" {
		fi.Metadata["content-type"] = mimedb.TypeByExtension(path.Ext(object))
	}

	modTime := opts.MTime
	if opts.MTime.IsZero() {
		modTime = UTCNow()
	}

	fi.Size = cReader.bytesRead
	fi.ModTime = modTime

	// rename
	defer NSUpdated(fs.disk.MetaTmpBucket(), tempObjFolder)
	defer NSUpdated(bucket, object)
	err = fs.disk.RenameData(ctx, fs.disk.MetaTmpBucket(), pathJoin(tempObjFolder), fi, bucket, object)

	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	// Success.
	return fi.ToObjectInfo(bucket, object), nil
}

func (fs *FSObjects) deleteObject(ctx context.Context, bucket, object string) error {
	object = fs.disk.EncodeDirObject(object)
	var err error
	defer NSUpdated(bucket, object)

	tmpObj := mustGetUUID()
	if bucket == fs.disk.MetaTmpBucket() {
		tmpObj = object
	} else {
		err = fs.disk.RenameFile(ctx, bucket, object, fs.disk.MetaTmpBucket(), tmpObj)
		if err != nil {
			return toObjectErr(err, bucket, object)
		}
	}

	return fs.disk.Delete(ctx, fs.disk.MetaTmpBucket(), tmpObj, true)
}

// DeleteObjects - deletes an object from a bucket, this operation is destructive
// and there are no rollbacks supported.
func (fs *FSObjects) DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete, opts ObjectOptions) ([]DeletedObject, []error) {
	dobjects := make([]DeletedObject, len(objects))
	errs := make([]error, len(objects))
	objSets := set.NewStringSet()
	for i := range objects {
		if objects[i].VersionID != "" && !fs.disk.VersioningSupported() {
			errs[i] = VersionNotFound{
				Bucket:    bucket,
				Object:    objects[i].ObjectName,
				VersionID: objects[i].VersionID,
			}
			continue
		}

		objects[i].ObjectName = fs.disk.EncodeDirObject(objects[i].ObjectName)
		errs[i] = checkDelObjArgs(ctx, bucket, objects[i].ObjectName)
		if errs[i] == nil {
			objSets.Add(objects[i].ObjectName)
		}
	}

	// Acquire a bulk write lock across 'objects'
	objLockSlice := objSets.ToSlice()
	multiDeleteLock := fs.NewNSLock(bucket, objLockSlice...)
	lkctx, err := multiDeleteLock.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		for i := range errs {
			errs[i] = toObjectErr(err, bucket, objects[i].ObjectName)
		}
		return dobjects, errs
	}
	defer multiDeleteLock.Unlock(lkctx.Cancel)
	ctx = lkctx.Context()

	versions := make([]FileInfo, 0)
	objIdxToVerIdx := make(map[int]int, len(objects))
	for i := range objects {
		if errs[i] != nil {
			continue
		}
		objIdxToVerIdx[i] = len(versions)
		if objects[i].VersionID == "" {
			modTime := opts.MTime
			if opts.MTime.IsZero() {
				modTime = UTCNow()
			}
			uuid := opts.VersionID
			if uuid == "" {
				uuid = mustGetUUID()
			}
			if opts.Versioned || opts.VersionSuspended {
				version := FileInfo{
					Name:                          objects[i].ObjectName,
					ModTime:                       modTime,
					Deleted:                       true, // delete marker
					DeleteMarkerReplicationStatus: objects[i].DeleteMarkerReplicationStatus,
					VersionPurgeStatus:            objects[i].VersionPurgeStatus,
				}
				if opts.Versioned {
					version.VersionID = uuid
				}
				versions = append(versions, version)
				continue
			}
		}
		versions = append(versions, FileInfo{
			Name:                          objects[i].ObjectName,
			VersionID:                     objects[i].VersionID,
			DeleteMarkerReplicationStatus: objects[i].DeleteMarkerReplicationStatus,
			VersionPurgeStatus:            objects[i].VersionPurgeStatus,
		})
	}

	ctx, cleanup, err := fs.disk.ContextWithMetaLock(ctx, writeLock, bucket, objLockSlice...)
	if err != nil {
		for i := range errs {
			errs[i] = err
		}
		return dobjects, errs
	}

	cleanupErrs := make([]error, len(objSets))
	defer func() {
		cleanup(cleanupErrs...)
	}()

	// Initialize list of errors.
	delObjErrs := fs.disk.DeleteVersions(ctx, bucket, versions)

	// Reduce errors for each object

	for objIndex := range objects {
		verIdx := objIdxToVerIdx[objIndex]

		var err error
		if errs[objIndex] != nil {
			err = errs[objIndex]
		} else if delObjErrs[verIdx] != errFileNotFound {
			err = delObjErrs[verIdx]
		}

		if delObjErrs[verIdx] != nil {
			lockPos, ok := findFirst(objLockSlice, versions[verIdx].Name)
			if ok {
				cleanupErrs[lockPos] = delObjErrs[verIdx]
			}
		}

		if objects[objIndex].VersionID != "" {
			errs[objIndex] = toObjectErr(err, bucket, objects[objIndex].ObjectName, objects[objIndex].VersionID)
		} else {
			errs[objIndex] = toObjectErr(err, bucket, objects[objIndex].ObjectName)
		}

		if errs[objIndex] != nil {
			continue
		}
		NSUpdated(bucket, objects[objIndex].ObjectName)

		if versions[verIdx].Deleted {
			dobjects[objIndex] = DeletedObject{
				DeleteMarker:                  versions[verIdx].Deleted,
				DeleteMarkerVersionID:         versions[verIdx].VersionID,
				DeleteMarkerMTime:             DeleteMarkerMTime{versions[verIdx].ModTime},
				DeleteMarkerReplicationStatus: versions[verIdx].DeleteMarkerReplicationStatus,
				ObjectName:                    versions[verIdx].Name,
				VersionPurgeStatus:            versions[verIdx].VersionPurgeStatus,
			}
		} else {
			dobjects[objIndex] = DeletedObject{
				ObjectName:                    versions[verIdx].Name,
				VersionID:                     versions[verIdx].VersionID,
				VersionPurgeStatus:            versions[verIdx].VersionPurgeStatus,
				DeleteMarkerReplicationStatus: versions[verIdx].DeleteMarkerReplicationStatus,
			}
		}
	}

	return dobjects, errs
}

func findFirst(strSlice []string, name string) (int, bool) {
	for idx, element := range strSlice {
		if element == name {
			return idx, true
		}
	}

	return 0, false
}

// DeleteObject - deletes an object from a bucket, this operation is destructive
// and there are no rollbacks supported.
func (fs *FSObjects) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID && !fs.disk.VersioningSupported() {
		return ObjectInfo{}, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}
	var err error
	object = fs.disk.EncodeDirObject(object)
	if err = checkDelObjArgs(ctx, bucket, object); err != nil {
		return ObjectInfo{}, err
	}

	atomic.AddInt64(&fs.activeIOCount, 1)
	defer func() {
		atomic.AddInt64(&fs.activeIOCount, -1)
	}()

	versionFound := true
	objInfo := ObjectInfo{VersionID: opts.VersionID} // version id needed in Delete API response.
	oi, err := fs.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil && oi.Name == "" {
		switch err.(type) {
		case InsufficientReadQuorum:
			return objInfo, InsufficientWriteQuorum{}
		}
		// For delete marker replication, versionID being replicated will not exist on disk
		if opts.DeleteMarker {
			versionFound = false
		} else {
			return objInfo, err
		}
	}

	// Acquire a write lock before deleting the object.
	lk := fs.NewNSLock(bucket, object)
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return objInfo, err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx.Cancel)

	ctx, cleanup, err := fs.disk.ContextWithMetaLock(ctx, writeLock, bucket, object)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	defer func() {
		cleanup(err)
	}()
	defer NSUpdated(bucket, object)

	var markDelete bool
	// Determine whether to mark object deleted for replication
	if oi.VersionID != "" {
		markDelete = true
	}

	// Default deleteMarker to true if object is under versioning
	deleteMarker := opts.Versioned

	if opts.VersionID != "" {
		// case where replica version needs to be deleted on target cluster
		if versionFound && opts.DeleteMarkerReplicationStatus == replication.Replica.String() {
			markDelete = false
		}
		if opts.VersionPurgeStatus.Empty() && opts.DeleteMarkerReplicationStatus == "" {
			markDelete = false
		}
		if opts.VersionPurgeStatus == Complete {
			markDelete = false
		}
		// determine if the version represents an object delete
		// deleteMarker = true
		if versionFound && !oi.DeleteMarker { // implies a versioned delete of object
			deleteMarker = false
		}
	}

	modTime := opts.MTime
	if opts.MTime.IsZero() {
		modTime = UTCNow()
	}
	if markDelete {
		if opts.Versioned || opts.VersionSuspended {
			fi := FileInfo{
				Name:                          object,
				Deleted:                       deleteMarker,
				MarkDeleted:                   markDelete,
				ModTime:                       modTime,
				DeleteMarkerReplicationStatus: opts.DeleteMarkerReplicationStatus,
				VersionPurgeStatus:            opts.VersionPurgeStatus,
				TransitionStatus:              opts.Transition.Status,
				ExpireRestored:                opts.Transition.ExpireRestored,
			}
			if opts.Versioned {
				fi.VersionID = mustGetUUID()
				if opts.VersionID != "" {
					fi.VersionID = opts.VersionID
				}
			}

			// versioning suspended means we add `null`
			// version as delete marker
			// Add delete marker, since we don't have any version specified explicitly.
			// Or if a particular version id needs to be replicated.
			if err = fs.disk.DeleteVersion(ctx, bucket, object, fi, opts.DeleteMarker); err != nil {
				return objInfo, toObjectErr(err, bucket, object)
			}
			return fi.ToObjectInfo(bucket, object), nil
		}
	}

	// Delete the object version on all disks.
	if err = fs.disk.DeleteVersion(ctx, bucket, object, FileInfo{
		Name:                          object,
		VersionID:                     opts.VersionID,
		MarkDeleted:                   markDelete,
		Deleted:                       deleteMarker,
		ModTime:                       modTime,
		DeleteMarkerReplicationStatus: opts.DeleteMarkerReplicationStatus,
		VersionPurgeStatus:            opts.VersionPurgeStatus,
		TransitionStatus:              opts.Transition.Status,
		ExpireRestored:                opts.Transition.ExpireRestored,
	}, opts.DeleteMarker); err != nil {
		return objInfo, toObjectErr(err, bucket, object)
	}

	return ObjectInfo{
		Bucket:             bucket,
		Name:               object,
		VersionID:          opts.VersionID,
		VersionPurgeStatus: opts.VersionPurgeStatus,
		ReplicationStatus:  replication.StatusType(opts.DeleteMarkerReplicationStatus),
	}, nil
}

// ListObjectVersions - lists objects with versions
func (fs *FSObjects) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (ListObjectVersionsInfo, error) {
	loi := ListObjectVersionsInfo{}
	if !fs.disk.VersioningSupported() {
		return loi, NotImplemented{}
	}

	if marker == "" && versionMarker != "" {
		return loi, NotImplemented{}
	}

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
		Limit:              maxKeysPlusOne(maxKeys, marker != ""),
		Marker:             marker,
		Recursive:          recursive,
		InclDeleted:        true,
		IncludeDirectories: delimiter == SlashSeparator,
	}

	entries, err := fs.fsListObjects(ctx, opts)

	var eof bool
	if err != nil {
		if err == io.EOF {
			eof = true
		} else {
			return ListObjectVersionsInfo{}, err
		}
	}

	if opts.Limit > 0 && entries.len() > opts.Limit {
		entries.truncate(opts.Limit)
		eof = false
	}

	if opts.Marker != "" && len(entries.o) > 0 && entries.o[0].name == opts.Marker && versionMarker == "" {
		entries.o = entries.o[1:]
	}

	objInfos := entries.fileInfoVersions(bucket, prefix, delimiter, versionMarker)

	if maxKeys > 0 && len(objInfos) > maxKeys {
		objInfos = objInfos[:maxKeys]
		eof = false
	}
	for _, objInfo := range objInfos {
		if objInfo.IsDir && objInfo.ModTime.IsZero() && opts.Separator != "" {
			loi.Prefixes = append(loi.Prefixes, objInfo.Name)
			continue
		}
		loi.Objects = append(loi.Objects, objInfo)
	}

	if !eof && len(objInfos) > 0 {
		loi.IsTruncated = true
		last := objInfos[len(objInfos)-1]
		loi.NextMarker = last.Name
		loi.NextVersionIDMarker = last.VersionID
	}

	return loi, nil
}

// ListObjects - list all objects at prefix upto maxKeys., optionally delimited by '/'. Maintains the list pool
// state for future re-entrant list requests.
func (fs *FSObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
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
		Limit:              maxKeysPlusOne(maxKeys, marker != ""),
		Marker:             marker,
		Recursive:          recursive,
		InclDeleted:        false,
		IncludeDirectories: delimiter == SlashSeparator,
	}

	entries, err := fs.fsListObjects(ctx, opts)

	var eof bool
	if err != nil {
		if err == io.EOF {
			eof = true
		} else {
			return ListObjectsInfo{}, err
		}
	}

	if opts.Limit > 0 && entries.len() > opts.Limit {
		entries.truncate(opts.Limit)
		eof = false
	}

	if opts.Marker != "" && len(entries.o) > 0 && entries.o[0].name == opts.Marker {
		entries.o = entries.o[1:]
	}

	objInfos := entries.fileInfos(bucket, prefix, delimiter)

	result := ListObjectsInfo{}
	for _, objInfo := range objInfos {
		if objInfo.IsDir && opts.Separator != "" {
			result.Prefixes = append(result.Prefixes, objInfo.Name)
			continue
		}
		result.Objects = append(result.Objects, objInfo)
	}

	if !eof && len(objInfos) > 0 {
		result.IsTruncated = true
		if len(objInfos) > 0 {
			result.NextMarker = objInfos[len(objInfos)-1].Name
		}
	}

	return result, nil
}

func (fs *FSObjects) fsListObjects(ctx context.Context, opts listPathOptions) (metaCacheEntriesSorted, error) {
	var m metaCacheEntriesSorted

	// Cancel upstream if we finish before we expect.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := checkListObjsArgs(ctx, opts.Bucket, opts.Prefix, opts.Marker, fs); err != nil {
		return m, err
	}

	// Marker is set validate pre-condition.
	if opts.Marker != "" && opts.Prefix != "" {
		// Marker not common with prefix is not implemented. Send an empty response
		if !HasPrefix(opts.Marker, opts.Prefix) {
			return m, io.EOF
		}
	}

	// With max keys of zero we have reached eof, return right here.
	if opts.Limit == 0 {
		return m, io.EOF
	}

	// For delimiter and prefix as '/' we do not list anything at all
	// since according to s3 spec we stop at the 'delimiter'
	// along // with the prefix. On a flat namespace with 'prefix'
	// as '/' we don't have any entries, since all the keys are
	// of form 'keyName/...'
	if opts.Separator == SlashSeparator && opts.Prefix == SlashSeparator {
		return m, io.EOF
	}

	r, w := io.Pipe()
	metaR := newMetacacheReader(r)

	// Make sure we close the pipe so blocked writes doesn't stay around.
	defer r.CloseWithError(context.Canceled)

	opts.BaseDir = baseDirFromPrefix(opts.Prefix)
	opts.SetFilter()
	go func() {
		werr := fs.disk.WalkDir(ctx, WalkDirOptions{
			Bucket:         opts.Bucket,
			BaseDir:        opts.BaseDir,
			Recursive:      opts.Recursive,
			ReportNotFound: false,
			FilterPrefix:   opts.FilterPrefix,
		}, w)
		w.CloseWithError(werr)
		if werr != io.EOF && werr != nil &&
			werr.Error() != errFileNotFound.Error() &&
			werr.Error() != errVolumeNotFound.Error() &&
			!errors.Is(werr, context.Canceled) {
			logger.LogIf(ctx, werr)
		}
	}()

	return metaR.filter(opts)
}

// GetObjectTags - get object tags from an existing object
func (fs *FSObjects) GetObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (*tags.Tags, error) {
	if opts.VersionID != "" && opts.VersionID != nullVersionID && !fs.disk.VersioningSupported() {
		return nil, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}

	object = fs.disk.EncodeDirObject(object)
	oi, err := fs.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return nil, err
	}

	return tags.ParseObjectTags(oi.UserTags)
}

// PutObjectTags - replace or add tags to an existing object
func (fs *FSObjects) PutObjectTags(ctx context.Context, bucket, object string, tags string, opts ObjectOptions) (ObjectInfo, error) {
	object = fs.disk.EncodeDirObject(object)

	if opts.VersionID != "" && opts.VersionID != nullVersionID && !fs.disk.VersioningSupported() {
		return ObjectInfo{}, VersionNotFound{
			Bucket:    bucket,
			Object:    object,
			VersionID: opts.VersionID,
		}
	}

	var err error
	// Lock the object before updating tags.
	lk := fs.NewNSLock(bucket, object)
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer lk.Unlock(lkctx.Cancel)
	ctx = lkctx.Context()

	ctx, cleanup, err := fs.disk.ContextWithMetaLock(ctx, writeLock, bucket, object)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	defer func() {
		cleanup(err)
	}()

	fi, err := fs.disk.ReadVersion(ctx, bucket, object, opts.VersionID, false)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	if fi.Deleted {
		if opts.VersionID == "" {
			return ObjectInfo{}, toObjectErr(errFileNotFound, bucket, object)
		}
		return ObjectInfo{}, toObjectErr(errMethodNotAllowed, bucket, object)
	}

	fi.Metadata[xhttp.AmzObjectTagging] = tags
	for k, v := range opts.UserDefined {
		fi.Metadata[k] = v
	}

	if err = fs.disk.UpdateMetadata(ctx, bucket, object, fi); err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	return fi.ToObjectInfo(bucket, object), nil
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
	if _, err := fs.disk.StatVol(ctx, minioMetaBucket); err != nil {
		return HealthResult{}
	}
	return HealthResult{
		Healthy: newObjectLayerFn() != nil,
	}
}

// ReadHealth returns "read" health of the object layer
func (fs *FSObjects) ReadHealth(ctx context.Context) bool {
	_, err := fs.disk.StatVol(ctx, minioMetaBucket)
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
