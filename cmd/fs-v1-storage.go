/*
 * MinIO Cloud Storage, (C) 2021 iTernity GmbH
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

package cmd

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"io/ioutil"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/klauspost/readahead"
	"github.com/minio/minio/pkg/lock"

	"github.com/minio/minio/cmd/logger"

	pathutil "path"

	"github.com/minio/minio/pkg/disk"
	"github.com/minio/minio/pkg/env"
)

type fsv1Storage struct {
	diskPath string
	endpoint Endpoint

	diskID        string
	rootDisk      bool
	diskInfoCache timedValue

	ctx context.Context

	// FS rw pool.
	rwPool *fsIOPool

	// Indexes, will be -1 until assigned a set.
	poolIndex, setIndex, diskIndex int
}

func newLocalFSV1Storage(path string) (*fsv1Storage, error) {
	u := url.URL{Path: path}
	return newFSV1Storage(Endpoint{
		URL:     &u,
		IsLocal: true,
	})

}

// Initialize a new storage disk.
func newFSV1Storage(ep Endpoint) (*fsv1Storage, error) {
	path := ep.Path
	var err error
	if path, err = getValidPath(path); err != nil {
		return nil, err
	}

	var rootDisk bool
	if env.Get("MINIO_CI_CD", "") != "" {
		rootDisk = true
	} else {
		if IsDocker() || IsKubernetes() {
			// Start with overlay "/" to check if
			// possible the path has device id as
			// "overlay" that would mean the path
			// is emphemeral and we should treat it
			// as root disk from the baremetal
			// terminology.
			rootDisk, err = disk.IsRootDisk(path, SlashSeparator)
			if err != nil {
				return nil, err
			}
			if !rootDisk {
				// No root disk was found, its possible that
				// path is referenced at "/etc/hosts" which has
				// different device ID that points to the original
				// "/" on the host system, fall back to that instead
				// to verify of the device id is same.
				rootDisk, err = disk.IsRootDisk(path, "/etc/hosts")
				if err != nil {
					return nil, err
				}
			}

		} else {
			// On baremetal setups its always "/" is the root disk.
			rootDisk, err = disk.IsRootDisk(path, SlashSeparator)
			if err != nil {
				return nil, err
			}
		}
	}

	// Assign a new UUID for FS minio mode. Each server instance
	// gets its own UUID for temporary file transaction.
	diskID := mustGetUUID()
	p := &fsv1Storage{
		diskPath: path,
		endpoint: ep,
		ctx:      GlobalContext,
		rootDisk: rootDisk,
		diskID:   diskID,
		rwPool: &fsIOPool{
			readersMap: make(map[string]*lock.RLockedFile),
		},
		poolIndex: -1,
		setIndex:  -1,
		diskIndex: -1,
	}

	metaTmpPath := pathJoin(minioMetaTmpBucket, diskID)

	// Create all necessary bucket folders if possible.
	if err = p.MakeVolBulk(context.TODO(), minioMetaBucket, metaTmpPath, minioMetaMultipartBucket, dataUsageBucket); err != nil {
		return nil, err
	}

	// Check if backend is writable and supports O_DIRECT
	var rnd [8]byte
	_, _ = rand.Read(rnd[:])
	tmpFile := ".writable-check-" + hex.EncodeToString(rnd[:]) + ".tmp"
	filePath := pathJoin(p.diskPath, metaTmpPath, tmpFile)
	w, err := disk.OpenFileDirectIO(filePath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0666)
	if err != nil {
		return p, err
	}
	if _, err = w.Write(alignedBuf[:]); err != nil {
		w.Close()
		return p, err
	}
	w.Close()
	defer Remove(filePath)

	// Success.
	return p, nil
}

func (s *fsv1Storage) String() string {
	return s.diskPath
}

func (s *fsv1Storage) IsOnline() bool {
	return true
}

func (s *fsv1Storage) IsLocal() bool {
	return true
}

func (s *fsv1Storage) Hostname() string {
	return s.endpoint.Host
}

func (s *fsv1Storage) Endpoint() Endpoint {
	return s.endpoint
}

func (s *fsv1Storage) Healing() *healingTracker {
	return nil
}

func (s *fsv1Storage) CrawlAndGetDataUsage(ctx context.Context, cache dataUsageCache) (dataUsageCache, error) {
	return dataUsageCache{}, NotImplemented{}
}

func (s *fsv1Storage) GetDiskID() (string, error) {
	return s.diskID, nil
}

func (s *fsv1Storage) SetDiskID(id string) {
}

func (s *fsv1Storage) DiskInfo(ctx context.Context) (info DiskInfo, err error) {
	s.diskInfoCache.Once.Do(func() {
		s.diskInfoCache.TTL = time.Second
		s.diskInfoCache.Update = func() (interface{}, error) {
			dcinfo := DiskInfo{
				RootDisk:  s.rootDisk,
				MountPath: s.String(),
				Endpoint:  s.endpoint.String(),
			}
			di, err := getDiskInfo(s.String())
			if err != nil {
				return dcinfo, err
			}
			dcinfo.Total = di.Total
			dcinfo.Free = di.Free
			dcinfo.Used = di.Used
			dcinfo.UsedInodes = di.Files - di.Ffree
			dcinfo.FSType = di.FSType

			diskID, err := s.GetDiskID()
			if errors.Is(err, errUnformattedDisk) {
				// if we found an unformatted disk then
				// healing is automatically true.
				dcinfo.Healing = true
			} else {
				// Check if the disk is being healed if GetDiskID
				// returned any error other than fresh disk
				dcinfo.Healing = s.Healing() != nil
			}

			dcinfo.ID = diskID
			return dcinfo, err
		}
	})

	v, err := s.diskInfoCache.Get()
	info = v.(DiskInfo)
	return info, err

}

func (s *fsv1Storage) NSScanner(ctx context.Context, cache dataUsageCache) (dataUsageCache, error) {
	return dataUsageCache{}, NotImplemented{}
}

func (s *fsv1Storage) MakeVolBulk(ctx context.Context, volumes ...string) (err error) {
	for _, volume := range volumes {
		if err := s.MakeVol(ctx, volume); err != nil {
			if errors.Is(err, errDiskAccessDenied) {
				return errDiskAccessDenied
			}
		}
	}
	return nil
}

func (s *fsv1Storage) MakeVol(ctx context.Context, volume string) (err error) {
	if !isValidVolname(volume) {
		return errInvalidArgument
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	if _, err := os.Lstat(volumeDir); err != nil {
		// Volume does not exist we proceed to create.
		if osIsNotExist(err) {
			// Make a volume entry, with mode 0777 mkdir honors system umask.
			err = reliableMkdirAll(volumeDir, 0777)
		}
		if osIsPermission(err) {
			return errDiskAccessDenied
		} else if isSysErrIO(err) {
			return errFaultyDisk
		}
		return err
	}

	// Stat succeeds we return errVolumeExists.
	return errVolumeExists
}

func (s *fsv1Storage) getVolDir(volume string) (string, error) {
	if volume == "" || volume == "." || volume == ".." {
		return "", errVolumeNotFound
	}
	volumeDir := pathJoin(s.String(), volume)
	return volumeDir, nil
}

func (s *fsv1Storage) ListVols(ctx context.Context) (vols []VolInfo, err error) {
	if err := checkPathLength(s.String()); err != nil {
		return nil, err
	}
	entries, err := readDir(s.String())
	if err != nil {
		return nil, errDiskNotFound
	}
	volsInfo := make([]VolInfo, 0, len(entries))
	for _, entry := range entries {
		if !HasSuffix(entry, SlashSeparator) || !isValidVolname(pathutil.Clean(entry)) {
			// Skip if entry is neither a directory not a valid volume name.
			continue
		}
		var vi VolInfo
		vi, err = s.StatVol(ctx, pathutil.Clean(entry))
		// There seems like no practical reason to check for errors
		// at this point, if there are indeed errors we can simply
		// just ignore such buckets and list only those which
		// return proper Stat information instead.
		if err != nil {
			// Ignore any errors returned here.
			continue
		}

		volsInfo = append(volsInfo, vi)
	}
	return volsInfo, nil
}

func (s *fsv1Storage) StatVol(ctx context.Context, volume string) (vol VolInfo, err error) {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return VolInfo{}, err
	}
	// Stat a volume entry.
	var st os.FileInfo
	st, err = Lstat(volumeDir) // TODO: why xl uses lstat
	if err != nil {
		switch {
		case osIsNotExist(err):
			return VolInfo{}, errVolumeNotFound
		case osIsPermission(err):
			return VolInfo{}, errDiskAccessDenied
		case isSysErrIO(err):
			return VolInfo{}, errFaultyDisk
		default:
			return VolInfo{}, err
		}
	}
	// As os.Lstat() doesn't carry other than ModTime(), use ModTime()
	// as CreatedTime.
	createdTime := st.ModTime()
	return VolInfo{
		Name:    volume,
		Created: createdTime,
	}, nil

}

func (s *fsv1Storage) DeleteVol(ctx context.Context, volume string, forceDelete bool) (err error) {
	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	if forceDelete {
		if isMinioMetaBucketName(volume) || HasPrefix(volume, minioMetaBucket) {
			err = os.RemoveAll(volumeDir)
		} else {
			var delDir string
			delBucket := pathJoin(minioMetaTmpBucket, volume+"."+mustGetUUID())
			// move to a temporary directory and delete afterwards (move to deletefiles probably)
			if delDir, err = s.getVolDir(delBucket); err != nil {
				return err
			}
			if err := renameAll(volumeDir, delDir); err != nil {
				return toObjectErr(err, volume)
			}
			go func() {
				s.DeleteVol(ctx, delBucket, true) // ignore returned error if any.
			}()

		}
	} else {
		err = os.Remove(volumeDir)
	}

	if err != nil {
		switch {
		case osIsNotExist(err):
			return errVolumeNotFound
		case isSysErrNotEmpty(err):
			return errVolumeNotEmpty
		case osIsPermission(err):
			return errDiskAccessDenied
		case isSysErrIO(err):
			return errFaultyDisk
		default:
			return err
		}
	}
	return nil
}

func (s *fsv1Storage) AppendFile(ctx context.Context, volume string, path string, buf []byte) error {
	return NotImplemented{}
}

func (s *fsv1Storage) CreateFile(ctx context.Context, volume, path string, fileSize int64, r io.Reader) error {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}
	filePath := pathJoin(volumeDir, path)

	if fileSize < -1 {
		return errInvalidArgument
	}

	// TODO: handle locking with rwPool
	parentFilePath := pathutil.Dir(filePath)
	defer func() {
		if err != nil {
			if volume == minioMetaTmpBucket {
				_ = removeAll(parentFilePath)
			}
		}
	}()
	written, err := fsCreateFile(ctx, filePath, r, fileSize)
	if err != nil {
		return err
	}

	if written < fileSize {
		return errLessData
	}

	return nil
}

func (s *fsv1Storage) WriteMetadata(ctx context.Context, volume, path string, fi FileInfo) error {
	fsMeta := &fsMetaV1{}
	fsMeta.FromFileInfo(fi)

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	buf, err := json.Marshal(&fsMeta)
	if err != nil {
		return err
	}

	return s.WriteAll(ctx, volume, pathJoin(path, fsMetaJSONFile), buf)
}

func (s *fsv1Storage) DeleteVersion(ctx context.Context, volume, path string, fi FileInfo, forceDelMarker bool) error {
	return NotImplemented{}
}

func (s *fsv1Storage) WriteAll(ctx context.Context, volume string, path string, b []byte) error {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return err
	}

	//metaLock, unlocker, cleanUp, err := s.getMetaWriteLock(ctx, volume, path)
	//defer unlocker()
	//
	//if err != nil {
	//	return err
	//}
	//
	//var w io.WriteCloser
	//if volume != minioMetaBucket {
	//	w, err = openFile(filePath, writeMode)
	//	if err != nil {
	//		logger.LogIf(ctx, err)
	//		cleanUp()
	//		return err
	//	}
	//	defer w.Close()
	//} else {
	//	w = metaLock
	//}

	w, err := openFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	if err != nil {
		logger.LogIf(ctx, err)
		// cleanUp()
		return err
	}
	defer w.Close()


	n, err := w.Write(b)
	if err != nil {
		// cleanUp()
		return err
	}

	if n != len(b) {
		// cleanUp()
		return io.ErrShortWrite
	}

	return nil
}

func (s *fsv1Storage) CheckFile(ctx context.Context, volume string, path string) error {
	return NotImplemented{}
}

func (s *fsv1Storage) CheckParts(ctx context.Context, volume string, path string, fi FileInfo) error {
	return NotImplemented{}
}

// srcVolume: srcBucket
// srcPath: path to folder that contains fs.json (meta) and <dataDir>/part.1 (object)
// dstVolume: dstBucket
// dstPath: 1. destination path for "object": <dstVolume>/<dstPath>
//          2. destination path for "meta": <minioMetaBucket>/<bucketMetaPrefix>/<dstVolume>/dstPath/fs.json
func (s *fsv1Storage) RenameData(ctx context.Context, srcVolume, srcPath, dataDir, dstVolume, dstPath string) (err error) {
	srcVolumeDir, err := s.getVolDir(srcVolume)
	if err != nil {
		return err
	}

	dstVolumeDir, err := s.getVolDir(dstVolume)
	if err != nil {
		return err
	}

	// Stat a volume entry.
	_, err = Lstat(srcVolumeDir)
	if err != nil {
		if osIsNotExist(err) {
			return errVolumeNotFound
		} else if isSysErrIO(err) {
			return errFaultyDisk
		}
		return err
	}

	if _, err = Lstat(dstVolumeDir); err != nil {
		if osIsNotExist(err) {
			return errVolumeNotFound
		} else if isSysErrIO(err) {
			return errFaultyDisk
		}
		return err
	}

	srcMetaFilePath := pathutil.Join(srcVolumeDir, pathJoin(srcPath, fsMetaJSONFile))

	var dstMetaFilePath string
	if dstVolume != minioMetaBucket { // TODO: lock
		metaVolume, err := s.getVolDir(minioMetaBucket)
		if err != nil {
			return err
		}
		dstMetaFilePath = pathutil.Join(metaVolume, bucketMetaPrefix, dstVolume, pathJoin(dstPath, fsMetaJSONFile))
	}

	objectSrcPath := pathutil.Join(srcVolumeDir, pathJoin(srcPath, dataDir, "part.1"))
	objectDstPath := pathutil.Join(dstVolumeDir, pathJoin(dstPath))

	err = fsRenameFile(ctx, objectSrcPath, objectDstPath)
	if err != nil {
		return err
	}

	if dstVolume != minioMetaBucket { // TODO: unlock
		err = fsRenameFile(ctx, srcMetaFilePath, dstMetaFilePath)
		if err != nil {
			return err
		}
	}

	return nil
}

func openFile(filePath string, mode int) (f *os.File, err error) {
	// Create top level directories if they don't exist.
	// with mode 0777 mkdir honors system umask.
	if err = mkdirAll(pathutil.Dir(filePath), 0777); err != nil {
		return nil, osErrToFileErr(err)
	}

	w, err := OpenFile(filePath, mode|writeMode, 0666)
	if err != nil {
		// File path cannot be verified since one of the parents is a file.
		switch {
		case isSysErrIsDir(err):
			return nil, errIsNotRegular
		case osIsPermission(err):
			return nil, errFileAccessDenied
		case isSysErrIO(err):
			return nil, errFaultyDisk
		case isSysErrTooManyFiles(err):
			return nil, errTooManyOpenFiles
		default:
			return nil, err
		}
	}

	return w, nil
}

type StatReadSeekCloser interface {
	io.ReadSeekCloser
	Stat() (fs.FileInfo, error)
}

type StatReadWriteSeekCloser interface {
	io.ReadWriteCloser
	io.Seeker
	Stat() (fs.FileInfo, error)
}

func (s *fsv1Storage) getOptionalMetaReadLock(ctx context.Context, lockType LockType, volume, path string) (StatReadSeekCloser, func(), error) {
	volDir, err := s.getVolDir(minioMetaBucket)
	if err != nil {
		return nil, func() {}, err
	}

	var fsMetaPath string
	if volume == minioMetaBucket {
		fsMetaPath = pathJoin(volDir, path)
	} else {
		fsMetaPath = pathJoin(volDir, bucketMetaPrefix, volume, path, fsMetaJSONFile)
	}

	if lockType == noLock {
		l, err := openFile(fsMetaPath, readMode)
		if err != nil {
			logger.LogIf(ctx, err)
			return nil, func() {}, err
		}
		return l, func() {
			l.Close()
		}, nil
	}

	rlk, err := s.rwPool.Open(fsMetaPath)
	if err != nil {
		logger.LogIf(ctx, err)
		return nil, func() {}, err
	}
	return rlk, func() {
		s.rwPool.Close(fsMetaPath)
	}, nil
}

func (s *fsv1Storage) getMetaWriteLock(ctx context.Context, volume, path string) (StatReadWriteSeekCloser, func(), func(), error) {
	volDir, err := s.getVolDir(minioMetaBucket)
	if err != nil {
		return nil, func() {}, func() {}, err
	}

	var fsMetaPath string
	if volume == minioMetaBucket {
		fsMetaPath = pathJoin(volDir, path)
	} else {
		fsMetaPath = pathJoin(volDir, bucketMetaPrefix, volume, path, fsMetaJSONFile)
	}

	cleanUp := func() {}
	rlk, err := s.rwPool.Write(fsMetaPath)
	if err != nil {
		rlk, err = s.rwPool.Create(fsMetaPath)
		if err != nil {
			logger.LogIf(ctx, err)
			return nil, func() {}, cleanUp, err
		}

		cleanUp = func() { // TODO: we create an empty meta file just for locking, the caller should delete this file if the caller function fails...
			tmpDir := pathJoin(s.String(), minioMetaTmpBucket, s.diskID)
			fsRemoveMeta(ctx, pathJoin(volDir, bucketMetaPrefix), fsMetaPath, tmpDir)
		}
	}
	return rlk, func() {
		s.rwPool.Close(fsMetaPath)
	}, cleanUp, nil
}

// ReadVersion - reads metadata and returns FileInfo at path `fs.json`
// for all objects less than `32KiB` this call returns data as well
// along with metadata.
func (s *fsv1Storage) ReadVersion(ctx context.Context, volume, path, versionID string, readData bool) (fi FileInfo, err error) {
	if _, err := s.StatVol(ctx, volume); err != nil {
		return fi, err
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return fi, err
	}

	fsMeta := fsMetaV1{}
	if HasSuffix(path, SlashSeparator) {
		dirInfo, err := fsStatDir(ctx, pathJoin(s.String(), volume, path))
		if err != nil {
			return fi, err
		}
		return fsMeta.ToFileInfo(volume, path, dirInfo), nil
	}

	buf, err := s.ReadAll(ctx, minioMetaBucket, pathJoin(bucketMetaPrefix, volume, path, fsMetaJSONFile))
	if err != nil {
		if err == errFileNotFound {
			fsMeta = defaultFsJSON(path)
		} else {
			logger.LogIf(ctx, err)
			return fi, err
		}
	} else {
		if len(buf) == 0 {
			if versionID != "" {
				return fi, errFileVersionNotFound
			}
			return fi, errFileNotFound
		}

		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		err = json.Unmarshal(buf, &fsMeta)
		if err != nil {
			logger.LogIf(ctx, err)
			return fi, errCorruptedFormat
		}
	}

	if !isFSMetaValid(fsMeta.Version) {
		return fi, errCorruptedFormat
	}

	// Stat the file to get file size.
	fileInfo, err := fsStatFile(ctx, pathJoin(volumeDir, path))
	if err != nil {
		return fi, err
	}

	fi = fsMeta.ToFileInfo(volume, path, fileInfo)

	if readData {
		// Reading data for small objects when
		// - object has not yet transitioned
		// - object size lesser than 32KiB
		// - object has maximum of 1 parts
		if fi.Size <= smallFileThreshold {
			fi.Data, err = s.ReadAll(ctx, volume, path)
			if err != nil {
				return FileInfo{}, err
			}
		}
	}

	return fi, nil
}

func (s *fsv1Storage) ReadAll(ctx context.Context, volume string, path string) ([]byte, error) {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return nil, err
	}

	// Validate file path length, before reading.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return nil, err
	}
	var lockType = readLock
	// Open the file for reading.
	if tmpLockType, ok := ctx.Value(lockTypeKey).(LockType); ok {
		lockType = tmpLockType
	}
	metaFile, unlockMeta, err := s.getOptionalMetaReadLock(ctx, lockType, volume, path)
	defer unlockMeta()

	if  err != nil &&
		(volume == minioMetaBucket || (volume != minioMetaBucket && err != errFileNotFound)) {
		return nil, err
	}

	var file io.ReadCloser
	if volume != minioMetaBucket {
		file, err = OpenFile(filePath, readMode, 0)

		if err != nil {
			if osIsNotExist(err) {
				// Check if the object doesn't exist because its bucket
				// is missing in order to return the correct error.
				_, err = Lstat(volumeDir)
				if err != nil && osIsNotExist(err) {
					return nil, errVolumeNotFound
				}
				return nil, errFileNotFound
			} else if osIsPermission(err) {
				return nil, errFileAccessDenied
			} else if isSysErrNotDir(err) || isSysErrIsDir(err) {
				return nil, errFileNotFound
			} else if isSysErrHandleInvalid(err) {
				// This case is special and needs to be handled for windows.
				return nil, errFileNotFound
			} else if isSysErrIO(err) {
				return nil, errFaultyDisk
			} else if isSysErrTooManyFiles(err) {
				return nil, errTooManyOpenFiles
			} else if isSysErrInvalidArg(err) {
				st, _ := Lstat(filePath)
				if st != nil && st.IsDir() {
					// Linux returns InvalidArg for directory O_DIRECT
					// we need to keep this fallback code to return correct
					// errors upwards.
					return nil, errFileNotFound
				}
				return nil, errUnsupportedDisk
			}
			return nil, err
		}
		defer file.Close()
	} else {
		file = metaFile
	}

	buf, err := ioutil.ReadAll(file)
	if err != nil {
		err = osErrToFileErr(err)
	}
	return buf, err
}

// ReadFileStream - Returns the read stream of the file.
func (s *fsv1Storage) ReadFileStream(ctx context.Context, volume, path string, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 {
		return nil, errInvalidArgument
	}

	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return nil, err
	}

	// Validate effective path length before reading.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return nil, err
	}

	var lockType = readLock
	// Open the file for reading.
	if tmpLockType, ok := ctx.Value(lockTypeKey).(LockType); ok {
		lockType = tmpLockType
	}

	_, unlockMeta, err := s.getOptionalMetaReadLock(ctx, lockType, volume, path)
	defer unlockMeta()
	if  err != nil &&
		(volume == minioMetaBucket || (volume != minioMetaBucket && err != errFileNotFound)) {
		return nil, err
	}

	file, err := openFile(filePath, readMode)
	if err != nil {
		switch {
		case osIsNotExist(err):
			_, err = Lstat(volumeDir)
			if err != nil && osIsNotExist(err) {
				return nil, errVolumeNotFound
			}
			return nil, errFileNotFound
		case osIsPermission(err):
			return nil, errFileAccessDenied
		case isSysErrNotDir(err):
			return nil, errFileAccessDenied
		case isSysErrIO(err):
			return nil, errFaultyDisk
		case isSysErrTooManyFiles(err):
			return nil, errTooManyOpenFiles
		case isSysErrInvalidArg(err):
			return nil, errUnsupportedDisk
		default:
			return nil, err
		}
	}

	st, err := file.Stat()
	if err != nil {
		unlockMeta()
		return nil, err
	}

	// Verify it is a regular file, otherwise subsequent Seek is
	// undefined.
	if !st.Mode().IsRegular() {
		unlockMeta()
		return nil, errIsNotRegular
	}

	r := struct {
		io.Reader
		io.Closer
	}{Reader: io.LimitReader(file, length), Closer: closeWrapper(func() error {
		unlockMeta()
		return nil
	})}

	if offset > 0 {
		if _, err = file.Seek(offset, io.SeekStart); err != nil {
			unlockMeta()
			return nil, err
		}
	}

	// Add readahead to big reads
	if length >= readAheadSize {
		rc, err := readahead.NewReadCloserSize(r, readAheadBuffers, readAheadBufSize)
		if err != nil {
			unlockMeta()
			return nil, err
		}
		return rc, nil
	}

	// Just add a small 64k buffer.
	r.Reader = bufio.NewReaderSize(r.Reader, 64<<10)
	return r, nil
}

func (s *fsv1Storage) ReadFile(ctx context.Context, volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (int64, error) {
	return 0, NotImplemented{}
}

func (s *fsv1Storage) WalkVersions(ctx context.Context, volume, dirPath, marker string, recursive bool, endWalkCh <-chan struct{}) (chan FileInfoVersions, error) {
	return nil, NotImplemented{}
}

// ListDir - return all the entries at the given directory path.
// If an entry is a directory it will be returned with a trailing SlashSeparator.
func (s *fsv1Storage) ListDir(ctx context.Context, volume, dirPath string, count int) (entries []string, err error) {
	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return nil, err
	}

	dirPathAbs := pathJoin(volumeDir, dirPath)
	if count > 0 {
		entries, err = readDirN(dirPathAbs, count)
	} else {
		entries, err = readDir(dirPathAbs)
	}
	if err != nil {
		if err == errFileNotFound {
			if _, verr := Lstat(volumeDir); verr != nil {
				if osIsNotExist(verr) {
					return nil, errVolumeNotFound
				} else if isSysErrIO(verr) {
					return nil, errFaultyDisk
				}
			}
		}
		return nil, err
	}

	return entries, nil
}

func (s *fsv1Storage) Delete(ctx context.Context, volume string, path string, recursive bool) error {
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return err
	}

	// Stat a volume entry.
	_, err = Lstat(volumeDir)
	if err != nil {
		if osIsNotExist(err) {
			return errVolumeNotFound
		} else if osIsPermission(err) {
			return errVolumeAccessDenied
		} else if isSysErrIO(err) {
			return errFaultyDisk
		}
		return err
	}

	// Following code is needed so that we retain SlashSeparator suffix if any in
	// path argument.
	filePath := pathJoin(volumeDir, path)
	if err = checkPathLength(filePath); err != nil {
		return err
	}

	// Delete file and delete parent directory as well if it's empty.
	return s.deleteFile(volumeDir, filePath, recursive)
}

// deleteFile deletes a file or a directory if its empty unless recursive
// is set to true. If the target is successfully deleted, it will recursively
// move up the tree, deleting empty parent directories until it finds one
// with files in it. Returns nil for a non-empty directory even when
// recursive is set to false.
func (s *fsv1Storage) deleteFile(basePath, deletePath string, recursive bool) error {
	if basePath == "" || deletePath == "" {
		return nil
	}
	isObjectDir := HasSuffix(deletePath, SlashSeparator)
	basePath = pathutil.Clean(basePath)
	deletePath = pathutil.Clean(deletePath)
	if !strings.HasPrefix(deletePath, basePath) || deletePath == basePath {
		return nil
	}

	var err error
	if recursive {
		err = renameAll(deletePath, pathutil.Join(s.diskPath, minioMetaTmpDeletedBucket, mustGetUUID()))
	} else {
		err = Remove(deletePath)
	}
	if err != nil {
		switch {
		case isSysErrNotEmpty(err):
			// if object is a directory, but if its not empty
			// return FileNotFound to indicate its an empty prefix.
			if isObjectDir {
				return errFileNotFound
			}
			// Ignore errors if the directory is not empty. The server relies on
			// this functionality, and sometimes uses recursion that should not
			// error on parent directories.
			return nil
		case osIsNotExist(err):
			return errFileNotFound
		case osIsPermission(err):
			return errFileAccessDenied
		case isSysErrIO(err):
			return errFaultyDisk
		default:
			return err
		}
	}

	deletePath = pathutil.Dir(deletePath)

	// Delete parent directory obviously not recursively. Errors for
	// parent directories shouldn't trickle down.
	s.deleteFile(basePath, deletePath, false)

	return nil
}

func (s *fsv1Storage) DeleteVersions(ctx context.Context, volume string, versions []FileInfo) (errs []error) {
	return nil
}

func (s *fsv1Storage) RenameFile(ctx context.Context, srcVolume, srcPath, dstVolume, dstPath string) (err error) {
	return NotImplemented{}
}

func (s *fsv1Storage) VerifyFile(ctx context.Context, volume, path string, fi FileInfo) error {
	return NotImplemented{}
}

func (s *fsv1Storage) Close() error {
	return nil
}

// isObjectDir returns true if the specified bucket & prefix exists
// and the prefix represents an empty directory. An S3 empty directory
// is also an empty directory in the FS backend.
func (s *fsv1Storage) isObjectDir(bucket, prefix string) bool {
	entries, err := readDirN(pathJoin(s.String(), bucket, prefix), 1)
	if err != nil {
		return false
	}
	return len(entries) == 0
}

func (s *fsv1Storage) getObjectInfoNoFSLockBuf(bucket, object string) ([]byte, error) {
	objInfo, err := s.getObjectInfoNoFSLock(bucket, object)
	if err != nil {
		return nil, err
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	return json.Marshal(objInfo)
}

func (s *fsv1Storage) getObjectInfoNoFSLock(bucket, object string) (oi ObjectInfo, e error) {
	fsMeta := fsMetaV1{}
	if HasSuffix(object, SlashSeparator) {
		fi, err := fsStatDir(s.ctx, pathJoin(s.String(), bucket, object))
		if err != nil {
			return oi, err
		}
		return fsMeta.ToObjectInfo(bucket, object, fi), nil
	}

	fsMetaPath := pathJoin(s.String(), minioMetaBucket, bucketMetaPrefix, bucket, object, fsMetaJSONFile)
	// Read `fs.json` to perhaps contend with
	// parallel Put() operations.

	rc, _, err := fsOpenFile(s.ctx, fsMetaPath, 0)
	if err == nil {
		fsMetaBuf, rerr := ioutil.ReadAll(rc)
		rc.Close()
		if rerr == nil {
			var json = jsoniter.ConfigCompatibleWithStandardLibrary
			if rerr = json.Unmarshal(fsMetaBuf, &fsMeta); rerr != nil {
				// For any error to read fsMeta, set default ETag and proceed.
				fsMeta = defaultFsJSON(object)
			}
		} else {
			// For any error to read fsMeta, set default ETag and proceed.
			fsMeta = defaultFsJSON(object)
		}
	}

	// Return a default etag and content-type based on the object's extension.
	if err == errFileNotFound {
		fsMeta = defaultFsJSON(object)
	}

	// Ignore if `fs.json` is not available, this is true for pre-existing data.
	if err != nil && err != errFileNotFound {
		logger.LogIf(s.ctx, err)
		return oi, err
	}

	// Stat the file to get file size.
	fi, err := fsStatFile(s.ctx, pathJoin(s.String(), bucket, object))
	if err != nil {
		return oi, err
	}

	return fsMeta.ToObjectInfo(bucket, object, fi), nil
}

// WalkDir will traverse a directory and return all entries found.
// On success a sorted meta cache stream will be returned.
func (s *fsv1Storage) WalkDir(ctx context.Context, opts WalkDirOptions, wr io.Writer) error {
	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolDir(opts.Bucket)
	if err != nil {
		return err
	}

	// Stat a volume entry.
	_, err = os.Lstat(volumeDir)
	if err != nil {
		if osIsNotExist(err) {
			return errVolumeNotFound
		} else if isSysErrIO(err) {
			return errFaultyDisk
		}
		return err
	}

	// Use a small block size to start sending quickly
	w := newMetacacheWriter(wr, 16<<10)
	defer w.Close()
	out, err := w.stream()
	if err != nil {
		return err
	}
	defer close(out)

	prefix := opts.FilterPrefix
	forward := opts.ForwardTo
	var scanDir func(path string) error
	scanDir = func(current string) error {
		if contextCanceled(ctx) {
			return ctx.Err()
		}
		entries, err := s.ListDir(ctx, opts.Bucket, current, -1)
		if err != nil {
			// Folder could have gone away in-between
			if err != errVolumeNotFound && err != errFileNotFound {
				logger.LogIf(ctx, err)
			}
			if opts.ReportNotFound && err == errFileNotFound && current == opts.BaseDir {
				return errFileNotFound
			}
			// Forward some errors?
			return nil
		}

		sort.Strings(entries)

		for _, entry := range entries {
			if len(prefix) > 0 && !strings.HasPrefix(pathJoin(current, entry), prefix) {
				continue
			}
			if len(forward) > 0 && pathJoin(current, entry) < forward {
				continue
			}

			if strings.HasSuffix(entry, slashSeparator) {
				cacheEntry := metaCacheEntry{name: pathJoin(current, entry)}
				if s.isObjectDir(opts.Bucket, pathJoin(current, entry)) {
					cacheEntry.metadata, err = s.getObjectInfoNoFSLockBuf(opts.Bucket, pathJoin(current, entry))
					if err != nil {
						logger.LogIf(ctx, err)
						continue
					}
					out <- cacheEntry
					continue
				}
				out <- cacheEntry
				if opts.Recursive {
					// Scan folder we found. Should be in correct sort order where we are.
					forward = ""
					entry = entry[:len(entry)-1]
					if len(opts.ForwardTo) > 0 && strings.HasPrefix(opts.ForwardTo, pathJoin(current, entry)) {
						forward = strings.TrimPrefix(opts.ForwardTo, pathJoin(current, entry))
					}
					logger.LogIf(ctx, scanDir(pathJoin(current, entry)))
				}
				// Trim slash, maybe compiler is clever?
				continue
			}
			// Do do not retain the file.

			if contextCanceled(ctx) {
				return ctx.Err()
			}
			// If root was an object return it as such.
			var meta metaCacheEntry
			meta.metadata, err = s.getObjectInfoNoFSLockBuf(opts.Bucket, pathJoin(current, entry))
			if err != nil {
				logger.LogIf(ctx, err)
				continue
			}
			meta.name = pathJoin(current, entry)
			out <- meta
		}
		return nil
	}

	// Stream output.
	return scanDir(opts.BaseDir)
}

func (s *fsv1Storage) GetDiskLoc() (poolIdx, setIdx, diskIdx int) {
	return s.poolIndex, s.setIndex, s.diskIndex
}

func (s *fsv1Storage) SetDiskLoc(poolIdx, setIdx, diskIdx int) {
	s.poolIndex = poolIdx
	s.setIndex = setIdx
	s.diskIndex = diskIdx
}

type ContextKeys string

const lockTypeKey ContextKeys = "lockTypeKey"