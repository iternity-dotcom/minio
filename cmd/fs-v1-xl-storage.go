package cmd

import (
	"bytes"
	"context"
	"io"
	"os"

	"github.com/minio/minio/pkg/etag"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/lock"
)

type fsXlStorage struct {
	*xlStorage
	metaTmpBucket string
	// FS rw pool.
	rwPool *fsIOPool
}

func (x *fsXlStorage) CreateFile(ctx context.Context, volume, path string, fileSize int64, r io.Reader) error {
	err := x.xlStorage.CreateFile(ctx, volume, path, fileSize, r)
	if err != nil {
		return err
	}

	if countR, ok := r.(*countingReader); ok {
		hashR := countR.PutObjReader.Reader
		expectedMD5 := hashR.MD5()
		calculatedMD5 := hashR.MD5Current()
		if len(expectedMD5) != 0 && !bytes.Equal(expectedMD5, calculatedMD5) {
			return hash.BadDigest{
				ExpectedMD5:   etag.ETag(expectedMD5).String(),
				CalculatedMD5: etag.ETag(calculatedMD5).String(),
			}
		}
	}

	return nil
}

func (x *fsXlStorage) ContextWithMetaLock(ctx context.Context, lockType LockType, volume string, paths ...string) (context.Context, func(err ...error), error) {
	// return ctx, func(err ...error) {}, nil
	if l := ctx.Value(objectLock); l != nil {
		return nil, nil, lock.ErrAlreadyLocked
	}
	var fLocks locks
	var cleanup func(err ...error)
	var err error
	switch lockType {
	case writeLock:
		fLocks, cleanup, err = x.rwPool.newRWMetaLock(x, volume, paths...)
	case readLock:
		fLocks, cleanup, err = x.rwPool.newRMetaLock(x, volume, paths...)
	case noLock:
		fLocks, cleanup, err = x.rwPool.newNMetaLock(x, volume, paths...)
	default:
		return nil, nil, errInvalidArgument
	}

	if err != nil {
		return nil, nil, err
	}
	return context.WithValue(ctx, objectLock, fLocks), cleanup, nil
}

func (x *fsXlStorage) getLockPath(volume string, path string) (string, string, error) {
	volDir, err := x.getVolDir(volume)
	if err != nil {
		return "", "", err
	}
	return volDir, x.getMetaPathFile(volume, path), nil
}

func (x *fsXlStorage) getMetaPathFile(volume string, path string) string {
	return pathJoin(path, xlStorageFormatFile)
}

func (x *fsXlStorage) rMetaLocker(ctx context.Context, volume string, path string) (FileWriter, error) {
	locks := getLocks(ctx)
	return locks.rMetaLocker(x, volume, path)
}
func (x *fsXlStorage) rwMetaLocker(ctx context.Context, volume string, path string, truncate bool) (FileWriter, error) {
	locks := getLocks(ctx)
	return locks.rwMetaLocker(x, volume, path, truncate)
}

func (x *fsXlStorage) MetaTmpBucket() string {
	return x.metaTmpBucket
}

func (x *fsXlStorage) CacheEntriesToObjInfos(cacheEntries metaCacheEntriesSorted, opts listPathOptions) []ObjectInfo {
	return cacheEntries.fileInfos(opts.Bucket, opts.Prefix, opts.Separator)
}

func (x *fsXlStorage) EncodeDirObject(object string) string {
	return encodeDirObject(object)
}

func (x *fsXlStorage) DecodeDirObject(object string) string {
	return decodeDirObject(object)
}

func (x *fsXlStorage) IsDirObject(object string) bool {
	return HasSuffix(object, globalDirSuffix)
}

func newLocalFSXLStorage(fsPath string) (fsStorageAPI, error) {
	storage, err := newLocalXLStorage(fsPath)
	if err != nil {
		return nil, err
	}
	x := &fsXlStorage{
		xlStorage:     storage,
		metaTmpBucket: minioMetaTmpBucket,
		rwPool: &fsIOPool{
			readersMap: make(map[string]*lock.RLockedFile),
		},
	}
	x.xlStorage.openFileNormal = x._openFile
	x.xlStorage.openFileDirectIO = x._openFile
	return x, nil
}

func (x *fsXlStorage) _openFile(ctx context.Context, volume string, path string, flag int, perm os.FileMode) (FileWriter, error) {
	locks := getLocks(ctx)
	truncate := false
	append := false

	if (flag & os.O_TRUNC) > 0 {
		truncate = true
	}
	if (flag & os.O_APPEND) > 0 {
		append = true
	}
	if flag&os.O_WRONLY == 0 && flag&os.O_RDWR == 0 {
		return locks.rMetaLocker(x, volume, path)
	} else {
		fw, err := locks.rwMetaLocker(x, volume, path, truncate)
		if err != nil {
			return nil, err
		}
		if append {
			_, err := fw.Seek(0, io.SeekEnd)
			if err != nil {
				return nil, err
			}
		}
		return fw, nil
	}
}
