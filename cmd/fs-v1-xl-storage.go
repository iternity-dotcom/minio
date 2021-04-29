package cmd

import (
	"bytes"
	"context"
	"github.com/minio/minio/pkg/etag"
	"github.com/minio/minio/pkg/hash"
	"io"
)

type fsXlStorage struct {
	*xlStorage
	metaTmpBucket string
}

func (x *fsXlStorage) CreateFile(ctx context.Context, volume, path string, fileSize int64, r io.Reader) (error) {
	err := x.xlStorage.CreateFile(ctx, volume, path, fileSize, r)
	if err != nil {
		return err
	}

	if hashR, ok := r.(*hash.Reader); ok {
		if len(hashR.MD5()) != 0 && !bytes.Equal(hashR.MD5(), hashR.MD5Current()) {
			return hash.BadDigest{
				ExpectedMD5:   etag.ETag(hashR.MD5()).String(),
				CalculatedMD5: etag.ETag(hashR.MD5Current()).String(),
			}

		}
	}

	return nil
}

func (x *fsXlStorage) ContextWithMetaLock(ctx context.Context, l LockType, volume string, paths ...string) (context.Context, func(err ...error), error) {
	// noop
	return ctx, func(err ...error) {}, nil
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
	return &fsXlStorage{
		xlStorage:     storage,
		metaTmpBucket: minioMetaTmpBucket,
	}, err
}
