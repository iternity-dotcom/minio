package cmd

import "context"

type fsXlStorage struct {
	*xlStorage
	metaTmpBucket string
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
