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
	"io"
	"io/fs"
	"os"
	pathutil "path"
	"sync"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/lock"
)

// fsIOPool represents a protected list to keep track of all
// the concurrent readers at a given path.
type fsIOPool struct {
	sync.Mutex
	readersMap map[string]*lock.RLockedFile
}

type truncatingFileWriter interface {
	FileWriter
	Truncate(int64) error
}

// FileWriter is a wrapper interface, which mimics *os.File
type FileWriter interface {
	io.ReadWriteSeeker
	io.ReaderAt
	io.Closer
	Fd() uintptr
	Stat() (fs.FileInfo, error)
}

type lockPaths struct {
	volume   string
	object   string
	lockPath string
}

func (lp *lockPaths) Volume() string {
	return lp.volume
}

func (lp *lockPaths) Object() string {
	return lp.object
}

func (lp *lockPaths) LockPath() string {
	return lp.lockPath
}

type rwLock struct {
	*lockPaths
	*lock.LockedFile
}

func (l *rwLock) Close() error {
	return nil
}
func (l *rwLock) LockType() LockType {
	return writeLock
}

type rLock struct {
	*lockPaths
	*lock.RLockedFile
}

func (l *rLock) Close() error {
	return nil
}
func (l *rLock) LockType() LockType {
	return readLock
}

type nLock struct {
	*lockPaths
}

func (l *nLock) LockType() LockType {
	return noLock
}

type metaLock interface {
	Volume() string
	Object() string
	LockPath() string
	LockType() LockType
}
type locks map[string]metaLock

func getLocks(ctx context.Context) locks {
	var l interface{}
	var fl locks
	var ok bool
	if l = ctx.Value(objectLock); l == nil {
		return nil
	}

	if fl, ok = l.(locks); !ok {
		return nil
	}
	return fl
}

type storageFunctions interface {
	getLockPath(volume string, path string) (string, string, error)
	checkPathInObject(path string, object string) bool
	getVolDir(volume string) (string, error)
	deleteFile(volDir string, filePath string, recurse bool) error
}

// lookupToRead - looks up an fd from readers map and
// returns read locked fd for caller to read from, if
// fd found increments the reference count. If the fd
// is found to be closed then purges it from the
// readersMap and returns nil instead.
//
// NOTE: this function is not protected and it is callers
// responsibility to lock this call to be thread safe. For
// implementation ideas look at the usage inside Open() call.
func (fsi *fsIOPool) lookupToRead(path string) (*lock.RLockedFile, bool) {
	rlkFile, ok := fsi.readersMap[path]
	// File reference exists on map, validate if its
	// really closed and we are safe to purge it.
	if ok && rlkFile != nil {
		// If the file is closed and not removed from map is a bug.
		if rlkFile.IsClosed() {
			// Log this as an error.
			reqInfo := (&logger.ReqInfo{}).AppendTags("path", path)
			ctx := logger.SetReqInfo(GlobalContext, reqInfo)
			logger.LogIf(ctx, errUnexpected)

			// Purge the cached lock path from map.
			delete(fsi.readersMap, path)

			// Indicate that we can populate the new fd.
			ok = false
		} else {
			// Increment the lock ref, since the file is not closed yet
			// and caller requested to read the file again.
			rlkFile.IncLockRef()
		}
	}
	return rlkFile, ok
}

// Open is a wrapper call to read locked file which
// returns a ReadAtCloser.
//
// ReaderAt is provided so that the fd is non seekable, since
// we are sharing fd's with concurrent threads, we don't want
// all readers to change offsets on each other during such
// concurrent operations. Using ReadAt allows us to read from
// any offsets.
//
// Closer is implemented to track total readers and to close
// only when there no more readers, the fd is purged if the lock
// count has reached zero.
func (fsi *fsIOPool) Open(path string) (*lock.RLockedFile, error) {
	if err := checkPathLength(path); err != nil {
		return nil, err
	}

	fsi.Lock()
	rlkFile, ok := fsi.lookupToRead(path)
	fsi.Unlock()
	// Locked path reference doesn't exist, acquire a read lock again on the file.
	if !ok {
		// Open file for reading with read lock.
		newRlkFile, err := lock.RLockedOpenFile(path)
		if err != nil {
			switch {
			case osIsNotExist(err):
				return nil, errFileNotFound
			case osIsPermission(err):
				return nil, errFileAccessDenied
			case isSysErrIsDir(err):
				return nil, errIsNotRegular
			case isSysErrNotDir(err):
				return nil, errFileAccessDenied
			case isSysErrPathNotFound(err):
				return nil, errFileNotFound
			default:
				return nil, err
			}
		}

		/// Save new reader on the map.

		// It is possible by this time due to concurrent
		// i/o we might have another lock present. Lookup
		// again to check for such a possibility. If no such
		// file exists save the newly opened fd, if not
		// reuse the existing fd and close the newly opened
		// file
		fsi.Lock()
		rlkFile, ok = fsi.lookupToRead(path)
		if ok {
			// Close the new fd, since we already seem to have
			// an active reference.
			newRlkFile.Close()
		} else {
			// Save the new rlk file.
			rlkFile = newRlkFile
		}

		// Save the new fd on the map.
		fsi.readersMap[path] = rlkFile
		fsi.Unlock()

	}

	// Success.
	return rlkFile, nil
}

// Write - Attempt to lock the file if it exists,
// - if the file exists. Then we try to get a write lock this
//   will block if we can't get a lock perhaps another write
//   or read is in progress. Concurrent calls are protected
//   by the global namspace lock within the same process.
func (fsi *fsIOPool) Write(path string) (wlk *lock.LockedFile, err error) {
	if err = checkPathLength(path); err != nil {
		return nil, err
	}

	wlk, err = lock.LockedOpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		switch {
		case osIsNotExist(err):
			return nil, errFileNotFound
		case osIsPermission(err):
			return nil, errFileAccessDenied
		case isSysErrIsDir(err):
			return nil, errIsNotRegular
		default:
			if isSysErrPathNotFound(err) {
				return nil, errFileNotFound
			}
			return nil, err
		}
	}
	return wlk, nil
}

// Create - creates a new write locked file instance.
// - if the file doesn't exist. We create the file and hold lock.
func (fsi *fsIOPool) Create(path string) (wlk *lock.LockedFile, err error) {
	if err = checkPathLength(path); err != nil {
		return nil, err
	}

	// Creates parent if missing.
	if err = mkdirAll(pathutil.Dir(path), 0777); err != nil {
		return nil, err
	}

	// Attempt to create the file.
	wlk, err = lock.LockedOpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		switch {
		case osIsPermission(err):
			return nil, errFileAccessDenied
		case isSysErrIsDir(err):
			return nil, errIsNotRegular
		case isSysErrPathNotFound(err):
			return nil, errFileAccessDenied
		default:
			return nil, err
		}
	}

	// Success.
	return wlk, nil
}

// Close implements closing the path referenced by the reader in such
// a way that it makes sure to remove entry from the map immediately
// if no active readers are present.
func (fsi *fsIOPool) Close(path string) error {
	fsi.Lock()
	defer fsi.Unlock()

	if err := checkPathLength(path); err != nil {
		return err
	}

	// Pop readers from path.
	rlkFile, ok := fsi.readersMap[path]
	if !ok {
		return nil
	}

	// Close the reader.
	rlkFile.Close()

	// If the file is closed, remove it from the reader pool map.
	if rlkFile.IsClosed() {

		// Purge the cached lock path from map.
		delete(fsi.readersMap, path)
	}

	// Success.
	return nil
}

func (ls locks) metaLocker(s storageFunctions, volume string, path string, flag int, perm os.FileMode) (FileWriter, error) {

	readLock := false
	metaVolDir, err := s.getVolDir(minioMetaBucket)
	if err != nil {
		return nil, err
	}

	volDir, err := s.getVolDir(volume)
	if err != nil {
		return nil, err
	}
	fsPath := pathJoin(volDir, path)

	var f FileWriter
	if flag&os.O_WRONLY == 0 && flag&os.O_RDWR == 0 {
		readLock = true
		f, err = ls.rMetaLocker(s, fsPath, volume, path, flag, perm)
	} else {
		f, err = ls.rwMetaLocker(s, fsPath, volume, path, flag, perm)
	}

	if err != nil {
		return nil, err
	}
	if f != nil {
		return f, nil
	}

	// it is in the meta bucket, but it is not in the meta path
	if ls == nil || (HasPrefix(fsPath, metaVolDir) && !HasPrefix(fsPath, pathJoin(metaVolDir, bucketMetaPrefix))) {
		if readLock {
			return OpenFile(pathJoin(volDir, path), flag, perm)
		}
		return createFile(pathJoin(volDir, path), flag)
	}
	return nil, errFileNotFound
}

func (ls locks) rMetaLocker(s storageFunctions, fsPath string, volume string, path string, flag int, perm os.FileMode) (FileWriter, error) {
	if l, ok := ls[fsPath]; ok {
		if l.LockType() == writeLock || l.LockType() == readLock {

			rl, ok := l.(FileWriter)
			if !ok {
				return nil, errInvalidArgument
			}

			return rl, nil

		} else if l.LockType() == noLock {

			return OpenFile(fsPath, flag, perm)
		}

		return nil, errInvalidArgument
	}

	for _, l := range ls {
		if l.Volume() == volume && s.checkPathInObject(path, l.Object()) {
			return OpenFile(fsPath, flag, perm)
		}
	}
	return nil, nil
}

func (ls locks) rwMetaLocker(s storageFunctions, fsPath string, volume string, path string, flag int, perm os.FileMode) (FileWriter, error) {

	truncate := false
	append := false
	if (flag & os.O_TRUNC) > 0 {
		truncate = true
	}
	if (flag & os.O_APPEND) > 0 {
		append = true
	}

	if l, ok := ls[fsPath]; ok {
		if l.LockType() == writeLock {
			wl, ok := l.(truncatingFileWriter)
			if !ok {
				return nil, errInvalidArgument
			}
			if truncate {
				wl.Seek(0, io.SeekStart)
				wl.Truncate(0)
			} else if append {
				wl.Seek(0, io.SeekEnd)
			} else {
				wl.Seek(0, io.SeekStart)
			}

			return wl, nil
		}

		return nil, errInvalidArgument
	}

	for _, l := range ls {
		if l.LockType() != writeLock {
			continue
		}

		if l.Volume() == volume && s.checkPathInObject(path, l.Object()) {
			return createFile(fsPath, flag)
		}
	}

	return nil, nil
}

func (fsi *fsIOPool) newNMetaLock(s storageFunctions, volume string, paths ...string) (locks, func(err ...error), error) {

	fLocks := make(locks, len(paths))
	errs := make([]error, len(paths))
	cleanups := make([]func(error), len(paths))
	cleanup := func(errs ...error) {
		for i, cleanupFunction := range cleanups {
			if cleanupFunction != nil {
				cleanupFunction(errs[i])
			}
		}
	}
	for i, path := range paths {
		if HasSuffix(path, slashSeparator) {
			continue
		}
		lockVolDir, lockFile, err := s.getLockPath(volume, path)
		if err != nil {
			errs[i] = err
			cleanup(errs...)
			return nil, nil, err
		}
		lockPath := pathJoin(lockVolDir, lockFile)

		if _, ok := fLocks[lockPath]; ok {
			continue
		}
		fLocks[lockPath] = &nLock{
			lockPaths: &lockPaths{
				volume:   volume,
				object:   path,
				lockPath: lockPath,
			},
		}
		cleanups[i] = func(err error) {}
	}

	return fLocks, cleanup, nil
}

func (fsi *fsIOPool) newRMetaLock(s storageFunctions, volume string, paths ...string) (locks, func(err ...error), error) {

	fLocks := make(locks, len(paths))
	errs := make([]error, len(paths))
	cleanups := make([]func(error), len(paths))
	cleanup := func(errs ...error) {
		for i, cleanupFunction := range cleanups {
			if cleanupFunction != nil {
				cleanupFunction(errs[i])
			}
		}
	}
	for i, path := range paths {
		if HasSuffix(path, slashSeparator) {
			continue
		}
		lockVolDir, lockFile, err := s.getLockPath(volume, path)
		if err != nil {
			errs[i] = err
			cleanup(errs...)
			return nil, nil, err
		}
		lockPath := pathJoin(lockVolDir, lockFile)

		if _, ok := fLocks[lockPath]; ok {
			continue
		}
		var rlk *lock.RLockedFile
		rlk, err = fsi.Open(lockPath)
		if err != nil {
			if err == errFileNotFound {
				fLocks[lockPath] = &nLock{
					lockPaths: &lockPaths{
						volume:   volume,
						object:   path,
						lockPath: lockPath,
					},
				}
				cleanups[i] = func(err error) {}
			} else {
				errs[i] = err
				cleanup(errs...)
				return nil, nil, err
			}
		} else {
			fLocks[lockPath] = &rLock{
				lockPaths: &lockPaths{
					volume:   volume,
					object:   path,
					lockPath: lockPath,
				},
				RLockedFile: rlk,
			}
			cleanups[i] = func(err error) {
				fsi.Close(lockPath)

			}
		}

	}
	return fLocks, cleanup, nil
}

func (fsi *fsIOPool) newRWMetaLock(s storageFunctions, volume string, paths ...string) (locks, func(err ...error), error) {

	fLocks := make(locks, len(paths))
	errs := make([]error, len(paths))
	cleanups := make([]func(error), len(paths))
	cleanup := func(errs ...error) {
		for i, cleanupFunction := range cleanups {
			if cleanupFunction != nil {
				cleanupFunction(errs[i])
			}
		}
	}
	for i, path := range paths {
		if HasSuffix(path, slashSeparator) {
			continue
		}
		lockVolDir, lockFile, err := s.getLockPath(volume, path)
		if err != nil {
			errs[i] = err
			cleanup(errs...)
			return nil, nil, err
		}
		lockPath := pathJoin(lockVolDir, lockFile)

		if _, ok := fLocks[lockPath]; ok {
			continue
		}
		var wlk *lock.LockedFile
		wlk, err = fsi.Write(lockPath)
		var freshFile bool
		if err != nil {
			wlk, err = fsi.Create(lockPath)
			if err != nil {
				errs[i] = err
				cleanup(errs...)
				return nil, nil, err
			}
			freshFile = true
		}
		fLocks[lockPath] = &rwLock{
			lockPaths: &lockPaths{
				volume:   volume,
				object:   path,
				lockPath: lockPath,
			},
			LockedFile: wlk,
		}
		cleanups[i] = func(err error) {
			wlk.Close()
			if err != nil && freshFile {
				s.deleteFile(lockVolDir, lockPath, true)
			}
		}
	}

	return fLocks, cleanup, nil
}
