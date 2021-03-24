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
	"context"
	"errors"
	"io"
	"net/url"
	"os"
)

type fsv1Storage struct {
	endpoint Endpoint
	// Indexes, will be -1 until assigned a set.
	poolIndex, setIndex, diskIndex int
}

func newfsv1Storage(path string) (*fsv1Storage, error) {
	u := url.URL{Path: path}
	return &fsv1Storage{
		endpoint: Endpoint{
			URL:     &u,
			IsLocal: true,
		},
	}, nil
}

func (s *fsv1Storage) String() string {
	return s.endpoint.String()
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
	return s.endpoint.String(), NotImplemented{}
}

func (s *fsv1Storage) SetDiskID(id string) {
}

func (s *fsv1Storage) DiskInfo(ctx context.Context) (info DiskInfo, err error) {
	return DiskInfo{}, NotImplemented{}
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

		var fi os.FileInfo
		fi, err = fsStatVolume(ctx, pathJoin(s.String(), entry))
		// There seems like no practical reason to check for errors
		// at this point, if there are indeed errors we can simply
		// just ignore such buckets and list only those which
		// return proper Stat information instead.
		if err != nil {
			// Ignore any errors returned here.
			continue
		}
		var created = fi.ModTime()
		meta, err := globalBucketMetadataSys.Get(fi.Name())
		if err == nil {
			created = meta.Created
		}

		volsInfo = append(volsInfo, VolInfo{
			Name:    fi.Name(),
			Created: created,
		})
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

func (s *fsv1Storage) CreateFile(ctx context.Context, volume, path string, size int64, reader io.Reader) error {
	return NotImplemented{}
}

func (s *fsv1Storage) WriteMetadata(ctx context.Context, volume, path string, fi FileInfo) error {
	return NotImplemented{}
}

func (s *fsv1Storage) DeleteVersion(ctx context.Context, volume, path string, fi FileInfo, forceDelMarker bool) error {
	return NotImplemented{}
}

func (s *fsv1Storage) WriteAll(ctx context.Context, volume string, path string, b []byte) error {
	return NotImplemented{}
}

func (s *fsv1Storage) CheckFile(ctx context.Context, volume string, path string) error {
	return NotImplemented{}
}

func (s *fsv1Storage) CheckParts(ctx context.Context, volume string, path string, fi FileInfo) error {
	return NotImplemented{}
}

func (s *fsv1Storage) RenameData(ctx context.Context, srcVolume, srcPath, dataDir, dstVolume, dstPath string) (err error) {
	return NotImplemented{}
}

func (s *fsv1Storage) ReadVersion(ctx context.Context, volume, path, versionID string, readData bool) (fi FileInfo, err error) {
	return FileInfo{}, NotImplemented{}
}

func (s *fsv1Storage) ReadAll(ctx context.Context, volume string, path string) ([]byte, error) {
	return nil, NotImplemented{}
}

func (s *fsv1Storage) ReadFileStream(ctx context.Context, volume, path string, offset, length int64) (io.ReadCloser, error) {
	return nil, NotImplemented{}
}

func (s *fsv1Storage) ReadFile(ctx context.Context, volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (int64, error) {
	return 0, NotImplemented{}
}

func (s *fsv1Storage) WalkVersions(ctx context.Context, volume, dirPath, marker string, recursive bool, endWalkCh <-chan struct{}) (chan FileInfoVersions, error) {
	return nil, NotImplemented{}
}

func (s *fsv1Storage) ListDir(ctx context.Context, volume, dirPath string, count int) (entries []string, err error) {
	return nil, NotImplemented{}
}

func (s *fsv1Storage) Delete(ctx context.Context, volume string, path string, recursive bool) error {
	return NotImplemented{}
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

func (s *fsv1Storage) WalkDir(ctx context.Context, opts WalkDirOptions, wr io.Writer) error {
	return NotImplemented{}
}

func (s *fsv1Storage) GetDiskLoc() (poolIdx, setIdx, diskIdx int) {
	return s.poolIndex, s.setIndex, s.diskIndex
}

func (s *fsv1Storage) SetDiskLoc(poolIdx, setIdx, diskIdx int) {
	s.poolIndex = poolIdx
	s.setIndex = setIdx
	s.diskIndex = diskIdx
}
