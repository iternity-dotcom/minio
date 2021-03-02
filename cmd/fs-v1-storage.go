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
	"io"
	"net/url"
)

type fsv1Storage struct {
	endpoint Endpoint
}

func newfsv1Storage(path string) *fsv1Storage {
	u := url.URL{Path: path}
	return &fsv1Storage{
		endpoint: Endpoint{
			URL:     &u,
			IsLocal: true,
		},
	}
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

func (s *fsv1Storage) Healing() bool {
	return false
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

func (s *fsv1Storage) MakeVolBulk(ctx context.Context, volumes ...string) (err error) {
	return NotImplemented{}
}

func (s *fsv1Storage) MakeVol(ctx context.Context, volume string) (err error) {
	return NotImplemented{}
}

func (s *fsv1Storage) ListVols(ctx context.Context) (vols []VolInfo, err error) {
	return nil, NotImplemented{}
}

func (s *fsv1Storage) StatVol(ctx context.Context, volume string) (vol VolInfo, err error) {
	return VolInfo{}, NotImplemented{}
}

func (s *fsv1Storage) DeleteVol(ctx context.Context, volume string, forceDelete bool) (err error) {
	return NotImplemented{}
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
