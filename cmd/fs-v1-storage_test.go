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
	"io"
	"io/ioutil"
	"os"
	slashpath "path"
	"runtime"
	"strings"
	"syscall"
	"testing"
)

// creates a temp dir and sets up fsStorage layer.
// returns fsStorage layer, temp dir path to be used for the purpose of tests.
func newFSStorageTestSetup() (fsStorageAPI, string, error) {
	diskPath, err := ioutil.TempDir(globalTestTmpDir, "minio-")
	if err != nil {
		return nil, "", err
	}

	// Initialize a new fsStorage layer.
	storage, err := newLocalFSV1Storage(diskPath)
	if err != nil {
		return nil, "", err
	}
	// Create a sample format.json file
	err = storage.WriteAll(context.Background(), minioMetaBucket, formatConfigFile, []byte(`{"version":"1","format":"fs","id":"dd7dbffe-acee-4ae8-8e6d-a937095f687f","fs":{"version":"2"}}`))
	if err != nil {
		return nil, "", err
	}
	return storage, diskPath, nil
}

// TestFSStorageReadVersion - TestFSStorages the functionality implemented by fsStorage ReadVersion storage API.
func TestFSStorageReadVersion(t *testing.T) {
	// create fsStorage test setup
	fsStorage, path, err := newFSStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create fsStorage test setup, %s", err)
	}

	defer os.RemoveAll(path)

	fsJson, _ := ioutil.ReadFile("testdata/fs.json")
	corruptedFsJson, _ := ioutil.ReadFile("testdata/fs-corrupted.json")

	// Create files for the test cases.
	if err = fsStorage.MakeVol(context.Background(), "exists"); err != nil {
		t.Fatalf("Unable to create a volume \"exists\", %s", err)
	}

	createFileWithMeta(t, fsStorage, "exists", "as-directory/as-file", fsJson, []byte("Hello, World"))
	createFileWithMeta(t, fsStorage, "exists", "as-file", fsJson, []byte("Hello, World"))
	createFileWithMeta(t, fsStorage, "exists", "as-file-parent", fsJson, []byte("Hello, World"))
	createFileWithMeta(t, fsStorage, "exists", "corrupted-file", corruptedFsJson, []byte("Hello, World"))

	// TestFSStoragecases to validate different conditions for ReadVersion API.
	testCases := []struct {
		volume string
		path   string
		err    error
		expData []byte
	}{
		// TestFSStorage case - 1.
		// Validate volume does not exist.
		{
			volume: "i-dont-exist",
			path:   "",
			err:    errVolumeNotFound,
		},
		// TestFSStorage case - 2.
		// Validate bad condition file does not exist.
		{
			volume: "exists",
			path:   "as-file-not-found",
			err:    errFileNotFound,
		},
		// TestFSStorage case - 3.
		// Validate bad condition file exists as prefix/directory and
		// we are attempting to read it.
		{
			volume: "exists",
			path:   "as-directory",
			err:    errFileNotFound,
		},
		// TestFSStorage case - 4.
		{
			volume: "exists",
			path:   "as-file-parent/as-file",
			err:    errFileNotFound,
		},
		// TestFSStorage case - 5.
		// Validate the good condition file exists and we are able to read it.
		{
			volume: "exists",
			path:   "as-file",
			err:    nil,
			expData: []byte("Hello, World"),
		},
		// TestFSStorage case - 6.
		// TestFSStorage case with invalid volume name.
		{
			volume: "ab",
			path:   "as-file",
			err:    errVolumeNotFound,
		},
		// TestFSStorage case - 7.
		// Validate the good condition directory exists and
		// we are attempting to read it.
		{
			volume: "exists",
			path:   "as-directory/",
			err:    nil,
		},
		// TestFSStorage case - 8.
		// Validate the bad condition format of metafile is corrupted and
		// we are attempting to read it.
		{
			volume: "exists",
			path:   "corrupted-file",
			err:    errCorruptedFormat,
		},
	}

	// Run through all the test cases and validate for ReadVersion.
	for i, testCase := range testCases {
		fi, err := fsStorage.ReadVersion(context.Background(), testCase.volume, testCase.path, "", true)
		if err != testCase.err {
			t.Fatalf("TestFSStorage %d: Expected err \"%s\", got err \"%s\"", i+1, testCase.err, err)
		}

		if err == nil && !bytes.Equal(fi.Data, testCase.expData) {
			t.Fatalf("TestFSStorage %d: Expected data \"%s\", got data \"%s\"", i+1, string(testCase.expData), string(fi.Data))
		}
	}
}

// TestFSStorageReadAll - TestFSStorages the functionality implemented by fsStorage ReadAll storage API.
func TestFSStorageReadAll(t *testing.T) {
	// create fsStorage test setup
	fsStorage, path, err := newFSStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create fsStorage test setup, %s", err)
	}

	defer os.RemoveAll(path)

	// Create files for the test cases.
	if err = fsStorage.MakeVol(context.Background(), "exists"); err != nil {
		t.Fatalf("Unable to create a volume \"exists\", %s", err)
	}
	if err = fsStorage.AppendFile(context.Background(), "exists", "as-directory/as-file", []byte("Hello, World")); err != nil {
		t.Fatalf("Unable to create a file \"as-directory/as-file\", %s", err)
	}
	if err = fsStorage.AppendFile(context.Background(), "exists", "as-file", []byte("Hello, World")); err != nil {
		t.Fatalf("Unable to create a file \"as-file\", %s", err)
	}
	if err = fsStorage.AppendFile(context.Background(), "exists", "as-file-parent", []byte("Hello, World")); err != nil {
		t.Fatalf("Unable to create a file \"as-file-parent\", %s", err)
	}

	// TestFSStoragecases to validate different conditions for ReadAll API.
	testCases := []struct {
		volume string
		path   string
		err    error
	}{
		// TestFSStorage case - 1.
		// Validate volume does not exist.
		{
			volume: "i-dont-exist",
			path:   "",
			err:    errVolumeNotFound,
		},
		// TestFSStorage case - 2.
		// Validate bad condition file does not exist.
		{
			volume: "exists",
			path:   "as-file-not-found",
			err:    errFileNotFound,
		},
		// TestFSStorage case - 3.
		// Validate bad condition file exists as prefix/directory and
		// we are attempting to read it.
		{
			volume: "exists",
			path:   "as-directory",
			err:    errFileNotFound,
		},
		// TestFSStorage case - 4.
		{
			volume: "exists",
			path:   "as-file-parent/as-file",
			err:    errFileNotFound,
		},
		// TestFSStorage case - 5.
		// Validate the good condition file exists and we are able to read it.
		{
			volume: "exists",
			path:   "as-file",
			err:    nil,
		},
		// TestFSStorage case - 6.
		// TestFSStorage case with invalid volume name.
		{
			volume: "ab",
			path:   "as-file",
			err:    errVolumeNotFound,
		},
	}

	var dataRead []byte
	// Run through all the test cases and validate for ReadAll.
	for i, testCase := range testCases {
		dataRead, err = fsStorage.ReadAll(context.Background(), testCase.volume, testCase.path)
		if err != testCase.err {
			t.Fatalf("TestFSStorage %d: Expected err \"%s\", got err \"%s\"", i+1, testCase.err, err)
		}
		if err == nil {
			if string(dataRead) != "Hello, World" {
				t.Errorf("TestFSStorage %d: Expected the data read to be \"%s\", but instead got \"%s\"", i+1, "Hello, World", string(dataRead))
			}
		}
	}
}

// TestNewFSStorage all the cases handled in fsStorage storage layer initialization.
func TestNewFSStorage(t *testing.T) {
	// Temporary dir name.
	tmpDirName := globalTestTmpDir + SlashSeparator + "minio-" + nextSuffix()
	// Temporary file name.
	tmpFileName := globalTestTmpDir + SlashSeparator + "minio-" + nextSuffix()
	f, _ := os.Create(tmpFileName)
	f.Close()
	defer os.Remove(tmpFileName)

	// List of all tests for fsStorage initialization.
	testCases := []struct {
		name string
		err  error
	}{
		// Validates input argument cannot be empty.
		{
			"",
			errInvalidArgument,
		},
		// Validates if the directory does not exist and
		// gets automatically created.
		{
			tmpDirName,
			nil,
		},
		// Validates if the disk exists as file and returns error
		// not a directory.
		{
			tmpFileName,
			errDiskNotDir,
		},
	}

	// Validate all test cases.
	for i, testCase := range testCases {
		// Initialize a new fsStorage layer.
		_, err := newLocalFSV1Storage(testCase.name)
		if err != testCase.err {
			t.Fatalf("TestFSStorage %d failed wanted: %s, got: %s", i+1, err, testCase.err)
		}
	}
}

// TestFSStorageMakeVol - TestFSStorage validate the logic for creation of new fsStorage volume.
// Asserts the failures too against the expected failures.
func TestFSStorageMakeVol(t *testing.T) {
	// create fsStorage test setup
	fsStorage, path, err := newFSStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create fsStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// Setup test environment.
	// Create a file.
	if err := ioutil.WriteFile(slashpath.Join(path, "vol-as-file"), []byte{}, os.ModePerm); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}
	// Create a directory.
	if err := os.Mkdir(slashpath.Join(path, "existing-vol"), 0777); err != nil {
		t.Fatalf("Unable to create directory, %s", err)
	}

	testCases := []struct {
		volName     string
		expectedErr error
	}{
		// TestFSStorage case - 1.
		// A valid case, volume creation is expected to succeed.
		{
			volName:     "success-vol",
			expectedErr: nil,
		},
		// TestFSStorage case - 2.
		// Case where a file exists by the name of the volume to be created.
		{
			volName:     "vol-as-file",
			expectedErr: errVolumeExists,
		},
		// TestFSStorage case - 3.
		{
			volName:     "existing-vol",
			expectedErr: errVolumeExists,
		},
		// TestFSStorage case - 5.
		// TestFSStorage case with invalid volume name.
		{
			volName:     "ab",
			expectedErr: errInvalidArgument,
		},
	}

	for i, testCase := range testCases {
		if err := fsStorage.MakeVol(context.Background(), testCase.volName); err != testCase.expectedErr {
			t.Fatalf("TestFSStorage %d: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
		}
	}

	// TestFSStorage for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		permDeniedDir, err := ioutil.TempDir(globalTestTmpDir, "minio-")
		if err != nil {
			t.Fatalf("Unable to create temporary directory. %v", err)
		}
		defer os.RemoveAll(permDeniedDir)
		if err = os.Chmod(permDeniedDir, 0400); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		// Initialize fsStorage storage layer for permission denied error.
		_, err = newLocalFSV1Storage(permDeniedDir)
		if err != nil && err != errDiskAccessDenied {
			t.Fatalf("Unable to initialize fsStorage, %s", err)
		}

		if err = os.Chmod(permDeniedDir, 0755); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		fsStorageNew, err := newLocalFSV1Storage(permDeniedDir)
		if err != nil {
			t.Fatalf("Unable to initialize fsStorage, %s", err)
		}

		// change backend permissions for MakeVol error.
		if err = os.Chmod(permDeniedDir, 0400); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		if err := fsStorageNew.MakeVol(context.Background(), "test-vol"); err != errDiskAccessDenied {
			t.Fatalf("expected: %s, got: %s", errDiskAccessDenied, err)
		}
	}
}

// TestFSStorageDeleteVol - Validates the expected behavior of fsStorage.DeleteVol for various cases.
func TestFSStorageDeleteVol(t *testing.T) {
	// create fsStorage test setup
	fsStorage, path, err := newFSStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create fsStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// Setup test environment.
	if err = fsStorage.MakeVol(context.Background(), "success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	// TestFSStorage failure cases.
	vol := slashpath.Join(path, "nonempty-vol")
	if err = os.Mkdir(vol, 0777); err != nil {
		t.Fatalf("Unable to create directory, %s", err)
	}
	if err = ioutil.WriteFile(slashpath.Join(vol, "test-file"), []byte{}, os.ModePerm); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	testCases := []struct {
		volName     string
		expectedErr error
	}{
		// TestFSStorage case  - 1.
		// A valida case. Empty vol, should be possible to delete.
		{
			volName:     "success-vol",
			expectedErr: nil,
		},
		// TestFSStorage case - 2.
		// volume is non-existent.
		{
			volName:     "nonexistent-vol",
			expectedErr: errVolumeNotFound,
		},
		// TestFSStorage case - 3.
		// It shouldn't be possible to delete an non-empty volume, validating the same.
		{
			volName:     "nonempty-vol",
			expectedErr: errVolumeNotEmpty,
		},
		// TestFSStorage case - 5.
		// Invalid volume name.
		{
			volName:     "ab",
			expectedErr: errVolumeNotFound,
		},
	}

	for i, testCase := range testCases {
		if err = fsStorage.DeleteVol(context.Background(), testCase.volName, false); err != testCase.expectedErr {
			t.Fatalf("TestFSStorage: %d, expected: %s, got: %s", i+1, testCase.expectedErr, err)
		}
	}

	// TestFSStorage for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		var permDeniedDir string
		if permDeniedDir, err = ioutil.TempDir(globalTestTmpDir, "minio-"); err != nil {
			t.Fatalf("Unable to create temporary directory. %v", err)
		}
		defer removePermDeniedFile(permDeniedDir)
		if err = os.Mkdir(slashpath.Join(permDeniedDir, "mybucket"), 0400); err != nil {
			t.Fatalf("Unable to create temporary directory %v. %v", slashpath.Join(permDeniedDir, "mybucket"), err)
		}
		if err = os.Chmod(permDeniedDir, 0400); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		// Initialize fsStorage storage layer for permission denied error.
		_, err = newLocalFSV1Storage(permDeniedDir)
		if err != nil && err != errDiskAccessDenied {
			t.Fatalf("Unable to initialize fsStorage, %s", err)
		}

		if err = os.Chmod(permDeniedDir, 0755); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		fsStorageNew, err := newLocalFSV1Storage(permDeniedDir)
		if err != nil {
			t.Fatalf("Unable to initialize fsStorage, %s", err)
		}

		// change backend permissions for MakeVol error.
		if err = os.Chmod(permDeniedDir, 0400); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		if err = fsStorageNew.DeleteVol(context.Background(), "mybucket", false); err != errDiskAccessDenied {
			t.Fatalf("expected: Permission error, got: %s", err)
		}
	}

	fsStorageDeletedStorage, diskPath, err := newFSStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create fsStorage test setup, %s", err)
	}
	// removing the disk, used to recreate disk not found error.
	os.RemoveAll(diskPath)

	// TestFSStorage for delete on an removed disk.
	// should fail with volume not found (this test should not check the behaviour of disk-id-check).
	err = fsStorageDeletedStorage.DeleteVol(context.Background(), "Del-Vol", false)
	if err != errVolumeNotFound {
		t.Errorf("Expected: \"Volume not found\", got \"%s\"", err)
	}
}

// TestFSStorageDiskInfo - Validates the expected behavior of fsStorage.DiskInfo for various cases.
func TestFSStorageDiskInfo(t *testing.T) {
	// create fsStorage test setup
	fsStorage, path, err := newFSStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create fsStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	diskInfo, err := fsStorage.DiskInfo(context.Background())
	if err != nil {
		t.Fatalf("Failed to get DiskInfo: Expected: nil, got: \"%s\"", err)
	}

	if diskInfo.ID != "" {
		t.Fatalf("DiskInfo.ID: Expected: \"%s\", got: \"%s\"", "", diskInfo.ID)
	}
	if diskInfo.Healing {
		t.Fatalf("DiskInfo.Healing: Expected: \"%t\", got: \"%t\"", false, diskInfo.Healing)
	}
	if diskInfo.Error != "" {
		t.Fatalf("DiskInfo.Error: Expected: \"%s\", got: \"%s\"", "", diskInfo.Error)
	}
	if diskInfo.Endpoint == "" {
		t.Fatalf("DiskInfo.Endpoint: Expected not to be empty")
	}
	if diskInfo.MountPath == "" {
		t.Fatalf("DiskInfo.MountPath: Expected not to be empty")
	}
	if diskInfo.Total == 0 {
		t.Fatalf("DiskInfo.Total: Expected not to be 0")
	}
	if diskInfo.Used == 0 {
		t.Fatalf("DiskInfo.Used: Expected not to be 0")
	}
	if diskInfo.Free == 0 {
		t.Fatalf("DiskInfo.Free: Expected not to be 0")
	}
	if diskInfo.FreeInodes == 0 {
		t.Fatalf("DiskInfo.FreeInodes: Expected not to be 0")
	}
	if diskInfo.UsedInodes == 0 {
		t.Fatalf("DiskInfo.UsedInodes: Expected not to be 0")
	}
}

// TestFSStorageStatVol - TestFSStorages validate the volume info returned by fsStorage.StatVol() for various inputs.
func TestFSStorageStatVol(t *testing.T) {
	// create fsStorage test setup
	fsStorage, path, err := newFSStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create fsStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// Setup test environment.
	if err = fsStorage.MakeVol(context.Background(), "success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	testCases := []struct {
		volName     string
		expectedErr error
	}{
		// TestFSStorage case - 1.
		{
			volName:     "success-vol",
			expectedErr: nil,
		},
		// TestFSStorage case - 2.
		{
			volName:     "nonexistent-vol",
			expectedErr: errVolumeNotFound,
		},
		// TestFSStorage case - 3.
		{
			volName:     "ab",
			expectedErr: errVolumeNotFound,
		},
	}

	for i, testCase := range testCases {
		var volInfo VolInfo
		volInfo, err = fsStorage.StatVol(context.Background(), testCase.volName)
		if err != testCase.expectedErr {
			t.Fatalf("TestFSStorage case : %d, Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
		}

		if err == nil {
			if volInfo.Name != testCase.volName {
				t.Errorf("TestFSStorage case %d: Expected the volume name to be \"%s\", instead found \"%s\"",
					i+1, volInfo.Name, testCase.volName)
			}
		}
	}

	fsStorageDeletedStorage, diskPath, err := newFSStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create fsStorage test setup, %s", err)
	}
	// removing the disk, used to recreate disk not found error.
	os.RemoveAll(diskPath)

	// TestFSStorage for delete on an removed disk.
	// should fail with volume not found (this test should not check the behaviour of disk-id-check).
	_, err = fsStorageDeletedStorage.StatVol(context.Background(), "Stat vol")
	if err != errVolumeNotFound {
		t.Errorf("Expected: \"Volume not found\", got \"%s\"", err)
	}
}

// TestFSStorageListVols - Validates the result and the error output for fsStorage volume listing functionality fsStorage.ListVols().
func TestFSStorageListVols(t *testing.T) {
	// create fsStorage test setup
	fsStorage, path, err := newFSStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create fsStorage test setup, %s", err)
	}

	var volInfos []VolInfo
	// TestFSStorage empty list vols.
	if volInfos, err = fsStorage.ListVols(context.Background()); err != nil {
		t.Fatalf("expected: <nil>, got: %s", err)
	} else if len(volInfos) != 1 {
		t.Fatalf("expected: one entry, got: %s", volInfos)
	}

	// TestFSStorage non-empty list vols.
	if err = fsStorage.MakeVol(context.Background(), "success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	volInfos, err = fsStorage.ListVols(context.Background())
	if err != nil {
		t.Fatalf("expected: <nil>, got: %s", err)
	}
	if len(volInfos) != 2 {
		t.Fatalf("expected: 2, got: %d", len(volInfos))
	}
	volFound := false
	for _, info := range volInfos {
		if info.Name == "success-vol" {
			volFound = true
			break
		}
	}
	if !volFound {
		t.Errorf("expected: success-vol to be created")
	}

	// removing the path and simulating disk failure
	os.RemoveAll(path)
	// should fail with errDiskNotFound.
	if _, err = fsStorage.ListVols(context.Background()); err != errDiskNotFound {
		t.Errorf("Expected to fail with \"%s\", but instead failed with \"%s\"", errDiskNotFound, err)
	}
}

// TestFSStorageListDir -  TestFSStorages validate the directory listing functionality provided by fsStorage.ListDir .
func TestFSStorageListDir(t *testing.T) {
	// create fsStorage test setup
	fsStorage, path, err := newFSStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create fsStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// create fsStorage test setup.
	fsStorageDeletedStorage, diskPath, err := newFSStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create fsStorage test setup, %s", err)
	}
	// removing the disk, used to recreate disk not found error.
	os.RemoveAll(diskPath)
	// Setup test environment.
	if err = fsStorage.MakeVol(context.Background(), "success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}
	if err = fsStorage.AppendFile(context.Background(), "success-vol", "abc/def/ghi/success-file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}
	if err = fsStorage.AppendFile(context.Background(), "success-vol", "abc/xyz/ghi/success-file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	testCases := []struct {
		srcVol  string
		srcPath string
		// expected result.
		expectedListDir []string
		expectedErr     error
	}{
		// TestFSStorage case - 1.
		// valid case with existing volume and file to delete.
		{
			srcVol:          "success-vol",
			srcPath:         "abc",
			expectedListDir: []string{"def/", "xyz/"},
			expectedErr:     nil,
		},
		// TestFSStorage case - 1.
		// valid case with existing volume and file to delete.
		{
			srcVol:          "success-vol",
			srcPath:         "abc/def",
			expectedListDir: []string{"ghi/"},
			expectedErr:     nil,
		},
		// TestFSStorage case - 1.
		// valid case with existing volume and file to delete.
		{
			srcVol:          "success-vol",
			srcPath:         "abc/def/ghi",
			expectedListDir: []string{"success-file"},
			expectedErr:     nil,
		},
		// TestFSStorage case - 2.
		{
			srcVol:      "success-vol",
			srcPath:     "abcdef",
			expectedErr: errFileNotFound,
		},
		// TestFSStorage case - 3.
		// TestFSStorage case with invalid volume name.
		{
			srcVol:      "ab",
			srcPath:     "success-file",
			expectedErr: errVolumeNotFound,
		},
		// TestFSStorage case - 4.
		// TestFSStorage case with non existent volume.
		{
			srcVol:      "non-existent-vol",
			srcPath:     "success-file",
			expectedErr: errVolumeNotFound,
		},
	}

	for i, testCase := range testCases {
		var dirList []string
		dirList, err = fsStorage.ListDir(context.Background(), testCase.srcVol, testCase.srcPath, -1)
		if err != testCase.expectedErr {
			t.Errorf("TestFSStorage case %d: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
		}
		if err == nil {
			for _, expected := range testCase.expectedListDir {
				if !strings.Contains(strings.Join(dirList, ","), expected) {
					t.Errorf("TestFSStorage case %d: Expected the directory listing to be \"%v\", but got \"%v\"", i+1, testCase.expectedListDir, dirList)
				}
			}
		}
	}

	// TestFSStorage for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		permDeniedDir := createPermDeniedFile(t)
		defer removePermDeniedFile(permDeniedDir)

		// Initialize fsStorage storage layer for permission denied error.
		_, err = newLocalFSV1Storage(permDeniedDir)
		if err != nil && err != errDiskAccessDenied {
			t.Fatalf("Unable to initialize fsStorage, %s", err)
		}

		if err = os.Chmod(permDeniedDir, 0755); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		fsStorageNew, err := newLocalFSV1Storage(permDeniedDir)
		if err != nil {
			t.Fatalf("Unable to initialize fsStorage, %s", err)
		}

		if err = fsStorageNew.Delete(context.Background(), "mybucket", "myobject", false); err != errVolumeAccessDenied {
			t.Errorf("expected: %s, got: %s", errVolumeAccessDenied, err)
		}
	}

	// TestFSStorage for delete on an removed disk.
	// should fail with volume not found (this test should not check the behaviour of disk-id-check).
	err = fsStorageDeletedStorage.Delete(context.Background(), "del-vol", "my-file", false)
	if err != errVolumeNotFound {
		t.Errorf("Expected: \"Volume not found\", got \"%s\"", err)
	}
}

// TestFSStorageDeleteFile - Series of test cases construct valid and invalid input data and validates the result and the error response.
func TestFSStorageDeleteFile(t *testing.T) {
	if runtime.GOOS == globalWindowsOSName {
		t.Skip()
	}

	// create fsStorage test setup
	fsStorage, path, err := newFSStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create fsStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// create fsStorage test setup
	fsStorageDeletedStorage, diskPath, err := newFSStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create fsStorage test setup, %s", err)
	}
	// removing the disk, used to recreate disk not found error.
	os.RemoveAll(diskPath)
	// Setup test environment.
	if err = fsStorage.MakeVol(context.Background(), "success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}
	if err = fsStorage.AppendFile(context.Background(), "success-vol", "success-file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	if err = fsStorage.MakeVol(context.Background(), "no-permissions"); err != nil {
		t.Fatalf("Unable to create volume, %s", err.Error())
	}
	if err = fsStorage.AppendFile(context.Background(), "no-permissions", "dir/file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err.Error())
	}
	// Parent directory must have write permissions, this is read + execute.
	if err = os.Chmod(pathJoin(path, "no-permissions"), 0555); err != nil {
		t.Fatalf("Unable to chmod directory, %s", err.Error())
	}

	testCases := []struct {
		srcVol      string
		srcPath     string
		expectedErr error
	}{
		// TestFSStorage case - 1.
		// valid case with existing volume and file to delete.
		{
			srcVol:      "success-vol",
			srcPath:     "success-file",
			expectedErr: nil,
		},
		// TestFSStorage case - 2.
		// The file was deleted in the last  case, so Delete should fail.
		{
			srcVol:      "success-vol",
			srcPath:     "success-file",
			expectedErr: errFileNotFound,
		},
		// TestFSStorage case - 3.
		// TestFSStorage case with segment of the volume name > 255.
		{
			srcVol:      "my",
			srcPath:     "success-file",
			expectedErr: errVolumeNotFound,
		},
		// TestFSStorage case - 4.
		// TestFSStorage case with non-existent volume.
		{
			srcVol:      "non-existent-vol",
			srcPath:     "success-file",
			expectedErr: errVolumeNotFound,
		},
		// TestFSStorage case - 5.
		// TestFSStorage case with src path segment > 255.
		{
			srcVol:      "success-vol",
			srcPath:     "my-obj-del-0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
			expectedErr: errFileNameTooLong,
		},
		// TestFSStorage case - 6.
		// TestFSStorage case with undeletable parent directory.
		// File can delete, dir cannot delete because no-permissions doesn't have write perms.
		{
			srcVol:      "no-permissions",
			srcPath:     "dir/file",
			expectedErr: errVolumeAccessDenied,
		},
	}

	for i, testCase := range testCases {
		if err = fsStorage.Delete(context.Background(), testCase.srcVol, testCase.srcPath, false); err != testCase.expectedErr {
			t.Errorf("TestFSStorage case %d: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
		}
	}

	// TestFSStorage for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		permDeniedDir := createPermDeniedFile(t)
		defer removePermDeniedFile(permDeniedDir)

		// Initialize fsStorage storage layer for permission denied error.
		_, err = newLocalFSV1Storage(permDeniedDir)
		if err != nil && err != errDiskAccessDenied {
			t.Fatalf("Unable to initialize fsStorage, %s", err)
		}

		if err = os.Chmod(permDeniedDir, 0755); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		fsStorageNew, err := newLocalFSV1Storage(permDeniedDir)
		if err != nil {
			t.Fatalf("Unable to initialize fsStorage, %s", err)
		}

		if err = fsStorageNew.Delete(context.Background(), "mybucket", "myobject", false); err != errVolumeAccessDenied {
			t.Errorf("expected: %s, got: %s", errVolumeAccessDenied, err)
		}
	}

	// TestFSStorage for delete on an removed disk.
	// should fail with volume not found (this test should not check the behaviour of disk-id-check).
	err = fsStorageDeletedStorage.Delete(context.Background(), "del-vol", "my-file", false)
	if err != errVolumeNotFound {
		t.Errorf("Expected: \"Volume not found\", got \"%s\"", err)
	}
}

// TestFSStorageReadFileStream - TestFSStorages fsStorage.ReadFileStream with wide range of cases and asserts the result and error response.
func TestFSStorageReadFileStream(t *testing.T) {
	// create fsStorage test setup
	fsStorage, path, err := newFSStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create fsStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	volume := "success-vol"
	// Setup test environment.
	if err = fsStorage.MakeVol(context.Background(), volume); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	// Create directory to make errIsNotRegular
	if err = os.Mkdir(slashpath.Join(path, "success-vol", "object-as-dir"), 0777); err != nil {
		t.Fatalf("Unable to create directory, %s", err)
	}

	testCases := []struct {
		volume      string
		fileName    string
		offset      int64
		bufSize     int
		expectedBuf []byte
		expectedErr error
	}{
		// Successful read at offset 0 and proper buffer size. - 1
		{
			volume, "myobject", 0, 5,
			[]byte("hello"), nil,
		},
		// Success read at hierarchy. - 2
		{
			volume, "path/to/my/object", 0, 5,
			[]byte("hello"), nil,
		},
		// Object is a directory. - 3
		{
			volume, "object-as-dir",
			0, 5, nil, errIsNotRegular,},
		// One path segment length is > 255 chars long. - 4
		{
			volume, "path/to/my/object0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
			0, 5, nil, errFileNameTooLong},
		// Path length is > 1024 chars long. - 5
		{
			volume, "level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001/level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002/level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003/object000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
			0, 5, nil, errFileNameTooLong},
		// Buffer size greater than object size. - 6
		{
			volume, "myobject", 0, 16,
			[]byte("hello, world"),
			io.ErrUnexpectedEOF,
		},
		// Reading from an offset success. - 7
		{
			volume, "myobject", 7, 5,
			[]byte("world"), nil,
		},
		// Reading from an object but buffer size greater. - 8
		{
			volume, "myobject",
			7, 8,
			[]byte("world"),
			io.ErrUnexpectedEOF,
		},
		// Seeking ahead returns io.EOF. - 9
		{
			volume, "myobject", 14, 1, nil, io.EOF,
		},
		// Empty volume name. - 10
		{
			"", "myobject", 14, 1, nil, errVolumeNotFound,
		},
		// Empty filename name. - 11
		{
			volume, "", 14, 1, nil, errIsNotRegular,
		},
		// Non existent volume name - 12
		{
			"abcd", "", 14, 1, nil, errVolumeNotFound,
		},
		// Non existent filename - 13
		{
			volume, "abcd", 14, 1, nil, errFileNotFound,
		},
	}

	// Create all files needed during testing.
	appendFiles := testCases[:4]
	// Create test files for further reading.
	for i, appendFile := range appendFiles {
		err = fsStorage.AppendFile(context.Background(), volume, appendFile.fileName, []byte("hello, world"))
		if err != appendFile.expectedErr {
			t.Fatalf("Creating file failed: %d %#v, expected: %s, got: %s", i+1, appendFile, appendFile.expectedErr, err)
		}
	}

	{
		buf := make([]byte, 5)
		// Test for negative offset.
		if _, err = fsStorage.ReadFileStream(context.Background(), volume, "myobject", -1, int64(len(buf))); err == nil {
			t.Fatalf("expected: error, got: <nil>")
		}
	}

	// Following block validates all ReadFile test cases.
	for i, testCase := range testCases {
		var n int
		// Common read buffer.
		var buf = make([]byte, testCase.bufSize)
		rc, err := fsStorage.ReadFileStream(context.Background(), testCase.volume, testCase.fileName, testCase.offset, int64(testCase.bufSize))
		if err == nil {
			n, err = io.ReadFull(rc, buf)
			rc.Close()
		}
		if err != nil && testCase.expectedErr != nil {
			// Validate if the type string of the errors are an exact match.
			if err.Error() != testCase.expectedErr.Error() {
				if runtime.GOOS != globalWindowsOSName {
					t.Errorf("Case: %d %#v, expected: %s, got: %s", i+1, testCase, testCase.expectedErr, err)
				} else {
					var resultErrno, expectErrno uintptr
					if pathErr, ok := err.(*os.PathError); ok {
						if errno, pok := pathErr.Err.(syscall.Errno); pok {
							resultErrno = uintptr(errno)
						}
					}
					if pathErr, ok := testCase.expectedErr.(*os.PathError); ok {
						if errno, pok := pathErr.Err.(syscall.Errno); pok {
							expectErrno = uintptr(errno)
						}
					}
					if !(expectErrno != 0 && resultErrno != 0 && expectErrno == resultErrno) {
						t.Errorf("Case: %d %#v, expected: %s, got: %s", i+1, testCase, testCase.expectedErr, err)
					}
				}
			}
			// Err unexpected EOF special case, where we verify we have provided a larger
			// buffer than the data itself, but the results are in-fact valid. So we validate
			// this error condition specifically treating it as a good condition with valid
			// results. In this scenario return 'n' is always lesser than the input buffer.
			if err == io.ErrUnexpectedEOF {
				if !bytes.Equal(testCase.expectedBuf, buf[:n]) {
					t.Errorf("Case: %d %#v, expected: \"%s\", got: \"%s\"", i+1, testCase, string(testCase.expectedBuf), string(buf[:n]))
				}
				if n > len(buf) {
					t.Errorf("Case: %d %#v, expected: %d, got: %d", i+1, testCase, testCase.bufSize, n)
				}
			}
		}

		// ReadFileStream has returned success, but our expected error is non 'nil'.
		if err == nil && err != testCase.expectedErr {
			t.Errorf("Case: %d %#v, expected: %s, got :%s", i+1, testCase, testCase.expectedErr, err)
		}
		// Expected error returned, proceed further to validate the returned results.
		if err == nil && err == testCase.expectedErr {
			if !bytes.Equal(testCase.expectedBuf, buf) {
				t.Errorf("Case: %d %#v, expected: \"%s\", got: \"%s\"", i+1, testCase, string(testCase.expectedBuf), string(buf[:testCase.bufSize]))
			}
			if n != testCase.bufSize {
				t.Errorf("Case: %d %#v, expected: %d, got: %d", i+1, testCase, testCase.bufSize, n)
			}
		}
	}

	// TestFSStorage for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		permDeniedDir := createPermDeniedFile(t)
		defer removePermDeniedFile(permDeniedDir)

		// Initialize fsStorage storage layer for permission denied error.
		_, err = newLocalFSV1Storage(permDeniedDir)
		if err != nil && err != errDiskAccessDenied {
			t.Fatalf("Unable to initialize fsStorage, %s", err)
		}

		if err = os.Chmod(permDeniedDir, 0755); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		fsStoragePermStorage, err := newLocalFSV1Storage(permDeniedDir)
		if err != nil {
			t.Fatalf("Unable to initialize fsStorage, %s", err)
		}

		// Common read buffer.
		var buf = make([]byte, 10)
		rc, err := fsStoragePermStorage.ReadFileStream(context.Background(), "mybucket", "myobject", 0, int64(len(buf)))
		if err == nil {
			rc.Close()
		}
		if err != errFileAccessDenied {
			t.Errorf("expected: %s, got: %s", errFileAccessDenied, err)
		}
	}
}

// TestFSStorage fsStorage.AppendFile()
func TestFSStorageAppendFile(t *testing.T) {
	// create fsStorage test setup
	fsStorage, path, err := newFSStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create fsStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// Setup test environment.
	if err = fsStorage.MakeVol(context.Background(), "success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	// Create directory to make errIsNotRegular
	if err = os.Mkdir(slashpath.Join(path, "success-vol", "object-as-dir"), 0777); err != nil {
		t.Fatalf("Unable to create directory, %s", err)
	}

	testCases := []struct {
		fileName    string
		expectedErr error
	}{
		{"myobject", nil},
		{"path/to/my/object", nil},
		// TestFSStorage to append to previously created file.
		{"myobject", nil},
		// TestFSStorage to use same path of previously created file.
		{"path/to/my/testobject", nil},
		// TestFSStorage to use object is a directory now.
		{"object-as-dir", errIsNotRegular},
		// path segment uses previously uploaded object.
		{"myobject/testobject", errFileAccessDenied},
		// One path segment length is > 255 chars long.
		{"path/to/my/object0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001", errFileNameTooLong},
		// path length is > 1024 chars long.
		{"level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001/level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002/level0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003/object000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001", errFileNameTooLong},
	}

	for i, testCase := range testCases {
		if err = fsStorage.AppendFile(context.Background(), "success-vol", testCase.fileName, []byte("hello, world")); err != testCase.expectedErr {
			t.Errorf("Case: %d, expected: %s, got: %s", i+1, testCase.expectedErr, err)
		}
	}

	// TestFSStorage for permission denied.
	if runtime.GOOS != globalWindowsOSName {
		permDeniedDir := createPermDeniedFile(t)
		defer removePermDeniedFile(permDeniedDir)

		var fsStoragePermStorage StorageAPI
		// Initialize fsStorage storage layer for permission denied error.
		_, err = newLocalFSV1Storage(permDeniedDir)
		if err != nil && err != errDiskAccessDenied {
			t.Fatalf("Unable to initialize fsStorage, %s", err)
		}

		if err = os.Chmod(permDeniedDir, 0755); err != nil {
			t.Fatalf("Unable to change permission to temporary directory %v. %v", permDeniedDir, err)
		}

		fsStoragePermStorage, err = newLocalFSV1Storage(permDeniedDir)
		if err != nil {
			t.Fatalf("Unable to initialize fsStorage, %s", err)
		}

		if err = fsStoragePermStorage.AppendFile(context.Background(), "mybucket", "myobject", []byte("hello, world")); err != errVolumeAccessDenied {
			t.Fatalf("expected: errVolumeAccessDenied error, got: %s", err)
		}
	}

	// TestFSStorage case with invalid volume name.
	// A valid volume name should be atleast of size 3.
	err = fsStorage.AppendFile(context.Background(), "bn", "yes", []byte("hello, world"))
	if err != errVolumeNotFound {
		t.Fatalf("expected: \"Invalid argument error\", got: \"%s\"", err)
	}
}

// TestFSStorage fsStorage.RenameFile()
func TestFSStorageRenameFile(t *testing.T) {
	// create fsStorage test setup
	fsStorage, path, err := newFSStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create fsStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// Setup test environment.
	if err := fsStorage.MakeVol(context.Background(), "src-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	if err := fsStorage.MakeVol(context.Background(), "dest-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	fsJson, _ := ioutil.ReadFile("testdata/fs.json")

	createFileWithMeta(t, fsStorage, "src-vol", "file1", fsJson, []byte("Hello, world"))
	createFileWithMeta(t, fsStorage, "src-vol", "file2", fsJson, []byte("Hello, world"))
	createFileWithMeta(t, fsStorage, "src-vol", "file3", fsJson, []byte("Hello, world"))
	createFileWithMeta(t, fsStorage, "src-vol", "file4", fsJson, []byte("Hello, world"))
	createFileWithMeta(t, fsStorage, "src-vol", "file5", fsJson, []byte("Hello, world"))
	createFileWithMeta(t, fsStorage, "src-vol", "file6", fsJson, []byte("Hello, world"))
	createFileWithMeta(t, fsStorage, "src-vol", "path/to/file1", fsJson, []byte("Hello, world"))

	testCases := []struct {
		srcVol      string
		destVol     string
		srcPath     string
		destPath    string
		expectedErr error
	}{
		// TestFSStorage case - 1.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file1",
			destPath:    "file-one",
			expectedErr: nil,
		},
		// TestFSStorage case - 2.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "path/",
			destPath:    "new-path/",
			expectedErr: nil,
		},
		// TestFSStorage case - 3.
		// TestFSStorage to overwrite destination file.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file2",
			destPath:    "file-one",
			expectedErr: nil,
		},
		// TestFSStorage case - 4.
		// TestFSStorage case with io error count set to 1.
		// expected not to fail.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file3",
			destPath:    "file-two",
			expectedErr: nil,
		},
		// TestFSStorage case - 5.
		// TestFSStorage case with io error count set to maximum allowed count.
		// expected not to fail.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file4",
			destPath:    "file-three",
			expectedErr: nil,
		},
		// TestFSStorage case - 6.
		// TestFSStorage case with non-existent source file.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "non-existent-file",
			destPath:    "file-three",
			expectedErr: errFileNotFound,
		},
		// TestFSStorage case - 7.
		// TestFSStorage to check failure of source and destination are not same type.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "path/",
			destPath:    "file-one",
			expectedErr: errFileAccessDenied,
		},
		// TestFSStorage case - 8.
		// TestFSStorage to check failure of destination directory exists.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "path/",
			destPath:    "new-path/",
			expectedErr: errFileAccessDenied,
		},
		// TestFSStorage case - 9.
		// TestFSStorage case with source being a file and destination being a directory.
		// Either both have to be files or directories.
		// Expecting to fail with `errFileAccessDenied`.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file4",
			destPath:    "new-path/",
			expectedErr: errFileAccessDenied,
		},
		// TestFSStorage case - 10.
		// TestFSStorage case with non-existent source volume.
		// Expecting to fail with `errVolumeNotFound`.
		{
			srcVol:      "src-vol-non-existent",
			destVol:     "dest-vol",
			srcPath:     "file4",
			destPath:    "new-path/",
			expectedErr: errVolumeNotFound,
		},
		// TestFSStorage case - 11.
		// TestFSStorage case with non-existent destination volume.
		// Expecting to fail with `errVolumeNotFound`.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol-non-existent",
			srcPath:     "file4",
			destPath:    "new-path/",
			expectedErr: errVolumeNotFound,
		},
		// TestFSStorage case - 12.
		// TestFSStorage case with invalid src volume name. Length should be atleast 3.
		// Expecting to fail with `errInvalidArgument`.
		{
			srcVol:      "ab",
			destVol:     "dest-vol-non-existent",
			srcPath:     "file4",
			destPath:    "new-path/",
			expectedErr: errVolumeNotFound,
		},
		// TestFSStorage case - 13.
		// TestFSStorage case with invalid destination volume name. Length should be atleast 3.
		// Expecting to fail with `errInvalidArgument`.
		{
			srcVol:      "abcd",
			destVol:     "ef",
			srcPath:     "file4",
			destPath:    "new-path/",
			expectedErr: errVolumeNotFound,
		},
		// TestFSStorage case - 14.
		// TestFSStorage case with invalid destination volume name. Length should be atleast 3.
		// Expecting to fail with `errInvalidArgument`.
		{
			srcVol:      "abcd",
			destVol:     "ef",
			srcPath:     "file4",
			destPath:    "new-path/",
			expectedErr: errVolumeNotFound,
		},
		// TestFSStorage case - 15.
		// TestFSStorage case with the parent of the destination being a file.
		// expected to fail with `errFileAccessDenied`.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file5",
			destPath:    "file-one/parent-is-file",
			expectedErr: errFileAccessDenied,
		},
		// TestFSStorage case - 16.
		// TestFSStorage case with segment of source file name more than 255.
		// expected not to fail.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "path/to/my/object0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
			destPath:    "file-seven",
			expectedErr: errFileNameTooLong,
		},
		// TestFSStorage case - 17.
		// TestFSStorage case with segment of destination file name more than 255.
		// expected not to fail.
		{
			srcVol:      "src-vol",
			destVol:     "dest-vol",
			srcPath:     "file7",
			destPath:    "path/to/my/object0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
			expectedErr: errFileNameTooLong,
		},
		// TestFSStorage case - 18.
		// expected not to fail.
		{
			srcVol:      "src-vol",
			destVol:     fsStorage.MetaTmpBucket(),
			srcPath:     "file6",
			destPath:    "file-three",
			expectedErr: nil,
		},
	}

	for i, testCase := range testCases {
		if err := fsStorage.RenameFile(context.Background(), testCase.srcVol, testCase.srcPath, testCase.destVol, testCase.destPath); err != testCase.expectedErr {
			t.Fatalf("TestFSStorage %d:  Expected the error to be : \"%v\", got: \"%v\".", i+1, testCase.expectedErr, err)
		}
	}
}

// TestFSStorage fsStorage.CheckFile()
func TestFSStorageCheckFile(t *testing.T) {
	// create fsStorage test setup
	fsStorage, path, err := newFSStorageTestSetup()
	if err != nil {
		t.Fatalf("Unable to create fsStorage test setup, %s", err)
	}
	defer os.RemoveAll(path)

	// Setup test environment.
	if err := fsStorage.MakeVol(context.Background(), "success-vol"); err != nil {
		t.Fatalf("Unable to create volume, %s", err)
	}

	if err := fsStorage.AppendFile(context.Background(), "success-vol", "success-file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	if err := fsStorage.AppendFile(context.Background(), "success-vol", "path/to/success-file", []byte("Hello, world")); err != nil {
		t.Fatalf("Unable to create file, %s", err)
	}

	testCases := []struct {
		srcVol      string
		srcPath     string
		expectedErr error
	}{
		// TestFSStorage case - 1.
		// TestFSStorage case with valid inputs, expected to pass.
		{
			srcVol:      "success-vol",
			srcPath:     "success-file",
			expectedErr: nil,
		},
		// TestFSStorage case - 2.
		// TestFSStorage case with valid inputs, expected to pass.
		{
			srcVol:      "success-vol",
			srcPath:     "path/to/success-file",
			expectedErr: nil,
		},
		// TestFSStorage case - 3.
		// TestFSStorage case with non-existent file.
		{
			srcVol:      "success-vol",
			srcPath:     "nonexistent-file",
			expectedErr: errPathNotFound,
		},
		// TestFSStorage case - 4.
		// TestFSStorage case with non-existent file path.
		{
			srcVol:      "success-vol",
			srcPath:     "path/2/success-file",
			expectedErr: errPathNotFound,
		},
		// TestFSStorage case - 5.
		// TestFSStorage case with path being a directory.
		{
			srcVol:      "success-vol",
			srcPath:     "path",
			expectedErr: errPathNotFound,
		},
		// TestFSStorage case - 6.
		// TestFSStorage case with non existent volume.
		{
			srcVol:      "non-existent-vol",
			srcPath:     "success-file",
			expectedErr: errPathNotFound,
		},
	}

	for i, testCase := range testCases {
		if err := fsStorage.CheckFile(context.Background(), testCase.srcVol, testCase.srcPath); err != testCase.expectedErr {
			t.Errorf("TestFSStorage case %d: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, err)
		}
	}
}

func createFileWithMeta(t *testing.T, fsStorage fsStorageAPI, bucket, object string, fsJson []byte, fileContent []byte) {
	if err := fsStorage.AppendFile(context.Background(), minioMetaBucket, pathJoin(bucketMetaPrefix, bucket, object, "fs.json"), fsJson); err != nil {
		t.Fatalf("Unable to create metafile for \"%s\", %s", object, err)
	}
	if err := fsStorage.AppendFile(context.Background(), bucket, object, fileContent); err != nil {
		t.Fatalf("Unable to create a file \"%s\", %s", object, err)
	}
}
