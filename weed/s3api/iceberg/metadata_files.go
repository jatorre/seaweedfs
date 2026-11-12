package iceberg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stageTokenKey is the extended attribute stageOwnedMetadata tags a staged
// metadata file with: a value unique to the request that wrote it, which the
// request's own cleanup must find before it deletes the file. The internal
// prefix keeps it out of the headers S3 reads return.
const stageTokenKey = s3_constants.SeaweedFSInternalPrefix + "iceberg-stage-token"

// metadataDirPath returns the filer directory holding a table's metadata files.
func metadataDirPath(bucketName, tablePath string) string {
	dir := path.Join(s3tables.TablesPath, bucketName)
	if tablePath != "" {
		dir = path.Join(dir, tablePath)
	}
	return path.Join(dir, "metadata")
}

// saveMetadataFile saves the Iceberg metadata JSON file to the filer.
// It constructs the filer path from the S3 location components. With exclusive
// set the write fails with filer_pb.ErrEntryAlreadyExists rather than replacing
// a file that is already there: two commits racing off the same base pick the
// same v{N} name, and letting the second overwrite the first would leave the
// winner's catalog pointer aimed at the loser's metadata.
func (s *Server) saveMetadataFile(ctx context.Context, bucketName, tablePath, metadataFileName string, content []byte, exclusive bool) error {
	return s.saveMetadataBlob(ctx, bucketName, tablePath, metadataFileName, content, "application/json", exclusive, nil)
}

// saveMetadataBlob saves a file into the table's metadata directory, with
// extended stored on the entry alongside its mime type.
func (s *Server) saveMetadataBlob(ctx context.Context, bucketName, tablePath, metadataFileName string, content []byte, mimeType string, exclusive bool, extended map[string][]byte) error {
	attrs := map[string][]byte{"Mime-Type": []byte(mimeType)}
	for key, value := range extended {
		attrs[key] = value
	}

	// Create context with timeout for file operations
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		bucketsPath := s3tables.TablesPath

		ensureDir := func(parent, name, errorContext string) error {
			_, err := filer_pb.LookupEntry(opCtx, client, &filer_pb.LookupDirectoryEntryRequest{
				Directory: parent,
				Name:      name,
			})
			if err == nil {
				return nil
			}
			if err != filer_pb.ErrNotFound {
				return fmt.Errorf("lookup %s failed: %w", errorContext, err)
			}

			// If lookup fails with ErrNotFound, try to create the directory.
			resp, createErr := client.CreateEntry(opCtx, &filer_pb.CreateEntryRequest{
				Directory: parent,
				Entry: &filer_pb.Entry{
					Name:        name,
					IsDirectory: true,
					Attributes: &filer_pb.FuseAttributes{
						Mtime:    time.Now().Unix(),
						Crtime:   time.Now().Unix(),
						FileMode: uint32(0755 | os.ModeDir),
					},
				},
			})
			if createErr != nil {
				return fmt.Errorf("failed to create %s: %w", errorContext, createErr)
			}
			if resp.ErrorCode != filer_pb.FilerError_OK && resp.ErrorCode != filer_pb.FilerError_ENTRY_ALREADY_EXISTS {
				return fmt.Errorf("failed to create %s: %s", errorContext, resp.Error)
			}
			return nil
		}

		bucketDir := path.Join(bucketsPath, bucketName)
		// 1. Ensure bucket directory exists: <bucketsPath>/<bucket>
		if err := ensureDir(bucketsPath, bucketName, "bucket directory"); err != nil {
			return err
		}

		// 2. Ensure table path exists under the bucket directory
		tableDir := bucketDir
		if tablePath != "" {
			segments := strings.Split(tablePath, "/")
			for _, segment := range segments {
				if segment == "" {
					continue
				}
				if err := ensureDir(tableDir, segment, "table directory"); err != nil {
					return err
				}
				tableDir = path.Join(tableDir, segment)
			}
		}

		// 3. Ensure metadata directory exists: <bucketsPath>/<bucket>/<tablePath>/metadata
		metadataDir := path.Join(tableDir, "metadata")
		if err := ensureDir(tableDir, "metadata", "metadata directory"); err != nil {
			return err
		}

		// 4. Write the file
		resp, err := client.CreateEntry(opCtx, &filer_pb.CreateEntryRequest{
			Directory: metadataDir,
			OExcl:     exclusive,
			Entry: &filer_pb.Entry{
				Name: metadataFileName,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					Crtime:   time.Now().Unix(),
					FileMode: uint32(0644),
					FileSize: uint64(len(content)),
				},
				Content:  content,
				Extended: attrs,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to write metadata file: %w", err)
		}
		if resp.ErrorCode != filer_pb.FilerError_OK {
			if sentinel := filer_pb.FilerErrorToSentinel(resp.ErrorCode); sentinel != nil {
				return fmt.Errorf("failed to write metadata file: %w", sentinel)
			}
			return fmt.Errorf("failed to write metadata file: code=%v %s", resp.ErrorCode, resp.Error)
		}
		return nil
	})
}

func (s *Server) deleteMetadataFile(ctx context.Context, bucketName, tablePath, metadataFileName string) error {
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	metadataDir := metadataDirPath(bucketName, tablePath)
	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.DoRemove(opCtx, client, metadataDir, metadataFileName, true, false, true, false, nil)
	})
}

// errStagedFileReplaced reports that the entry at a staged file's path no
// longer carries the token it was staged with: it is another writer's file.
var errStagedFileReplaced = errors.New("staged metadata file was replaced")

// deleteStagedMetadataFile removes a file stageOwnedMetadata wrote, only while
// the entry at its path still carries token. The filer checks the token under
// the path lock the delete runs under, so a file that took the path since —
// the table was dropped and another create staged the same name — is refused
// with errStagedFileReplaced instead of deleted. A path that holds nothing is
// not an error.
func (s *Server) deleteStagedMetadataFile(ctx context.Context, bucketName, tablePath, metadataFileName, token string) error {
	if token == "" {
		return fmt.Errorf("%w: %s has no stage token", errStagedFileReplaced, metadataFileName)
	}
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	metadataDir := metadataDirPath(bucketName, tablePath)
	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := filer_pb.DoRemoveRequest(opCtx, client, &filer_pb.DeleteEntryRequest{
			Directory:            metadataDir,
			Name:                 metadataFileName,
			IsDeleteData:         true,
			IgnoreRecursiveError: true,
			ExpectedExtended:     map[string][]byte{stageTokenKey: []byte(token)},
		})
		if status.Code(err) == codes.FailedPrecondition {
			return fmt.Errorf("%w: %s/%s: %v", errStagedFileReplaced, metadataDir, metadataFileName, err)
		}
		return err
	})
}

// errEmptyMetadataFile is a metadata file that exists and holds no bytes: a
// staged create's placeholder is one, and is treated as absent there.
var errEmptyMetadataFile = errors.New("empty metadata file")

// maxMetadataFileSize bounds a metadata.json read whole into memory (and, for
// a remote-only file, fetched into the local cluster).
const maxMetadataFileSize = 64 << 20

// loadMetadataFile reads a file of a table's metadata directory whole: inline
// content (what the catalog writes), chunks (a file written through the S3
// gateway or the store's maintenance), or, in a remote-mounted bucket, a file
// that holds only its RemoteEntry here, fetched into the local cluster first
// as an S3 read does. A file that holds no bytes is errEmptyMetadataFile.
func (s *Server) loadMetadataFile(ctx context.Context, bucketName, tablePath, metadataFileName string) ([]byte, error) {
	content, err := s.readMetadataDirFile(ctx, bucketName, tablePath, metadataFileName, maxMetadataFileSize)
	if err != nil {
		return nil, err
	}
	if len(content) == 0 {
		return nil, fmt.Errorf("%s/%s: %w", metadataDirPath(bucketName, tablePath), metadataFileName, errEmptyMetadataFile)
	}
	return content, nil
}

// readMetadataDirFile reads a file of a table's metadata directory whole, at
// most limit bytes: inline content, chunks, or a remote-only file of a
// remote-mounted bucket, cached into the local cluster first (its remote size
// checked against limit before anything is fetched). Reading only inline
// content, or chunks, returned a remote-only file as empty bytes: in a
// remote-mounted bucket every file the write-back has uncached is one.
func (s *Server) readMetadataDirFile(ctx context.Context, bucketName, tablePath, fileName string, limit uint64) ([]byte, error) {
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	metadataDir := metadataDirPath(bucketName, tablePath)
	var entry *filer_pb.Entry
	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := filer_pb.LookupEntry(opCtx, client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: metadataDir,
			Name:      fileName,
		})
		if err != nil {
			return err
		}
		if resp == nil || resp.Entry == nil {
			return fmt.Errorf("lookup returned nil entry for %s/%s", metadataDir, fileName)
		}
		entry = resp.Entry
		// a remote file's size is known before it is fetched: one larger than the limit is not fetched at all
		if remoteOnlyEntry(entry) && uint64(entry.RemoteEntry.RemoteSize) > limit {
			return fmt.Errorf("%s/%s is %d bytes on the remote, larger than the %d byte limit", metadataDir, fileName, entry.RemoteEntry.RemoteSize, limit)
		}
		// one retry in place: a cache call can lose a race with an uncache or a write-back of the same entry
		for attempt := 0; attempt < 2 && remoteOnlyEntry(entry); attempt++ {
			cached, err := client.CacheRemoteObjectToLocalCluster(opCtx, &filer_pb.CacheRemoteObjectToLocalClusterRequest{
				Directory: metadataDir,
				Name:      fileName,
			})
			if err != nil {
				return fmt.Errorf("cache remote %s/%s: %w", metadataDir, fileName, err)
			}
			if cached == nil || cached.Entry == nil {
				return fmt.Errorf("cache remote %s/%s: no entry returned", metadataDir, fileName)
			}
			entry = cached.Entry
		}
		if remoteOnlyEntry(entry) {
			return fmt.Errorf("cache remote %s/%s: still remote-only after caching twice", metadataDir, fileName)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(entry.Content) > 0 {
		return append([]byte(nil), entry.Content...), nil
	}
	if len(entry.GetChunks()) == 0 {
		return nil, nil
	}
	size := filer.FileSize(entry)
	if size > limit {
		return nil, fmt.Errorf("%s/%s is %d bytes, larger than the %d byte limit", metadataDir, fileName, size, limit)
	}
	fullClient, ok := s.filerClient.(filer_pb.FilerClient)
	if !ok {
		return nil, fmt.Errorf("filer client cannot resolve chunk locations")
	}
	var buf bytes.Buffer
	buf.Grow(int(size))
	lookup := &lookupFileIDAdapter{fn: filer.LookupFn(fullClient)}
	if err := filer.StreamContent(lookup, &buf, entry.GetChunks(), 0, int64(size)); err != nil {
		return nil, fmt.Errorf("read %s/%s: %w", metadataDir, fileName, err)
	}
	return buf.Bytes(), nil
}

// remoteOnlyEntry is a file of a remote-mounted bucket that holds no bytes
// here: no chunks and no inline content, only its RemoteEntry.
func remoteOnlyEntry(entry *filer_pb.Entry) bool {
	return entry.IsInRemoteOnly() && len(entry.Content) == 0
}

// metadataLocationPath checks a metadata location without reading anything:
// in bucketName, without traversal segments, and a file directly in a table's
// metadata directory. It returns the bucket-relative table path.
func metadataLocationPath(bucketName, metadataLocation string) (string, error) {
	metadataBucket, tablePath, err := parseS3Location(tableLocationFromMetadataLocation(metadataLocation))
	if err != nil {
		return "", fmt.Errorf("invalid metadata location %s: %w", metadataLocation, err)
	}
	if err := confineMetadataLocation(metadataBucket, tablePath, bucketName); err != nil {
		return "", err
	}
	// the file itself, directly in the table's metadata directory: a deeper
	// path would be read as a same-named file there
	if path.Base(path.Dir(metadataLocation)) != "metadata" {
		return "", fmt.Errorf("metadata location %s is not a file of a table's metadata directory", metadataLocation)
	}
	return tablePath, nil
}

// metadataAtLocation reads the metadata.json a metadata location names,
// confined to bucketName (metadataLocationPath).
func (s *Server) metadataAtLocation(ctx context.Context, bucketName, metadataLocation string) ([]byte, error) {
	if s.filerClient == nil {
		return nil, fmt.Errorf("no filer to read %s from", metadataLocation)
	}
	tablePath, err := metadataLocationPath(bucketName, metadataLocation)
	if err != nil {
		return nil, err
	}
	return s.loadMetadataFile(ctx, bucketName, tablePath, path.Base(metadataLocation))
}

// currentMetadataBytes returns the Iceberg metadata a table serves: the copy
// kept in its catalog record or, for a record that holds only a metadata
// location, the file at that location (metadataAtLocation: confined to the
// bucket and to a table's metadata directory, chunks followed, a remote-only
// file of a mounted bucket fetched first). It returns nil, for the caller's
// placeholder, only when the record has neither: a table created without a
// location, a new empty table until its first update. A location whose file
// cannot be read, or is gone, is an error on every path (load, commit,
// transaction): an empty table in its place reads as no rows, refuses every
// client commit on a UUID mismatch, and lets a commit without requirements
// replace the table's history.
//
// A catalog-only table (a Lance dataset) has no Iceberg metadata: its
// record's metadata and location are the Lance client's, never read here, and
// it is served as an empty read-only Iceberg table.
func (s *Server) currentMetadataBytes(ctx context.Context, getResp s3tables.GetTableResponse, bucketName string) ([]byte, error) {
	if s3tables.IsCatalogOnlyFormat(getResp.Format) {
		return nil, nil
	}
	if getResp.Metadata != nil && len(getResp.Metadata.FullMetadata) > 0 {
		return getResp.Metadata.FullMetadata, nil
	}
	if getResp.MetadataLocation == "" {
		return nil, nil
	}
	return s.metadataAtLocation(ctx, bucketName, getResp.MetadataLocation)
}
