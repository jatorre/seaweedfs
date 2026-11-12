package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"google.golang.org/grpc"
)

// registeredMetadata is a table's metadata.json with a schema and a snapshot,
// as a table that was written to and then unmounted leaves it.
const registeredMetadata = `{
 "format-version": 2, "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1", "location": "s3://warehouse/finance/orders",
 "last-sequence-number": 1, "last-updated-ms": 1700000000000, "last-column-id": 2,
 "current-schema-id": 0, "schemas": [{"type": "struct", "schema-id": 0, "fields": [
  {"id": 1, "name": "id", "required": true, "type": "long"}, {"id": 2, "name": "name", "required": false, "type": "string"}]}],
 "default-spec-id": 0, "partition-specs": [{"spec-id": 0, "fields": []}], "last-partition-id": 999,
 "default-sort-order-id": 0, "sort-orders": [{"order-id": 0, "fields": []}], "properties": {},
 "current-snapshot-id": 3055729675574597004,
 "snapshots": [{"snapshot-id": 3055729675574597004, "sequence-number": 1, "timestamp-ms": 1700000000000,
  "manifest-list": "s3://warehouse/finance/orders/metadata/snap-3055729675574597004-1.avro", "summary": {"operation": "append"}, "schema-id": 0}],
 "snapshot-log": [{"snapshot-id": 3055729675574597004, "timestamp-ms": 1700000000000}], "metadata-log": [],
 "refs": {"main": {"snapshot-id": 3055729675574597004, "type": "branch"}}
}`

const registeredLocation = "s3://warehouse/finance/orders/metadata/00001-4c1f6d0e-2b1a-4d39-9d37-0f6a7c7f1b11.metadata.json"

// remoteMemFiler holds some files only as their RemoteEntry, as a remote-mounted
// bucket does for files not cached here; caching one gives it its content.
type remoteMemFiler struct {
	*memFiler
	remote map[string][]byte
	cached int
}

func (m *remoteMemFiler) WithFilerClient(_ bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return fn(m)
}

func (m *remoteMemFiler) CacheRemoteObjectToLocalCluster(_ context.Context, in *filer_pb.CacheRemoteObjectToLocalClusterRequest, _ ...grpc.CallOption) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {
	p := path.Join(in.Directory, in.Name)
	content, ok := m.remote[p]
	if !ok {
		return nil, filer_pb.ErrNotFound
	}
	m.cached++
	entry := &filer_pb.Entry{Name: in.Name, Content: content, RemoteEntry: m.entries[p].RemoteEntry}
	m.entries[p] = entry
	return &filer_pb.CacheRemoteObjectToLocalClusterResponse{Entry: entry}, nil
}

func seedRegisteredFile(fc *memFiler, content []byte, remoteOnly bool) {
	dir := metadataDirPath("warehouse", "finance/orders")
	name := path.Base(registeredLocation)
	entry := &filer_pb.Entry{Name: name, Content: content}
	if remoteOnly {
		entry = &filer_pb.Entry{Name: name, RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: int64(len(content))}}
	}
	fc.seed(path.Join(dir, name), entry)
}

func registerRequest(t *testing.T, body string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/v1/warehouse/namespaces/finance/register", strings.NewReader(body))
	r = mux.SetURLVars(r, map[string]string{"prefix": "warehouse", "namespace": "finance"})
	return r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice"))
}

func loadTable(t *testing.T, s *Server) (int, table.Metadata, string) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/v1/warehouse/namespaces/finance/tables/orders", nil)
	r = mux.SetURLVars(r, map[string]string{"prefix": "warehouse", "namespace": "finance", "table": "orders"})
	r = r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice"))
	w := httptest.NewRecorder()
	s.handleLoadTable(w, r)
	if w.Code != http.StatusOK {
		return w.Code, nil, w.Body.String()
	}
	var resp struct {
		MetadataLocation string          `json:"metadata-location"`
		Metadata         json.RawMessage `json:"metadata"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode load: %v", err)
	}
	md, err := table.ParseMetadataBytes(resp.Metadata)
	if err != nil {
		t.Fatalf("parse loaded metadata: %v (%s)", err, resp.Metadata)
	}
	return w.Code, md, resp.MetadataLocation
}

// assertSameTable checks a loaded table against the metadata.json it was
// registered from: schema, snapshots, current snapshot.
func assertSameTable(t *testing.T, got table.Metadata) {
	t.Helper()
	want, err := table.ParseMetadataBytes([]byte(registeredMetadata))
	if err != nil {
		t.Fatal(err)
	}
	if !got.CurrentSchema().Equals(want.CurrentSchema()) {
		t.Errorf("schema = %v, want %v", got.CurrentSchema(), want.CurrentSchema())
	}
	if len(got.Snapshots()) != 1 || got.CurrentSnapshot() == nil || got.CurrentSnapshot().SnapshotID != want.CurrentSnapshot().SnapshotID {
		t.Errorf("snapshots = %v, current = %v; want the one snapshot %d current", got.Snapshots(), got.CurrentSnapshot(), want.CurrentSnapshot().SnapshotID)
	}
	if got.TableUUID() != want.TableUUID() {
		t.Errorf("table uuid = %v, want %v", got.TableUUID(), want.TableUUID())
	}
}

// RegisterTable stored the metadata location alone, and LoadTable serves the
// stored metadata: a registered table loaded with no schema and no snapshots,
// and DuckDB read it as empty. The metadata.json is now read
// and stored with the table, whether it is inline here or held only on a
// mounted remote, as after an unmount and attach.
func TestRegisteredTableLoadsWithItsMetadata(t *testing.T) {
	for _, remoteOnly := range []bool{false, true} {
		name := map[bool]string{false: "inline", true: "remote-only"}[remoteOnly]
		t.Run(name, func(t *testing.T) {
			fc := &remoteMemFiler{memFiler: newMemFiler(), remote: map[string][]byte{}}
			seedNamespace(fc.memFiler, "warehouse", "finance", "alice")
			seedRegisteredFile(fc.memFiler, []byte(registeredMetadata), remoteOnly)
			if remoteOnly {
				fc.remote[path.Join(metadataDirPath("warehouse", "finance/orders"), path.Base(registeredLocation))] = []byte(registeredMetadata)
			}
			s := NewServer(fc, nil)

			w := httptest.NewRecorder()
			s.handleRegisterTable(w, registerRequest(t, `{"name":"orders","metadata-location":"`+registeredLocation+`"}`))
			if w.Code != http.StatusOK {
				t.Fatalf("register: status %d, body %s", w.Code, w.Body.String())
			}
			if remoteOnly && fc.cached != 1 {
				t.Errorf("remote file cached %d times, want once (at register)", fc.cached)
			}
			// stored with the table, as a commit stores it: a load does not depend on the file
			entry := fc.entries[s3tables.GetTablePath("warehouse", "finance", "orders")]
			var stored struct {
				Metadata *s3tables.TableMetadata `json:"metadata"`
			}
			if entry == nil || json.Unmarshal(entry.Extended[s3tables.ExtendedKeyMetadata], &stored) != nil ||
				stored.Metadata == nil || len(stored.Metadata.FullMetadata) == 0 {
				t.Fatalf("the registered table holds no full metadata")
			}
			if md, err := table.ParseMetadataBytes(stored.Metadata.FullMetadata); err != nil {
				t.Fatalf("stored metadata: %v", err)
			} else {
				assertSameTable(t, md)
			}

			code, md, location := loadTable(t, s)
			if code != http.StatusOK {
				t.Fatalf("load: status %d (%s)", code, location)
			}
			if location != registeredLocation {
				t.Errorf("metadata-location = %q, want %q", location, registeredLocation)
			}
			assertSameTable(t, md)
			if remoteOnly && fc.cached != 1 {
				t.Errorf("remote file cached %d times, want once: the load serves the stored metadata", fc.cached)
			}
		})
	}
}

// A table registered before the fix holds its location alone: it loads from
// the metadata.json there, and a location that cannot be read fails the load
// rather than serving an empty table.
func TestATableStoredWithItsLocationAloneLoadsFromIt(t *testing.T) {
	fc := newMemFiler()
	seedNamespace(fc, "warehouse", "finance", "alice")
	seedRegisteredFile(fc, []byte(registeredMetadata), false)
	s := NewServer(fc, nil)
	// the registration as the unpatched store made it: no metadata
	err := fc.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		var resp s3tables.RegisterTableResponse
		return s.tablesManager.Execute(context.Background(), s3tables.NewManagerClient(client), "RegisterTable", &s3tables.RegisterTableRequest{
			TableBucketARN: buildTableBucketARN("warehouse"), Namespace: []string{"finance"}, Name: "orders", MetadataLocation: registeredLocation,
		}, &resp, "alice")
	})
	if err != nil {
		t.Fatalf("register without metadata: %v", err)
	}

	code, md, body := loadTable(t, s)
	if code != http.StatusOK {
		t.Fatalf("load: status %d (%s)", code, body)
	}
	assertSameTable(t, md)

	// the file gone: the load fails
	delete(fc.entries, path.Join(metadataDirPath("warehouse", "finance/orders"), path.Base(registeredLocation)))
	if code, _, _ := loadTable(t, s); code == http.StatusOK {
		t.Error("load with its metadata.json gone: 200, want an error rather than an empty table")
	}
}

// A file at the location that is not table metadata is refused at register.
func TestRegisterRefusesAFileThatIsNotTableMetadata(t *testing.T) {
	fc := newMemFiler()
	seedNamespace(fc, "warehouse", "finance", "alice")
	seedRegisteredFile(fc, []byte(`{"not":"metadata"}`), false)
	s := NewServer(fc, nil)
	w := httptest.NewRecorder()
	s.handleRegisterTable(w, registerRequest(t, `{"name":"orders","metadata-location":"`+registeredLocation+`"}`))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400 (body %s)", w.Code, w.Body.String())
	}
	if _, ok := fc.entries[s3tables.GetTablePath("warehouse", "finance", "orders")]; ok {
		t.Error("a refused register left a catalog entry")
	}
}

// registerLocationOnly registers orders as the unpatched store did: its
// location alone, no metadata.
func registerLocationOnly(t *testing.T, fc *memFiler, s *Server) {
	t.Helper()
	err := fc.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		var resp s3tables.RegisterTableResponse
		return s.tablesManager.Execute(context.Background(), s3tables.NewManagerClient(client), "RegisterTable", &s3tables.RegisterTableRequest{
			TableBucketARN: buildTableBucketARN("warehouse"), Namespace: []string{"finance"}, Name: "orders", MetadataLocation: registeredLocation,
		}, &resp, "alice")
	})
	if err != nil {
		t.Fatalf("register without metadata: %v", err)
	}
}

// A commit to a table stored with its location alone builds on the metadata
// there. It used to build on an empty table with a new UUID: a client's commit
// (its requirements taken from the load) was refused for ever on a UUID
// mismatch, and a commit without requirements replaced the table's history
// with an empty one. Both commit paths, the table
// commit and the multi-table transaction, are checked; with the file gone,
// both fail and the table is left as it was.
func TestACommitToALocationOnlyTableKeepsItsHistory(t *testing.T) {
	want, err := table.ParseMetadataBytes([]byte(registeredMetadata))
	if err != nil {
		t.Fatal(err)
	}
	requirements := `[{"type":"assert-table-uuid","uuid":"` + want.TableUUID().String() + `"},` +
		`{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":3055729675574597004}]`
	commit := map[string]func(s *Server, requirements string) int{
		"commit-table": func(s *Server, requirements string) int {
			r := httptest.NewRequest(http.MethodPost, "/v1/warehouse/namespaces/finance/tables/orders",
				strings.NewReader(`{"requirements":`+requirements+`,"updates":[{"action":"set-properties","updates":{"k":"v"}}]}`))
			r = mux.SetURLVars(r, map[string]string{"prefix": "warehouse", "namespace": "finance", "table": "orders"})
			r = r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice"))
			w := httptest.NewRecorder()
			s.handleUpdateTable(w, r)
			return w.Code
		},
		"commit-transaction": func(s *Server, requirements string) int {
			body := `{"table-changes":[{"identifier":{"namespace":["finance"],"name":"orders"},"requirements":` + requirements +
				`,"updates":[{"action":"set-properties","updates":{"k":"v"}}]}]}`
			r := httptest.NewRequest(http.MethodPost, "/v1/warehouse/transactions/commit", strings.NewReader(body))
			r = mux.SetURLVars(r, map[string]string{"prefix": "warehouse"})
			r = r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice"))
			w := httptest.NewRecorder()
			s.handleCommitTransaction(w, r)
			return w.Code
		},
	}
	for name, do := range commit {
		for _, reqs := range map[string]string{"with the load's requirements": requirements, "without requirements": "[]"} {
			t.Run(name+" "+map[bool]string{true: "with requirements", false: "without requirements"}[reqs != "[]"], func(t *testing.T) {
				fc := newMemFiler()
				seedNamespace(fc, "warehouse", "finance", "alice")
				seedRegisteredFile(fc, []byte(registeredMetadata), false)
				s := NewServer(fc, nil)
				registerLocationOnly(t, fc, s)

				if code := do(s, reqs); code != http.StatusOK && code != http.StatusNoContent {
					t.Fatalf("commit: status %d, want success", code)
				}
				code, md, body := loadTable(t, s)
				if code != http.StatusOK {
					t.Fatalf("load after commit: status %d (%s)", code, body)
				}
				assertSameTable(t, md)
				if md.Properties()["k"] != "v" {
					t.Errorf("properties = %v, want the commit's k=v", md.Properties())
				}
				// the stored UUID is the metadata's
				if _, stored := storedTableEntry(t, fc, "orders"); stored == nil || stored.Iceberg == nil || stored.Iceberg.TableUUID != want.TableUUID().String() {
					t.Errorf("stored table uuid = %+v, want %s", stored, want.TableUUID())
				}
			})
		}
		t.Run(name+" with the file gone", func(t *testing.T) {
			fc := newMemFiler()
			seedNamespace(fc, "warehouse", "finance", "alice")
			s := NewServer(fc, nil)
			seedRegisteredFile(fc, []byte(registeredMetadata), false)
			registerLocationOnly(t, fc, s)
			delete(fc.entries, path.Join(metadataDirPath("warehouse", "finance/orders"), path.Base(registeredLocation)))
			if code := do(s, "[]"); code < 500 {
				t.Errorf("commit with the metadata.json gone: status %d, want a server error", code)
			}
			entry := fc.entries[s3tables.GetTablePath("warehouse", "finance", "orders")]
			var stored struct {
				MetadataLocation string                  `json:"metadataLocation"`
				Metadata         *s3tables.TableMetadata `json:"metadata"`
			}
			if err := json.Unmarshal(entry.Extended[s3tables.ExtendedKeyMetadata], &stored); err != nil || stored.MetadataLocation != registeredLocation || stored.Metadata != nil {
				t.Errorf("the table moved after a refused commit: %+v (err %v)", stored, err)
			}
		})
	}
}

// A remote file is checked against the metadata size limit before it is
// fetched, and a location below the metadata directory is refused rather
// than read as a same-named file there.
func TestRegisterRefusesOversizedRemoteFilesAndNestedLocations(t *testing.T) {
	fc := &remoteMemFiler{memFiler: newMemFiler(), remote: map[string][]byte{}}
	seedNamespace(fc.memFiler, "warehouse", "finance", "alice")
	p := path.Join(metadataDirPath("warehouse", "finance/orders"), path.Base(registeredLocation))
	fc.seed(p, &filer_pb.Entry{Name: path.Base(registeredLocation), RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: maxMetadataFileSize + 1}})
	fc.remote[p] = []byte(registeredMetadata)
	s := NewServer(fc, nil)
	w := httptest.NewRecorder()
	s.handleRegisterTable(w, registerRequest(t, `{"name":"orders","metadata-location":"`+registeredLocation+`"}`))
	if w.Code != http.StatusBadRequest || fc.cached != 0 {
		t.Errorf("an oversized remote file: status %d, cached %d times; want 400 and no fetch", w.Code, fc.cached)
	}

	fc2 := newMemFiler()
	seedNamespace(fc2, "warehouse", "finance", "alice")
	seedRegisteredFile(fc2, []byte(registeredMetadata), false)
	s2 := NewServer(fc2, nil)
	nested := strings.Replace(registeredLocation, "/metadata/", "/metadata/sub/", 1)
	w = httptest.NewRecorder()
	s2.handleRegisterTable(w, registerRequest(t, `{"name":"orders","metadata-location":"`+nested+`"}`))
	if w.Code != http.StatusBadRequest {
		t.Errorf("a nested location %s: status %d, want 400 (it would read %s)", nested, w.Code, registeredLocation)
	}
	// control: the same file at its own location registers
	w = httptest.NewRecorder()
	s2.handleRegisterTable(w, registerRequest(t, `{"name":"orders","metadata-location":"`+registeredLocation+`"}`))
	if w.Code != http.StatusOK {
		t.Errorf("control: status %d, want 200 (%s)", w.Code, w.Body.String())
	}
}

// failingFlipFiler fails the catalog update of one table: the pointer flip of
// that table in a transaction.
type failingFlipFiler struct {
	*memFiler
	failPath string
	// onFail runs as the failing flip is refused; beforeUpdate before every
	// other update: each can land a concurrent write
	onFail       func()
	beforeUpdate func(p string, conditional bool)
}

func (m *failingFlipFiler) WithFilerClient(_ bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return fn(m)
}

func (m *failingFlipFiler) UpdateEntry(ctx context.Context, in *filer_pb.UpdateEntryRequest, opts ...grpc.CallOption) (*filer_pb.UpdateEntryResponse, error) {
	if path.Join(in.Directory, in.Entry.Name) == m.failPath {
		if m.onFail != nil {
			m.onFail()
		}
		return nil, errors.New("injected flip failure")
	}
	if m.beforeUpdate != nil {
		m.beforeUpdate(path.Join(in.Directory, in.Entry.Name), len(in.ExpectedExtended) > 0)
	}
	return m.memFiler.UpdateEntry(ctx, in, opts...)
}

func storedTableEntry(t *testing.T, fc *memFiler, name string) (location string, md *s3tables.TableMetadata) {
	t.Helper()
	entry := fc.entries[s3tables.GetTablePath("warehouse", "finance", name)]
	var stored struct {
		MetadataLocation string                  `json:"metadataLocation"`
		Metadata         *s3tables.TableMetadata `json:"metadata"`
	}
	if entry == nil || json.Unmarshal(entry.Extended[s3tables.ExtendedKeyMetadata], &stored) != nil {
		t.Fatalf("no catalog entry for %s", name)
	}
	return stored.MetadataLocation, stored.Metadata
}

// A transaction over a location-only table and a second table whose flip
// fails rolls the first back to the table as it was read; with no stored
// metadata the restore used to keep the failed transaction's metadata, and
// readers saw the rolled-back change.
func TestARolledBackTransactionRestoresALocationOnlyTable(t *testing.T) {
	fc := &failingFlipFiler{memFiler: newMemFiler(), failPath: s3tables.GetTablePath("warehouse", "finance", "b")}
	seedNamespace(fc.memFiler, "warehouse", "finance", "alice")
	seedRegisteredFile(fc.memFiler, []byte(registeredMetadata), false)
	seedOwnedTable(fc.memFiler, "warehouse", "finance", "b", "alice")
	s := NewServer(fc, nil)
	registerLocationOnly(t, fc.memFiler, s)

	body := `{"table-changes":[` +
		`{"identifier":{"namespace":["finance"],"name":"orders"},"requirements":[],"updates":[{"action":"set-properties","updates":{"k":"v"}}]},` +
		`{"identifier":{"namespace":["finance"],"name":"b"},"requirements":[],"updates":[{"action":"set-properties","updates":{"k":"v"}}]}]}`
	r := httptest.NewRequest(http.MethodPost, "/v1/warehouse/transactions/commit", strings.NewReader(body))
	r = mux.SetURLVars(r, map[string]string{"prefix": "warehouse"})
	r = r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice"))
	w := httptest.NewRecorder()
	s.handleCommitTransaction(w, r)
	if w.Code < 400 {
		t.Fatalf("transaction with a failing flip: status %d, want a failure", w.Code)
	}

	location, _ := storedTableEntry(t, fc.memFiler, "orders")
	if location != registeredLocation {
		t.Errorf("orders points at %s after the rollback, want %s", location, registeredLocation)
	}
	code, md, body2 := loadTable(t, s)
	if code != http.StatusOK {
		t.Fatalf("load after rollback: %d (%s)", code, body2)
	}
	assertSameTable(t, md)
	if _, ok := md.Properties()["k"]; ok {
		t.Errorf("properties = %v: the rolled-back change is visible", md.Properties())
	}
}

// Register checks the caller's right to create the table before it reads,
// or fetches, anything at the location: a caller without rights gets one
// answer whether the file exists, is metadata, or is absent.
func TestRegisterRefusesACallerWithoutRightsBeforeReading(t *testing.T) {
	for _, file := range []string{"metadata", "absent"} {
		t.Run(file, func(t *testing.T) {
			fc := &remoteMemFiler{memFiler: newMemFiler(), remote: map[string][]byte{}}
			seedNamespace(fc.memFiler, "warehouse", "finance", "alice")
			if file == "metadata" {
				seedRegisteredFile(fc.memFiler, []byte(registeredMetadata), true)
				fc.remote[path.Join(metadataDirPath("warehouse", "finance/orders"), path.Base(registeredLocation))] = []byte(registeredMetadata)
			}
			s := NewServer(fc, nil)
			r := httptest.NewRequest(http.MethodPost, "/v1/warehouse/namespaces/finance/register",
				strings.NewReader(`{"name":"orders","metadata-location":"`+registeredLocation+`"}`))
			r = mux.SetURLVars(r, map[string]string{"prefix": "warehouse", "namespace": "finance"})
			r = r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "mallory"))
			w := httptest.NewRecorder()
			s.handleRegisterTable(w, r)
			if w.Code != http.StatusForbidden {
				t.Errorf("status %d, want 403 (body %s)", w.Code, w.Body.String())
			}
			if fc.cached != 0 {
				t.Errorf("the remote file was fetched %d times for a caller without rights", fc.cached)
			}
		})
	}
}

// A register may not point at the location another table of the bucket holds
// in any namespace: a rename moves the catalog record and leaves
// the files, so a table renamed into hr still owns finance/orders, and an
// alias registered there would read it and be vended credentials for its files
// under finance's grants. The refusal names no table. Once the owner is
// dropped the location is free again. The metadata must describe the table at
// the location it is registered from.
func TestRegisterRefusesAnotherTablesLocationAndAForeignMetadataLocation(t *testing.T) {
	fc := &remoteMemFiler{memFiler: newMemFiler(), remote: map[string][]byte{}}
	seedNamespace(fc.memFiler, "warehouse", "finance", "alice")
	seedNamespace(fc.memFiler, "warehouse", "hr", "alice")
	seedRegisteredFile(fc.memFiler, []byte(registeredMetadata), false)
	s := NewServer(fc, nil)
	w := httptest.NewRecorder()
	s.handleRegisterTable(w, registerRequest(t, `{"name":"orders","metadata-location":"`+registeredLocation+`"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("control register: %d (%s)", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	s.handleRegisterTable(w, registerRequest(t, `{"name":"orders_copy","metadata-location":"`+registeredLocation+`"}`))
	// a location another record holds: 409, naming no table (#12473)
	if w.Code != http.StatusConflict || strings.Contains(w.Body.String(), "orders") {
		t.Errorf("a second entry for orders' files: status %d (%s), want 409 naming no table", w.Code, w.Body.String())
	}

	// orders renamed into hr keeps its files under finance/orders
	r := httptest.NewRequest(http.MethodPost, "/v1/warehouse/tables/rename", strings.NewReader(
		`{"source":{"namespace":["finance"],"name":"orders"},"destination":{"namespace":["hr"],"name":"orders"}}`))
	r = mux.SetURLVars(r, map[string]string{"prefix": "warehouse"})
	w = httptest.NewRecorder()
	s.handleRenameTable(w, r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice")))
	if w.Code != http.StatusNoContent {
		t.Fatalf("rename finance.orders to hr.orders: %d (%s)", w.Code, w.Body.String())
	}
	// refused before the file is read: in a mounted bucket, not even fetched
	seedRegisteredFile(fc.memFiler, []byte(registeredMetadata), true)
	fc.remote[path.Join(metadataDirPath("warehouse", "finance/orders"), path.Base(registeredLocation))] = []byte(registeredMetadata)
	w = httptest.NewRecorder()
	s.handleRegisterTable(w, registerRequest(t, `{"name":"alias","metadata-location":"`+registeredLocation+`"}`))
	if w.Code != http.StatusConflict || fc.cached != 0 {
		t.Errorf("an alias of the files hr.orders kept under finance: status %d (%s), fetched %d times, want 409 and none", w.Code, w.Body.String(), fc.cached)
	}

	// positive control: the same register goes through once hr.orders is gone
	r = httptest.NewRequest(http.MethodDelete, "/v1/warehouse/namespaces/hr/tables/orders", nil)
	r = mux.SetURLVars(r, map[string]string{"prefix": "warehouse", "namespace": "hr", "table": "orders"})
	w = httptest.NewRecorder()
	s.handleDropTable(w, r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice")))
	if w.Code != http.StatusNoContent {
		t.Fatalf("drop hr.orders: %d (%s)", w.Code, w.Body.String())
	}
	seedRegisteredFile(fc.memFiler, []byte(registeredMetadata), false) // the drop purged it
	w = httptest.NewRecorder()
	s.handleRegisterTable(w, registerRequest(t, `{"name":"alias","metadata-location":"`+registeredLocation+`"}`))
	if w.Code != http.StatusOK {
		t.Errorf("register at a location no table holds any more: status %d (%s), want 200", w.Code, w.Body.String())
	}

	fc2 := newMemFiler()
	seedNamespace(fc2, "warehouse", "finance", "alice")
	seedRegisteredFile(fc2, []byte(strings.Replace(registeredMetadata, `"location": "s3://warehouse/finance/orders"`, `"location": "s3://warehouse/finance/elsewhere"`, 1)), false)
	s2 := NewServer(fc2, nil)
	w = httptest.NewRecorder()
	s2.handleRegisterTable(w, registerRequest(t, `{"name":"orders","metadata-location":"`+registeredLocation+`"}`))
	if w.Code != http.StatusBadRequest {
		t.Errorf("metadata describing another location: status %d (%s), want 400", w.Code, w.Body.String())
	}
}

// An empty staged-create file is a placeholder, as an absent one is: the
// create commit goes on instead of failing.
func TestAnEmptyMetadataFileIsAPlaceholderForAStagedCreate(t *testing.T) {
	fc := newMemFiler()
	seedNamespace(fc, "warehouse", "finance", "alice")
	s := NewServer(fc, nil)
	w := httptest.NewRecorder()
	s.handleCreateTable(w, newCreateTableRequest(t, "warehouse", "finance", `{"name":"orders","stage-create":true}`, "alice"))
	if w.Code != http.StatusOK {
		t.Fatalf("stage-create: %d (%s)", w.Code, w.Body.String())
	}
	emptied := 0
	for p, entry := range fc.entries {
		if !entry.IsDirectory && strings.HasSuffix(p, ".metadata.json") {
			fc.entries[p] = &filer_pb.Entry{Name: entry.Name}
			emptied++
			if _, err := s.loadMetadataFile(context.Background(), "warehouse", strings.TrimPrefix(path.Dir(path.Dir(p)), s3tables.GetTableBucketPath("warehouse")+"/"), entry.Name); !errors.Is(err, errEmptyMetadataFile) {
				t.Errorf("err = %v, want errEmptyMetadataFile", err)
			}
		}
	}
	if emptied != 1 {
		t.Fatalf("staged metadata files = %d, want 1", emptied)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/warehouse/namespaces/finance/tables/orders",
		strings.NewReader(`{"requirements":[{"type":"assert-create"}],"updates":[]}`))
	r = mux.SetURLVars(r, map[string]string{"prefix": "warehouse", "namespace": "finance", "table": "orders"})
	r = r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice"))
	w = httptest.NewRecorder()
	s.handleUpdateTable(w, r)
	if w.Code >= 500 {
		t.Errorf("create commit over an empty staged file: status %d (%s)", w.Code, w.Body.String())
	}
}

// A location in another namespace of the bucket is refused before anything is
// read or fetched: the ownership check lists only the table's own namespace,
// and the catalog's own namespace check came after the read.
func TestRegisterRefusesAnotherNamespaceBeforeReading(t *testing.T) {
	fc := &remoteMemFiler{memFiler: newMemFiler(), remote: map[string][]byte{}}
	seedNamespace(fc.memFiler, "warehouse", "finance", "alice")
	seedNamespace(fc.memFiler, "warehouse", "hr", "alice")
	other := strings.Replace(registeredLocation, "/finance/", "/hr/", 1)
	p := path.Join(metadataDirPath("warehouse", "hr/orders"), path.Base(other))
	fc.seed(p, &filer_pb.Entry{Name: path.Base(other), RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: int64(len(registeredMetadata))}})
	fc.remote[p] = []byte(strings.Replace(registeredMetadata, "/finance/", "/hr/", -1))
	s := NewServer(fc, nil)
	w := httptest.NewRecorder()
	s.handleRegisterTable(w, registerRequest(t, `{"name":"orders","metadata-location":"`+other+`"}`))
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "within namespace") {
		t.Errorf("another namespace's location: status %d (%s), want 400", w.Code, w.Body.String())
	}
	if fc.cached != 0 {
		t.Errorf("the file was fetched %d times before the refusal", fc.cached)
	}
}

// A commit or a transaction stores the UUID of the metadata it builds on, not
// a record's that disagrees with it: the next client that asserts the UUID it
// loaded would otherwise be refused.
func TestACommitStoresTheBaseMetadatasUUID(t *testing.T) {
	const fileUUID = "9c12d441-03fe-4693-9a96-a0705ddf69c1"
	for _, mode := range []string{"commit", "transaction"} {
		t.Run(mode, func(t *testing.T) {
			fc := newMemFiler()
			seedNamespace(fc, "warehouse", "finance", "alice")
			internal := map[string]any{
				"name": "orders", "namespace": "finance", "format": "ICEBERG", "ownerAccountId": "alice",
				"versionToken": "v1", "metadataVersion": 1, "metadataLocation": registeredLocation,
				"metadata": s3tables.TableMetadata{
					Iceberg:      &s3tables.IcebergMetadata{TableUUID: "00000000-0000-0000-0000-000000000001"},
					FullMetadata: json.RawMessage(registeredMetadata),
				},
			}
			b, _ := json.Marshal(internal)
			fc.seed(s3tables.GetTablePath("warehouse", "finance", "orders"), &filer_pb.Entry{Name: "orders", IsDirectory: true,
				Extended: map[string][]byte{s3tables.ExtendedKeyMetadata: b}})
			s := NewServer(fc, nil)
			w := httptest.NewRecorder()
			if mode == "commit" {
				r := httptest.NewRequest(http.MethodPost, "/v1/warehouse/namespaces/finance/tables/orders",
					strings.NewReader(`{"requirements":[],"updates":[{"action":"set-properties","updates":{"k":"v"}}]}`))
				r = mux.SetURLVars(r, map[string]string{"prefix": "warehouse", "namespace": "finance", "table": "orders"})
				s.handleUpdateTable(w, r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice")))
			} else {
				r := httptest.NewRequest(http.MethodPost, "/v1/warehouse/transactions/commit", strings.NewReader(
					`{"table-changes":[{"identifier":{"namespace":["finance"],"name":"orders"},"requirements":[],"updates":[{"action":"set-properties","updates":{"k":"v"}}]}]}`))
				r = mux.SetURLVars(r, map[string]string{"prefix": "warehouse"})
				s.handleCommitTransaction(w, r.WithContext(s3_constants.SetIdentityNameInContext(r.Context(), "alice")))
			}
			if w.Code >= 300 {
				t.Fatalf("%s: %d %s", mode, w.Code, w.Body.String())
			}
			_, md := storedTableEntry(t, fc, "orders")
			if md == nil || md.Iceberg == nil || md.Iceberg.TableUUID != fileUUID {
				got := ""
				if md != nil && md.Iceberg != nil {
					got = md.Iceberg.TableUUID
				}
				t.Errorf("stored record UUID = %q, want the base metadata's %s (the stored metadata itself carries %s)", got, fileUUID, fileUUID)
			}
		})
	}
}
