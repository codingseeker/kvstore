package raft

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

type durableState struct {
	CurrentTerm   int64  `json:"current_term"`
	VotedFor      string `json:"voted_for"`
	CommitIndex   int64  `json:"commit_index"`
	SnapshotIndex int64  `json:"snapshot_index,omitempty"`
	SnapshotTerm  int64  `json:"snapshot_term,omitempty"`
}

type diskStorage struct {
	dir          string
	statePath    string
	logPath      string
	segmentBytes int64
	fsyncPolicy  string
}

type walRecord struct {
	Entry    Entry  `json:"entry"`
	Checksum string `json:"checksum"`
}

func newDiskStorage(dir string, segmentBytes int64, fsyncPolicy string) *diskStorage {
	if fsyncPolicy == "" {
		fsyncPolicy = "always"
	}
	return &diskStorage{
		dir:          dir,
		statePath:    filepath.Join(dir, "raft-state.json"),
		logPath:      filepath.Join(dir, "raft-log.jsonl"),
		segmentBytes: segmentBytes,
		fsyncPolicy:  fsyncPolicy,
	}
}

func (d *diskStorage) load() (durableState, []Entry, error) {
	var st durableState
	if err := os.MkdirAll(d.dir, 0o755); err != nil {
		return st, nil, err
	}
	stateBytes, err := os.ReadFile(d.statePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return st, nil, err
	}
	if len(stateBytes) > 0 {
		if err := json.Unmarshal(stateBytes, &st); err != nil {
			return st, nil, err
		}
	}

	f, err := os.Open(d.logPath)
	if errors.Is(err, os.ErrNotExist) {
		entries, loadErr := d.loadSegments()
		return st, entries, loadErr
	}
	if err != nil {
		return st, nil, err
	}
	defer func() { _ = f.Close() }()

	entries, err := readWALEntries(f)
	return st, entries, err
}

func (d *diskStorage) loadSegments() ([]Entry, error) {
	paths, err := filepath.Glob(filepath.Join(d.dir, "raft-log-*.jsonl"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	var entries []Entry
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		fileEntries, err := readWALEntries(f)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		entries = append(entries, fileEntries...)
		if err := f.Close(); err != nil {
			return nil, err
		}
	}
	return entries, nil
}

func (d *diskStorage) saveState(st durableState) error {
	if err := os.MkdirAll(d.dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := d.statePath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return atomicRename(tmp, d.statePath)
}

func (d *diskStorage) appendEntries(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	if err := os.MkdirAll(d.dir, 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(d.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	for _, entry := range entries {
		if err := writeWALEntry(f, entry); err != nil {
			return err
		}
	}
	if d.fsyncPolicy == "always" {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	if d.fsyncPolicy == "batch" {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	return d.appendSegment(entries)
}

func (d *diskStorage) rewriteLog(entries []Entry) error {
	if err := os.MkdirAll(d.dir, 0o755); err != nil {
		return err
	}
	tmp := d.logPath + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := writeWALEntry(f, entry); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := atomicRename(tmp, d.logPath); err != nil {
		return err
	}
	return d.rewriteSegments(entries)
}

func (d *diskStorage) appendSegment(entries []Entry) error {
	if d.segmentBytes <= 0 {
		return nil
	}
	path, err := d.activeSegmentPath()
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	for _, entry := range entries {
		if err := writeWALEntry(f, entry); err != nil {
			return err
		}
	}
	return f.Sync()
}

func (d *diskStorage) rewriteSegments(entries []Entry) error {
	if d.segmentBytes <= 0 {
		return nil
	}
	path := filepath.Join(d.dir, "raft-log-000001.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := writeWALEntry(f, entry); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (d *diskStorage) activeSegmentPath() (string, error) {
	index := 1
	for {
		path := filepath.Join(d.dir, "raft-log-"+leftPad(index)+".jsonl")
		info, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			return path, nil
		}
		if err != nil {
			return "", err
		}
		if info.Size() < d.segmentBytes {
			return path, nil
		}
		index++
	}
}

func leftPad(value int) string {
	raw := strconv.Itoa(value)
	for len(raw) < 6 {
		raw = "0" + raw
	}
	return raw
}

func decodeWALEntry(raw []byte) (Entry, error) {
	var record walRecord
	if err := json.Unmarshal(raw, &record); err == nil && record.Checksum != "" {
		expected, err := checksumEntry(record.Entry)
		if err != nil {
			return Entry{}, err
		}
		if !hmacEqualHex(expected, record.Checksum) {
			return Entry{}, fmt.Errorf("wal checksum mismatch at index %d", record.Entry.Index)
		}
		return record.Entry, nil
	}
	var entry Entry
	if err := json.Unmarshal(raw, &entry); err != nil {
		return Entry{}, err
	}
	if entry.Index == 0 {
		return Entry{}, errors.New("invalid wal entry")
	}
	return entry, nil
}

func readWALEntries(r io.Reader) ([]Entry, error) {
	reader := bufio.NewReaderSize(r, 1024*1024)
	var entries []Entry
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			complete := len(line) > 0 && line[len(line)-1] == '\n'
			line = bytes.TrimSpace(line)
			if len(line) > 0 {
				entry, decodeErr := decodeWALEntry(line)
				if decodeErr != nil {
					if errors.Is(err, io.EOF) || !complete {
						return entries, nil
					}
					return nil, decodeErr
				}
				entries = append(entries, entry)
			}
		}
		if errors.Is(err, io.EOF) {
			return entries, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func writeWALEntry(f *os.File, entry Entry) error {
	checksum, err := checksumEntry(entry)
	if err != nil {
		return err
	}
	b, err := json.Marshal(walRecord{Entry: entry, Checksum: checksum})
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		return err
	}
	return nil
}

func checksumEntry(entry Entry) (string, error) {
	b, err := json.Marshal(entry)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func hmacEqualHex(a string, b string) bool {
	ab, errA := hex.DecodeString(a)
	bb, errB := hex.DecodeString(b)
	if errA != nil || errB != nil {
		return false
	}
	return bytes.Equal(ab, bb)
}

func atomicRename(tmp string, path string) error {
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}
