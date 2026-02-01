package db

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// WAL constants
const (
	WALMagicNumber    = 0xCADB0001
	WALVersion        = 1
	WALMaxSize        = 64 * 1024 * 1024 // 64MB
	WALRetentionCount = 2                // Keep last 2 WAL files
	WALCheckpointFile = "wal.checkpoint"
	WALFilePrefix     = "wal-"
	WALBatchSize      = 100                    // Batch writes
	WALFlushInterval  = 100 * time.Millisecond // Flush every 100ms
)

// WALOperation types
const (
	WALOpInsert           = "insert"
	WALOpUpdate           = "update"
	WALOpDelete           = "delete"
	WALOpCreateDatabase   = "create_database"
	WALOpDeleteDatabase   = "delete_database"
	WALOpCreateCollection = "create_collection"
	WALOpDeleteCollection = "delete_collection"
	WALOpCreateIndex      = "create_index"
)

// WALEntry represents a single write-ahead log entry
type WALEntry struct {
	Offset     uint64    `json:"offset"`
	Timestamp  time.Time `json:"timestamp"`
	Database   string    `json:"database"`
	Collection string    `json:"collection,omitempty"`
	Operation  string    `json:"operation"`
	DocumentID string    `json:"document_id,omitempty"`
	Data       []byte    `json:"data"`
	Checksum   uint32    `json:"-"` // Computed, not serialized
}

// WALCheckpoint tracks the last successfully synced offset
type WALCheckpoint struct {
	Offset    uint64    `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
}

// WALManager manages write-ahead logging
type WALManager struct {
	rootDir       string
	currentFile   *os.File
	currentOffset uint64
	currentSize   int64
	writer        *bufio.Writer
	batch         []*WALEntry
	batchMu       sync.Mutex
	checkpoint    *WALCheckpoint
	mu            sync.RWMutex
	flushTicker   *time.Ticker
	stopChan      chan struct{}
}

// NewWALManager creates a new WAL manager
func NewWALManager(rootDir string) (*WALManager, error) {
	// WAL files are stored directly in rootDir (no separate wal subdirectory)
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	wm := &WALManager{
		rootDir:     rootDir,
		batch:       make([]*WALEntry, 0, WALBatchSize),
		stopChan:    make(chan struct{}),
		flushTicker: time.NewTicker(WALFlushInterval),
	}

	// Load checkpoint
	if err := wm.loadCheckpoint(); err != nil {
		return nil, err
	}

	// Open or create current WAL file
	if err := wm.openCurrentWAL(); err != nil {
		return nil, err
	}

	// Start background flusher
	go wm.backgroundFlusher()

	return wm, nil
}

// AppendEntry appends an entry to the WAL (batched)
func (wm *WALManager) AppendEntry(entry *WALEntry) error {
	wm.batchMu.Lock()
	defer wm.batchMu.Unlock()

	// Assign offset
	wm.mu.Lock()
	entry.Offset = wm.currentOffset
	wm.currentOffset++
	wm.mu.Unlock()

	entry.Timestamp = time.Now()

	// Add to batch
	wm.batch = append(wm.batch, entry)

	// Flush if batch is full
	if len(wm.batch) >= WALBatchSize {
		return wm.flushBatchLocked()
	}

	return nil
}

// AppendEntrySync appends an entry to the WAL and flushes immediately (sync)
// This ensures durability - when this returns, the entry is on disk
func (wm *WALManager) AppendEntrySync(entry *WALEntry) error {
	wm.batchMu.Lock()
	defer wm.batchMu.Unlock()

	// Assign offset
	wm.mu.Lock()
	entry.Offset = wm.currentOffset
	wm.currentOffset++
	wm.mu.Unlock()

	entry.Timestamp = time.Now()

	// Add to batch
	wm.batch = append(wm.batch, entry)

	// Flush immediately for sync write
	if err := wm.flushBatchLocked(); err != nil {
		return err
	}

	// Sync to disk for durability
	wm.mu.Lock()
	defer wm.mu.Unlock()
	if wm.currentFile != nil {
		if err := wm.currentFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync WAL to disk: %w", err)
		}
	}

	return nil
}

// Flush forces a flush of pending entries
func (wm *WALManager) Flush() error {
	wm.batchMu.Lock()
	defer wm.batchMu.Unlock()
	return wm.flushBatchLocked()
}

// flushBatchLocked flushes the current batch (caller must hold batchMu)
func (wm *WALManager) flushBatchLocked() error {
	if len(wm.batch) == 0 {
		return nil
	}

	wm.mu.Lock()
	defer wm.mu.Unlock()

	for _, entry := range wm.batch {
		if err := wm.writeEntryLocked(entry); err != nil {
			return err
		}
	}

	// Clear batch
	wm.batch = wm.batch[:0]

	// Flush to disk
	if err := wm.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL: %w", err)
	}

	// Check if rotation needed
	if wm.currentSize >= WALMaxSize {
		if err := wm.rotateLocked(); err != nil {
			return err
		}
	}

	return nil
}

// writeEntryLocked writes a single entry (caller must hold mu)
func (wm *WALManager) writeEntryLocked(entry *WALEntry) error {
	// Serialize entry
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal WAL entry: %w", err)
	}

	// Calculate checksum
	entry.Checksum = crc32.ChecksumIEEE(data)

	// Write: [length:4][checksum:4][data:N]
	length := uint32(len(data))
	if err := binary.Write(wm.writer, binary.LittleEndian, length); err != nil {
		return err
	}
	if err := binary.Write(wm.writer, binary.LittleEndian, entry.Checksum); err != nil {
		return err
	}
	if _, err := wm.writer.Write(data); err != nil {
		return err
	}

	wm.currentSize += int64(8 + len(data)) // 4+4+N
	return nil
}

// backgroundFlusher periodically flushes pending entries
func (wm *WALManager) backgroundFlusher() {
	for {
		select {
		case <-wm.flushTicker.C:
			wm.Flush()
		case <-wm.stopChan:
			return
		}
	}
}

// ReadFrom reads WAL entries starting from the given offset
func (wm *WALManager) ReadFrom(startOffset uint64) ([]*WALEntry, error) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	files, err := wm.getWALFilesLocked()
	if err != nil {
		return nil, err
	}

	var entries []*WALEntry

	for _, filename := range files {
		path := filepath.Join(wm.rootDir, filename)
		fileEntries, err := wm.readWALFile(path, startOffset)
		if err != nil {
			return nil, err
		}
		entries = append(entries, fileEntries...)
	}

	return entries, nil
}

// readWALFile reads entries from a specific WAL file
func (wm *WALManager) readWALFile(path string, startOffset uint64) ([]*WALEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []*WALEntry
	reader := bufio.NewReader(file)

	for {
		// Read length
		var length uint32
		if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Read checksum
		var checksum uint32
		if err := binary.Read(reader, binary.LittleEndian, &checksum); err != nil {
			return nil, err
		}

		// Read data
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, err
		}

		// Verify checksum
		if crc32.ChecksumIEEE(data) != checksum {
			return nil, fmt.Errorf("WAL entry checksum mismatch")
		}

		// Deserialize entry
		var entry WALEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			return nil, err
		}

		// Filter by offset
		if entry.Offset >= startOffset {
			entries = append(entries, &entry)
		}
	}

	return entries, nil
}

// Checkpoint marks the given offset as successfully synced
func (wm *WALManager) Checkpoint(offset uint64) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	wm.checkpoint = &WALCheckpoint{
		Offset:    offset,
		Timestamp: time.Now(),
	}

	return wm.saveCheckpointLocked()
}

// GetCheckpoint returns the current checkpoint
func (wm *WALManager) GetCheckpoint() *WALCheckpoint {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	if wm.checkpoint == nil {
		return &WALCheckpoint{Offset: 0}
	}
	return wm.checkpoint
}

// rotateLocked creates a new WAL file (caller must hold mu)
func (wm *WALManager) rotateLocked() error {
	// Close current file
	if wm.writer != nil {
		wm.writer.Flush()
	}
	if wm.currentFile != nil {
		wm.currentFile.Close()
	}

	// Create new WAL file
	if err := wm.openCurrentWAL(); err != nil {
		return err
	}

	// Cleanup old files
	return wm.cleanupOldWALsLocked()
}

// openCurrentWAL opens or creates the current WAL file
func (wm *WALManager) openCurrentWAL() error {
	timestamp := time.Now().Unix()
	filename := fmt.Sprintf("%s%d-%06d.log", WALFilePrefix, timestamp, wm.currentOffset)
	path := filepath.Join(wm.rootDir, filename)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}

	wm.currentFile = file
	wm.currentSize = stat.Size()
	wm.writer = bufio.NewWriter(file)

	return nil
}

// getWALFilesLocked returns sorted list of WAL files (caller must hold mu)
func (wm *WALManager) getWALFilesLocked() ([]string, error) {
	entries, err := os.ReadDir(wm.rootDir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), WALFilePrefix) {
			files = append(files, entry.Name())
		}
	}

	sort.Strings(files)
	return files, nil
}

// cleanupOldWALsLocked removes old WAL files beyond retention (caller must hold mu)
func (wm *WALManager) cleanupOldWALsLocked() error {
	files, err := wm.getWALFilesLocked()
	if err != nil {
		return err
	}

	if len(files) <= WALRetentionCount {
		return nil
	}

	// Remove oldest files
	toRemove := files[:len(files)-WALRetentionCount]
	for _, filename := range toRemove {
		path := filepath.Join(wm.rootDir, filename)
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("failed to remove old WAL file: %w", err)
		}
	}

	return nil
}

// loadCheckpoint loads the checkpoint from disk
func (wm *WALManager) loadCheckpoint() error {
	path := filepath.Join(wm.rootDir, WALCheckpointFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			wm.checkpoint = &WALCheckpoint{Offset: 0}
			return nil
		}
		return err
	}

	var cp WALCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return err
	}

	wm.checkpoint = &cp
	wm.currentOffset = cp.Offset + 1

	return nil
}

// saveCheckpointLocked saves the checkpoint to disk (caller must hold mu)
func (wm *WALManager) saveCheckpointLocked() error {
	path := filepath.Join(wm.rootDir, WALCheckpointFile)
	data, err := json.Marshal(wm.checkpoint)
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}

// Close closes the WAL manager
func (wm *WALManager) Close() error {
	close(wm.stopChan)
	wm.flushTicker.Stop()

	// Final flush
	if err := wm.Flush(); err != nil {
		return err
	}

	wm.mu.Lock()
	defer wm.mu.Unlock()

	if wm.writer != nil {
		wm.writer.Flush()
	}
	if wm.currentFile != nil {
		return wm.currentFile.Close()
	}

	return nil
}

// Replay replays WAL entries to restore database state
func (wm *WALManager) Replay(dm *DatabaseManager, storage *StorageManager) error {
	checkpoint := wm.GetCheckpoint()

	// Read entries after checkpoint
	entries, err := wm.ReadFrom(checkpoint.Offset)
	if err != nil {
		return fmt.Errorf("failed to read WAL for replay: %w", err)
	}

	if len(entries) == 0 {
		return nil // Nothing to replay
	}

	fmt.Printf("Replaying %d WAL entries from offset %d\n", len(entries), checkpoint.Offset)

	// Replay each entry
	for _, entry := range entries {
		if err := wm.replayEntry(entry, dm, storage); err != nil {
			return fmt.Errorf("failed to replay entry at offset %d: %w", entry.Offset, err)
		}
	}

	// Update checkpoint to latest offset
	if len(entries) > 0 {
		lastOffset := entries[len(entries)-1].Offset
		if err := wm.Checkpoint(lastOffset); err != nil {
			return fmt.Errorf("failed to checkpoint after replay: %w", err)
		}
	}

	fmt.Printf("WAL replay complete\n")
	return nil
}

// replayEntry replays a single WAL entry
func (wm *WALManager) replayEntry(entry *WALEntry, dm *DatabaseManager, storage *StorageManager) error {
	switch entry.Operation {
	case WALOpCreateDatabase:
		db := dm.CreateDatabase(entry.Database)
		return storage.SaveDatabase(db)

	case WALOpDeleteDatabase:
		dm.DeleteDatabase(entry.Database)
		return storage.DeleteDatabase(entry.Database)

	case WALOpCreateCollection:
		db := dm.GetDatabase(entry.Database)
		if db == nil {
			return fmt.Errorf("database %s not found during replay", entry.Database)
		}

		// Deserialize collection data
		var collData struct {
			Name   string  `json:"name"`
			Schema *Schema `json:"schema"`
		}
		if err := json.Unmarshal(entry.Data, &collData); err != nil {
			return err
		}

		if err := db.CreateCollection(collData.Name, collData.Schema); err != nil {
			return err
		}
		return storage.SaveDatabase(db)

	case WALOpInsert:
		db := dm.GetDatabase(entry.Database)
		if db == nil {
			return fmt.Errorf("database %s not found during replay", entry.Database)
		}

		coll, err := db.GetCollection(entry.Collection)
		if err != nil {
			return err
		}

		// Deserialize document
		var doc Document
		if err := json.Unmarshal(entry.Data, &doc); err != nil {
			return err
		}

		if err := coll.Insert(&doc); err != nil {
			return err
		}
		return storage.SaveCollection(entry.Database, coll)

	case WALOpUpdate:
		db := dm.GetDatabase(entry.Database)
		if db == nil {
			return fmt.Errorf("database %s not found during replay", entry.Database)
		}

		coll, err := db.GetCollection(entry.Collection)
		if err != nil {
			return err
		}

		// Deserialize updates
		var updates map[string]any
		if err := json.Unmarshal(entry.Data, &updates); err != nil {
			return err
		}

		if err := coll.Update(entry.DocumentID, updates); err != nil {
			return err
		}
		return storage.SaveCollection(entry.Database, coll)

	case WALOpDelete:
		db := dm.GetDatabase(entry.Database)
		if db == nil {
			return fmt.Errorf("database %s not found during replay", entry.Database)
		}

		coll, err := db.GetCollection(entry.Collection)
		if err != nil {
			return err
		}

		if err := coll.Delete(entry.DocumentID); err != nil {
			return err
		}
		return storage.SaveCollection(entry.Database, coll)

	case WALOpCreateIndex:
		db := dm.GetDatabase(entry.Database)
		if db == nil {
			return fmt.Errorf("database %s not found during replay", entry.Database)
		}

		coll, err := db.GetCollection(entry.Collection)
		if err != nil {
			return err
		}

		// Deserialize index data
		var indexData struct {
			IndexName string `json:"index_name"`
			FieldName string `json:"field_name"`
		}
		if err := json.Unmarshal(entry.Data, &indexData); err != nil {
			return err
		}

		if err := coll.CreateIndex(indexData.IndexName, indexData.FieldName); err != nil {
			return err
		}
		return storage.SaveCollection(entry.Database, coll)

	default:
		return fmt.Errorf("unknown WAL operation: %s", entry.Operation)
	}
}
