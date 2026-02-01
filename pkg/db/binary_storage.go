package db

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	// Magic number for collection data files
	CollectionMagic = 0x43414348 // "CACH" in hex

	// Version for binary format
	BinaryFormatVersion = 1

	// Header size: magic(4) + version(2) + flags(2) = 8 bytes
	HeaderSize = 8

	// Document entry header: offset(8) + size(4) + compressed_size(4) + checksum(4) = 20 bytes
	DocEntryHeaderSize = 20
)

// BinaryHeader represents the file header for binary storage
type BinaryHeader struct {
	Magic   uint32 // Magic number to identify file type
	Version uint16 // Format version
	Flags   uint16 // Flags (bit 0: compressed)
}

// DocumentEntry represents a single document entry in the binary file
type DocumentEntry struct {
	Offset         int64  // Offset in the data file
	Size           uint32 // Original size
	CompressedSize uint32 // Size after compression (0 if not compressed)
	Checksum       uint32 // CRC32 checksum
}

// OffsetIndex maps document IDs to their locations in the binary file
type OffsetIndex struct {
	Entries map[string]*DocumentEntry `json:"entries"`
}

// BinaryCollectionWriter handles writing documents to binary storage
type BinaryCollectionWriter struct {
	dataFile  *os.File
	indexFile *os.File
	offset    int64
	index     *OffsetIndex
}

// NewBinaryCollectionWriter creates a new binary collection writer
func NewBinaryCollectionWriter(dataDir, dbName, collName string) (*BinaryCollectionWriter, error) {
	collDir := filepath.Join(dataDir, dbName, collName)
	if err := os.MkdirAll(collDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create collection directory: %w", err)
	}

	dataPath := filepath.Join(collDir, "collection.data")

	// Open or create data file
	dataFile, err := os.OpenFile(dataPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	// Check if we need to write header
	stat, err := dataFile.Stat()
	if err != nil {
		dataFile.Close()
		return nil, fmt.Errorf("failed to stat data file: %w", err)
	}

	writer := &BinaryCollectionWriter{
		dataFile: dataFile,
		offset:   stat.Size(),
		index: &OffsetIndex{
			Entries: make(map[string]*DocumentEntry),
		},
	}

	// Write header if file is new
	if stat.Size() == 0 {
		if err := writer.writeHeader(); err != nil {
			dataFile.Close()
			return nil, fmt.Errorf("failed to write header: %w", err)
		}
	}

	// Try to load existing index
	existingIndex, err := LoadOffsetIndex(dataDir, dbName, collName)
	if err == nil {
		writer.index = existingIndex
	}

	return writer, nil
}

// writeHeader writes the file header
func (w *BinaryCollectionWriter) writeHeader() error {
	header := BinaryHeader{
		Magic:   CollectionMagic,
		Version: BinaryFormatVersion,
		Flags:   1, // Bit 0 set = compressed
	}

	buf := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint32(buf[0:4], header.Magic)
	binary.LittleEndian.PutUint16(buf[4:6], header.Version)
	binary.LittleEndian.PutUint16(buf[6:8], header.Flags)

	n, err := w.dataFile.Write(buf)
	if err != nil {
		return err
	}

	w.offset = int64(n)
	return nil
}

// WriteDocument writes a document to the binary file
func (w *BinaryCollectionWriter) WriteDocument(doc *Document) error {
	// Serialize document to JSON
	jsonData, err := doc.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal document: %w", err)
	}

	// Compress the data
	compressedData, err := Compress(jsonData)
	if err != nil {
		return fmt.Errorf("failed to compress document: %w", err)
	}

	// Calculate checksum
	checksum := crc32.ChecksumIEEE(compressedData)

	// Create entry header
	entryBuf := make([]byte, DocEntryHeaderSize)
	binary.LittleEndian.PutUint64(entryBuf[0:8], uint64(w.offset))
	binary.LittleEndian.PutUint32(entryBuf[8:12], uint32(len(jsonData)))
	binary.LittleEndian.PutUint32(entryBuf[12:16], uint32(len(compressedData)))
	binary.LittleEndian.PutUint32(entryBuf[16:20], checksum)

	// Write entry header + compressed data
	if _, err := w.dataFile.Write(entryBuf); err != nil {
		return fmt.Errorf("failed to write entry header: %w", err)
	}

	if _, err := w.dataFile.Write(compressedData); err != nil {
		return fmt.Errorf("failed to write compressed data: %w", err)
	}

	// Update index
	w.index.Entries[doc.ID] = &DocumentEntry{
		Offset:         w.offset,
		Size:           uint32(len(jsonData)),
		CompressedSize: uint32(len(compressedData)),
		Checksum:       checksum,
	}

	// Update offset for next write
	w.offset += int64(DocEntryHeaderSize + len(compressedData))

	return nil
}

// Flush syncs the data file and saves the index
func (w *BinaryCollectionWriter) Flush(dataDir, dbName, collName string) error {
	if err := w.dataFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync data file: %w", err)
	}

	if err := SaveOffsetIndex(w.index, dataDir, dbName, collName); err != nil {
		return fmt.Errorf("failed to save index: %w", err)
	}

	return nil
}

// Close closes the writer and saves the index
func (w *BinaryCollectionWriter) Close(dataDir, dbName, collName string) error {
	if err := w.Flush(dataDir, dbName, collName); err != nil {
		return err
	}

	return w.dataFile.Close()
}

// BinaryCollectionReader handles reading documents from binary storage
type BinaryCollectionReader struct {
	dataFile *os.File
	index    *OffsetIndex
}

// NewBinaryCollectionReader creates a new binary collection reader
func NewBinaryCollectionReader(dataDir, dbName, collName string) (*BinaryCollectionReader, error) {
	dataPath := filepath.Join(dataDir, dbName, collName, "collection.data")

	// Check if data file exists
	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("collection data file does not exist")
	}

	dataFile, err := os.Open(dataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	// Verify header
	header, err := readHeader(dataFile)
	if err != nil {
		dataFile.Close()
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	if header.Magic != CollectionMagic {
		dataFile.Close()
		return nil, fmt.Errorf("invalid magic number: expected 0x%X, got 0x%X", CollectionMagic, header.Magic)
	}

	// Load index
	index, err := LoadOffsetIndex(dataDir, dbName, collName)
	if err != nil {
		dataFile.Close()
		return nil, fmt.Errorf("failed to load index: %w", err)
	}

	return &BinaryCollectionReader{
		dataFile: dataFile,
		index:    index,
	}, nil
}

// readHeader reads and validates the file header
func readHeader(f *os.File) (*BinaryHeader, error) {
	buf := make([]byte, HeaderSize)
	if _, err := f.ReadAt(buf, 0); err != nil {
		return nil, err
	}

	header := &BinaryHeader{
		Magic:   binary.LittleEndian.Uint32(buf[0:4]),
		Version: binary.LittleEndian.Uint16(buf[4:6]),
		Flags:   binary.LittleEndian.Uint16(buf[6:8]),
	}

	return header, nil
}

// ReadDocument reads a document by ID from the binary file
func (r *BinaryCollectionReader) ReadDocument(docID string) (*Document, error) {
	entry, exists := r.index.Entries[docID]
	if !exists {
		return nil, fmt.Errorf("document not found: %s", docID)
	}

	// Read entry header + data
	buf := make([]byte, DocEntryHeaderSize+entry.CompressedSize)
	if _, err := r.dataFile.ReadAt(buf, entry.Offset); err != nil {
		return nil, fmt.Errorf("failed to read document data: %w", err)
	}

	// Verify checksum
	compressedData := buf[DocEntryHeaderSize:]
	checksum := crc32.ChecksumIEEE(compressedData)
	if checksum != entry.Checksum {
		return nil, fmt.Errorf("checksum mismatch for document %s", docID)
	}

	// Decompress
	jsonData, err := Decompress(compressedData)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress document: %w", err)
	}

	// Unmarshal document
	var doc Document
	if err := doc.UnmarshalJSON(jsonData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal document: %w", err)
	}

	return &doc, nil
}

// ReadAllDocuments reads all documents from the binary file
func (r *BinaryCollectionReader) ReadAllDocuments() ([]*Document, error) {
	documents := make([]*Document, 0, len(r.index.Entries))

	for docID := range r.index.Entries {
		doc, err := r.ReadDocument(docID)
		if err != nil {
			return nil, fmt.Errorf("failed to read document %s: %w", docID, err)
		}
		documents = append(documents, doc)
	}

	return documents, nil
}

// Close closes the reader
func (r *BinaryCollectionReader) Close() error {
	return r.dataFile.Close()
}

// SaveOffsetIndex saves the offset index to disk
func SaveOffsetIndex(index *OffsetIndex, dataDir, dbName, collName string) error {
	indexPath := filepath.Join(dataDir, dbName, collName, "collection.idx")

	f, err := os.Create(indexPath)
	if err != nil {
		return fmt.Errorf("failed to create index file: %w", err)
	}
	defer f.Close()

	// Write number of entries
	numEntries := uint32(len(index.Entries))
	if err := binary.Write(f, binary.LittleEndian, numEntries); err != nil {
		return fmt.Errorf("failed to write entry count: %w", err)
	}

	// Write each entry
	for docID, entry := range index.Entries {
		// Write document ID length + ID
		idLen := uint32(len(docID))
		if err := binary.Write(f, binary.LittleEndian, idLen); err != nil {
			return err
		}
		if _, err := f.Write([]byte(docID)); err != nil {
			return err
		}

		// Write entry data
		if err := binary.Write(f, binary.LittleEndian, entry.Offset); err != nil {
			return err
		}
		if err := binary.Write(f, binary.LittleEndian, entry.Size); err != nil {
			return err
		}
		if err := binary.Write(f, binary.LittleEndian, entry.CompressedSize); err != nil {
			return err
		}
		if err := binary.Write(f, binary.LittleEndian, entry.Checksum); err != nil {
			return err
		}
	}

	return nil
}

// LoadOffsetIndex loads the offset index from disk
func LoadOffsetIndex(dataDir, dbName, collName string) (*OffsetIndex, error) {
	indexPath := filepath.Join(dataDir, dbName, collName, "collection.idx")

	f, err := os.Open(indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &OffsetIndex{Entries: make(map[string]*DocumentEntry)}, nil
		}
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}
	defer f.Close()

	// Read number of entries
	var numEntries uint32
	if err := binary.Read(f, binary.LittleEndian, &numEntries); err != nil {
		if err == io.EOF {
			return &OffsetIndex{Entries: make(map[string]*DocumentEntry)}, nil
		}
		return nil, fmt.Errorf("failed to read entry count: %w", err)
	}

	index := &OffsetIndex{
		Entries: make(map[string]*DocumentEntry, numEntries),
	}

	// Read each entry
	for i := uint32(0); i < numEntries; i++ {
		// Read document ID
		var idLen uint32
		if err := binary.Read(f, binary.LittleEndian, &idLen); err != nil {
			return nil, err
		}

		idBuf := make([]byte, idLen)
		if _, err := io.ReadFull(f, idBuf); err != nil {
			return nil, err
		}
		docID := string(idBuf)

		// Read entry data
		entry := &DocumentEntry{}
		if err := binary.Read(f, binary.LittleEndian, &entry.Offset); err != nil {
			return nil, err
		}
		if err := binary.Read(f, binary.LittleEndian, &entry.Size); err != nil {
			return nil, err
		}
		if err := binary.Read(f, binary.LittleEndian, &entry.CompressedSize); err != nil {
			return nil, err
		}
		if err := binary.Read(f, binary.LittleEndian, &entry.Checksum); err != nil {
			return nil, err
		}

		index.Entries[docID] = entry
	}

	return index, nil
}
