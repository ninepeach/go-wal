package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// FrameHeader contains metadata for a physical frame.
type FrameHeader struct {
	Magic    uint32
	Version  uint16
	Type     FrameType
	Length   uint32
	Checksum uint32
}

// Codec encodes and decodes physical frames.
type Codec interface {
	Encode(w io.Writer, header FrameHeader, payload []byte) error
	Decode(r io.Reader) (FrameHeader, []byte, error)
}

// BinaryCodec encodes the v1 full-frame WAL format.
type BinaryCodec struct{}

// Encode writes one physical frame.
func (BinaryCodec) Encode(w io.Writer, header FrameHeader, payload []byte) error {
	if header.Magic == 0 {
		header.Magic = Magic
	}
	if header.Version == 0 {
		header.Version = FormatVersion
	}
	if header.Type == 0 {
		header.Type = FrameTypeFull
	}
	header.Length = uint32(len(payload))
	header.Checksum = checksumFrame(header.Type, payload)

	var buf [HeaderSize]byte
	binary.LittleEndian.PutUint32(buf[0:4], header.Magic)
	binary.LittleEndian.PutUint16(buf[4:6], header.Version)
	buf[6] = byte(header.Type)
	buf[7] = 0
	binary.LittleEndian.PutUint32(buf[8:12], header.Length)
	binary.LittleEndian.PutUint32(buf[12:16], header.Checksum)

	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := w.Write(payload)
	return err
}

// Decode reads one physical frame.
func (BinaryCodec) Decode(r io.Reader) (FrameHeader, []byte, error) {
	var buf [HeaderSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return FrameHeader{}, nil, err
	}

	header := FrameHeader{
		Magic:    binary.LittleEndian.Uint32(buf[0:4]),
		Version:  binary.LittleEndian.Uint16(buf[4:6]),
		Type:     FrameType(buf[6]),
		Length:   binary.LittleEndian.Uint32(buf[8:12]),
		Checksum: binary.LittleEndian.Uint32(buf[12:16]),
	}

	if header.Magic != Magic {
		return FrameHeader{}, nil, errors.New("wal: invalid frame magic")
	}
	if header.Version != FormatVersion {
		return FrameHeader{}, nil, errors.New("wal: invalid frame version")
	}
	if header.Type != FrameTypeFull {
		return FrameHeader{}, nil, errors.New("wal: unsupported frame type")
	}

	payload := make([]byte, header.Length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return FrameHeader{}, nil, err
	}
	if checksumFrame(header.Type, payload) != header.Checksum {
		return FrameHeader{}, nil, errors.New("wal: invalid frame checksum")
	}

	return header, payload, nil
}

func checksumFrame(frameType FrameType, payload []byte) uint32 {
	table := crc32.MakeTable(crc32.Castagnoli)
	sum := crc32.Update(0, table, []byte{byte(frameType)})
	return crc32.Update(sum, table, payload)
}
