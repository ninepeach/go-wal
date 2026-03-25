package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestBinaryCodecEncodeDecodeFullFrame(t *testing.T) {
	codec := BinaryCodec{}
	payload := []byte("hello wal")

	var buf bytes.Buffer
	err := codec.Encode(&buf, FrameHeader{
		Type: FrameTypeFull,
	}, payload)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	header, got, err := codec.Decode(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if header.Magic != Magic {
		t.Fatalf("header.Magic = %x, want %x", header.Magic, Magic)
	}
	if header.Version != FormatVersion {
		t.Fatalf("header.Version = %d, want %d", header.Version, FormatVersion)
	}
	if header.Type != FrameTypeFull {
		t.Fatalf("header.Type = %v, want %v", header.Type, FrameTypeFull)
	}
	if header.Length != uint32(len(payload)) {
		t.Fatalf("header.Length = %d, want %d", header.Length, len(payload))
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
}

func TestBinaryCodecEncodeDecodeZeroPayload(t *testing.T) {
	codec := BinaryCodec{}

	var buf bytes.Buffer
	err := codec.Encode(&buf, FrameHeader{
		Type: FrameTypeFull,
	}, nil)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	header, got, err := codec.Decode(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if header.Type != FrameTypeFull {
		t.Fatalf("header.Type = %v, want %v", header.Type, FrameTypeFull)
	}
	if header.Length != 0 {
		t.Fatalf("header.Length = %d, want 0", header.Length)
	}
	if len(got) != 0 {
		t.Fatalf("payload length = %d, want 0", len(got))
	}
}

func TestBinaryCodecEncodeDecodeChunkFrameTypes(t *testing.T) {
	codec := BinaryCodec{}

	tests := []struct {
		name      string
		frameType FrameType
		payload   []byte
	}{
		{name: "first", frameType: FrameTypeFirst, payload: []byte("aaa")},
		{name: "middle", frameType: FrameTypeMiddle, payload: []byte("bbb")},
		{name: "last", frameType: FrameTypeLast, payload: []byte("ccc")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := codec.Encode(&buf, FrameHeader{
				Type: tc.frameType,
			}, tc.payload)
			if err != nil {
				t.Fatalf("Encode() error = %v", err)
			}

			header, got, err := codec.Decode(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}

			if header.Type != tc.frameType {
				t.Fatalf("header.Type = %v, want %v", header.Type, tc.frameType)
			}
			if !bytes.Equal(got, tc.payload) {
				t.Fatalf("payload = %q, want %q", got, tc.payload)
			}
		})
	}
}

func TestBinaryCodecDecodeRejectsInvalidMagic(t *testing.T) {
	frame := encodeTestFrame(t, FrameTypeFull, []byte("alpha"))
	binary.LittleEndian.PutUint32(frame[0:4], 0xDEADBEEF)

	_, _, err := BinaryCodec{}.Decode(bytes.NewReader(frame))
	if !errors.Is(err, ErrInvalidFrameMagic) {
		t.Fatalf("Decode() error = %v, want ErrInvalidFrameMagic", err)
	}
}

func TestBinaryCodecDecodeRejectsInvalidVersion(t *testing.T) {
	frame := encodeTestFrame(t, FrameTypeFull, []byte("alpha"))
	binary.LittleEndian.PutUint16(frame[4:6], FormatVersion+1)

	_, _, err := BinaryCodec{}.Decode(bytes.NewReader(frame))
	if !errors.Is(err, ErrInvalidFrameVersion) {
		t.Fatalf("Decode() error = %v, want ErrInvalidFrameVersion", err)
	}
}

func TestBinaryCodecDecodeRejectsUnsupportedFrameType(t *testing.T) {
	frame := encodeTestFrame(t, FrameTypeFull, []byte("alpha"))
	frame[6] = 0xFF

	_, _, err := BinaryCodec{}.Decode(bytes.NewReader(frame))
	if !errors.Is(err, ErrUnsupportedFrameType) {
		t.Fatalf("Decode() error = %v, want ErrUnsupportedFrameType", err)
	}
}

func TestBinaryCodecDecodeRejectsInvalidChecksum(t *testing.T) {
	frame := encodeTestFrame(t, FrameTypeFull, []byte("alpha"))
	binary.LittleEndian.PutUint32(frame[12:16], 0)

	_, _, err := BinaryCodec{}.Decode(bytes.NewReader(frame))
	if !errors.Is(err, ErrInvalidFrameChecksum) {
		t.Fatalf("Decode() error = %v, want ErrInvalidFrameChecksum", err)
	}
}

func TestBinaryCodecDecodeRejectsTruncatedHeader(t *testing.T) {
	frame := encodeTestFrame(t, FrameTypeFull, []byte("alpha"))
	truncated := frame[:HeaderSize-1]

	_, _, err := BinaryCodec{}.Decode(bytes.NewReader(truncated))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Decode() error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestBinaryCodecDecodeRejectsTruncatedPayload(t *testing.T) {
	frame := encodeTestFrame(t, FrameTypeFull, []byte("alpha"))
	truncated := frame[:len(frame)-1]

	_, _, err := BinaryCodec{}.Decode(bytes.NewReader(truncated))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Decode() error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func encodeTestFrame(t *testing.T, frameType FrameType, payload []byte) []byte {
	t.Helper()

	var buf bytes.Buffer
	err := BinaryCodec{}.Encode(&buf, FrameHeader{
		Type: frameType,
	}, payload)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	return buf.Bytes()
}
