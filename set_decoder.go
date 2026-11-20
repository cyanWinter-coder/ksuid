package ksuid

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// decoderBufferSize is the fixed size of the staging buffer used by
// CompressedSetDecoder. The decoder never allocates memory proportionally to
// the length of the stream it reads: this buffer and a small scratch array
// are the only storage it needs.
const decoderBufferSize = 512

// Errors returned by CompressedSetDecoder when the stream it reads from does
// not contain a valid compressed KSUID set. Use errors.Is to test for them.
var (
	// ErrCompressedSetTruncated is returned when the stream ends in the
	// middle of an encoded entry.
	ErrCompressedSetTruncated = errors.New("ksuid: compressed set is truncated")

	// ErrCompressedSetOverflow is returned when decoding an entry would
	// overflow the uint128 payload space (or the uint32 timestamp space),
	// which the compression format can never produce.
	ErrCompressedSetOverflow = errors.New("ksuid: compressed set value overflows 128 bits")

	// ErrCompressedSetEncoding is returned when the stream contains bytes
	// that the compressor could never have emitted: unknown length
	// prefixes, non-canonical varints, zero deltas, or out-of-range
	// sequence lengths.
	ErrCompressedSetEncoding = errors.New("ksuid: invalid compressed set encoding")
)

// CompressedSetDecoder decodes a CompressedSet incrementally from an
// io.Reader, validating every entry as it goes. Unlike CompressedSet.Iter,
// it does not require the whole set to be held in memory, never panics on
// malformed input, and never allocates based on lengths found in the stream.
//
// Here's how the decoder type is commonly used:
//
//	dec := ksuid.NewCompressedSetDecoder(ctx, r)
//	for dec.Next() {
//		id := dec.KSUID
//		// ...
//	}
//	if err := dec.Err(); err != nil {
//		// the stream was truncated, corrupted, or the reader failed
//	}
//
// When the stream ends cleanly after the last entry, Err returns nil. When
// it is truncated or contains trailing garbage, Err returns a non-nil error
// and Count reports how many fully-validated KSUIDs were delivered before
// the failure; entries that fail validation are never delivered.
//
// After an error (including context cancellation), the decoder can be reused
// for a new stream by calling Reset.
//
// CompressedSetDecoder values are not safe to use concurrently from multiple
// goroutines.
type CompressedSetDecoder struct {
	// KSUID is modified by calls to the Next method to hold the KSUID
	// loaded by the decoder.
	KSUID KSUID

	ctx context.Context
	r   *bufio.Reader

	offset uint64 // number of bytes consumed from the stream
	count  uint64 // number of KSUIDs delivered
	err    error
	done   bool

	scratch [byteLength]byte

	seqlength uint64
	timestamp uint32
	lastValue uint128
}

// NewCompressedSetDecoder returns a decoder reading a compressed KSUID set
// from r. The context is checked between reads; when it is canceled, Next
// returns false and Err returns the context error. A nil context is
// equivalent to context.Background().
func NewCompressedSetDecoder(ctx context.Context, r io.Reader) *CompressedSetDecoder {
	d := &CompressedSetDecoder{}
	d.Reset(ctx, r)
	return d
}

// Reset discards the decoder state and makes it read a new compressed set
// from r. Bytes buffered from a previous stream are dropped, so no state
// leaks from one input to the next.
func (d *CompressedSetDecoder) Reset(ctx context.Context, r io.Reader) {
	if ctx == nil {
		ctx = context.Background()
	}
	*d = CompressedSetDecoder{
		ctx: ctx,
		r:   bufio.NewReaderSize(r, decoderBufferSize),
	}
}

// Count returns the number of KSUIDs delivered so far.
func (d *CompressedSetDecoder) Count() uint64 {
	return d.count
}

// Offset returns the number of bytes consumed from the stream so far.
func (d *CompressedSetDecoder) Offset() uint64 {
	return d.offset
}

// Err returns the error that stopped the decoder, or nil if the stream ended
// cleanly after the last entry (or the decoder has not stopped yet).
func (d *CompressedSetDecoder) Err() error {
	return d.err
}

// Next advances the decoder to the next KSUID in the set, returning true if
// one was decoded, or false when the stream is exhausted or an error
// occurred. After Next returns false, Err reports why.
func (d *CompressedSetDecoder) Next() bool {
	if d.err != nil || d.done {
		return false
	}

	if err := d.ctx.Err(); err != nil {
		d.err = err
		return false
	}

	if d.seqlength != 0 {
		value := incr128(d.lastValue)
		if cmp128(value, d.lastValue) <= 0 {
			return d.fail(d.errorf(ErrCompressedSetOverflow, "sequence increment"))
		}
		d.seqlength--
		d.lastValue = value
		d.KSUID = value.ksuid(d.timestamp)
		d.count++
		return true
	}

	b, err := d.r.ReadByte()
	if err != nil {
		if err == io.EOF {
			d.done = true
		} else {
			d.err = fmt.Errorf("ksuid: reading compressed set at offset %d: %w", d.offset, err)
		}
		return false
	}
	d.offset++

	const mask = rawKSUID | timeDelta | payloadDelta | payloadRange
	tag := int(b) & mask
	cnt := int(b) &^ mask

	switch tag {
	case rawKSUID:
		if cnt != 0 {
			return d.fail(d.errorf(ErrCompressedSetEncoding, "raw KSUID entry with length prefix %d", cnt))
		}
		if err := d.readFrame(byteLength); err != nil {
			return d.fail(err)
		}
		copy(d.KSUID[:], d.scratch[:byteLength])
		d.timestamp = d.KSUID.Timestamp()
		d.lastValue = uint128Payload(d.KSUID)

	case timeDelta:
		if cnt < 1 || cnt > 4 {
			return d.fail(d.errorf(ErrCompressedSetEncoding, "time delta of %d bytes", cnt))
		}
		if err := d.readVarint(cnt); err != nil {
			return d.fail(err)
		}
		delta := varint32(d.scratch[:cnt])
		if delta == 0 {
			return d.fail(d.errorf(ErrCompressedSetEncoding, "zero time delta"))
		}
		timestamp := d.timestamp + delta
		if timestamp < d.timestamp {
			return d.fail(d.errorf(ErrCompressedSetOverflow, "time delta"))
		}
		if err := d.readFrame(payloadLengthInBytes); err != nil {
			return d.fail(err)
		}
		d.timestamp = timestamp
		binary.BigEndian.PutUint32(d.KSUID[:timestampLengthInBytes], timestamp)
		copy(d.KSUID[timestampLengthInBytes:], d.scratch[:payloadLengthInBytes])
		d.lastValue = uint128Payload(d.KSUID)

	case payloadDelta:
		if cnt < 1 || cnt > 16 {
			return d.fail(d.errorf(ErrCompressedSetEncoding, "payload delta of %d bytes", cnt))
		}
		if err := d.readVarint(cnt); err != nil {
			return d.fail(err)
		}
		delta := varint128(d.scratch[:cnt])
		if delta == (uint128{}) {
			return d.fail(d.errorf(ErrCompressedSetEncoding, "zero payload delta"))
		}
		value := add128(d.lastValue, delta)
		if cmp128(value, d.lastValue) < 0 {
			return d.fail(d.errorf(ErrCompressedSetOverflow, "payload delta"))
		}
		d.KSUID = value.ksuid(d.timestamp)
		d.lastValue = value

	case payloadRange:
		if cnt < 1 || cnt > 8 {
			return d.fail(d.errorf(ErrCompressedSetEncoding, "payload range of %d bytes", cnt))
		}
		if err := d.readVarint(cnt); err != nil {
			return d.fail(err)
		}
		length := varint64(d.scratch[:cnt])
		if length == 0 {
			return d.fail(d.errorf(ErrCompressedSetEncoding, "zero payload range"))
		}
		value := incr128(d.lastValue)
		if cmp128(value, d.lastValue) <= 0 {
			return d.fail(d.errorf(ErrCompressedSetOverflow, "payload range"))
		}
		d.seqlength = length - 1
		d.lastValue = value
		d.KSUID = value.ksuid(d.timestamp)
	}

	d.count++
	return true
}

func (d *CompressedSetDecoder) fail(err error) bool {
	d.err = err
	return false
}

func (d *CompressedSetDecoder) errorf(err error, format string, args ...interface{}) error {
	return fmt.Errorf("%w: %s at offset %d", err, fmt.Sprintf(format, args...), d.offset)
}

// readFrame reads exactly n bytes into the scratch buffer. n must not exceed
// byteLength, so the buffer usage is bounded by a constant regardless of the
// stream content.
func (d *CompressedSetDecoder) readFrame(n int) error {
	if _, err := io.ReadFull(d.r, d.scratch[:n]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return d.errorf(ErrCompressedSetTruncated, "entry of %d bytes", n)
		}
		return fmt.Errorf("ksuid: reading compressed set at offset %d: %w", d.offset, err)
	}
	d.offset += uint64(n)
	return nil
}

// readVarint reads a varint of n bytes and rejects non-canonical encodings
// (leading zero bytes), which the compressor never emits.
func (d *CompressedSetDecoder) readVarint(n int) error {
	if err := d.readFrame(n); err != nil {
		return err
	}
	if d.scratch[0] == 0 {
		return d.errorf(ErrCompressedSetEncoding, "non-canonical varint of %d bytes", n)
	}
	return nil
}
