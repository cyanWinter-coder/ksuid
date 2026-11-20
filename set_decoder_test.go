package ksuid

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"
)

func TestCompressedSetDecoder(t *testing.T) {
	tests := []struct {
		scenario string
		function func(*testing.T)
	}{
		{
			scenario: "decoding a compressed set produces the same ids as the iterator",
			function: testCompressedSetDecoderMatchesIter,
		},
		{
			scenario: "decoding sequences produces the full ordered range",
			function: testCompressedSetDecoderSequence,
		},
		{
			scenario: "decoding an empty set returns no ids and no error",
			function: testCompressedSetDecoderEmpty,
		},
		{
			scenario: "decoding from a reader that returns one byte at a time",
			function: testCompressedSetDecoderSlowReader,
		},
		{
			scenario: "truncated streams are reported with the count of decoded ids",
			function: testCompressedSetDecoderTruncated,
		},
		{
			scenario: "trailing garbage is rejected",
			function: testCompressedSetDecoderTrailingGarbage,
		},
		{
			scenario: "forged length prefixes are rejected without large allocations",
			function: testCompressedSetDecoderForgedLength,
		},
		{
			scenario: "delta overflows are detected without panicking",
			function: testCompressedSetDecoderOverflow,
		},
		{
			scenario: "non-canonical encodings are rejected",
			function: testCompressedSetDecoderNonCanonical,
		},
		{
			scenario: "context cancellation stops the decoder and Reset allows reuse",
			function: testCompressedSetDecoderContextCancel,
		},
		{
			scenario: "reader errors are reported and Reset allows reuse",
			function: testCompressedSetDecoderReaderError,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, test.function)
	}
}

func decodeAll(t *testing.T, dec *CompressedSetDecoder) []KSUID {
	t.Helper()
	ids := []KSUID{}
	for dec.Next() {
		ids = append(ids, dec.KSUID)
	}
	return ids
}

func iterAll(set CompressedSet) []KSUID {
	ids := []KSUID{}
	for it := set.Iter(); it.Next(); {
		ids = append(ids, it.KSUID)
	}
	return ids
}

func equalKSUIDs(a, b []KSUID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func testCompressedSetDecoderMatchesIter(t *testing.T) {
	now := time.Now()

	times := [20]time.Time{}
	for i := range times {
		times[i] = now.Add(time.Duration(i) * 2 * time.Second)
	}

	sequences := [20]Sequence{}
	for i := range sequences {
		seed, _ := NewRandomWithTime(times[i%len(times)])
		sequences[i] = Sequence{Seed: seed}
	}

	ksuids := [1000]KSUID{}
	for i := range ksuids {
		ksuids[i], _ = sequences[i%len(sequences)].Next()
	}
	for i := 1; i < len(ksuids); i += 7 {
		ksuids[i] = ksuids[i-1] // generate dupes
	}

	set := Compress(ksuids[:]...)
	want := iterAll(set)

	dec := NewCompressedSetDecoder(context.Background(), bytes.NewReader(set))
	got := decodeAll(t, dec)

	if err := dec.Err(); err != nil {
		t.Fatalf("unexpected decoder error: %v", err)
	}
	if !equalKSUIDs(got, want) {
		t.Fatalf("decoder and iterator produced different ids")
	}
	if n := dec.Count(); n != uint64(len(want)) {
		t.Errorf("bad count: expected %d, got %d", len(want), n)
	}
	if n := dec.Offset(); n != uint64(len(set)) {
		t.Errorf("bad offset: expected %d, got %d", len(set), n)
	}
}

func testCompressedSetDecoderSequence(t *testing.T) {
	seq := Sequence{Seed: New()}

	ids := make([]KSUID, 100)
	for i := range ids {
		ids[i], _ = seq.Next()
	}

	set := Compress(ids...)
	dec := NewCompressedSetDecoder(nil, bytes.NewReader(set))
	got := decodeAll(t, dec)

	if err := dec.Err(); err != nil {
		t.Fatalf("unexpected decoder error: %v", err)
	}
	if !equalKSUIDs(got, ids) {
		t.Fatalf("decoder did not produce the original sequence in order")
	}
}

func testCompressedSetDecoderEmpty(t *testing.T) {
	dec := NewCompressedSetDecoder(nil, bytes.NewReader(Compress()))
	if dec.Next() {
		t.Error("decoder produced an id from an empty set")
	}
	if err := dec.Err(); err != nil {
		t.Errorf("unexpected error on empty set: %v", err)
	}
	if n := dec.Count(); n != 0 {
		t.Errorf("bad count on empty set: %d", n)
	}
}

// trickleReader returns at most one byte per Read call.
type trickleReader struct {
	r io.Reader
}

func (r trickleReader) Read(b []byte) (int, error) {
	if len(b) > 1 {
		b = b[:1]
	}
	return r.r.Read(b)
}

func testCompressedSetDecoderSlowReader(t *testing.T) {
	ksuids := [100]KSUID{}
	for i := range ksuids {
		ksuids[i] = New()
	}

	set := Compress(ksuids[:]...)
	want := iterAll(set)

	dec := NewCompressedSetDecoder(nil, trickleReader{bytes.NewReader(set)})
	got := decodeAll(t, dec)

	if err := dec.Err(); err != nil {
		t.Fatalf("unexpected decoder error: %v", err)
	}
	if !equalKSUIDs(got, want) {
		t.Fatalf("decoder produced different ids with a slow reader")
	}
}

func testCompressedSetDecoderTruncated(t *testing.T) {
	ksuids := [50]KSUID{}
	for i := range ksuids {
		ksuids[i] = New()
	}

	set := Compress(ksuids[:]...)
	full := uint64(len(iterAll(set)))

	for i := 0; i < len(set); i++ {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("decoding %d/%d bytes panicked: %v", i, len(set), r)
				}
			}()

			dec := NewCompressedSetDecoder(nil, bytes.NewReader(set[:i]))
			decodeAll(t, dec)

			err := dec.Err()
			if err == nil {
				return // truncation landed on an entry boundary
			}
			if !errors.Is(err, ErrCompressedSetTruncated) {
				t.Fatalf("decoding %d/%d bytes: expected ErrCompressedSetTruncated, got %v", i, len(set), err)
			}
			if n := dec.Count(); n >= full {
				t.Fatalf("decoding %d/%d bytes delivered %d ids, expected fewer than %d", i, len(set), n, full)
			}
		}()
	}
}

func testCompressedSetDecoderTrailingGarbage(t *testing.T) {
	id := New()
	set := Compress(id)

	for _, garbage := range [][]byte{
		{0xFF},                         // payload range with a 63-byte length prefix
		{0x3F},                         // raw KSUID entry with a length prefix
		{0x00, 0x01, 0x02},             // raw KSUID entry cut short
		{0x81},                         // payload delta cut short before its varint
		{0xC2, 0x00, 0x05},             // payload range with a non-canonical varint
		{0x82, 0x00, 0x2A},             // payload delta with a non-canonical varint
		{0x91, 0x01, 0x02, 0x03, 0x04}, // payload delta with a 17-byte length prefix
	} {
		stream := append(append([]byte{}, set...), garbage...)

		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("garbage %x panicked: %v", garbage, r)
				}
			}()

			dec := NewCompressedSetDecoder(nil, bytes.NewReader(stream))
			got := decodeAll(t, dec)

			if dec.Err() == nil {
				t.Fatalf("trailing garbage %x was silently accepted", garbage)
			}
			if len(got) != 1 || got[0] != id {
				t.Fatalf("garbage %x: expected only the original id, got %v", garbage, got)
			}
			if n := dec.Count(); n != 1 {
				t.Fatalf("garbage %x: bad count %d", garbage, n)
			}
		}()
	}
}

func testCompressedSetDecoderForgedLength(t *testing.T) {
	id := New()
	head := Compress(id) // valid raw KSUID entry

	forged := []byte{
		0xBF, // payload delta with a 63-byte length prefix (max is 16)
		0xFF, 0xFF, 0xFF, 0xFF,
	}
	stream := append(append([]byte{}, head...), forged...)

	dec := NewCompressedSetDecoder(nil, bytes.NewReader(stream))
	got := decodeAll(t, dec)

	if !errors.Is(dec.Err(), ErrCompressedSetEncoding) {
		t.Fatalf("expected ErrCompressedSetEncoding, got %v", dec.Err())
	}
	if len(got) != 1 || got[0] != id {
		t.Fatalf("forged id was delivered: %v", got)
	}
}

func encodeRawKSUID(id KSUID) []byte {
	return append([]byte{byte(rawKSUID)}, id[:]...)
}

func makeRawKSUID(timestamp uint32, payload []byte) (id KSUID) {
	binary.BigEndian.PutUint32(id[:timestampLengthInBytes], timestamp)
	copy(id[timestampLengthInBytes:], payload)
	return
}

func testCompressedSetDecoderOverflow(t *testing.T) {
	maxPayload := makeRawKSUID(0xFFFFFFFF, bytes.Repeat([]byte{0xFF}, payloadLengthInBytes))

	t.Run("payload delta overflowing 128 bits", func(t *testing.T) {
		stream := encodeRawKSUID(maxPayload)
		stream = append(stream, byte(payloadDelta)|1, 0x01)

		dec := NewCompressedSetDecoder(nil, bytes.NewReader(stream))
		got := decodeAll(t, dec)

		if !errors.Is(dec.Err(), ErrCompressedSetOverflow) {
			t.Fatalf("expected ErrCompressedSetOverflow, got %v", dec.Err())
		}
		if len(got) != 1 || got[0] != maxPayload {
			t.Fatalf("unexpected ids delivered: %v", got)
		}
	})

	t.Run("time delta overflowing 32 bits", func(t *testing.T) {
		stream := encodeRawKSUID(maxPayload)
		stream = append(stream, byte(timeDelta)|1, 0x01)
		stream = append(stream, bytes.Repeat([]byte{0x2A}, payloadLengthInBytes)...)

		dec := NewCompressedSetDecoder(nil, bytes.NewReader(stream))
		decodeAll(t, dec)

		if !errors.Is(dec.Err(), ErrCompressedSetOverflow) {
			t.Fatalf("expected ErrCompressedSetOverflow, got %v", dec.Err())
		}
	})

	t.Run("sequence increment overflowing 128 bits", func(t *testing.T) {
		// Start one below the maximum payload so the first range element is
		// valid and the second one wraps around.
		payload := bytes.Repeat([]byte{0xFF}, payloadLengthInBytes)
		payload[payloadLengthInBytes-1] = 0xFE
		seed := makeRawKSUID(42, payload)

		stream := encodeRawKSUID(seed)
		stream = append(stream, byte(payloadRange)|1, 0x03) // range of 3, only 1 fits

		dec := NewCompressedSetDecoder(nil, bytes.NewReader(stream))
		got := decodeAll(t, dec)

		if !errors.Is(dec.Err(), ErrCompressedSetOverflow) {
			t.Fatalf("expected ErrCompressedSetOverflow, got %v", dec.Err())
		}
		if len(got) != 2 {
			t.Fatalf("expected 2 ids before the overflow, got %d", len(got))
		}
	})
}

func testCompressedSetDecoderNonCanonical(t *testing.T) {
	id := New()
	head := encodeRawKSUID(id)

	streams := map[string][]byte{
		"zero time delta":       append(append([]byte{}, head...), byte(timeDelta)|1, 0x00),
		"zero payload delta":    append(append([]byte{}, head...), byte(payloadDelta)|1, 0x00),
		"zero payload range":    append(append([]byte{}, head...), byte(payloadRange)|1, 0x00),
		"padded payload delta":  append(append([]byte{}, head...), byte(payloadDelta)|2, 0x00, 0x05),
		"oversized time delta":  append(append([]byte{}, head...), byte(timeDelta)|5, 1, 2, 3, 4, 5),
		"oversized range count": append(append([]byte{}, head...), byte(payloadRange)|9, 1, 2, 3, 4, 5, 6, 7, 8, 9),
	}

	for name, stream := range streams {
		t.Run(name, func(t *testing.T) {
			dec := NewCompressedSetDecoder(nil, bytes.NewReader(stream))
			got := decodeAll(t, dec)

			if !errors.Is(dec.Err(), ErrCompressedSetEncoding) {
				t.Fatalf("expected ErrCompressedSetEncoding, got %v", dec.Err())
			}
			if len(got) != 1 || got[0] != id {
				t.Fatalf("unexpected ids delivered: %v", got)
			}
		})
	}
}

func testCompressedSetDecoderContextCancel(t *testing.T) {
	ksuids := [10]KSUID{}
	for i := range ksuids {
		ksuids[i] = New()
	}
	set := Compress(ksuids[:]...)
	want := iterAll(set)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	dec := NewCompressedSetDecoder(ctx, bytes.NewReader(set))
	if dec.Next() {
		t.Error("decoder produced an id after cancellation")
	}
	if err := dec.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if n := dec.Count(); n != 0 {
		t.Errorf("bad count after cancellation: %d", n)
	}

	// After cancellation the decoder can be reused on a new stream; no
	// state from the canceled run may leak into the next one.
	dec.Reset(context.Background(), bytes.NewReader(set))
	got := decodeAll(t, dec)

	if err := dec.Err(); err != nil {
		t.Fatalf("unexpected error after reset: %v", err)
	}
	if !equalKSUIDs(got, want) {
		t.Fatal("decoder produced wrong ids after reset")
	}
	if n := dec.Count(); n != uint64(len(want)) {
		t.Errorf("bad count after reset: %d", n)
	}
}

var errInjected = errors.New("injected reader failure")

type failingReader struct {
	data []byte
}

func (r *failingReader) Read(b []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, errInjected
	}
	n := copy(b, r.data)
	r.data = r.data[n:]
	return n, nil
}

func testCompressedSetDecoderReaderError(t *testing.T) {
	ksuids := [10]KSUID{}
	for i := range ksuids {
		ksuids[i] = New()
	}
	set := Compress(ksuids[:]...)
	want := iterAll(set)

	// Fail after delivering a prefix that ends mid-entry.
	dec := NewCompressedSetDecoder(nil, &failingReader{data: set[:len(set)-1]})
	decodeAll(t, dec)

	if err := dec.Err(); !errors.Is(err, errInjected) {
		t.Fatalf("expected the reader error, got %v", err)
	}
	if n := dec.Count(); n >= uint64(len(want)) {
		t.Errorf("decoder delivered %d ids from a truncated stream of %d", n, len(want))
	}

	// The decoder stays usable after a reader failure.
	dec.Reset(nil, bytes.NewReader(set))
	got := decodeAll(t, dec)

	if err := dec.Err(); err != nil {
		t.Fatalf("unexpected error after reset: %v", err)
	}
	if !equalKSUIDs(got, want) {
		t.Fatal("decoder produced wrong ids after reset")
	}
}
