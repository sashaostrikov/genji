package bytesutil_test

import (
	"testing"

	"github.com/asdine/genji/pkg/bytesutil"
)

var compareTests = []struct {
	a, b []byte
	i    int
}{
	{[]byte(""), []byte(""), 0},
	{[]byte("a"), []byte(""), 1},
	{[]byte(""), []byte("a"), -1},
	{[]byte("abc"), []byte("abc"), 0},
	{[]byte("abd"), []byte("abc"), 1},
	{[]byte("abc"), []byte("abd"), -1},
	{[]byte("ab"), []byte("abc"), -1},
	{[]byte("abc"), []byte("ab"), 1},
	{[]byte("x"), []byte("ab"), 1},
	{[]byte("ab"), []byte("x"), -1},
	{[]byte("x"), []byte("a"), 1},
	{[]byte("b"), []byte("x"), -1},
	// test runtime·memeq's chunked implementation
	{[]byte("abcdefgh"), []byte("abcdefgh"), 0},
	{[]byte("abcdefghi"), []byte("abcdefghi"), 0},
	{[]byte("abcdefghi"), []byte("abcdefghj"), -1},
	{[]byte("abcdefghj"), []byte("abcdefghi"), 1},
	// nil tests
	{nil, nil, 0},
	{[]byte(""), nil, 0},
	{nil, []byte(""), 0},
	{[]byte("a"), nil, 1},
	{nil, []byte("a"), -1},
}

func TestCompare(t *testing.T) {
	for _, tt := range compareTests {
		numShifts := 16
		buffer := make([]byte, len(tt.b)+numShifts)
		// vary the input alignment of tt.b
		for offset := 0; offset <= numShifts; offset++ {
			shiftedB := buffer[offset : len(tt.b)+offset]
			copy(shiftedB, tt.b)
			cmp := bytesutil.CompareBytes(tt.a, shiftedB)
			if cmp != tt.i {
				t.Errorf(`Compare(%q, %q), offset %d = %v; want %v`, tt.a, tt.b, offset, cmp, tt.i)
			}
		}
	}
}

func TestCompareIdenticalSlice(t *testing.T) {
	var b = []byte("Hello Gophers!")
	if bytesutil.CompareBytes(b, b) != 0 {
		t.Error("b != b")
	}
	if bytesutil.CompareBytes(b, b[:1]) != 1 {
		t.Error("b > b[:1] failed")
	}
}

func TestCompareBytes(t *testing.T) {
	lengths := make([]int, 0) // lengths to test in ascending order
	for i := 0; i <= 128; i++ {
		lengths = append(lengths, i)
	}
	lengths = append(lengths, 256, 512, 1024, 1333, 4095, 4096, 4097)

	n := lengths[len(lengths)-1]
	a := make([]byte, n+1)
	b := make([]byte, n+1)
	for _, len := range lengths {
		// randomish but deterministic data. No 0 or 255.
		for i := 0; i < len; i++ {
			a[i] = byte(1 + 31*i%254)
			b[i] = byte(1 + 31*i%254)
		}
		// data past the end is different
		for i := len; i <= n; i++ {
			a[i] = 8
			b[i] = 9
		}
		cmp := bytesutil.CompareBytes(a[:len], b[:len])
		if cmp != 0 {
			t.Errorf(`CompareIdentical(%d) = %d`, len, cmp)
		}
		if len > 0 {
			cmp = bytesutil.CompareBytes(a[:len-1], b[:len])
			if cmp != -1 {
				t.Errorf(`CompareAshorter(%d) = %d`, len, cmp)
			}
			cmp = bytesutil.CompareBytes(a[:len], b[:len-1])
			if cmp != 1 {
				t.Errorf(`CompareBshorter(%d) = %d`, len, cmp)
			}
		}
		for k := 0; k < len; k++ {
			b[k] = a[k] - 1
			cmp = bytesutil.CompareBytes(a[:len], b[:len])
			if cmp != 1 {
				t.Errorf(`CompareAbigger(%d,%d) = %d`, len, k, cmp)
			}
			b[k] = a[k] + 1
			cmp = bytesutil.CompareBytes(a[:len], b[:len])
			if cmp != -1 {
				t.Errorf(`CompareBbigger(%d,%d) = %d`, len, k, cmp)
			}
			b[k] = a[k]
		}
	}
}

func TestEndianBaseCompare(t *testing.T) {
	// This test compares byte slices that are almost identical, except one
	// difference that for some j, a[j]>b[j] and a[j+1]<b[j+1]. If the implementation
	// compares large chunks with wrong endianness, it gets wrong result.
	// no vector register is larger than 512 bytes for now
	const maxLength = 512
	a := make([]byte, maxLength)
	b := make([]byte, maxLength)
	// randomish but deterministic data. No 0 or 255.
	for i := 0; i < maxLength; i++ {
		a[i] = byte(1 + 31*i%254)
		b[i] = byte(1 + 31*i%254)
	}
	for i := 2; i <= maxLength; i <<= 1 {
		for j := 0; j < i-1; j++ {
			a[j] = b[j] - 1
			a[j+1] = b[j+1] + 1
			cmp := bytesutil.CompareBytes(a[:i], b[:i])
			if cmp != -1 {
				t.Errorf(`CompareBbigger(%d,%d) = %d`, i, j, cmp)
			}
			a[j] = b[j] + 1
			a[j+1] = b[j+1] - 1
			cmp = bytesutil.CompareBytes(a[:i], b[:i])
			if cmp != 1 {
				t.Errorf(`CompareAbigger(%d,%d) = %d`, i, j, cmp)
			}
			a[j] = b[j]
			a[j+1] = b[j+1]
		}
	}
}
