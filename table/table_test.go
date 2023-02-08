package table

import "testing"

func TestPrefix(t *testing.T) {
	t.Log(sharedPrefix([]byte{1, 2, 3}, []byte{}))
}

func TestBlockWriter(t *testing.T) {
	bw := &blockWriter{
		restartInterval: 2,
	}
	bw.append([]byte("a"), []byte("a"))
	bw.append([]byte("aa"), []byte("aa"))
	bw.append([]byte("aaaaa"), []byte("aaaaa"))
	bw.append([]byte("b"), []byte("b"))
	bw.append([]byte("bcd"), []byte("bcd"))
	bw.finish()
	
	bw.readAll()
}
