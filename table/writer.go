package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/parsleye/exdb/opt"
	"io"
)

type Writer struct {
	writer io.Writer

	opt *opt.Option
}

func NewWriter(writer io.Writer) (*Writer, error) {

	return nil, nil
}

type blockWriter struct {
	buf bytes.Buffer

	restartInterval int
	restarts        []uint32
	lastKey         []byte
	offset          int
	nEntries        int

	scratch [binary.MaxVarintLen64 * 3]byte
}

func (bw *blockWriter) append(key, value []byte) {
	var shared int
	if bw.nEntries%bw.restartInterval == 0 {
		// it is a restart point
		bw.restarts = append(bw.restarts, uint32(bw.offset))
	} else {
		shared = sharedPrefix(key, bw.lastKey)
	}

	n := binary.PutUvarint(bw.scratch[0:], uint64(shared))
	n += binary.PutUvarint(bw.scratch[n:], uint64(len(key)-shared))
	n += binary.PutUvarint(bw.scratch[n:], uint64(len(value)))

	bw.buf.Write(bw.scratch[:n])
	bw.buf.Write(key[shared:])
	bw.buf.Write(value)

	bw.lastKey = key
	bw.nEntries++
	bw.offset = bw.buf.Len()
}

func (bw *blockWriter) finish() {
	if bw.nEntries == 0 {
		bw.restarts = append(bw.restarts, 0)
	}
	bw.restarts = append(bw.restarts, uint32(len(bw.restarts)))

	bw.buf.Grow(len(bw.restarts) * 4)
	for _, restart := range bw.restarts {
		binary.LittleEndian.PutUint32(bw.scratch[:4], restart)
		bw.buf.Write(bw.scratch[:4])
	}
}

func (bw *blockWriter) reset() {
	bw.lastKey = nil
	bw.nEntries = 0
	bw.offset = 0
	bw.buf.Reset()
	bw.restarts = bw.restarts[:0]
}

func (bw *blockWriter) readAll() {
	buf := bw.buf.Bytes()
	var (
		restarts   []uint32
		restartOff int
	)
	restartNum := binary.LittleEndian.Uint32(buf[len(buf)-4:])

	for restartOff = len(buf) - int(restartNum+1)*4; restartOff < len(buf); restartOff += 4 {
		restarts = append(restarts, binary.LittleEndian.Uint32(buf[restartOff:restartOff+4]))
		fmt.Println(binary.LittleEndian.Uint32(buf[restartOff : restartOff+4]))
	}
}
