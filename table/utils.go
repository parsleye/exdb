package table

func sharedPrefix(x, y []byte) (i int) {
	n := len(x)
	if n > len(y) {
		n = len(y)
	}
	for i = 0; i < n && x[i] == y[i]; i++ {
	}
	return
}
