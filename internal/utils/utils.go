package utils

func PadOrTruncate(b []byte, size int) []byte {
	if len(b) >= size {
		return b[:size]
	}
	padded := make([]byte, size)
	copy(padded, b)
	return padded
}