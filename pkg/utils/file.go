package utils

import (
	"bufio"
	"bytes"
	"io"
	"os"
)

func FileLineCount(filePath string) (int, error) {

	r, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer r.Close()

	var count int
	const lineBreak = '\n'

	buf := make([]byte, bufio.MaxScanTokenSize)

	for {
		bufferSize, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return 0, err
		}

		var buffPosition int
		for {
			i := bytes.IndexByte(buf[buffPosition:], lineBreak)
			if i == -1 || bufferSize == buffPosition {
				break
			}
			buffPosition += i + 1
			count++
		}
		if err == io.EOF {
			break
		}
	}

	return count, nil
}
