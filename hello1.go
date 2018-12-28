package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"
)

const (
	CPU   = 6
	DEBUG = false
	K     = 1024
	M     = K * K
	G     = M * K
)

var startTime time.Time

func Max(x, y int64) int64 {
	if x < y {
		return y
	}
	return x
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
func splitFile(filePath string, fileLen int64, num int64) ([][2]int64, int) {
	if num > 0 {
		file, _ := os.Open(filePath)
		everySize := fileLen/num + 1
		bs := make([]byte, 129, 129)
		var start, end int64 = 0, 0
		var parts = make([][2]int64, num, num)
		var ln int
		var i, j int64
		realNum := 0
		for i = 0; i < num; i++ {
			end += everySize
			if end >= fileLen-1 {
				end = fileLen - 1
				parts[i] = [2]int64{start, end}
				realNum++
				break
			}
			ln, _ = file.ReadAt(bs, end)
			for j = 0; j < int64(ln); j++ {
				if bs[j] == byte('\n') {
					end += j
					break
				}
			}
			parts[i] = [2]int64{start, end}
			start = end + 1
			realNum++

		}
		return parts, realNum
	} else {
		return nil, 0
	}
}

func readAndPart(fileName string, index int, parts [][2]int64, partIndexCh chan int, bytesList *[26 * 27][2]byte, workDir string, fsAll *[128][128][CPU]int, stopCh chan int) {
	var files [128][128]*os.File
	var out [128][128]*bufio.Writer
	var filename string
	for _, b := range bytesList {
		filename = workDir + string(b[0])
		if b[1] != 0 {
			filename = filename + string(b[1])
		}
		filename = filename + "." + strconv.Itoa(index)
		//fmt.Printf("createFile=%s\n", filename);
		files[b[0]][b[1]], _ = os.Create(filename)
		out[b[0]][b[1]] = bufio.NewWriterSize(files[b[0]][b[1]], 2<<14)
	}
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("open file err!", err)
	}
	for partIndex := range partIndexCh {
		// func Mmap(fd int, offset int64, length int, prot int, flags int) (data []byte, err error)
		part := parts[partIndex]
		// fmt.Printf("readAndPart:index=%d,start=%d,part=%d\n", index, time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond), part)
		total := part[1] - part[0] + 1
		// mmap, err := syscall.Mmap(int(file.Fd()), part[0], int(total), syscall.PROT_READ, syscall.MAP_SHARED)
		mmap := make([]byte, total)
		file.ReadAt(mmap, part[0])
		start := 0
		ln := 0
		for i, b := range mmap {
			if b == byte('\n') {
				tmp := mmap[start : i+1]
				if tmp == nil {
					fmt.Println("ok")
				}
				if ln == 1 {
					out[tmp[0]][0].Write(tmp)
				} else {
					//fmt.Printf("debug:len=%d,tmp=%s",len(tmp), tmp);
					out[tmp[0]][tmp[1]].Write(tmp)
				}

				ln = 0
				start = i + 1
			} else {
				ln++
			}
		}
		if DEBUG {
			fmt.Printf("readAndPart:index=%d,end=%d,part=%d\n", index, time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond), part)
		}
	}
	var st os.FileInfo
	for _, b := range bytesList {
		out[b[0]][b[1]].Flush()
		st, err = files[b[0]][b[1]].Stat()
		if err != nil {
			fmt.Println("error!", err)
			os.Exit(2)
		}
		fsAll[b[0]][b[1]][index] = int(st.Size())
		files[b[0]][b[1]].Close()
	}
	stopCh <- index
}

func readFileLines(filename string) [][]byte {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error readFileLines!", filename, err)
		os.Exit(3)
	}
	fs, _ := file.Stat()
	len := fs.Size()
	if len > 0 {
		var list = make([][]byte, 0, len/64)
		var mmap = make([]byte, len)
		file.Read(mmap)
		buf := bytes.NewBuffer(mmap)
		var line, err1 = buf.ReadBytes('\n')
		for err1 == nil {
			list = append(list, line)
			line, err1 = buf.ReadBytes('\n')
		}
		file.Close()
		return list
	}
	file.Close()
	return nil
}

func getLines(bb [2]byte, workDir string) [][]byte {
	var list = make([][][]byte, CPU)
	filename := workDir + string(bb[0])
	if bb[1] != 0 {
		filename += string(bb[1])
	}
	size := 0
	var tmp [][]byte
	for i := 0; i < CPU; i++ {
		tmp = readFileLines(filename + "." + strconv.Itoa(i))
		list[i] = tmp
		if tmp != nil {
			size += len(tmp)
		}
	}
	var lines = make([][]byte, 0, size)
	for i, items := range list {
		if items != nil {
			lines = append(lines, items...)
			list[i] = nil
		}
	}
	return lines
}

func compareTo(left []byte, right []byte) int {
	leftLen := len(left)
	rightLen := len(right)
	min := Min(leftLen, rightLen)
	diff := 0
	for i := 2; i < min; i++ {
		diff = int(left[i]) - int(right[i])
		if diff != 0 {
			// fmt.Printf("%s - %s = %d\n", left, right, diff)
			return diff
		}
	}
	diff = leftLen - rightLen
	// fmt.Printf("%s - %s = %d\n", left, right, diff)
	return diff
}

func merge(array [][]byte, tmp [][]byte, leftStart int, leftEnd int, rightStart int, rightEnd int) {
	start := leftStart
	tmpPos := leftStart
	for leftStart <= leftEnd && rightStart <= rightEnd {
		if compareTo(array[leftStart], array[rightStart]) <= 0 {
			tmp[tmpPos] = array[leftStart]
			tmpPos++
			leftStart++
		} else {
			tmp[tmpPos] = array[rightStart]
			tmpPos++
			rightStart++
		}
	}
	for leftStart <= leftEnd {
		tmp[tmpPos] = array[leftStart]
		tmpPos++
		leftStart++
	}
	for rightStart <= rightEnd {
		tmp[tmpPos] = array[rightStart]
		tmpPos++
		rightStart++
	}

	for i := start; i < tmpPos; i++ {
		array[i] = tmp[i]
	}
}

func mergeSort(array [][]byte, tmp [][]byte, start int, end int) {
	size := 1
	n := end - start + 1
	n1 := n - 1
	var low, high, mid int
	for size <= n1 {
		low = start
		for low+size <= end {
			mid = low + size - 1
			high = mid + size
			if high > end {
				high = end
			}
			merge(array, tmp, low, mid, mid+1, high)
			low = high + 1
		}
		size *= 2
	}
}

func sortAndWrite(workDir string, file *os.File, index int, bytesList *[26 * 27][2]byte, fsPos *[128][128]int64, fs *[128][128]int, sortIndexCh chan int, stop chan int) {
	var tmp [][]byte
	var lines [][]byte
	var sum int
	var pos int64
	for sortIndex := range sortIndexCh {
		bb := bytesList[sortIndex]
		sum = fs[bb[0]][bb[1]]
		if sum > 0 {
			pos = fsPos[bb[0]][bb[1]]
			lines = getLines(bb, workDir)
			// sort.Slice(lines, func(i, j int) bool {
			// 	return compareTo(lines[i], lines[j]) < 0
			// })
			tmp = make([][]byte, len(lines))
			// fmt.Println(len(tmp))
			mergeSort(lines, tmp, 0, len(lines)-1)
			// buf := make([]byte, 0, sum)
			buf := bytes.NewBuffer(make([]byte, sum))

			for _, item := range lines {
				buf.Write(item)
				// buf = append(buf, item...)
				// fmt.Printf("sortAndWrite:line:%s\n", item)
			}
			if bb[0] == 'a' {
				fmt.Printf("sortAndWrite%d:%s,pos=%d,bytes.len=%d\n", index, bb, pos, len(buf.Bytes()))
			}
			file.WriteAt(buf.Bytes(), pos)
			if DEBUG {
				fmt.Printf("sortAndWrite:%s,end=%d\n", bb, time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond))
			}
		}

	}
	file.Close()
	stop <- 1
}

func main() {

	startTime = time.Now()
	runtime.GOMAXPROCS(CPU)
	BYTES := []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'}
	var bytesList [26 * 27][2]byte
	in := "/Users/gaopanfeng/workspace/gotest/test.txt"
	out := "/Users/gaopanfeng/workspace/gotest/o1.txt"
	workDir := "/Users/gaopanfeng/workspace/gotestDir/"
	for i, arg := range os.Args {
		fmt.Println(arg)
		if i == 1 {
			in = arg
		} else if i == 2 {
			out = arg
		} else if i == 3 {
			workDir = arg
		}
	}
	bytesListIndex := 0
	for _, b1 := range BYTES {
		bytesList[bytesListIndex] = [2]byte{b1, 0}
		bytesListIndex++
		for _, b2 := range BYTES {
			bytesList[bytesListIndex] = [2]byte{b1, b2}
			bytesListIndex++
		}
	}

	fi, _ := os.Stat(in)
	fSize := fi.Size()
	partNum := Max(fSize/2/G/CPU, 10) * CPU
	fmt.Printf("inputFileLen=%d,partNum=%d\n", fSize, partNum)
	parts, realNum := splitFile(in, fSize, partNum)
	fmt.Printf("0.splitFileCost = %d,realNum=%d\n", time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond), realNum)

	var fsAll [128][128][CPU]int

	partIndexCh := make(chan int, realNum)
	for i := 0; i < realNum; i++ {
		partIndexCh <- i
	}
	close(partIndexCh)
	stopCh := make(chan int, CPU)
	for i := 0; i < CPU; i++ {
		go readAndPart(in, i, parts, partIndexCh, &bytesList, workDir, &fsAll, stopCh)
	}
	for i := 0; i < CPU; i++ {
		<-stopCh
	}
	close(stopCh)
	fmt.Printf("1.readAndPart = %d\n", time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond))

	var fs [128][128]int
	var fsPos [128][128]int64
	var startPos int64
	var sum int
	for _, b := range bytesList {
		fsPos[b[0]][b[1]] = startPos
		sum = 0
		for _, item := range fsAll[b[0]][b[1]] {
			sum += item
		}
		fs[b[0]][b[1]] = sum
		startPos += int64(sum)
	}
	// for _, b := range bytesList {
	// 	fmt.Printf("%s,fs = %d ,fsPos=%d\n", b, fs[b[0]][b[1]], fsPos[b[0]][b[1]])
	// }

	fmt.Printf("2.fileSizeCost = %d\n", time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond))

	// 3. sort and output
	taskNum := 0
	for _, b := range bytesList {
		if fs[b[0]][b[1]] > 0 {
			taskNum++
		}
	}
	sortIndexCh := make(chan int, taskNum)
	for i, b := range bytesList {
		if fs[b[0]][b[1]] > 0 {
			// if b[0] == 'a' {
			// 	fmt.Printf("sort:%s,%d\n", b, fsPos[b[0]][b[1]])
			// }
			// //if b[0] == 'a' && b[1] == 'a' {
			//fmt.Printf("sort:%s\n", b)
			//fmt.Printf("sort:%s\n", b)
			sortIndexCh <- i
			//}

		}
	}
	close(sortIndexCh)
	sortAndWriteStopCh := make(chan int, CPU)

	outFile, err := os.Create(out)
	if err != nil {
		fmt.Println("open outFile err!", err)
		os.Exit(4)
	}

	for i := 0; i < CPU; i++ {
		// outFile, err := os.Create(out)
		// if err != nil {
		// 	fmt.Println("open outFile err!", err)
		// 	os.Exit(4)
		// }
		go sortAndWrite(workDir, outFile, i, &bytesList, &fsPos, &fs, sortIndexCh, sortAndWriteStopCh)
	}
	for i := 0; i < CPU; i++ {
		<-sortAndWriteStopCh
	}
	fmt.Printf("3.sortAndWriteStopChCost=%d\n", time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond))
}
