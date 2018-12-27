package main

import (
	"bufio"
	"fmt"
	"os"
	"time"

	// "syscall"
	"runtime"
	"strconv"
)

//import "math"
//import "io/ioutil"

const (
	CPU = 6
	K   = 1024
	M   = K * K
	G   = M * K
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
		var parts [][2]int64 = make([][2]int64, num, num)
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

func readAndPart(fileName string, index int, parts [][2]int64, realNum int, partIndexCh chan int, bytesList *[26 * 27][2]byte, workDir string, fsAll *[128][128][6]int, stopCh chan int) {
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
		out[b[0]][b[1]] = bufio.NewWriterSize(files[b[0]][b[1]], 2<<10)
	}
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("open file err!", err)
	}
	for {
		partIndex := <-partIndexCh
		if partIndex >= realNum {
			break
		}
		// func Mmap(fd int, offset int64, length int, prot int, flags int) (data []byte, err error)
		part := parts[partIndex]
		fmt.Printf("readAndPart:index=%d,start=%d,part=%d\n", index, time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond), part)
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
		fmt.Printf("readAndPart:index=%d,end=%d,part=%d\n", index, time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond), part)
	}
	var st os.FileInfo
	for _, b := range bytesList {
		// fmt.Printf("createFile=%s\n", filename);
		out[b[0]][b[1]].Flush()
		st, err = files[b[0]][b[1]].Stat()
		if err != nil {
			fmt.Println("error!", err)
			os.Exit(2)
		}
		fsAll[b[0]][b[1]][index] = int(st.Size())
		//files[b[0]][b[1]].Close()
	}
	stopCh <- index
}

func readFileLines(filename string) [][]byte {
	file, _ := os.Open(filename)
	fs, _ := file.Stat()
	len := fs.Size()
	if len > 0 {
		var list [][]byte = make([][]byte, 0, len/64)
		var mmap []byte = make([]byte, len)
		file.Read(mmap)
		start := 0
		len := 0
		for i, b := range mmap {
			if b == byte('\n') {
				list = append(list, mmap[start:i])
				len = 0
				start = i + 1
			} else {
				len++
			}
		}
		return list
	}
	return nil
}

func getLines(bb [2]byte, workDir string) [][]byte {
	var list [][][]byte = make([][][]byte, CPU)
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
	var lines [][]byte = make([][]byte, 0, size)
	for _, items := range list {
		if items != nil {
			lines = append(lines, items...)
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

func sortAndWrite(bb [2]byte, workDir string, pos int64, sum int, fileCh chan *os.File, stop chan int) {
	var lines [][]byte = getLines(bb, workDir)
	var tmp [][]byte = make([][]byte, len(lines))
	mergeSort(lines, tmp, 0, len(lines)-1)
	var buf []byte = make([]byte, 0, sum)
	for _, item := range lines {
		buf = append(buf, item...)
		buf = append(buf, byte('\n'))
	}
	file := <-fileCh
	file.WriteAt(buf, pos)
	fileCh <- file
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
	//fmt.Printf("%s\n%s\n", BYTES, bytesList);

	fi, _ := os.Stat(in)
	fSize := fi.Size()
	partNum := Max(fSize/2/G/6, 10) * 6
	// inFile,_:=os.Open(in);
	fmt.Printf("inputFileLen=%d,partNum=%d\n", fSize, partNum)
	parts, realNum := splitFile(in, fSize, partNum)
	fmt.Printf("0.splitFileCost = %d,realNum=%d\n", time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond), realNum)

	var fsAll [128][128][6]int

	partIndexCh := make(chan int, realNum+CPU)
	for i := 0; i < realNum+CPU; i++ {
		partIndexCh <- i
	}
	stopCh := make(chan int, CPU)
	for i := 0; i < CPU; i++ {
		go readAndPart(in, i, parts, realNum, partIndexCh, &bytesList, workDir, &fsAll, stopCh)
	}
	for i := 0; i < CPU; i++ {
		<-stopCh
	}
	fmt.Printf("1.readAndPart = %d\n", time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond))

	var fs [128][128]int
	var fsPos [128][128]int64
	var startPos int64 = 0
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
	fmt.Printf("2.fileSizeCost = %d\n", time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond))

	// 3. sort and output
	sortAndWriteStopCh := make(chan int)
	fc := make(chan *os.File, CPU)
	for i := 0; i < CPU; i++ {
		fi, _ := os.Create(out)
		fc <- fi
	}
	taskNum := 0
	for _, b := range bytesList {
		if fs[b[0]][b[1]] > 0 {
			//sortAndWrite(bb [2]byte, workDir string, pos int64, sum int, fc chan *os.File) {
			taskNum++
			go sortAndWrite(b, workDir, fsPos[b[0]][b[1]], fs[b[0]][b[1]], fc, sortAndWriteStopCh)
		}
	}
	for i := 0; i < taskNum; i++ {
		<-sortAndWriteStopCh
	}
	fmt.Printf("3.sortAndWriteStopChCost=%d\n", time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond))
}
