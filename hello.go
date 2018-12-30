package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"runtime"
	"strconv"
	"sync"
)

//import "math"
//import "io/ioutil"

const (
	MMAP           = false
	SORT_READ_MMAP = true
	CPU            = 6
	K              = 1024
	M              = K * K
	G              = M * K
)

var startTime time.Time
var stopWg sync.WaitGroup
var partIndexAtomic uint32
var lock sync.Mutex
var fsFile [128][128][CPU]*os.File

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

func readFileBytes(bb [2]byte, index int) []byte {
	file := fsFile[bb[0]][bb[1]][index]
	defer file.Close()
	fs, _ := file.Stat()
	len := fs.Size()
	// fmt.Printf("%s%d:%d\n", bb, index, len)
	if len > 0 {
		var mmap []byte
		if SORT_READ_MMAP {
			mmap, _ = syscall.Mmap(int(file.Fd()), 0, int(len), syscall.PROT_READ, syscall.MAP_SHARED)
		} else {
			mmap = make([]byte, len)
			file.Seek(0, 0)
			file.Read(mmap)
		}
		// var mmap = make([]byte, len)
		// file.Seek(0, 0)
		// file.Read(mmap)
		return mmap

	}
	return nil
}

func asyncReadFileBytes(bb [2]byte, index int, ch chan []byte) {
	file := fsFile[bb[0]][bb[1]][index]
	defer file.Close()
	fs, _ := file.Stat()
	len := fs.Size()
	// fmt.Printf("%s%d:%d\n", bb, index, len)
	if len > 0 {
		var mmap []byte
		if SORT_READ_MMAP {
			mmap, _ = syscall.Mmap(int(file.Fd()), 0, int(len), syscall.PROT_READ, syscall.MAP_SHARED)
		} else {
			mmap = make([]byte, len)
			file.Seek(0, 0)
			file.Read(mmap)
		}
		ch <- mmap
	} else {
		ch <- nil
	}

}

func bytes2Lines(bytesArray [][]byte) [][]byte {
	var ln int
	for _, bs := range bytesArray {
		if bs != nil {
			ln += (len(bs))
		}
	}
	if ln > 0 {
		var list = make([][]byte, 0, ln/10)

		for _, bs := range bytesArray {
			buf := bytes.NewBuffer(bs)
			var line, err = buf.ReadBytes('\n')
			for err == nil {
				list = append(list, line)
				line, err = buf.ReadBytes('\n')
			}
		}

		return list
	}
	return nil
}

func aGetLines(bb [2]byte, workDir string) [][]byte {
	// var list = make([][][]byte, CPU)
	filename := workDir + string(bb[0])
	if bb[1] != 0 {
		filename += string(bb[1])
	}
	// size := 0
	var tmpCh = make(chan []byte, CPU)
	for i := 0; i < CPU; i++ {
		go asyncReadFileBytes(bb, i, tmpCh)
	}
	var tmp = make([][]byte, CPU)
	for i := 0; i < CPU; i++ {
		tmpItem := <-tmpCh
		tmp[i] = tmpItem
	}
	close(tmpCh)
	// var lines = make([][]byte, 0, size)
	// for i, items := range list {
	// 	if items != nil {
	// 		lines = append(lines, items...)
	// 		list[i] = nil
	// 	}
	// }
	return bytes2Lines(tmp)
}

func getLines(bb [2]byte, workDir string) [][]byte {
	// var list = make([][][]byte, CPU)
	filename := workDir + string(bb[0])
	if bb[1] != 0 {
		filename += string(bb[1])
	}
	// size := 0
	var tmp = make([][]byte, CPU)
	var tmpItem []byte
	for i := 0; i < CPU; i++ {
		tmpItem = readFileBytes(bb, i)
		tmp[i] = tmpItem
	}
	// var lines = make([][]byte, 0, size)
	// for i, items := range list {
	// 	if items != nil {
	// 		lines = append(lines, items...)
	// 		list[i] = nil
	// 	}
	// }
	return bytes2Lines(tmp)
}

func compareTo(left []byte, right []byte) bool {
	leftLen := len(left)
	rightLen := len(right)
	var lenDiff bool
	var min int
	if leftLen < rightLen {
		lenDiff = true
		min = leftLen - 1
	} else {
		min = rightLen - 1
	}

	for i := 2; i < min; i++ {
		if left[i] > right[i] {
			return false
		}
		if left[i] < right[i] {
			return true
		}
	}
	return lenDiff
}

func merge(array [][]byte, tmp [][]byte, leftStart int, leftEnd int, rightStart int, rightEnd int) {
	start := leftStart
	tmpPos := leftStart
	for leftStart <= leftEnd && rightStart <= rightEnd {
		if compareTo(array[leftStart], array[rightStart]) {
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

func readAndPart(fileName string, index int, parts [][2]int64, realNum int, bytesList *[26 * 27][2]byte, workDir string, fsAll *[128][128][6]int) {
	defer stopWg.Done()
	var files [128][128]*os.File
	var out [128][128]*bufio.Writer
	var filename string
	var tmpFile *os.File
	for _, b := range bytesList {
		filename = workDir + string(b[0])
		if b[1] != 0 {
			filename = filename + string(b[1])
		}
		filename = filename + "." + strconv.Itoa(index)
		//fmt.Printf("createFile=%s\n", filename);
		tmpFile, _ = os.Create(filename)
		files[b[0]][b[1]] = tmpFile
		fsFile[b[0]][b[1]][index] = tmpFile
		out[b[0]][b[1]] = bufio.NewWriterSize(tmpFile, 2<<14)
	}
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("open file err!", err)
	}
	var cost int64
	for {
		partIndex := atomic.AddUint32(&partIndexAtomic, 1) - 1
		if partIndex >= uint32(realNum) {
			break
		}
		part := parts[partIndex]
		// fmt.Printf("readAndPart:index=%d,start=%d,partIndex=%d,part=%d\n", index, time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond), partIndex, part)
		// func Mmap(fd int, offset int64, length int, prot int, flags int) (data []byte, err error)

		// fmt.Printf("readAndPart:index=%d,start=%d,part=%d\n", index, time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond), part)
		total := part[1] - part[0] + 1
		s := time.Now().UnixNano()
		var mmap []byte
		if MMAP {
			mmap, _ = syscall.Mmap(int(file.Fd()), part[0], int(total), syscall.PROT_READ, syscall.MAP_SHARED)
		} else {
			mmap = make([]byte, total)
			file.ReadAt(mmap, part[0])
		}
		cost += (time.Now().UnixNano() - s)
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
		// fmt.Printf("readAndPart:index=%d,end=%d,part=%d\n", index, time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond), part)
	}
	fmt.Printf("read%d real-cost:%d\n", index, cost/int64(time.Millisecond))
	var st os.FileInfo
	for _, b := range bytesList {
		// fmt.Printf("createFile=%s\n", filename);
		out[b[0]][b[1]].Flush()
		st, err = files[b[0]][b[1]].Stat()
		if err != nil {
			fmt.Println("error!", err)
			os.Exit(0)
		}
		//fmt.Printf("createFile=%s,size=%d\n", b, st.Size())
		fsAll[b[0]][b[1]][index] = int(st.Size())
		//files[b[0]][b[1]].Close()
	}
}

func sortAndWrite(index int, bytesList [26 * 27][2]byte, workDir string, fsPos *[128][128]int64, fs *[128][128]int, outList chan OutPart, tasks int) {

	max := len(bytesList)
	var cost int64
	var sortCost int64
	for {
		partIndex := int(atomic.AddUint32(&partIndexAtomic, 1) - 1)
		if partIndex >= max {
			if partIndex >= max+tasks-1 {
				close(outList)
			}
			break
		}
		bb := bytesList[partIndex]
		sum := fs[bb[0]][bb[1]]
		if sum > 0 {
			pos := fsPos[bb[0]][bb[1]]
			// fmt.Printf("sortAndWrite:%s,start=%d\n", bb, time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond))
			s := time.Now().UnixNano()
			var lines = aGetLines(bb, workDir)
			cost += (time.Now().UnixNano() - s)
			s = time.Now().UnixNano()
			var tmp = make([][]byte, len(lines))
			mergeSort(lines, tmp, 0, len(lines)-1)
			// sort.Slice(lines, func(i, j int) bool {
			// 	// less func should return sortitems[i] < sortitems[j]
			// 	//return bytes.Compare(sortitems[i], sortitems[j]) < 0
			// 	lj := len(lines[j])
			// 	li := len(lines[i])
			// 	var lenCom bool
			// 	var min int
			// 	if li < lj {
			// 		lenCom = true
			// 		min = li
			// 	} else {
			// 		min = lj
			// 	}
			// 	min--
			// 	for k := 2; k < min; k++ {
			// 		if lines[i][k] < lines[j][k] {
			// 			return true
			// 		}
			// 		if lines[i][k] > lines[j][k] {
			// 			return false
			// 		}
			// 	}
			// 	return lenCom
			// })
			sortCost += (time.Now().UnixNano() - s)
			var buf = make([]byte, 0, sum)
			for _, item := range lines {
				buf = append(buf, item...)
				// buf = append(buf, byte('\n'))
			}

			part := OutPart{
				bytes:  buf,
				offset: pos,
			}

			outList <- part
			//fmt.Printf("sortAndWrite%d:%s,end=%d\n", index, bb, time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond))
		}
	}
	fmt.Printf("sort read%d real-cost:%d;sort-cost=%d\n", index, cost/int64(time.Millisecond), sortCost/int64(time.Millisecond))
	stopWg.Done()

}

type OutPart struct {
	bytes  []byte
	offset int64
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
	partNum := Max(fSize/2/G/CPU, 10) * CPU
	// outListLen := 26 * 27 * Max(fSize/2/G, 1)
	// inFile,_:=os.Open(in);
	fmt.Printf("inputFileLen=%d,partNum=%d\n", fSize, partNum)
	parts, realNum := splitFile(in, fSize, partNum)
	fmt.Printf("0.splitFileCost = %d,realNum=%d\n", time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond), realNum)

	var fsAll [128][128][CPU]int

	for i := 0; i < CPU; i++ {
		stopWg.Add(1)
		go readAndPart(in, i, parts, realNum, &bytesList, workDir, &fsAll)
	}
	stopWg.Wait()
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
	fmt.Printf("2.fileSizeCost = %d\n", time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond))

	// 3. sort and output
	partIndexAtomic = 0
	tasks := CPU
	outList := make(chan OutPart, tasks)
	for i := 0; i < tasks; i++ {
		stopWg.Add(1)
		go sortAndWrite(i, bytesList, workDir, &fsPos, &fs, outList, tasks)
	}

	outFile, _ := os.Create(out)
	stopWg.Add(1)
	go func() {
		var cost int64
		for outPart := range outList {
			s := time.Now().UnixNano()
			outFile.WriteAt(outPart.bytes, outPart.offset)
			cost += (time.Now().UnixNano() - s)
		}
		fmt.Println("write real-cost:", cost/int64(time.Millisecond))
		stopWg.Done()
	}()

	stopWg.Wait()
	fmt.Printf("3.sortAndWriteStopChCost=%d\n", time.Now().Sub(startTime).Nanoseconds()/int64(time.Millisecond))
}
