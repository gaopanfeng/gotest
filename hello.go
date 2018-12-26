package main
import "fmt"
//import "math"
//import "io/ioutil"
import "os"

const (
  K = 1024
  M = K * K
  G = M * K

)

func Max(x, y int64) int64 {
    if x < y {
        return y
    }
    return x
}

func main(){
	in:="/Users/gaopanfeng/workspace/gotest/test.txt";
	fi, _ := os.Stat(in);

	var fSize = fi.Size();
	var partNum int64 = Max(fSize / 2 / G / 6,1) * 6;
	fmt.Printf("%d\n",partNum);
	var parts [][]int64 = make([][]int64,partNum,partNum);

	var index int64;
	for index = 0; index < partNum; index++ {
		parts[index] = []int64{1,2};
	}

	var i,j int64;
	for  i = 0; i < partNum; i++ {
      for j = 0; j < int64(len(parts[i])); j++ {
         fmt.Printf("parts[%d][%d] = %d\n", i,j, parts[i][j] );
      }
    }




}
