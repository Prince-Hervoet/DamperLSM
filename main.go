package main

import (
	"DamperLSM/core"
	"fmt"
	"strconv"
	"time"
)

func main() {
	bs, err := core.NewDamperDb("/workspaces/DamperLSM/test/")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	start := time.Now().UnixMicro()
	for i := 0; i < 400000; i++ {
		bs.Set(strconv.FormatInt(int64(i), 10), []byte("123324241agsdfgsdfgsdfg23kjadfkj123"))
	}
	b, _ := bs.Get("1")
	end := time.Now().UnixMicro()

	fmt.Print("耗时: ")
	fmt.Print((end - start))
	fmt.Println("ns")
	fmt.Println("ans: " + string(b))

	for {
		time.Sleep(2 * time.Second)
	}
}
