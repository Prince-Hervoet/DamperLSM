package main

import (
	"DamperLSM/core"
	"fmt"
	"time"
)

func main() {
	bs, err := core.NewBootstrap("/workspaces/DamperLSM/test/")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	start := time.Now().UnixMicro()
	bs.Set("1", []byte("2jk"))
	bs.Set("3", []byte("2213"))
	bs.Set("4", []byte("2gfg"))
	b, _ := bs.Get("1")
	end := time.Now().UnixMicro()

	fmt.Println((end - start))
	fmt.Println(string(b))
}
