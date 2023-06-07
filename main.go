package main

import (
	"DamperLSM/core"
	"fmt"
	"time"
)

func main() {
	bs, err := core.NewDamperDb("/workspaces/DamperLSM/test/")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	start := time.Now().UnixMicro()
	// bs.Set("1", []byte("2jk3434"))
	// bs.Set("1231", []byte("3434"))
	b, _ := bs.Get("1231")
	end := time.Now().UnixMicro()

	fmt.Println((end - start))
	fmt.Println("ans: " + string(b))

	for {
		time.Sleep(2 * time.Second)
	}
}
