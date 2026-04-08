package main

import (
	"fmt"
	"math"
	"strconv"
)

func main() {
	for _, s := range []string{"nan", "NaN", "+Inf", "-Inf", "inf"} {
		p, err := strconv.ParseFloat(s, 64)
		fmt.Printf("%q err=%v -> <=0:%v <0:%v\n", s, err, p <= 0, p < 0)
	}
	n := math.NaN()
	fmt.Printf("NaN <= 0: %v, NaN < 0: %v\n", n <= 0, n < 0)
}
