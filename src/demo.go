package main

import "fmt"

func main() {
	a := []int{3, 4, 5}
	b := []int{}
	b = append(b, a...)
	fmt.Println(a[0])
}
