package goramda

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestStream(t *testing.T) {

	Convey("Given a repeating stream of values and a context", t, func() {

		multiply := func(_ context.Context, in int) int {
			return in * 2
		}

		values := Repeat(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		emptyValues := make(<-chan int)

		Convey("When the stream takes 15 elements, is multiplied but quits at the value 20", func() {
			ctx, cancel := context.WithCancel(context.Background())
			quitter := func(ctx context.Context, in int) int {
				if in == 20 {
					cancel()
				}
				return in
			}
			stream := Stream(values, Take[int](15), Stage(multiply), Stage(quitter))(ctx)(emptyValues)
			tests := []int{2, 4, 6, 8, 10, 12, 14, 16, 18, 20}

			Convey("Then the stream should be 2, 4, 6, 8, 10, 12, 14, 16, 18, 20", func() {
				for out := range stream {
					So(out, ShouldEqual, tests[0])
					tests = tests[1:]
				}
			})
			Convey("And the number of elements should be 10", func() {
				numberOfElements := 0
				for range stream {
					numberOfElements++
				}
				So(numberOfElements, ShouldEqual, len(tests))
			})

		})

		Convey("When the stream takes 5 elements, is multiplied and quits at the value 20", func() {
			ctx, cancel := context.WithCancel(context.Background())
			quitter := func(ctx context.Context, in int) int {
				if in == 20 {
					cancel()
				}
				return in
			}
			stream := Stream(values, Take[int](5), Stage(multiply), Stage(quitter))(ctx)(emptyValues)
			tests := []int{2, 4, 6, 8, 10}

			Convey("Then the stream should be 2, 4, 6, 8, 10", func() {
				for out := range stream {
					So(out, ShouldEqual, tests[0])
					tests = tests[1:]
				}
			})

			Convey("And the number of elements should be 5", func() {
				numberOfElements := 0
				for range stream {
					numberOfElements++
				}
				So(numberOfElements, ShouldEqual, len(tests))
			})

		})

		Convey("When the stream takes 5 elements, is multiplied and quits at the value 39", func() {
			ctx := context.Background()

			add := func(_ context.Context, in int) int {
				return in + 3
			}

			multiplier := func(multiple int) func(context.Context, int) int {
				return func(_ context.Context, in int) int {
					return in * multiple
				}
			}

			stream := Stream(values, Take[int](5), Stage(multiplier(2)), Stream(Stage(add), Stage(multiplier(3))))(ctx)(emptyValues)
			tests := []int{15, 21, 27, 33, 39}

			Convey("Then the stream should be 15, 21, 27, 33, 39", func() {
				for out := range stream {
					So(out, ShouldEqual, tests[0])
					tests = tests[1:]
				}
			})

			Convey("And the number of elements should be 5", func() {
				numberOfElements := 0
				for range stream {
					numberOfElements++
				}
				So(numberOfElements, ShouldEqual, len(tests))
			})

		})
	})

}

func TestPipe(t *testing.T) {
	Convey("Given a repeating stream of values and a context", t, func() {

		values := Repeat(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		emptyValues := make(<-chan int)
		Convey("When the stream takes 5 elements, is multiplied and quits at the value 39", func() {
			ctx := context.Background()

			toStr := func(_ context.Context, in int) string {
				return fmt.Sprintf("%v", in)
			}

			stream := Pipe(Stream(values, Take[int](5)), Stage(toStr))(ctx)(emptyValues)
			tests := []string{"1", "2", "3", "4", "5"}

			Convey("Then the stream should be \"1\", \"2\", \"3\", \"4\", \"5\"", func() {
				for out := range stream {
					So(out, ShouldEqual, tests[0])
					tests = tests[1:]
				}
			})
		})
	})
}

type customType1 struct {
	value  int
	name   string
	isTrue bool
}

type customType2 struct {
	Value1  int    `json:"value1"`
	Value2  int    `json:"value2"`
	NewName string `json:"newName"`
}

func TestCustomTypePipe(t *testing.T) {
	toCustomType2 := func(_ context.Context, in customType1) *customType2 {
		return &customType2{
			Value1:  in.value,
			Value2:  in.value * 2,
			NewName: strings.ToUpper(in.name),
		}
	}

	toJson := func(_ context.Context, in *customType2) json.RawMessage {
		b, _ := json.Marshal(in)
		return b
	}
	values := Repeat(customType1{1, "one", true}, customType1{2, "two", false}, customType1{3, "three", true}, customType1{4, "four", false}, customType1{5, "five", true})
	Convey("Given a repeating stream of customType1 values and a context", t, func() {

		Convey("When the stream takes 5 elements, is converted to customType2 and then to json", func() {

			Convey("Then the stream should be 5 json objects", func() {
				ctx := context.Background()
				emptyValues := make(<-chan customType1)
				stream := Pipe(Pipe(Stream(values, Take[customType1](5)), Stage(toCustomType2)), Stage(toJson))(ctx)(emptyValues)
				tests := []string{`{"value1":1,"value2":2,"newName":"ONE"}`, `{"value1":2,"value2":4,"newName":"TWO"}`, `{"value1":3,"value2":6,"newName":"THREE"}`, `{"value1":4,"value2":8,"newName":"FOUR"}`, `{"value1":5,"value2":10,"newName":"FIVE"}`}

				for out := range stream {
					So(string(out), ShouldEqual, tests[0])
					tests = tests[1:]
				}
			})

		})
	})
}

func TestFanStage(t *testing.T) {
	Convey("Given a repeating stream of values and a context", t, func() {
		rand.Seed(time.Now().UnixNano())
		multiply := func(_ context.Context, in int) int {
			n := rand.Intn(10)
			time.Sleep(time.Duration(n) * time.Millisecond)
			return in * 2
		}

		values := Repeat(1, 2, 3, 4, 5)
		emptyValues := make(<-chan int)

		take := 10

		Convey("When the stream takes 5 elements, is multiplied and quits at the value 20", func() {
			ctx := context.Background()

			stream := Stream(values, Take[int](take), FanStage(4, multiply))(ctx)(emptyValues)

			Convey("Then the stream should return 2 sets of {2, 4, 6, 8, 10} in a radom order", func() {
				actualValues := map[int]int{
					2:  0,
					4:  0,
					6:  0,
					8:  0,
					10: 0,
				}

				expectedValues := map[int]int{
					2:  2,
					4:  2,
					6:  2,
					8:  2,
					10: 2,
				}

				for out := range stream {
					actualValues[out]++
				}

				So(actualValues, ShouldResemble, expectedValues)
			})

			Convey("And the number of elements should be 10", func() {
				numberOfElements := 0
				for range stream {
					numberOfElements++
				}
				So(numberOfElements, ShouldEqual, take)
			})

		})

	})
}

func TestFanStageOrdered(t *testing.T) {
	Convey("Given a repeating stream of values and a context", t, func() {

		multiplyWithRandomDelay := func(_ context.Context, in int) int {
			n := rand.Intn(10)
			time.Sleep(time.Duration(n) * time.Millisecond)
			return in * 2
		}

		Convey("When the stream takes 5 elements, is multiplied and quits at the value 20", func() {
			Convey("Then the stream should return 2 sets of {2, 4, 6, 8, 10} in a radom order", func() {

				values := Repeat(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
				emptyValues := make(<-chan int)

				ctx := context.Background()

				stream1 := Stream(values, Take[int](10), Stage(multiplyWithRandomDelay))(ctx)(emptyValues)
				stream2 := Stream(values, Take[int](10), Stage(multiplyWithRandomDelay))(ctx)(emptyValues)

				fannedIn := fanInOrdered(ctx, stream1, stream2)

				for out := range fannedIn {
					fmt.Println(out)
				}

			})
		})
	})
}
