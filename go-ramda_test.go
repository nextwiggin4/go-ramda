package goramda

import (
	"context"
	"fmt"
	"testing"

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
