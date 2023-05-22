package goramda

import (
	"context"
)

func Stage[I any, O any](fn func(context.Context, I) O) func(ctx context.Context) func(stream <-chan I) <-chan O {
	return func(ctx context.Context) func(stream <-chan I) <-chan O {
		return func(stream <-chan I) <-chan O {
			out := make(chan O)
			go func() {
				defer close(out)
				for {
					select {
					case <-ctx.Done():
						return
					case in, ok := <-stream:
						if !ok {
							return
						}
						out <- fn(ctx, in)
					}
				}
			}()
			return out
		}
	}
}

func Repeat[T any](values ...T) func(ctx context.Context) func(<-chan T) <-chan T {
	return func(ctx context.Context) func(<-chan T) <-chan T {
		return func(stream <-chan T) <-chan T {
			out := make(chan T)
			go func() {
				defer close(out)
				for {
					for _, in := range values {
						select {
						case <-ctx.Done():
							return
						case out <- in:
						}
					}
				}
			}()
			return out
		}
	}
}

func Take[T any](n int) func(ctx context.Context) func(stream <-chan T) <-chan T {
	return func(ctx context.Context) func(stream <-chan T) <-chan T {
		return func(stream <-chan T) <-chan T {
			out := make(chan T)
			go func() {
				defer close(out)
				for i := 0; i < n; i++ {
					select {
					case <-ctx.Done():
						return
					case out <- <-stream:
					}
				}
			}()
			return out
		}
	}
}

func Stream[T any](fn ...func(context.Context) func(<-chan T) <-chan T) func(ctx context.Context) func(<-chan T) <-chan T {
	return func(ctx context.Context) func(<-chan T) <-chan T {
		return func(stream <-chan T) <-chan T {
			for _, f := range fn {
				stream = f(ctx)(stream)
			}

			return stream
		}
	}
}

func Pipe[I any, Intermediate any, O any](fn1 func(context.Context) func(<-chan I) <-chan Intermediate, fn2 func(context.Context) func(<-chan Intermediate) <-chan O) func(ctx context.Context) func(<-chan I) <-chan O {
	return func(ctx context.Context) func(<-chan I) <-chan O {
		return func(stream <-chan I) <-chan O {
			return fn2(ctx)(fn1(ctx)(stream))
		}
	}
}
