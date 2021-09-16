# *pipeline* - create fast Go pipelines ![build](https://github.com/ele7ija/pipeline/actions/workflows/go.yml/badge.svg)

<img src="https://raw.githubusercontent.com/ashleymcnamara/gophers/master/NERDY.png" width="100" height="100" alt="nerd" class="emoji" title="nerd"/> `pipeline` is a package enabling you to create custom pipelines in Go.

It provides you with filters like *Serial* or *Parallel* and leaves it to you, 
the user, to focus only on what transformations to do on the data. No external library used.

Works well anywhere where multi-stage processing
takes place, for example [REST API for image management](https://github.com/ele7ija/go-pipelines).
Great to couple with message queues.

```go
package example

import (
	"context"
	"fmt"
	"github.com/ele7ija/pipeline"
)

type LShiftWorker struct {
}

func (w *LShiftWorker) Work(ctx context.Context, in pipeline.Item) (pipeline.Item, error) {

	n := in.(int)
	return n << 1, nil
}

func main() {
	
	f := pipeline.NewSerialFilter(&LShiftWorker{}, &LShiftWorker{})
	p := pipeline.NewPipeline("My pipeline", f)

	items := make(chan pipeline.Item, 5)
	errors := make(chan error, 5)
	items <- 0
	items <- 1
	items <- 2
	items <- 3
	items <- 4
	close(items)

	filteredItems := p.Filter(context.Background(), items, errors)

	go func() {
		for range errors {
			fmt.Println("Oh no, an error!")
		}
	}()
	for filteredItem := range filteredItems {
		fmt.Println(filteredItem.(int))
	}
	close(errors)
}

// Output:
// 0
// 4
// 8
// 12
// 16
```

