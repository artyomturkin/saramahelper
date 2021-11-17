package saramahelper

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/artyomturkin/go-from-uri/kafka"
)

func ExampleFetch() {
	ku := "kafka://localhost:9092"
	tn := "test"
	size := 5

	c, err := kafka.NewSaramaClient(ku)
	if err != nil {
		panic(err)
	}

	msgch, errch := Fetch(c, tn, size)

	msgs := []*sarama.ConsumerMessage{}
	for m := range msgch {
		msgs = append(msgs, m)
	}

	errs := []error{}
	for err := range errch {
		errs = append(errs, err)
	}

	fmt.Printf("MSGs: %d\n", len(msgs))
	fmt.Printf("ERRs: %d\n", len(errs))

	//Output:
	//MSGs: 1
	//ERRs: 0
}
