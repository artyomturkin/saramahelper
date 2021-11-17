package saramahelper

import (
	"sync"

	"github.com/Shopify/sarama"
)

// Fetch get provided number of messages from each partition of provided topic
func Fetch(c sarama.Client, topic string, count int) (<-chan *sarama.ConsumerMessage, <-chan error) {
	errch := make(chan error, 10)
	msgch := make(chan *sarama.ConsumerMessage)

	parts, err := c.Partitions(topic)
	if err != nil {
		errch <- err

		close(msgch)
		close(errch)

		return msgch, errch
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(parts))

	go func() {
		wg.Wait()
		close(msgch)
		close(errch)
	}()

	for _, p := range parts {
		go fetchFromPartition(c, topic, p, count, msgch, errch, wg)
	}

	return msgch, errch
}

func pushErr(errch chan<- error, err error) {
	select {
	case errch <- err:
	default:
	}
}

func fetchFromPartition(c sarama.Client, topic string, partition int32, count int, msgch chan<- *sarama.ConsumerMessage, errch chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	hw, err := c.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		pushErr(errch, err)
		return
	}

	lw, err := c.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		pushErr(errch, err)
		return
	}

	con, err := sarama.NewConsumerFromClient(c)
	if err != nil {
		pushErr(errch, err)
		return
	}

	start := hw - int64(count)
	if start < lw {
		start = lw
	}

	if start >= hw {
		return
	}

	pc, err := con.ConsumePartition(topic, partition, start)
	if err != nil {
		pushErr(errch, err)
		return
	}
	defer pc.Close()

	for m := range pc.Messages() {
		msgch <- m
		if m.Offset == hw-1 {
			return
		}
	}
}
