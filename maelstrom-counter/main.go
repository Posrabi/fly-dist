package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const KEY = "counter"

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	var deferred uint32 = 0
	mu := sync.Mutex{}

	n.Handle("init", func(msg maelstrom.Message) error {
		return kv.Write(context.Background(), KEY, 0)
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		body := map[string]any{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := uint32(body["delta"].(float64))
		if delta > 0 {
			mu.Lock()
			deferred += delta
			mu.Unlock()
		}

		resp := map[string]any{}
		resp["type"] = "add_ok"

		return n.Reply(msg, resp)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		body := map[string]any{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if err := kv.Write(context.Background(), uuid.NewString(), 0); err != nil {
			return err
		}

		val, err := kv.ReadInt(context.Background(), KEY)
		if err != nil {
			return err
		}

		resp := map[string]any{}
		resp["type"] = "read_ok"
		resp["value"] = val

		return n.Reply(msg, resp)
	})

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	done := make(chan bool)

	go func() {
		for {
			select {
			case <-ticker.C:
				mu.Lock()
				for deferred > 0 {
					current, err := kv.ReadInt(context.Background(), KEY)
					if err != nil {
						log.Fatal(err)
					}

					if err = kv.CompareAndSwap(context.Background(), KEY, current, current+int(deferred), false); err == nil {
						deferred = 0
					} else {
						log.Println("retrying", deferred)
					}
				}
				mu.Unlock()
			case <-done:
				done <- true
				log.Println("goroutine returned")
				return
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

	done <- true
	<-done
}
