package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

/*
We need to implement MVCC with a data replication between nodes if we want TA
*/

var storage = map[int]map[int]int{} // ts : { key : val }
var mu = sync.RWMutex{}

type Txn struct {
	ops []any
	ts  int
}

func getLatestState() (map[int]int, int) {
	max := 0
	for k := range storage {
		if k > max {
			max = k
		}
	}

	return storage[max], max
}

func copyLatestState() map[int]int {
	ret := map[int]int{}

	latestState, _ := getLatestState()

	for k, v := range latestState {
		ret[k] = v
	}

	return ret
}

func processTxn(txn Txn) error {
	mu.RLock()
	snapshot := copyLatestState()
	mu.RUnlock()

	for i, o := range txn.ops {
		op := o.([]any)
		t := op[0].(string)
		k := int(op[1].(float64))

		switch t {
		case "r":
			{
				if current, ok := snapshot[k]; ok {
					op[2] = current
				}
			}
		case "w":
			{
				snapshot[k] = int(op[2].(float64))
			}
		default:
			log.Fatal("bad op", t)
		}

		txn.ops[i] = op
	}

	mu.Lock()
	storage[txn.ts] = snapshot
	mu.Unlock()
	return nil
}

func abort(n *maelstrom.Node, msg maelstrom.Message) error {
	return n.Reply(msg, map[string]any{
		"type": "error",
		"code": maelstrom.TxnConflict,
		"text": "txn abort",
	})
}

func main() {
	storage[0] = map[int]int{}
	rand.Seed(time.Now().UnixMilli())
	n := maelstrom.NewNode()

	n.Handle("txn", func(msg maelstrom.Message) error {
		body := map[string]any{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		id := int(body["msg_id"].(float64))
		ops := body["txn"].([]any)
		if err := processTxn(Txn{
			ops: ops,
			ts:  id,
		}); err != nil {
			if err.Error() == "abort" {
				return abort(n, msg)
			}

			return err
		}

		resp := map[string]any{}
		resp["type"] = "txn_ok"
		resp["txn"] = ops
		resp["msg_id"] = id
		resp["in_reply_to"] = id

		return n.Reply(msg, resp)
	})

	n.Handle("sync", func(msg maelstrom.Message) error {
		body := map[string]any{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ts := int(body["ts"].(float64))
		malformed := body["state"].(map[string]any)
		state := map[int]int{}

		for k, v := range malformed {
			key, err := strconv.Atoi(k)
			if err != nil {
				return err
			}

			val := int(v.(float64))
			state[key] = val
		}

		mu.Lock()
		storage[ts] = state
		mu.Unlock()

		log.Println("Sync successful with ts:", ts)

		return nil
	})

	ticker := time.NewTicker(time.Millisecond * 10)
	done := make(chan bool)

	// goroutine that syncs with other nodes
	go func() {
		for {
			select {
			case <-done:
				{
					done <- true
					return
				}
			case <-ticker.C:
				{
					toSend := map[string]any{}
					mu.RLock()
					state, ts := getLatestState()
					toSend["state"] = state
					toSend["ts"] = ts
					toSend["type"] = "sync"
					mu.RUnlock()

					for _, dst := range n.NodeIDs() {
						if dst == n.ID() {
							continue
						}

						if err := n.Send(dst, toSend); err != nil {
							log.Fatal(err)
						}
					}

				}
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

	done <- true
	<-done
}
