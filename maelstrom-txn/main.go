package main

import (
	"encoding/json"
	"log"
	"math/rand"
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

func copyLatestState(src map[int]map[int]int) map[int]int {
	ret := map[int]int{}

	max := 0
	for k := range src {
		if k > max {
			max = k
		}
	}

	for k, v := range src[max] {
		ret[k] = v
	}

	return ret
}

func processTxn(txn Txn) error {
	mu.RLock()
	snapshot := copyLatestState(storage)
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
			return err
		}

		resp := map[string]any{}
		resp["type"] = "txn_ok"
		resp["txn"] = ops
		resp["msg_id"] = id
		resp["in_reply_to"] = id

		return n.Reply(msg, resp)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
