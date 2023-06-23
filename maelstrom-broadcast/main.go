package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func composeGossipMsg(msg []int) map[string]any {
	ret := map[string]any{}
	ret["type"] = "gossip"
	ret["message"] = msg

	return ret
}

func composeGossipAckMsg(msg []int) map[string]any {
	ret := map[string]any{}
	ret["type"] = "gossip_ack"
	ret["message"] = msg

	return ret
}

func main() {
	n := maelstrom.NewNode()

	messages := map[int]int{}
	known := map[string]map[int]int{}
	neighborhood := []string{}

	msgMu, knownMu := sync.RWMutex{}, sync.RWMutex{}

	n.Handle("gossip", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// store msg
		mess := body["message"].([]any)
		ack := []int{}
		msgMu.Lock()
		knownMu.Lock()
		for _, mf := range mess {
			m := int(mf.(float64))
			messages[m] = m
			known[msg.Src][m] = m
			ack = append(ack, m)
		}
		knownMu.Unlock()
		msgMu.Unlock()

		if len(ack) > 0 {
			if err := n.Send(msg.Src, composeGossipAckMsg(ack)); err != nil {
				log.Panic(err)
			}
		}

		return nil
	})

	n.Handle("gossip_ack", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		knownMu.Lock()
		defer knownMu.Unlock()

		mess := body["message"].([]any)
		for _, mf := range mess {
			m := int(mf.(float64))
			known[msg.Src][m] = m
		}

		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// store msg
		mess := int(body["message"].(float64))
		msgMu.Lock()
		messages[mess] = mess
		msgMu.Unlock()

		// return resp
		resp := map[string]any{}
		resp["type"] = "broadcast_ok"

		return n.Reply(msg, resp)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		resp := map[string]any{}
		msgMu.RLock()
		ret := make([]int, 0, len(messages))
		for k := range messages {
			ret = append(ret, k)
		}
		msgMu.RUnlock()

		resp["type"] = "read_ok"
		resp["messages"] = ret

		return n.Reply(msg, resp)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if len(neighborhood) == 0 {
			// topology := body["topology"].(map[string]any)
			for _, ne := range n.NodeIDs() {
				if ne == n.ID() {
					continue
				}

				neighborhood = append(neighborhood, ne)
				known[ne] = map[int]int{}
			}
		}

		resp := map[string]any{}
		resp["type"] = "topology_ok"
		return n.Reply(msg, resp)
	})

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	done := make(chan bool)

	rand.Seed(time.Now().UnixMilli())

	go func() {
		for {
			select {
			case <-done:
				done <- true
				return
			case <-ticker.C:
				for _, neighbor := range neighborhood {
					// only gossip to 20% of the nodes
					if rand.Intn(9) >= 1 {
						continue
					}

					msgMu.RLock()
					knownMu.RLock()

					toSend := []int{}
					for _, msg := range messages {
						if _, ok := known[neighbor][msg]; !ok {
							toSend = append(toSend, msg)
						}
					}

					if len(toSend) > 0 {
						if err := n.Send(neighbor, composeGossipMsg(toSend)); err != nil {
							log.Panic(err)
						}
					}

					msgMu.RUnlock()
					knownMu.RUnlock()
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
