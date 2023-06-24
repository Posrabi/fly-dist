package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	LAST_OFFSET = "last_offset"
	CO          = "commited_offsets"
	LOGS        = "logs"
)

func createCOKey(key string) string {
	return fmt.Sprintf("%s/%s", CO, key)
}

func createLogsKey(key string) string {
	return fmt.Sprintf("%s/%s", LOGS, key)
}

func send(linKv *maelstrom.KV, key string, msg, offset int) {
	for {
		logs := []mess{}
		if err := linKv.ReadInto(context.Background(), createLogsKey(key), &logs); err != nil {
			e := err.(*maelstrom.RPCError)
			if e.Code != maelstrom.KeyDoesNotExist {
				log.Fatal(err)
			}
		}

		tmp := append([]mess{}, logs...)
		tmp = append(tmp, mess{
			Msg:    msg,
			Offset: offset,
		})

		if err := linKv.CompareAndSwap(context.Background(), createLogsKey(key), logs, tmp, true); err != nil {
			continue
		}

		return
	}
}

type mess struct {
	Msg    int
	Offset int
}

func main() {
	n := maelstrom.NewNode()
	seqKv := maelstrom.NewSeqKV(n)
	linKv := maelstrom.NewLinKV(n)

	n.Handle("init", func(_ maelstrom.Message) error {
		return seqKv.Write(context.Background(), LAST_OFFSET, 1)
	})

	n.Handle("send", func(m maelstrom.Message) error {
		body := map[string]any{}
		if err := json.Unmarshal(m.Body, &body); err != nil {
			return err
		}

		key := body["key"].(string)
		msg := int(body["msg"].(float64))
		resp := map[string]any{}
		resp["type"] = "send_ok"

		for {
			offset, err := seqKv.ReadInt(context.Background(), LAST_OFFSET)
			if err != nil {
				return err
			}

			if err := seqKv.CompareAndSwap(context.Background(), LAST_OFFSET, offset, offset+1, false); err != nil {
				continue
			}

			resp["offset"] = offset
			break
		}

		send(linKv, key, msg, resp["offset"].(int))

		return n.Reply(m, resp)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		body := map[string]any{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		resp := map[string]any{}
		resp["type"] = "poll_ok"
		msgs := map[string][][]int{}
		offsets := body["offsets"].(map[string]any)

		for key, o := range offsets {
			offset := int(o.(float64))

			logs := []mess{}
			if err := linKv.ReadInto(context.Background(), createLogsKey(key), &logs); err != nil {
				e := err.(*maelstrom.RPCError)
				if e.Code != maelstrom.KeyDoesNotExist {
					return err
				}
			}

			for _, l := range logs {
				if l.Offset >= offset {
					msgs[key] = append(msgs[key], []int{l.Offset, l.Msg})
				}
			}

			sort.Slice(msgs[key], func(i, j int) bool {
				return msgs[key][i][0] < msgs[key][j][0]
			})
		}

		resp["msgs"] = msgs
		return n.Reply(msg, resp)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		body := map[string]any{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := body["offsets"].(map[string]any)
		for key, o := range offsets {
			offset := int(o.(float64))

			if err := seqKv.Write(context.Background(), createCOKey(key), offset); err != nil {
				return err
			}
		}

		// commit(linKv, offsets)

		resp := map[string]any{}
		resp["type"] = "commit_offsets_ok"

		return n.Reply(msg, resp)
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		body := map[string]any{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		keys := body["keys"].([]any)

		resp := map[string]any{}
		resp["type"] = "list_committed_offsets_ok"

		offsets := map[string]any{}
		for _, k := range keys {
			key := k.(string)

			commited_offset, err := seqKv.ReadInt(context.Background(), createCOKey(key))
			if err != nil {
				e := err.(*maelstrom.RPCError)
				if e.Code != maelstrom.KeyDoesNotExist {
					return err
				}

				commited_offset = 0
			}

			offsets[key] = commited_offset
		}

		resp["offsets"] = offsets
		return n.Reply(msg, resp)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
