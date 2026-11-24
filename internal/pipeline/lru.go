package pipeline

import (
	"container/list"

	"skiresort/tail-sampling/internal/model"
)

type decisionCache struct {
	capacity int
	ll       *list.List
	items    map[uint64]*list.Element
}

type cacheEntry struct {
	decision model.Decision
}

func newDecisionCache(capacity int) *decisionCache {
	if capacity <= 0 {
		capacity = 1
	}
	return &decisionCache{
		capacity: capacity,
		ll:       list.New(),
		items:    make(map[uint64]*list.Element),
	}
}

// add inserts a decision and returns an evicted decision if capacity was exceeded.
func (c *decisionCache) add(d model.Decision) (evicted *model.Decision) {
	if el, ok := c.items[d.TraceID]; ok {
		c.ll.MoveToFront(el)
		el.Value.(*cacheEntry).decision = d
		return nil
	}
	el := c.ll.PushFront(&cacheEntry{decision: d})
	c.items[d.TraceID] = el
	if c.ll.Len() > c.capacity {
		oldest := c.ll.Back()
		if oldest != nil {
			c.ll.Remove(oldest)
			entry := oldest.Value.(*cacheEntry)
			delete(c.items, entry.decision.TraceID)
			evicted = &entry.decision
		}
	}
	return evicted
}

func (c *decisionCache) get(id uint64) (model.Decision, bool) {
	el, ok := c.items[id]
	if !ok {
		return model.Decision{}, false
	}
	c.ll.MoveToFront(el)
	return el.Value.(*cacheEntry).decision, true
}

func (c *decisionCache) len() int { return c.ll.Len() }
