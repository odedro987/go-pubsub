package maps

import "sync"

type SyncMap[T any] struct {
	sm sync.Map
}

func New[T any]() SyncMap[T] {
	return SyncMap[T]{
		sm: sync.Map{},
	}
}

func (m *SyncMap[T]) Load(key string) (T, bool) {
	val, found := m.sm.Load(key)
	t, converted := val.(T)
	return t, found && converted
}

func (m *SyncMap[T]) Store(key string, value T) {
	m.sm.Store(key, value)
}

func (m *SyncMap[T]) Range(f func(key string, value T) bool) {
	m.sm.Range(func(key, value any) bool {
		return f(key.(string), value.(T))
	})
}
