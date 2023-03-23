package watchset

type iterable[V any] interface {
	ForEach(fn func(v V))
}

func unique[V comparable](values []V) []V {
	m := make(map[V]struct{})
	for _, v := range values {
		m[v] = struct{}{}
	}
	var out []V
	for k := range m {
		out = append(out, k)
	}
	return out
}

func transform[V any, V2 any](items []V, fn func(v V) V2) []V2 {
	var out []V2
	for _, v := range items {
		out = append(out, fn(v))
	}
	return out
}

type iterableSlice[V any] struct {
	values []V
}

func (s *iterableSlice[V]) ForEach(fn func(v V)) {
	for _, v := range s.values {
		fn(v)
	}
}

func sliceToIterable[V any](t []V) iterable[V] {
	return &iterableSlice[V]{values: t}
}
