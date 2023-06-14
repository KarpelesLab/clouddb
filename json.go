package clouddb

import "fmt"

// ApplyUpdate will update the value of obj with upd, merging changes if both
// are objects. In most other cases upd will be returned as is. Both values
// should be of the same type, or nil.
func ApplyUpdate(obj any, upd any) (any, error) {
	if obj == nil {
		return upd, nil
	}
	if upd == nil {
		return nil, nil
	}

	switch o := obj.(type) {
	case map[string]any:
		u, ok := upd.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected map[string]any update, got %T", upd)
		}
		for k, v := range u {
			if v == nil {
				delete(o, k)
				continue
			}
			prev, ok := o[k]
			if !ok {
				o[k] = v
			} else {
				if nv, err := ApplyUpdate(prev, v); err != nil {
					return nil, err
				} else if nv == nil {
					delete(o, k)
				} else {
					o[k] = nv
				}
			}
		}
		return o, nil
	default:
		return upd, nil
	}
}

// ComputeDelta compares two objects (of type map[string]any) and returns an
// object that contains all the changes found in now while skipping any value
// that might be equal.
func ComputeDelta(old, now any) (any, error) {
	if old == nil {
		return now, nil
	}
	if now == nil {
		return nil, nil
	}

	switch o := old.(type) {
	case map[string]any:
		u, ok := now.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected map[string]any now, got %T", now)
		}

		delta := make(map[string]any)

		for k, v := range u {
			prev, ok := o[k]
			if !ok {
				// new value
				delta[k] = v
				continue
			}
			if v == prev {
				// will this work for stuff like map[string]any? ideally we want such cases to always return false
				continue
			}
			if nd, err := ComputeDelta(prev, v); err != nil {
				return nil, err
			} else if m, ok := nd.(map[string]any); ok && len(m) == 0 {
				// do nothing
			} else {
				delta[k] = nd
			}
		}
		return delta, nil
	default:
		return now, nil
	}
}
