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
