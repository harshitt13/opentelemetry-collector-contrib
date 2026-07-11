// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottlfuncs // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/internal/metadata"
)

var warnOnce sync.Once

type SetArguments[K any] struct {
	Target ottl.GetSetter[K]
	Value  ottl.Getter[K]
}

func NewSetFactory[K any]() ottl.Factory[K] {
	return ottl.NewFactory("set", &SetArguments[K]{}, createSetFunction[K])
}

func createSetFunction[K any](_ ottl.FunctionContext, oArgs ottl.Arguments) (ottl.ExprFunc[K], error) {
	args, ok := oArgs.(*SetArguments[K])

	if !ok {
		return nil, errors.New("SetFactory args must be of type *SetArguments[K]")
	}

	return set(args.Target, args.Value), nil
}

func set[K any](target ottl.GetSetter[K], value ottl.Getter[K]) ottl.ExprFunc[K] {
	return func(ctx context.Context, tCtx K) (any, error) {
		val, err := value.Get(ctx, tCtx)
		if err != nil {
			return nil, err
		}

		if val == nil {
			if !metadata.OttlSetAllowNilFeatureGate.IsEnabled() {
				warnOnce.Do(func() {
					log.Printf("WARNING: The OTTL 'set' function silently ignored a nil value. This behavior is deprecated and will change in a future release. Enable the feature gate 'ottl.set.allowNil' to use the new zero-value behavior.")
				})
				return nil, nil
			}

			targetVal, targetErr := target.Get(ctx, tCtx)
			if targetErr != nil {
				return nil, fmt.Errorf("error getting target value to infer zero value in set: %w", targetErr)
			}
			// non-nil target value means the path is probably not-nilable
			if targetVal != nil {
				zero := reflect.Zero(reflect.TypeOf(targetVal))
				if !zero.CanInterface() {
					return nil, fmt.Errorf("cannot infer zero value for type %T in set", targetVal)
				}
				val = zero.Interface()
			}
		}

		err = target.Set(ctx, tCtx, val)
		if err != nil {
			return nil, err
		}

		return nil, nil
	}
}
