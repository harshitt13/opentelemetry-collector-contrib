// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottlfuncs

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

func Test_set(t *testing.T) {
	input := pcommon.NewValueStr("original name")

	target := &ottl.StandardGetSetter[pcommon.Value]{
		Setter: func(_ context.Context, tCtx pcommon.Value, val any) error {
			if val == nil {
				return errors.New("cannot set nil to strict string field")
			}
			tCtx.SetStr(val.(string))
			return nil
		},
	}

	tests := []struct {
		name    string
		setter  ottl.Setter[pcommon.Value]
		getter  ottl.Getter[pcommon.Value]
		want    func(pcommon.Value)
		wantErr bool
	}{
		{
			name:   "set name",
			setter: target,
			getter: ottl.StandardGetSetter[pcommon.Value]{
				Getter: func(_ context.Context, _ pcommon.Value) (any, error) {
					return "new name", nil
				},
			},
			want: func(expectedValue pcommon.Value) {
				expectedValue.SetStr("new name")
			},
			wantErr: false,
		},
		{
			name:   "set nil value",
			setter: target,
			getter: ottl.StandardGetSetter[pcommon.Value]{
				Getter: func(_ context.Context, _ pcommon.Value) (any, error) {
					return nil, nil
				},
			},
			want: func(expectedValue pcommon.Value) {
				expectedValue.SetStr("original name")
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scenarioValue := pcommon.NewValueStr(input.Str())

			exprFunc := set(tt.setter, tt.getter)

			result, err := exprFunc(nil, scenarioValue)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Nil(t, result)

			expected := pcommon.NewValueStr("")
			tt.want(expected)

			assert.Equal(t, expected, scenarioValue)
		})
	}
}

func Test_set_get_nil(t *testing.T) {
	setterCalled := false
	setter := &ottl.StandardGetSetter[any]{
		Setter: func(_ context.Context, _, val any) error {
			setterCalled = true
			assert.Nil(t, val)
			return nil
		},
	}

	getter := &ottl.StandardGetSetter[any]{
		Getter: func(_ context.Context, _ any) (any, error) {
			return nil, nil
		},
	}

	exprFunc := set[any](setter, getter)

	result, err := exprFunc(nil, nil)
	require.NoError(t, err)
	assert.Nil(t, result)
	assert.True(t, setterCalled, "setter should have been called with nil")
}
