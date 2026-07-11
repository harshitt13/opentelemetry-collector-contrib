// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottlfuncs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottltest"
)

func Test_set(t *testing.T) {
	t.Cleanup(ottltest.SetFeatureGateForTest(t, metadata.OttlSetAllowNilFeatureGate, true))

	input := pcommon.NewValueStr("original name")

	target := &ottl.StandardGetSetter[pcommon.Value]{
		Getter: func(_ context.Context, tCtx pcommon.Value) (any, error) {
			return tCtx.Str(), nil
		},
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
		setter  ottl.GetSetter[pcommon.Value]
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
				expectedValue.SetStr("")
			},
			wantErr: false,
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
	t.Cleanup(ottltest.SetFeatureGateForTest(t, metadata.OttlSetAllowNilFeatureGate, true))

	tests := []struct {
		name              string
		targetType        any
		expectedZeroValue any
	}{
		{"string", "old string", ""},
		{"int64", int64(100), int64(0)},
		{"float64", float64(1.5), float64(0)},
		{"bool", true, false},
		{"[]byte", []byte{1, 2, 3}, []byte(nil)},
		{"time.Time", time.Now(), time.Time{}},
		{"time.Duration", time.Second, time.Duration(0)},
		{"[]any", []any{"val"}, []any(nil)},
		{"map[string]any", map[string]any{"key": "val"}, map[string]any(nil)},
		{"pcommon.Map", pcommon.NewMap(), pcommon.Map{}},
		{"pcommon.Slice", pcommon.NewSlice(), pcommon.Slice{}},
		{"pcommon.Value", pcommon.NewValueStr("foo"), pcommon.Value{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setterCalled := false
			setter := &ottl.StandardGetSetter[any]{
				Getter: func(_ context.Context, _ any) (any, error) {
					return tt.targetType, nil
				},
				Setter: func(_ context.Context, _, val any) error {
					setterCalled = true
					assert.Equal(t, tt.expectedZeroValue, val)
					return nil
				},
			}

			getter := &ottl.StandardGetSetter[any]{
				Getter: func(_ context.Context, _ any) (any, error) {
					return nil, nil // Simulate passing 'nil' to set()
				},
			}

			exprFunc := set[any](setter, getter)

			result, err := exprFunc(nil, nil)
			require.NoError(t, err)
			assert.Nil(t, result)
			assert.True(t, setterCalled, "setter should have been called with the zero-value")
		})
	}
}
