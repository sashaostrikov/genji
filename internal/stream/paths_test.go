package stream_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/genjidb/genji/types"
	"github.com/stretchr/testify/require"
)

func TestSet(t *testing.T) {
	tests := []struct {
		path  string
		e     expr.Expr
		in    []expr.Expr
		out   []types.Document
		fails bool
	}{
		{
			"a[0].b",
			parser.MustParseExpr(`10`),
			testutil.ParseExprs(t, `{"a": [{}]}`),
			testutil.MakeDocuments(t, `{"a": [{"b": 10}]}`),
			false,
		},
		{
			"a[2]",
			parser.MustParseExpr(`10`),
			testutil.ParseExprs(t, `{"a": [1]}`, `{"a": [1, 2, 3]}`),
			testutil.MakeDocuments(t, `{"a": [1, 2, 10]}`),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			p, err := parser.ParsePath(test.path)
			assert.NoError(t, err)
			s := stream.New(stream.DocsEmit(test.in...)).Pipe(stream.PathsSet(p, test.e))
			i := 0
			err = s.Iterate(new(environment.Environment), func(out *environment.Environment) error {
				d, _ := out.GetDocument()
				require.Equal(t, test.out[i], d)
				i++
				return nil
			})
			if test.fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, stream.PathsSet(document.NewPath("a", "b"), parser.MustParseExpr("1")).String(), "paths.Set(a.b, 1)")
	})
}

func TestUnset(t *testing.T) {
	tests := []struct {
		path  string
		in    []expr.Expr
		out   []types.Document
		fails bool
	}{
		{
			"a",
			testutil.ParseExprs(t, `{"a": 10, "b": 20}`),
			testutil.MakeDocuments(t, `{"b": 20}`),
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			s := stream.New(stream.DocsEmit(test.in...)).Pipe(stream.PathsUnset(test.path))
			i := 0
			err := s.Iterate(new(environment.Environment), func(out *environment.Environment) error {
				d, _ := out.GetDocument()
				require.Equal(t, test.out[i], d)
				i++
				return nil
			})
			if test.fails {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}

	t.Run("String", func(t *testing.T) {
		require.Equal(t, stream.PathsUnset("a").String(), "paths.Unset(a)")
	})
}