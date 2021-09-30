package parser_test

import (
	"testing"

	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/expr/functions"
	"github.com/genjidb/genji/internal/query/statement"
	"github.com/genjidb/genji/internal/sql/parser"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/testutil"
	"github.com/genjidb/genji/internal/testutil/assert"
	"github.com/stretchr/testify/require"
)

func TestParserSelect(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		expected *stream.Stream
		readOnly bool
		mustFail bool
	}{
		{"NoTable", "SELECT 1",
			stream.New(stream.Project(testutil.ParseNamedExpr(t, "1"))),
			true, false,
		},
		{"NoTableWithTuple", "SELECT (1, 2)",
			stream.New(stream.Project(testutil.ParseNamedExpr(t, "[1, 2]"))),
			true, false,
		},
		{"NoTableWithBrackets", "SELECT [1, 2]",
			stream.New(stream.Project(testutil.ParseNamedExpr(t, "[1, 2]"))),
			true, false,
		},
		{"NoTableWithINOperator", "SELECT 1 in (1, 2), 3",
			stream.New(stream.Project(
				testutil.ParseNamedExpr(t, "1 IN [1, 2]"),
				testutil.ParseNamedExpr(t, "3"),
			)),
			true, false,
		},
		{"NoCond", "SELECT * FROM test",
			stream.New(stream.SeqScan("test")).Pipe(stream.Project(expr.Wildcard{})),
			true, false,
		},
		{"WithFields", "SELECT a, b FROM test",
			stream.New(stream.SeqScan("test")).Pipe(stream.Project(testutil.ParseNamedExpr(t, "a"), testutil.ParseNamedExpr(t, "b"))),
			true, false,
		},
		{"WithFieldsWithQuotes", "SELECT `long \"path\"` FROM test",
			stream.New(stream.SeqScan("test")).Pipe(stream.Project(testutil.ParseNamedExpr(t, "`long \"path\"`", "long \"path\""))),
			true, false,
		},
		{"WithAlias", "SELECT a AS A, b FROM test",
			stream.New(stream.SeqScan("test")).Pipe(stream.Project(testutil.ParseNamedExpr(t, "a", "A"), testutil.ParseNamedExpr(t, "b"))),
			true, false,
		},
		{"WithFields and wildcard", "SELECT a, b, * FROM test",
			stream.New(stream.SeqScan("test")).Pipe(stream.Project(testutil.ParseNamedExpr(t, "a"), testutil.ParseNamedExpr(t, "b"), expr.Wildcard{})),
			true, false,
		},
		{"WithExpr", "SELECT a    > 1 FROM test",
			stream.New(stream.SeqScan("test")).Pipe(stream.Project(testutil.ParseNamedExpr(t, "a > 1"))),
			true, false,
		},
		{"WithCond", "SELECT * FROM test WHERE age = 10",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.Project(expr.Wildcard{})),
			true, false,
		},
		{"WithGroupBy", "SELECT a.b.c FROM test WHERE age = 10 GROUP BY a.b.c",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.Sort(parser.MustParseExpr("a.b.c"))).
				Pipe(stream.GroupAggregate(parser.MustParseExpr("a.b.c"))).
				Pipe(stream.Project(&expr.NamedExpr{ExprName: "a.b.c", Expr: expr.Path(document.NewPath("a.b.c"))})),
			true, false,
		},
		{"WithOrderBy", "SELECT * FROM test WHERE age = 10 ORDER BY a.b.c",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.Project(expr.Wildcard{})).
				Pipe(stream.Sort(testutil.ParsePath(t, "a.b.c"))),
			true, false,
		},
		{"WithOrderBy ASC", "SELECT * FROM test WHERE age = 10 ORDER BY a.b.c ASC",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.Project(expr.Wildcard{})).
				Pipe(stream.Sort(testutil.ParsePath(t, "a.b.c"))),
			true, false,
		},
		{"WithOrderBy DESC", "SELECT * FROM test WHERE age = 10 ORDER BY a.b.c DESC",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.Project(expr.Wildcard{})).
				Pipe(stream.SortReverse(testutil.ParsePath(t, "a.b.c"))),
			true, false,
		},
		{"WithLimit", "SELECT * FROM test WHERE age = 10 LIMIT 20",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.Project(expr.Wildcard{})).
				Pipe(stream.Take(20)),
			true, false,
		},
		{"WithOffset", "SELECT * FROM test WHERE age = 10 OFFSET 20",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.Project(expr.Wildcard{})).
				Pipe(stream.Skip(20)),
			true, false,
		},
		{"WithLimitThenOffset", "SELECT * FROM test WHERE age = 10 LIMIT 10 OFFSET 20",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
				Pipe(stream.Project(expr.Wildcard{})).
				Pipe(stream.Skip(20)).
				Pipe(stream.Take(10)),
			true, false,
		},
		{"WithOffsetThenLimit", "SELECT * FROM test WHERE age = 10 OFFSET 20 LIMIT 10", nil, true, true},
		{"With aggregation function", "SELECT COUNT(*) FROM test",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.GroupAggregate(nil, &functions.Count{Wildcard: true})).
				Pipe(stream.Project(testutil.ParseNamedExpr(t, "COUNT(*)"))),
			true, false},
		{"With NEXT VALUE FOR", "SELECT NEXT VALUE FOR foo FROM test",
			stream.New(stream.SeqScan("test")).
				Pipe(stream.Project(testutil.ParseNamedExpr(t, "NEXT VALUE FOR foo"))),
			false, false},
		{"WithUnionAll", "SELECT * FROM test1 UNION ALL SELECT * FROM test2",
			stream.New(stream.Concat(
				stream.New(stream.SeqScan("test1")).Pipe(stream.Project(expr.Wildcard{})),
				stream.New(stream.SeqScan("test2")).Pipe(stream.Project(expr.Wildcard{})),
			)),
			true, false,
		},
		{"CondWithUnionAll", "SELECT * FROM test1 WHERE age = 10 UNION ALL SELECT * FROM test2",
			stream.New(stream.Concat(
				stream.New(stream.SeqScan("test1")).
					Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
					Pipe(stream.Project(expr.Wildcard{})),
				stream.New(stream.SeqScan("test2")).Pipe(stream.Project(expr.Wildcard{})),
			)),
			true, false,
		},
		{"WithUnionAllAfterOrderBy", "SELECT * FROM test1 ORDER BY a UNION ALL SELECT * FROM test2",
			nil,
			true, true,
		},
		{"WithUnionAllAfterLimit", "SELECT * FROM test1 LIMIT 10 UNION ALL SELECT * FROM test2",
			nil,
			true, true,
		},
		{"WithUnionAllAfterOffset", "SELECT * FROM test1 OFFSET 10 UNION ALL SELECT * FROM test2",
			nil,
			true, true,
		},
		{"WithUnionAllAndOrderBy", "SELECT * FROM test1 UNION ALL SELECT * FROM test2 ORDER BY a",
			stream.New(stream.Concat(
				stream.New(stream.SeqScan("test1")).Pipe(stream.Project(expr.Wildcard{})),
				stream.New(stream.SeqScan("test2")).Pipe(stream.Project(expr.Wildcard{})),
			)).Pipe(stream.Sort(testutil.ParsePath(t, "a"))),
			true, false,
		},
		{"WithUnionAllAndLimit", "SELECT * FROM test1 UNION ALL SELECT * FROM test2 LIMIT 10",
			stream.New(stream.Concat(
				stream.New(stream.SeqScan("test1")).Pipe(stream.Project(expr.Wildcard{})),
				stream.New(stream.SeqScan("test2")).Pipe(stream.Project(expr.Wildcard{})),
			)).Pipe(stream.Take(10)),
			true, false,
		},
		{"WithUnionAllAndOffset", "SELECT * FROM test1 UNION ALL SELECT * FROM test2 OFFSET 20",
			stream.New(stream.Concat(
				stream.New(stream.SeqScan("test1")).Pipe(stream.Project(expr.Wildcard{})),
				stream.New(stream.SeqScan("test2")).Pipe(stream.Project(expr.Wildcard{})),
			)).Pipe(stream.Skip(20)),
			true, false,
		},
		{"WithUnionAllAndOrderByAndLimitAndOffset", "SELECT * FROM test1 UNION ALL SELECT * FROM test2 ORDER BY a LIMIT 10 OFFSET 20",
			stream.New(stream.Concat(
				stream.New(stream.SeqScan("test1")).Pipe(stream.Project(expr.Wildcard{})),
				stream.New(stream.SeqScan("test2")).Pipe(stream.Project(expr.Wildcard{})),
			)).Pipe(stream.Sort(testutil.ParsePath(t, "a"))).Pipe(stream.Skip(20)).Pipe(stream.Take(10)),
			true, false,
		},

		{"WithUnion", "SELECT * FROM test1 UNION SELECT * FROM test2",
			stream.New(stream.Union(
				stream.New(stream.SeqScan("test1")).Pipe(stream.Project(expr.Wildcard{})),
				stream.New(stream.SeqScan("test2")).Pipe(stream.Project(expr.Wildcard{})),
			)),
			true, false,
		},
		{"CondWithUnion", "SELECT * FROM test1 WHERE age = 10 UNION SELECT * FROM test2",
			stream.New(stream.Union(
				stream.New(stream.SeqScan("test1")).
					Pipe(stream.Filter(parser.MustParseExpr("age = 10"))).
					Pipe(stream.Project(expr.Wildcard{})),
				stream.New(stream.SeqScan("test2")).Pipe(stream.Project(expr.Wildcard{})),
			)),
			true, false,
		},
		{"WithUnionAfterOrderBy", "SELECT * FROM test1 ORDER BY a UNION SELECT * FROM test2",
			nil,
			true, true,
		},
		{"WithUnionAfterLimit", "SELECT * FROM test1 LIMIT 10 UNION SELECT * FROM test2",
			nil,
			true, true,
		},
		{"WithUnionAfterOffset", "SELECT * FROM test1 OFFSET 10 UNION SELECT * FROM test2",
			nil,
			true, true,
		},
		{"WithUnionAndOrderBy", "SELECT * FROM test1 UNION SELECT * FROM test2 ORDER BY a",
			stream.New(stream.Union(
				stream.New(stream.SeqScan("test1")).Pipe(stream.Project(expr.Wildcard{})),
				stream.New(stream.SeqScan("test2")).Pipe(stream.Project(expr.Wildcard{})),
			)).Pipe(stream.Sort(testutil.ParsePath(t, "a"))),
			true, false,
		},
		{"WithUnionAndLimit", "SELECT * FROM test1 UNION SELECT * FROM test2 LIMIT 10",
			stream.New(stream.Union(
				stream.New(stream.SeqScan("test1")).Pipe(stream.Project(expr.Wildcard{})),
				stream.New(stream.SeqScan("test2")).Pipe(stream.Project(expr.Wildcard{})),
			)).Pipe(stream.Take(10)),
			true, false,
		},
		{"WithUnionAndOffset", "SELECT * FROM test1 UNION SELECT * FROM test2 OFFSET 20",
			stream.New(stream.Union(
				stream.New(stream.SeqScan("test1")).Pipe(stream.Project(expr.Wildcard{})),
				stream.New(stream.SeqScan("test2")).Pipe(stream.Project(expr.Wildcard{})),
			)).Pipe(stream.Skip(20)),
			true, false,
		},
		{"WithUnionAndOrderByAndLimitAndOffset", "SELECT * FROM test1 UNION SELECT * FROM test2 ORDER BY a LIMIT 10 OFFSET 20",
			stream.New(stream.Union(
				stream.New(stream.SeqScan("test1")).Pipe(stream.Project(expr.Wildcard{})),
				stream.New(stream.SeqScan("test2")).Pipe(stream.Project(expr.Wildcard{})),
			)).Pipe(stream.Sort(testutil.ParsePath(t, "a"))).Pipe(stream.Skip(20)).Pipe(stream.Take(10)),
			true, false,
		},
		{"WithMultipleCompoundOps/1", "SELECT * FROM a UNION ALL SELECT * FROM b UNION ALL SELECT * FROM c",
			stream.New(stream.Concat(
				stream.New(stream.SeqScan("a")).Pipe(stream.Project(expr.Wildcard{})),
				stream.New(stream.SeqScan("b")).Pipe(stream.Project(expr.Wildcard{})),
				stream.New(stream.SeqScan("c")).Pipe(stream.Project(expr.Wildcard{})),
			)),
			true, false,
		},
		{"WithMultipleCompoundOps/2", "SELECT * FROM a UNION ALL SELECT * FROM b UNION SELECT * FROM c",
			stream.New(stream.Union(
				stream.New(stream.Concat(
					stream.New(stream.SeqScan("a")).Pipe(stream.Project(expr.Wildcard{})),
					stream.New(stream.SeqScan("b")).Pipe(stream.Project(expr.Wildcard{})),
				)),
				stream.New(stream.SeqScan("c")).Pipe(stream.Project(expr.Wildcard{})),
			)),
			true, false,
		},
		{"WithMultipleCompoundOps/2", "SELECT * FROM a UNION ALL SELECT * FROM b UNION ALL SELECT * FROM c UNION SELECT * FROM d",
			stream.New(stream.Union(
				stream.New(stream.Concat(
					stream.New(stream.SeqScan("a")).Pipe(stream.Project(expr.Wildcard{})),
					stream.New(stream.SeqScan("b")).Pipe(stream.Project(expr.Wildcard{})),
					stream.New(stream.SeqScan("c")).Pipe(stream.Project(expr.Wildcard{})),
				)),
				stream.New(stream.SeqScan("d")).Pipe(stream.Project(expr.Wildcard{})),
			)),
			true, false,
		},
		{"WithMultipleCompoundOps/3", "SELECT * FROM a UNION ALL SELECT * FROM b UNION SELECT * FROM c UNION SELECT * FROM d",
			stream.New(stream.Union(
				stream.New(stream.Concat(
					stream.New(stream.SeqScan("a")).Pipe(stream.Project(expr.Wildcard{})),
					stream.New(stream.SeqScan("b")).Pipe(stream.Project(expr.Wildcard{})),
				)),
				stream.New(stream.SeqScan("c")).Pipe(stream.Project(expr.Wildcard{})),
				stream.New(stream.SeqScan("d")).Pipe(stream.Project(expr.Wildcard{})),
			)),
			true, false,
		},
		{"WithMultipleCompoundOps/4", "SELECT * FROM a UNION ALL SELECT * FROM b UNION SELECT * FROM c UNION ALL SELECT * FROM d",
			stream.New(stream.Concat(
				stream.New(stream.Union(
					stream.New(stream.Concat(
						stream.New(stream.SeqScan("a")).Pipe(stream.Project(expr.Wildcard{})),
						stream.New(stream.SeqScan("b")).Pipe(stream.Project(expr.Wildcard{})),
					)),
					stream.New(stream.SeqScan("c")).Pipe(stream.Project(expr.Wildcard{})),
				)),
				stream.New(stream.SeqScan("d")).Pipe(stream.Project(expr.Wildcard{})),
			)),
			true, false,
		},
		{"WithMultipleCompoundOpsAndNextValueFor/4", "SELECT * FROM a UNION ALL SELECT * FROM b UNION SELECT * FROM c UNION ALL SELECT NEXT VALUE FOR foo FROM d",
			stream.New(stream.Concat(
				stream.New(stream.Union(
					stream.New(stream.Concat(
						stream.New(stream.SeqScan("a")).Pipe(stream.Project(expr.Wildcard{})),
						stream.New(stream.SeqScan("b")).Pipe(stream.Project(expr.Wildcard{})),
					)),
					stream.New(stream.SeqScan("c")).Pipe(stream.Project(expr.Wildcard{})),
				)),
				stream.New(stream.SeqScan("d")).Pipe(stream.Project(testutil.ParseNamedExpr(t, "NEXT VALUE FOR foo"))),
			)),
			false, false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q, err := parser.ParseQuery(test.s)
			if !test.mustFail {
				assert.NoError(t, err)
				require.Len(t, q.Statements, 1)
				require.EqualValues(t, &statement.StreamStmt{Stream: test.expected, ReadOnly: test.readOnly}, q.Statements[0].(*statement.StreamStmt))
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func BenchmarkSelect(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = parser.ParseQuery("SELECT a, b.c[100].d AS `foo` FROM `some table` WHERE d.e[100] >= 12 AND c.d IN ([1, true], [2, false]) GROUP BY d.e[0] LIMIT 10 + 10 OFFSET 20 - 20 ORDER BY d DESC")
	}
}
