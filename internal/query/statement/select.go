package statement

import (
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/internal/environment"
	"github.com/genjidb/genji/internal/errors"
	"github.com/genjidb/genji/internal/expr"
	"github.com/genjidb/genji/internal/sql/scanner"
	"github.com/genjidb/genji/internal/stream"
	"github.com/genjidb/genji/internal/stringutil"
)

type SelectCoreStmt struct {
	TableName       string
	Distinct        bool
	WhereExpr       expr.Expr
	GroupByExpr     expr.Expr
	ProjectionExprs []expr.Expr
}

func (stmt *SelectCoreStmt) ToStream() (*StreamStmt, error) {
	isReadOnly := true

	var s *stream.Stream

	if stmt.TableName != "" {
		s = s.Pipe(stream.SeqScan(stmt.TableName))
	}

	if stmt.WhereExpr != nil {
		s = s.Pipe(stream.Filter(stmt.WhereExpr))
	}

	// when using GROUP BY, only aggregation functions or GroupByExpr can be selected
	if stmt.GroupByExpr != nil {
		var invalidProjectedField expr.Expr
		var aggregators []expr.AggregatorBuilder

		for i, pe := range stmt.ProjectionExprs {
			ne, ok := pe.(*expr.NamedExpr)
			if !ok {
				invalidProjectedField = pe
				break
			}
			e := ne.Expr

			// check if the projected expression is an aggregation function
			if agg, ok := e.(expr.AggregatorBuilder); ok {
				aggregators = append(aggregators, agg)
				continue
			}

			// check if this is the same expression as the one used in the GROUP BY clause
			if expr.Equal(e, stmt.GroupByExpr) {
				// if so, replace the expression with a path expression
				stmt.ProjectionExprs[i] = &expr.NamedExpr{
					ExprName: ne.ExprName,
					Expr:     expr.Path(document.NewPath(e.String())),
				}
				continue
			}

			// otherwise it's an error
			invalidProjectedField = ne
			break
		}

		if invalidProjectedField != nil {
			return nil, stringutil.Errorf("field %q must appear in the GROUP BY clause or be used in an aggregate function", invalidProjectedField)
		}
		// add Aggregation node
		s = s.Pipe(stream.Sort(stmt.GroupByExpr))
		s = s.Pipe(stream.GroupAggregate(stmt.GroupByExpr, aggregators...))
	} else {
		// if there is no GROUP BY clause, check if there are any aggregation function
		// and if so add an aggregation node
		var aggregators []expr.AggregatorBuilder

		for _, pe := range stmt.ProjectionExprs {
			ne, ok := pe.(*expr.NamedExpr)
			if !ok {
				continue
			}
			e := ne.Expr

			// check if the projected expression is an aggregation function
			if agg, ok := e.(expr.AggregatorBuilder); ok {
				aggregators = append(aggregators, agg)
			}
		}

		// add Aggregation node
		if len(aggregators) > 0 {
			s = s.Pipe(stream.GroupAggregate(nil, aggregators...))
		}
	}

	// If there is no FROM clause ensure there is no wildcard or path
	if stmt.TableName == "" {
		var err error

		for _, e := range stmt.ProjectionExprs {
			expr.Walk(e, func(e expr.Expr) bool {
				switch e.(type) {
				case expr.Path, expr.Wildcard:
					err = errors.New("no tables specified")
					return false
				default:
					return true
				}
			})
			if err != nil {
				return nil, err
			}
		}
	}
	s = s.Pipe(stream.Project(stmt.ProjectionExprs...))

	// SELECT is read-only most of the time, unless it's using some expressions
	// that require write access and that are allowed to be run, such as NEXT VALUE FOR
	for _, e := range stmt.ProjectionExprs {
		expr.Walk(e, func(e expr.Expr) bool {
			switch e.(type) {
			case expr.NextValueFor:
				isReadOnly = false
				return false
			default:
				return true
			}
		})
	}

	if stmt.Distinct {
		s = stream.New(stream.Union(s))
	}

	return &StreamStmt{
		Stream:   s,
		ReadOnly: isReadOnly,
	}, nil
}

// SelectStmt holds SELECT configuration.
type SelectStmt struct {
	CompoundSelect   *StreamStmt
	OrderBy          expr.Path
	OrderByDirection scanner.Token
	OffsetExpr       expr.Expr
	LimitExpr        expr.Expr
}

func (stmt *SelectStmt) ToStream() (*StreamStmt, error) {
	s := stmt.CompoundSelect.Stream

	if stmt.OrderBy != nil {
		if stmt.OrderByDirection == scanner.DESC {
			s = s.Pipe(stream.SortReverse(stmt.OrderBy))
		} else {
			s = s.Pipe(stream.Sort(stmt.OrderBy))
		}
	}

	if stmt.OffsetExpr != nil {
		v, err := stmt.OffsetExpr.Eval(&environment.Environment{})
		if err != nil {
			return nil, err
		}

		if !v.Type().IsNumber() {
			return nil, stringutil.Errorf("offset expression must evaluate to a number, got %q", v.Type())
		}

		v, err = document.CastAsInteger(v)
		if err != nil {
			return nil, err
		}

		s = s.Pipe(stream.Skip(v.V().(int64)))
	}

	if stmt.LimitExpr != nil {
		v, err := stmt.LimitExpr.Eval(&environment.Environment{})
		if err != nil {
			return nil, err
		}

		if !v.Type().IsNumber() {
			return nil, stringutil.Errorf("limit expression must evaluate to a number, got %q", v.Type())
		}

		v, err = document.CastAsInteger(v)
		if err != nil {
			return nil, err
		}

		s = s.Pipe(stream.Take(v.V().(int64)))
	}

	return &StreamStmt{
		Stream:   s,
		ReadOnly: stmt.CompoundSelect.ReadOnly,
	}, nil
}
