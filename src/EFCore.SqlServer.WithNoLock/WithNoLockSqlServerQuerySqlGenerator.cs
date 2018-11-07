using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Expressions;
using Microsoft.EntityFrameworkCore.Query.Sql;
using Microsoft.EntityFrameworkCore.SqlServer.Query.Sql.Internal;
using Microsoft.EntityFrameworkCore.Storage;

namespace Microsoft.EntityFrameworkCore.SqlServer.WithNoLock
{
    internal class WithNoLockSqlServerQuerySqlGenerator : SqlServerQuerySqlGenerator
    {
        private static readonly Func<SelectExpression, RelationalQueryCompilationContext> _getQueryCompilationContext;

        static WithNoLockSqlServerQuerySqlGenerator()
        {
            _getQueryCompilationContext = Compile();
        }

        public WithNoLockSqlServerQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies,
            SelectExpression selectExpression, bool rowNumberPagingEnabled) : base(dependencies, selectExpression,
            rowNumberPagingEnabled)
        {

        }


        public override Expression VisitTable(TableExpression tableExpression)
        {
            var expression = base.VisitTable(tableExpression);

            if (tableExpression is WithNoLockTableExpression)
                Sql.Append(" With(nolock) ");
            return expression;
        }

        public override Expression VisitSelect(SelectExpression selectExpression)
        {
            RelationalQueryCompilationContext queryCompilationContext = _getQueryCompilationContext(selectExpression);
            var isWithNoLock = queryCompilationContext?.QueryAnnotations?.OfType<WithNoLockResultOperator>().Any();
            if (isWithNoLock.HasValue && isWithNoLock.Value)
            {
                IEnumerable<TableExpression> tableExpressions = selectExpression.Tables.OfType<TableExpression>().ToArray();
                foreach (TableExpression tableExpression in tableExpressions)
                {
                    WithNoLockTableExpression withNoLockTableExpression =
                        new WithNoLockTableExpression(tableExpression.Table, tableExpression.Schema,
                            tableExpression.Alias, tableExpression.QuerySource);
                    selectExpression.RemoveTable(tableExpression);
                    selectExpression.AddTable(withNoLockTableExpression);
                }
            }

            return base.VisitSelect(selectExpression);
        }
        
        private static Func<SelectExpression, RelationalQueryCompilationContext> Compile()
        {
            var selectExpressionType = typeof(SelectExpression);

            var fieldInfo = selectExpressionType.GetTypeInfo().GetRuntimeFields()
                .Single(f => f.FieldType == typeof(RelationalQueryCompilationContext));

            var parameterExpression = Expression.Parameter(selectExpressionType, "selectExpression");

            var contextExpression = Expression.Field(parameterExpression, fieldInfo);

            return Expression
                .Lambda<Func<SelectExpression, RelationalQueryCompilationContext>>(contextExpression,
                    parameterExpression).Compile();

        }
    }
}
