package com.ctrip.platform.dal.dao.sqlbuilder;

import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractSqlBuilder.wrapField;
import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.expression;

import java.util.Objects;

import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.Clause;
import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.ClauseList;

/**
 * A factory of static expression methods.
 * 
 * @author jhhe
 *
 */
public class Expressions {
    public static Expression createColumnExpression(String template, String columnName) {
        return new ColumnExpression(template, columnName);
    }

    public static Expression expression(boolean condition, String template) {
        return condition ? new Expression(template) : NULL;
    }
    
    public static Clause expression(boolean condition, String template, String elseTemplate) {
        return condition ? AbstractFreeSqlBuilder.expression(template) : AbstractFreeSqlBuilder.expression(elseTemplate);
    }
    
    public static Clause leftBracket() {
        return new Bracket(true);
    }

    public static Clause rightBracket() {
        return new Bracket(false);
    }
    
    public static Clause bracket(Clause... clauses) {
        ClauseList list = new ClauseList();
        return list.add(leftBracket()).add(clauses).add(rightBracket());
    }

    public static Operator and() {
        return Operator.AND;
    }
    
    public static Operator or() {
        return Operator.OR;
    }
    
    public static Operator not() {
        return Operator.NOT;
    }
    
    public static Expression equal(String columnName) {
        return createColumnExpression("%s = ?", columnName);
    }
    
    public static Expression notEqual(String columnName) {
        return createColumnExpression("%s <> ?", columnName);
    }
    
    public static Expression greaterThan(String columnName) {
        return createColumnExpression("%s > ?", columnName);
    }

    public static Expression greaterThanEquals(String columnName) {
        return createColumnExpression("%s >= ?", columnName);
    }

    public static Expression lessThan(String columnName) {
        return createColumnExpression("%s < ?", columnName);
    }

    public static Expression lessThanEquals(String columnName) {
        return createColumnExpression("%s <= ?", columnName);
    }

    public static Expression between(String columnName) {
        return createColumnExpression("%s BETWEEN ? AND ?", columnName);
    }
    
    public static Expression like(String columnName) {
        return createColumnExpression("%s LIKE ?", columnName);
    }
    
    public static Expression notLike(String columnName) {
        return createColumnExpression("%s NOT LIKE ?", columnName);
    }
    
    public static Expression in(String columnName) {
        return createColumnExpression("%s IN(?)", columnName);
    }
    
    public static Expression notIn(String columnName) {
        return createColumnExpression("%s NOT IN(?)", columnName);
    }
    
    public static Expression isNull(String columnName) {
        return createColumnExpression("%s IS NULL ?", columnName);
    }
    
    public static Expression isNotNull(String columnName) {
        return createColumnExpression("%s IS NOT NULL ?", columnName);
    }
    
    public static class Operator extends Clause {
        private String operator;
        public Operator(String operator) {
            this.operator = operator;
        }
        
        @Override
        public String build() {
            return operator;
        }
        
        public boolean isOperator() {
            return true;
        }
        
        public boolean isClause() {
            return this == NOT;
        }
        
        public static Operator AND = new Operator(" AND ");
        
        public static Operator OR = new Operator(" OR ");
        
        public static Operator NOT = new Operator(" NOT ");
    }
    
    private Operator AND = new Operator(" NOT ");
    
    public static class Bracket extends Clause {
        private boolean left;
        public Bracket(boolean isLeft) {
            left = isLeft;
        }
        
        public String build() {
            return left? "(" : ")";
        }
        
        public boolean isBracket() {
            return true;
        }

        public boolean isLeft() {
            return left;
        }
    }
    
    public static class Expression extends Clause {
        private String template;
        private boolean nullValue = false;
        
        public Expression(String template) {
            this.template = template;
        }
        
        public void nullable(Object o) {
            nullValue = (o == null);
        }
        
        public boolean isNull() {
            return nullValue;
        }
        
        public String build() {
            if(nullValue)
                throw new IllegalStateException("Null expression should not be removed instead of build");
            
            return template;
        }
    }
    
    public static class ColumnExpression extends Expression {
        private String columnName;
        
        public ColumnExpression(String template, String columnName) {
            super(template);
            Objects.requireNonNull(columnName, "column name can not be null");
            this.columnName = columnName;
        }
        
        public String build() {
            String template = super.build();
            return columnName == null ? template : String.format(template, wrapField(getDbCategory(), columnName));
        }
    }
    
    /**
     * This clause is just a placeholder that can be removed from the expression clause list.
     * @author jhhe
     *
     */
    public static class NullClause extends Expression {
        public NullClause() {
            super("");
        }
        
        @Override
        public String build() {
            return "";
        }
    }
    
    public static final Expression NULL = new NullClause();

}
