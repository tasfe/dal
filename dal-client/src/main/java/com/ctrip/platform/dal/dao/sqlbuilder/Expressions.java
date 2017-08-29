package com.ctrip.platform.dal.dao.sqlbuilder;

import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.BracketClause;
import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.Clause;
import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.ClauseList;
import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.ExpressionClause;
import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.NULL;

public class Expressions {
    
    
    public Clause leftBracket() {
        return new BracketClause(true);
    }

    public Clause rightBracket() {
        return new BracketClause(false);
    }
    
    public Clause bracket(ExpressionClause... clauses) {
        ClauseList list = new ClauseList();
        return list.add(leftBracket()).add(clauses).add(rightBracket());
    }

    public static Clause expression(String template) {
        return new ExpressionClause(template);
    }
    public static Clause expression(boolean condition, String template) {
        return condition ? expression(template) : NULL;
    }
    
    public static Clause expression(boolean condition, String template, String elseTemplate) {
        return condition ? expression(template) : expression(elseTemplate);
    }
    
    public ExpressionClause equal(String fieldName) {
        return new ExpressionClause("%s = ?", fieldName);
    }
    
    public ExpressionClause notEqual(String fieldName) {
        return new ExpressionClause("%s <> ?", fieldName);
    }
    
    public ExpressionClause greaterThan(String fieldName) {
        return new ExpressionClause("%s > ?", fieldName);
    }

    public ExpressionClause greaterThanEquals(String fieldName) {
        return new ExpressionClause("%s >= ?", fieldName);
    }

    public ExpressionClause lessThan(String fieldName) {
        return new ExpressionClause("%s < ?", fieldName);
    }

    public ExpressionClause lessThanEquals(String fieldName) {
        return new ExpressionClause("%s <= ?", fieldName);
    }

    public ExpressionClause between(String fieldName) {
        return new ExpressionClause("%s  BETWEEN ? AND ?", fieldName);
    }
    
    public ExpressionClause like(String fieldName) {
        return new ExpressionClause("%s LIKE ?", fieldName);
    }
    
    public ExpressionClause in(String fieldName) {
        return new ExpressionClause("%s IN(?)", fieldName);
    }
    
    public ExpressionClause isNull(String fieldName) {
        return new ExpressionClause("%s IS NULL ?", fieldName);
    }
    
    public ExpressionClause isNotNull(String fieldName) {
        return new ExpressionClause("%s IS NOT NULL ?", fieldName);
    }
}
