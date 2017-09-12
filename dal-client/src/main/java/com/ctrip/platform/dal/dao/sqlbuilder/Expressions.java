package com.ctrip.platform.dal.dao.sqlbuilder;

import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.Bracket;
import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.Clause;
import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.ClauseList;
import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.Expression;
import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.NULL;

public class Expressions {
    
    
    public Clause leftBracket() {
        return new Bracket(true);
    }

    public Clause rightBracket() {
        return new Bracket(false);
    }
    
    public Clause bracket(Expression... clauses) {
        ClauseList list = new ClauseList();
        return list.add(leftBracket()).add(clauses).add(rightBracket());
    }

    public static Clause expression(String template) {
        return new Expression(template);
    }
    public static Clause expression(boolean condition, String template) {
        return condition ? expression(template) : NULL;
    }
    
    public static Clause expression(boolean condition, String template, String elseTemplate) {
        return condition ? expression(template) : expression(elseTemplate);
    }
    
    public Expression equal(String fieldName) {
        return new Expression("%s = ?", fieldName);
    }
    
    public Expression notEqual(String fieldName) {
        return new Expression("%s <> ?", fieldName);
    }
    
    public Expression greaterThan(String fieldName) {
        return new Expression("%s > ?", fieldName);
    }

    public Expression greaterThanEquals(String fieldName) {
        return new Expression("%s >= ?", fieldName);
    }

    public Expression lessThan(String fieldName) {
        return new Expression("%s < ?", fieldName);
    }

    public Expression lessThanEquals(String fieldName) {
        return new Expression("%s <= ?", fieldName);
    }

    public Expression between(String fieldName) {
        return new Expression("%s  BETWEEN ? AND ?", fieldName);
    }
    
    public Expression like(String fieldName) {
        return new Expression("%s LIKE ?", fieldName);
    }
    
    public Expression in(String fieldName) {
        return new Expression("%s IN(?)", fieldName);
    }
    
    public Expression isNull(String fieldName) {
        return new Expression("%s IS NULL ?", fieldName);
    }
    
    public Expression isNotNull(String fieldName) {
        return new Expression("%s IS NOT NULL ?", fieldName);
    }
}
