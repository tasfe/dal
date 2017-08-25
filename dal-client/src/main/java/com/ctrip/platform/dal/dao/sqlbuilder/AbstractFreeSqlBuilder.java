package com.ctrip.platform.dal.dao.sqlbuilder;

import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractSqlBuilder.wrapField;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.ctrip.platform.dal.common.enums.DatabaseCategory;
import com.ctrip.platform.dal.dao.DalHints;

/**
 * This sql builder only handles template creation. It will not do with the parameters
 * for now.
 * 
 * @author jhhe
 *
 */
public abstract class AbstractFreeSqlBuilder implements SqlBuilder {
    protected DatabaseCategory dbCategory = DatabaseCategory.MySql;

    private String logicDbName;
    private DalHints hints;
    private List<Clause> clauses = new ArrayList<>();
    
    public AbstractFreeSqlBuilder setLogicDbName() {
        this.logicDbName = logicDbName;
        return this;
    }
    
    public void setHints(DalHints hints) {
        this.hints = hints;
    }

    public AbstractFreeSqlBuilder setDatabaseCategory(DatabaseCategory dbCategory) throws SQLException {
        Objects.requireNonNull(dbCategory, "DatabaseCategory can't be null.");
        this.dbCategory = dbCategory;
        return this;
    }
    
    public AbstractFreeSqlBuilder append(String template) {
        return addSimpleClause(template);
    }
    
    /**
     * Append when the condition is met
     * @param condition
     * @param template
     * @return
     */
    public AbstractFreeSqlBuilder appendIf(boolean condition, String template) {
        return condition ? addSimpleClause(template): this;
    }
    
    public AbstractFreeSqlBuilder appendIf(boolean condition, Clause clause) {
        return condition ? add(clause) : this;
    }
    
    /**
     * Append template depends on whether the condition is met.
     * @param condition
     * @param template value to be appended when condition is true
     * @param elseTemplate value to be appended when condition is true
     * @return
     */
    public AbstractFreeSqlBuilder appendIf(boolean condition, String template, String elseTemplate) {
        return condition ? addSimpleClause(template): addSimpleClause(elseTemplate);
    }
    
    public AbstractFreeSqlBuilder appendIf(boolean condition, Clause clause, Clause elseClause) {
        return condition ? add(clause) : add(elseClause);
    }

    
    /**
     * Append names separate by seperator
     * @param names
     * @param separator
     * @return
     */
    public AbstractFreeSqlBuilder append(String[] names, String separator) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < names.length; i++) {
            sb.append(names[i]);
            if(i != names.length -1)
                sb.append(separator);    
        }
        return addSimpleClause(sb.toString());
    }
    
    /**
     * Append template with %s replaced by names and separate by seperator
     * @param template must have %s to be replaced by names
     * @param names
     * @param separator
     * @return
     */
    public AbstractFreeSqlBuilder append(String template, String[] names, String separator) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < names.length; i++) {
            sb.append(String.format(template, names[i]));
            if(i != names.length -1)
                sb.append(separator);    
        }
        return addSimpleClause(sb.toString());
    }

    public AbstractFreeSqlBuilder append(Clause clause) {
        return add(clause);
    }

    /**
     * The tableName will be replaced by true table name if it is a logic table that allow shard
     * @param tableName table name. The table can be sharded
     * @return
     */
    public AbstractFreeSqlBuilder appendTable(String tableName) {
        return add(new TableClause(tableName));
    }
    
    public AbstractFreeSqlBuilder appendWithTable(String template, String tableName) {
        return this;
    }
    
    public AbstractFreeSqlBuilder appendWithTable(String template, TableClause tableClause) {
        return this;
    }
    
    public AbstractFreeSqlBuilder bracket(Clause clause) {
        return leftBracket().add(clause).rightBracket();
    }
    
    public AbstractFreeSqlBuilder leftBracket() {
        return this;
    }

    public AbstractFreeSqlBuilder rightBracket() {
        return this;
    }
    
    public AbstractFreeSqlBuilder and() {
        return add(OperatorClause.and());
    }
    
    public AbstractFreeSqlBuilder or() {
        return add(OperatorClause.or());
    }
    
    public AbstractFreeSqlBuilder not() {
        return add(OperatorClause.not());
    }
    
    /**
     * Below are handy methods that append common clauses
     */

    public AbstractFreeSqlBuilder selectFrom(String[] names, TableClause table) {
        return this;
    }
    
    public AbstractFreeSqlBuilder selectFrom(List<String> names, TableClause table) {
        return this;
    }
    
    public AbstractFreeSqlBuilder where() {
        return this;
    }
    
    public AbstractFreeSqlBuilder equal(String fieldName) {
        return addExpClause("%s = ?", fieldName);
    }
    
    public AbstractFreeSqlBuilder notEqual(String fieldName) {
        return addExpClause("%s <> ?", fieldName);
    }
    
    public AbstractFreeSqlBuilder greaterThan(String fieldName) {
        return addExpClause("%s > ?", fieldName);
    }

    public AbstractFreeSqlBuilder greaterThanEquals(String fieldName) {
        return addExpClause("%s >= ?", fieldName);
    }

    public AbstractFreeSqlBuilder lessThan(String fieldName) {
        return addExpClause("%s < ?", fieldName);
    }

    public AbstractFreeSqlBuilder lessThanEquals(String fieldName) {
        return addExpClause("%s <= ?", fieldName);
    }

    public AbstractFreeSqlBuilder between(String fieldName) {
        return addExpClause("%s  BETWEEN ? AND ?", fieldName);
    }
    
    public AbstractFreeSqlBuilder like(String fieldName) {
        return addExpClause("%s LIKE ?", fieldName);
    }
    
    public AbstractFreeSqlBuilder in(String fieldName) {
        return addExpClause("%s IN(?)", fieldName);
    }
    
    public AbstractFreeSqlBuilder isNull(String fieldName) {
        return addExpClause("%s IS NULL ?", fieldName);
    }
    
    public AbstractFreeSqlBuilder isNotNull(String fieldName) {
        return addExpClause("%s IS NOT NULL ?", fieldName);
    }
    
    private interface Clause {
        String build(DatabaseCategory dbCategory);
    }
    
    private class SimpleClause implements Clause {
        private String template;
        SimpleClause(String template) {
            this.template =template;
        }
        public String build(DatabaseCategory dbCategory) {
            return template;
        }
    }
    
    private static class ExpClause implements Clause {
        private String template;
        private String fieldName;
        ExpClause(String template, String fieldName) {
            this.template =template;
            this.fieldName = fieldName;
        }

        public String build(DatabaseCategory dbCategory) {
            return String.format(template, wrapField(dbCategory, fieldName));
        }
    }
    
    private static class OperatorClause implements Clause {
        private String operator;
        OperatorClause(String operator) {
            this.operator = operator;
        }
        
        @Override
        public String build(DatabaseCategory dbCategory) {
            // TODO Auto-generated method stub
            return null;
        }
        
        public static OperatorClause and() {
            return new OperatorClause("AND");
        }
        
        public static OperatorClause or() {
            return new OperatorClause("OR");
        }
        
        public static OperatorClause not() {
            return new OperatorClause("NOT");
        }
    }
    
    private AbstractFreeSqlBuilder add(Clause clause) {
        clauses.add(clause);
        return this;
    }
    
    private AbstractFreeSqlBuilder addSimpleClause(String template) {
        return add(new SimpleClause(template));
    }
    
    private AbstractFreeSqlBuilder addExpClause(String template, String fieldName) {
        return add(new ExpClause(template, fieldName));
    }
    
    public static ExpClause expression(String template) {
        
    }
    
    private static class TableClause implements Clause{
        private String tableName;
        private String tableShardId;
        private Object tableShardValue;
        private DalHints hints;
        
        TableClause(String tableName) {
            this.tableName = tableName;
        }
        
        public void setHints() {
            this.hints = hints;
        }
        
        @Override
        public String build(DatabaseCategory dbCategory) {
            if(hints == null)
                throw new RuntimeException("Just to remind that the hints s not set");
            
            // Check if table is sharded
            // compute the table shard if only value is provided
            return null;
        }
    }
    
    public static TableClause table(String tableName) {
        return new TableClause(tableName);
    }
    
    public static TableClause table(String tableName, String tableShardId) {
        TableClause tc = new TableClause(tableName);
        tc.tableShardId = tableShardId;
        return tc;
    }
    
    
    public static TableClause table(String tableName, Object tableShardValue) {
        TableClause tc = new TableClause(tableName);
        tc.tableShardValue = tableShardValue;
        return tc;
    }
}
