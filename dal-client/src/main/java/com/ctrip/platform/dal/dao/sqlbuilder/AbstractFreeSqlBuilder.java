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
 * rules:
 * if bracket has no content, bracket will be removed
 * expression can be evaluated and can be wrapped by bracket and connect to each other by and/or
 * expression should have no leading and tailing and/or, it there is, the and/or will be removed during validating 
 * 
 * @author jhhe
 *
 */
public abstract class AbstractFreeSqlBuilder implements SqlBuilder {
    protected DatabaseCategory dbCategory = DatabaseCategory.MySql;

    private String logicDbName;
    private DalHints hints;
    private ClauseList clauses = new ClauseList();
    
    public AbstractFreeSqlBuilder setLogicDbName(String logicDbName) {
        Objects.requireNonNull(logicDbName, "Logic Db Name can't be null.");
        this.logicDbName = logicDbName;
        return this;
    }
    
    public AbstractFreeSqlBuilder setHints(DalHints hints) {
        this.hints = hints;
        return this;
    }
    
    public String build() {
        return clauses.build(dbCategory, hints, logicDbName);
    }

    public AbstractFreeSqlBuilder setDatabaseCategory(DatabaseCategory dbCategory) throws SQLException {
        Objects.requireNonNull(dbCategory, "DatabaseCategory can't be null.");
        this.dbCategory = dbCategory;
        return this;
    }
    
    public AbstractFreeSqlBuilder append(String template) {
        return addTextClause(template);
    }
    
    /**
     * Usage like:
     * append(
     *          and(),
     *          leftBracket(),
     *          equals(),
     *          expression("abc"),
     *          rightBracket(),
     *          or(),
     *          ...
     *       )
     * @param clauses
     * @return
     */
    public AbstractFreeSqlBuilder append(Clause... clauses) {
        for(Clause c: clauses)
            add(c);
        return this;
    }
    
    public AbstractFreeSqlBuilder append(ClauseList clauses) {
        for(Clause c: clauses.list)
            add(c);
        return this;
    }
    
    /**
     * Append when the condition is met
     * @param condition
     * @param template
     * @return
     */
    public AbstractFreeSqlBuilder append(boolean condition, String template) {
        return condition ? addTextClause(template): this;
    }
    
    /**
     * Append template depends on whether the condition is met.
     * @param condition
     * @param template value to be appended when condition is true
     * @param elseTemplate value to be appended when condition is true
     * @return
     */
    public AbstractFreeSqlBuilder append(boolean condition, String template, String elseTemplate) {
        return condition ? addTextClause(template): addTextClause(elseTemplate);
    }
    
    public AbstractFreeSqlBuilder append(boolean condition, Clause clause) {
        return condition ? add(clause) : this;
    }
    
    public AbstractFreeSqlBuilder append(boolean condition, Clause clause, Clause elseClause) {
        return condition ? add(clause) : add(elseClause);
    }
    
    public AbstractFreeSqlBuilder appendColumn(String columnName) {
        add(new ColumnClause(columnName));
        return this;
    }
    
    /**
     * Append names separate by seperator
     * @param names
     * @param separator
     * @return
     */
    public AbstractFreeSqlBuilder appendColumns(String[] columnNames, String separator) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columnNames.length; i++) {
            sb.append(columnNames[i]);
            if(i != columnNames.length -1)
                sb.append(separator);    
        }
        return addTextClause(sb.toString());
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
        return addTextClause(sb.toString());
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
    
    public AbstractFreeSqlBuilder and(Clause... clauses) {
        return add(OperatorClause.and());
    }
    
    public AbstractFreeSqlBuilder or(Clause... clauses) {
        return add(OperatorClause.or());
    }
    
    public AbstractFreeSqlBuilder not(Clause... clauses) {
        return add(OperatorClause.not());
    }
    
    public AbstractFreeSqlBuilder and(List<Clause> clauses) {
        return add(OperatorClause.and());
    }
    
    public AbstractFreeSqlBuilder or(List<Clause> clauses) {
        return add(OperatorClause.or());
    }
    
    public AbstractFreeSqlBuilder not(List<Clause> clauses) {
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
    
    /**
     * It will append where and check if the value is start of "and" or "or", of so, the leading 
     * "and" or "or" will be removed
     * @param template
     * @return
     */
    public AbstractFreeSqlBuilder where(String template) {
        return this;
    }
    
    public AbstractFreeSqlBuilder where(Clause... clauses) {
        return this;
    }
    
    public AbstractFreeSqlBuilder where(ClauseList clauses) {
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
        String build(DatabaseCategory dbCategory, DalHints hints, String logicDbName);
    }
    
    public static class ClauseList implements Clause {
        private List<Clause> list = new ArrayList<>();
        
        
        public ClauseList add(Clause clause) {
            list.add(clause);
            return this;
        }
        
        @Override
        public String build(DatabaseCategory dbCategory, DalHints hints, String logicDbName) {
            StringBuilder sb = new StringBuilder();
            validate();
            for(Clause c: list)
                sb.append(c.build(dbCategory, hints, logicDbName));
            return sb.toString();
        }
        
        private void validate() {
            
        }
    }
    
    private static class TextClause implements Clause {
        private String template;
        TextClause(String template) {
            this.template =template;
        }
        public String build(DatabaseCategory dbCategory, DalHints hints, String logicDbName) {
            return template;
        }
    }
    
    private static class ColumnClause implements Clause {
        private String columnName;
        ColumnClause(String columnName) {
            this.columnName = columnName;
        }
        
        public String build(DatabaseCategory dbCategory, DalHints hints, String logicDbName) {
            return wrapField(dbCategory, columnName);
        }
    }
    
    private static class ExpClause implements Clause {
        private String template;
        private String fieldName;
        
        ExpClause(String template) {
            this.template = template;
        }
        
        ExpClause(String template, String fieldName) {
            this(template);
            this.fieldName = fieldName;
        }

        public String build(DatabaseCategory dbCategory, DalHints hints, String logicDbName) {
            return fieldName == null ? template : String.format(template, wrapField(dbCategory, fieldName));
        }
    }
    
    private static class OperatorClause implements Clause {
        private String operator;
        OperatorClause(String operator) {
            this.operator = operator;
        }
        
        @Override
        public String build(DatabaseCategory dbCategory, DalHints hints, String logicDbName) {
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
    
    private AbstractFreeSqlBuilder addTextClause(String template) {
        return add(new TextClause(template));
    }
    
    private AbstractFreeSqlBuilder addExpClause(String template, String fieldName) {
        return add(new ExpClause(template, fieldName));
    }
    
    public static TextClause text(String template) {
        return new TextClause(template);
    }
    
    public static ExpClause expression(String template) {
        return new ExpClause(template);
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
        public String build(DatabaseCategory dbCategory, DalHints hints, String logicDbName) {
            if(hints == null)
                throw new RuntimeException("Just to remind that the hints s not set");
            
            // Check if table is sharded
            // compute the table shard if only value is provided
            // And need to wrap it against db category
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
