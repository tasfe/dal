package com.ctrip.platform.dal.dao.sqlbuilder;

import static com.ctrip.platform.dal.dao.helper.DalShardingHelper.*;
import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractSqlBuilder.wrapField;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.ctrip.platform.dal.common.enums.DatabaseCategory;
import com.ctrip.platform.dal.dao.DalClientFactory;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.StatementParameters;
import com.ctrip.platform.dal.exceptions.DalException;

/**
 * This sql builder only handles template creation. It will not do with the parameters
 * for now.
 * 
 * rules:
 * if bracket has no content, bracket will be removed
 * expression can be evaluated and can be wrapped by bracket and connect to each other by and/or
 * expression should have no leading and tailing and/or, it there is, the and/or will be removed during validating
 * 
 * To be align with FreeSelectSqlBuilder and FreeUpdateSqlBuilder, this class will not handle methods that dealing with 
 * some parameter. To deal with parameter, you should use Expressions.*
 * 
 * @author jhhe
 *
 */
public class AbstractFreeSqlBuilder implements SqlBuilder {
    public static final String EMPTY = "";
    public static final String SPACE = " ";
    public static final String COMMA = ", ";
    public static final String SELECT = "SELECT ";
    public static final String FROM = " FROM ";
    public static final String WHERE= " WHERE ";
    public static final String AS = " AS ";
    public static final String GROUP_BY = " GROUP BY ";
    public static final String HAVING = "HAVING ";
    
    private String logicDbName;
    private DatabaseCategory dbCategory;
    private DalHints hints;
    private StatementParameters parameters;
    private ClauseList clauses = new ClauseList();
    
    public AbstractFreeSqlBuilder setLogicDbName(String logicDbName) {
        Objects.requireNonNull(logicDbName, "Logic Db Name can't be null.");
        
        this.logicDbName = logicDbName;
        this.dbCategory = DalClientFactory.getDalConfigure().getDatabaseSet(logicDbName).getDatabaseCategory();
        
        clauses.setLogicDbName(logicDbName);
        clauses.setDbCategory(dbCategory);
        
        return this;
    }
    
    public AbstractFreeSqlBuilder setHints(DalHints hints) {
        Objects.requireNonNull(hints, "DalHints can't be null.");

        this.hints = hints;
        clauses.setHints(hints.clone());

        return this;
    }
    
    public AbstractFreeSqlBuilder with(StatementParameters parameters) {
        Objects.requireNonNull(parameters, "parameters can't be null.");
        if(this.parameters == parameters)
            return this;

        if(this.parameters != null && this.parameters.size() > 0)
            throw new IllegalStateException("The parameters has already be set and processed. " + 
                    "You can only set parameters at the begining of build");
            
        this.parameters = parameters;
        clauses.setParameters(parameters);
        return this;
    }
    
    public String build() {
        try {
            return clauses.build();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public StatementParameters buildParameters() {
        return parameters;
    }
    
    /**
     * Basic append methods definition
     */
    
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

    /**
     * Append methods for column and table
     */
    
    public AbstractFreeSqlBuilder appendColumn(String columnName) {
        add(new Column(columnName));
        return this;
    }
    
    /**
     * Append columns separate by COMMA
     * @param columns 
     * @return
     */
    public AbstractFreeSqlBuilder appendColumns(String... columns) {
        return appendColumns(columns(columns));
    }

    /**
     * Append columns separate by COMMA
     * @param columns The type of column can be Column or other clause
     * @return
     */
    public AbstractFreeSqlBuilder appendColumns(Clause... columns) {
        for (int i = 0; i < columns.length; i++) {
            append(columns[i]);
            if(i != columns.length -1)
                append(COMMA);    
        }
        
        return this;
    }

    /**
     * The tableName will be replaced by true table name if it is a logic table that allow shard
     * @param tableName table name. The table can be sharded
     * @return
     */
    public AbstractFreeSqlBuilder appendTable(String tableName) {
        return add(new Table(tableName));
    }
    
    /**
     * Combined append for SELECT
     */

    /**
     * Using the columns and table to build a SELECT column1, column2,... FROM table
     * @param columns The type of column can be Column or other clause
     * @param table
     * @return
     */
    public AbstractFreeSqlBuilder selectFrom(String[] columns, Table table) {
        return selectFrom(columns(columns), table);
    }
    
    /**
     * Using the columns and table to build a SELECT column1, column2,... FROM table
     * @param columns The type of column can be Column or other clause
     * @param table
     * @return
     */
    public AbstractFreeSqlBuilder selectFrom(Clause[] columns, Table table) {
        append(SELECT);
        appendColumns(columns);
        append(FROM);
        append(table);
        append(new SqlServerWithNoLock());
        return this;
    }
    
    /**
     * It will append where and check if the value is start of "and" or "or", of so, the leading 
     * "and" or "or" will be removed
     * @param template
     * @return
     */
    public AbstractFreeSqlBuilder where(String template) {
        return append(WHERE + template);
    }
    
    public AbstractFreeSqlBuilder where(Clause... clauses) {
        return this;
    }
    
    public AbstractFreeSqlBuilder groupBy(String condition) {
        return append(GROUP_BY).append(condition);
    }
    
    public AbstractFreeSqlBuilder having(String condition) {
        return append(HAVING).append(condition);
    }
    
    public AbstractFreeSqlBuilder bracket(Clause clause) {
        return leftBracket().add(clause).rightBracket();
    }
    
    public AbstractFreeSqlBuilder leftBracket() {
        return add(new Bracket(true));
    }

    public AbstractFreeSqlBuilder rightBracket() {
        return add(new Bracket(false));
    }
    
    public AbstractFreeSqlBuilder bracket(Clause... clauses) {
        return leftBracket().append(clauses).rightBracket();
    }
    
    public AbstractFreeSqlBuilder and() {
        return add(Operator.and());
    }
    
    public AbstractFreeSqlBuilder or() {
        return add(Operator.or());
    }
    
    public AbstractFreeSqlBuilder not() {
        return add(Operator.not());
    }
    
    public AbstractFreeSqlBuilder and(Clause... clauses) {
        for (int i = 0; i < clauses.length; i++) {
            append(clauses[i]);
            if(i != clauses.length -1)
                and();    
        }
        
        return this;
    }
    
    public AbstractFreeSqlBuilder or(Clause... clauses) {
        for (int i = 0; i < clauses.length; i++) {
            append(clauses[i]);
            if(i != clauses.length -1)
                or();    
        }
        
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
    
    /**
     * Because certain infomation may not be in place during sql construction,
     * the build process is separated into two phases. One is preparing: setBuilderCOntext(), this 
     * is invoked immediately after clause is been constructed. The other is build(), which 
     * actually build part of the final sql.
     * 
     * @author jhhe
     *
     */
    public static abstract class Clause {

        /**
         * The context for building sql. Please not that each of them may be set at different timing.
         * The only assumption is all of tem will be set before build is invoked.
         * For clause that may add parameter, it can do it when parameter is set.
         * For clause that need hints, it should be done in build, because hints will only be ready
         * before build is called.
         */
        
        protected DatabaseCategory dbCategory;
        protected String logicDbName;
        protected DalHints hints;
        protected StatementParameters parameters;
        
        public void setDbCategory(DatabaseCategory dbCategory) {
            this.dbCategory = dbCategory;
        }

        public void setLogicDbName(String logicDbName) {
            this.logicDbName = logicDbName;
        }

        public void setHints(DalHints hints) {
            this.hints = hints;
        }

        public void setParameters(StatementParameters parameters) {
            if(this.parameters == parameters)
                return;

            if(this.parameters != null && this.parameters.size() > 0)
                throw new IllegalStateException("The parameters has already be set and processed. " + 
                        "You can only set parameters once before build");
            
            this.parameters = parameters;
        }

        public abstract String build() throws SQLException;
    }
    
    public static class ClauseList extends Clause {
        private List<Clause> list = new ArrayList<>();
        
        public void setDbCategory(DatabaseCategory dbCategory) {
            this.dbCategory = dbCategory;
            for(Clause c: list)
                c.setDbCategory(dbCategory);
        }

        public void setLogicDbName(String logicDbName) {
            this.logicDbName = logicDbName;
            for(Clause c: list)
                c.setLogicDbName(logicDbName);
        }

        public void setHints(DalHints hints) {
            this.hints = hints;
            for(Clause c: list)
                c.setHints(hints);
        }

        public void setParameters(StatementParameters parameters) {
            for(Clause c: list)
                c.setParameters(parameters);
        }
        
        public ClauseList add(Clause... clauses) {
            for(Clause c: clauses) {
                
                c.setDbCategory(dbCategory);
                c.setHints(hints);
                c.setLogicDbName(logicDbName);
                c.setParameters(parameters);
                
                list.add(c);
            }
            return this;
        }
        
        public boolean isEmpty() {
            return list.isEmpty();
        }
        
        @Override
        public String build() throws SQLException {
            StringBuilder sb = new StringBuilder();
            validate();
            for(Clause c: list)
                sb.append(c.build());
            return sb.toString();
        }
        
        private void validate() {
            
        }
    }
    
    public static class Text extends Clause {
        private String template;
        public Text(String template) {
            this.template =template;
        }
        
        public String build() throws SQLException {
            return template;
        }
    }
    
    public static class Column extends Clause {
        private String columnName;
        private String alias;
        public Column(String columnName) {
            this.columnName = columnName;
        }
        
        public Column(String columnName, String alias) {
            this(columnName);
            this.alias = alias;
        }
        
        public Column as(String alias) {
            this.alias = alias;
            return this;
        }
        
        public String build() {
            return alias == null ? wrapField(dbCategory, columnName): wrapField(dbCategory, columnName) + " AS " + alias;
        }
    }
    
    public static class Expression extends Clause {
        private boolean valid;
        private String template;
        private String fieldName;
        
        public Expression(String template) {
            this.template = template;
        }
        
        public Expression(String template, String fieldName) {
            this(template);
            this.fieldName = fieldName;
        }
        
        public String build() {
            return fieldName == null ? template : String.format(template, wrapField(dbCategory, fieldName));
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
    
    public static final Clause NULL = new NullClause();
    
    public static class Operator extends Clause {
        private String operator;
        public Operator(String operator) {
            this.operator = operator;
        }
        
        @Override
        public String build() {
            // TODO Auto-generated method stub
            return null;
        }
        
        public static Operator and() {
            return new Operator("AND");
        }
        
        public static Operator or() {
            return new Operator("OR");
        }
        
        public static Operator not() {
            return new Operator("NOT");
        }
    }
    
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
    
    
    private AbstractFreeSqlBuilder add(Clause clause) {
        clauses.add(clause);
        return this;
    }
    
    private AbstractFreeSqlBuilder addTextClause(String template) {
        return add(new Text(template));
    }
    
    private AbstractFreeSqlBuilder addExpClause(String template, String fieldName) {
        return add(new Expression(template, fieldName));
    }
    
    public static Text text(String template) {
        return new Text(template);
    }
    
    public static Clause expression(String template) {
        return new Expression(template);
    }
    
    public static Clause expression(Expression... clauses) {
        return new ClauseList().add(clauses);
    }
    
    private static class Table extends Clause{
        private String rawTableName;
        private String tableShardId;
        private Object tableShardValue;
        
        public Table(String rawTableName) {
            this.rawTableName = rawTableName;
        }
        
        public Table inShard(String tableShardId) {
            this.tableShardId = tableShardId;
            return this;
        }
        
        public Table shardValue(String tableShardValue) {
            this.tableShardValue = tableShardValue;
            return this;
        }
        
        @Override
        public String build() throws SQLException {
            if(!isTableShardingEnabled(logicDbName, rawTableName))
                return wrapField(dbCategory, rawTableName);
            
            if(tableShardId!= null)
                return wrapField(dbCategory, rawTableName + buildShardStr(logicDbName, tableShardId));
            
            if(tableShardValue != null) {
                return wrapField(dbCategory, rawTableName + buildShardStr(logicDbName, locateTableShardId(logicDbName, rawTableName, new DalHints().setTableShardValue(tableShardValue), null, null)));
            }
                
            return wrapField(dbCategory, rawTableName + buildShardStr(logicDbName, locateTableShardId(logicDbName, rawTableName, hints, parameters, null)));
        }
    }
    
    /**
     * Special Clause that only works when DB is sql server. It will append WITH (NOLOCK) after table
     * name against guideline.
     * @author jhhe
     *
     */
    private static class SqlServerWithNoLock extends Clause {
        private static final String SQL_SERVER_NOLOCK = "WITH (NOLOCK)";
        public String build() throws SQLException {
            return dbCategory == DatabaseCategory.SqlServer ? SPACE + SQL_SERVER_NOLOCK : EMPTY;
        }
    }
    
    public static Column column(String columnName) {
        return new Column(columnName);
    }
    
    public static Column[] columns(String... columnNames) {
        Column[] cl = new Column[columnNames.length];
        for (int i = 0; i < columnNames.length; i++)
            cl[i] = column(columnNames[i]);

        return cl;
    }
    
    public static Table table(String tableName) {
        return new Table(tableName);
    }
    
    public static <T> T[] toArray(T... ts) {
        return ts;
    }
}
