package com.ctrip.platform.dal.dao.sqlbuilder;

import static com.ctrip.platform.dal.dao.helper.DalShardingHelper.buildShardStr;
import static com.ctrip.platform.dal.dao.helper.DalShardingHelper.isTableShardingEnabled;
import static com.ctrip.platform.dal.dao.helper.DalShardingHelper.locateTableShardId;
import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractSqlBuilder.wrapField;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.ctrip.platform.dal.common.enums.DatabaseCategory;
import com.ctrip.platform.dal.dao.DalClientFactory;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.StatementParameters;
import com.ctrip.platform.dal.dao.sqlbuilder.Expressions.Expression;

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
    public static final String HAVING = " HAVING ";
    
    private StatementParameters parameters;
    private ClauseList clauses = new ClauseList();
    
    public AbstractFreeSqlBuilder setLogicDbName(String logicDbName) {
        DalClientFactory.getDalConfigure().getDatabaseSet(logicDbName);
        clauses.setLogicDbName(logicDbName);
        return this;
    }
    
    public AbstractFreeSqlBuilder setHints(DalHints hints) {
        Objects.requireNonNull(hints, "DalHints can't be null.");

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
        return append(new Text(template));
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
            this.clauses.add(c);
        return this;
    }
    
    /**
     * Append when the condition is met
     * @param condition
     * @param template
     * @return
     */
    public AbstractFreeSqlBuilder append(boolean condition, String template) {
        return condition ? append(template): this;
    }
    
    /**
     * Append template depends on whether the condition is met.
     * @param condition
     * @param template value to be appended when condition is true
     * @param elseTemplate value to be appended when condition is true
     * @return
     */
    public AbstractFreeSqlBuilder append(boolean condition, String template, String elseTemplate) {
        return condition ? append(template): append(elseTemplate);
    }
    
    public AbstractFreeSqlBuilder append(boolean condition, Clause clause) {
        return condition ? append(clause) : this;
    }
    
    public AbstractFreeSqlBuilder append(boolean condition, Clause clause, Clause elseClause) {
        return condition ? append(clause) : append(elseClause);
    }

    /**
     * Append methods for column and table
     */
    
    public AbstractFreeSqlBuilder appendColumn(String columnName) {
        append(new Column(columnName));
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
        return append(new Table(tableName));
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
        return append(WHERE).append(clauses);
    }
    
    public AbstractFreeSqlBuilder groupBy(String condition) {
        return append(GROUP_BY).append(condition);
    }
    
    public AbstractFreeSqlBuilder having(String condition) {
        return append(HAVING).append(condition);
    }
    
    public AbstractFreeSqlBuilder leftBracket() {
        return append(Expressions.leftBracket());
    }

    public AbstractFreeSqlBuilder rightBracket() {
        return append(Expressions.rightBracket());
    }
    
    public AbstractFreeSqlBuilder bracket(Clause... clauses) {
        return leftBracket().append(clauses).rightBracket();
    }
    
    public AbstractFreeSqlBuilder and() {
        return append(Expressions.and());
    }
    
    public AbstractFreeSqlBuilder or() {
        return append(Expressions.or());
    }
    
    public AbstractFreeSqlBuilder not() {
        return append(Expressions.not());
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
    
    public AbstractFreeSqlBuilder nullable(Object o) {
        clauses.nullable(o);
        return this;
    }
    
    /**
     * Append = expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder equal(String columnName) {
        return append(Expressions.equal(columnName));
    }
    
    /**
     * Append <> expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder notEqual(String columnName) {
        return append(Expressions.notEqual(columnName));
    }
    
    /**
     * Append > expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder greaterThan(String columnName) {
        return append(Expressions.greaterThan(columnName));
    }

    /**
     * Append >= expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder greaterThanEquals(String columnName) {
        return append(Expressions.greaterThanEquals(columnName));
    }

    /**
     * Append < expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder lessThan(String columnName) {
        return append(Expressions.lessThan(columnName));
    }

    /**
     * Append <= expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder lessThanEquals(String columnName) {
        return appendColumnExpression("%s <= ?", columnName);
    }

    /**
     * Append BETWEEN expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder between(String columnName) {
        return append(Expressions.between(columnName));
    }
    
    /**
     * Append LIKE expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder like(String columnName) {
        return append(Expressions.like(columnName));
    }
    
    /**
     * Append BOT LIKE expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder notLike(String columnName) {
        return append(Expressions.notLike(columnName));
    }
    
    /**
     * Append IN expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder in(String columnName) {
        return append(Expressions.in(columnName));
    }
    
    /**
     * Append NOT IN expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder notIn(String columnName) {
        return append(Expressions.notIn(columnName));
    }
    
    /**
     * Append IS NULL expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder isNull(String columnName) {
        return append(Expressions.isNull(columnName));
    }
    
    /**
     * Append IS NOT NULL expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder isNotNull(String columnName) {
        return append(Expressions.isNotNull(columnName));
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
         * The context for building sql. Please note that each of them may be set at different timing.
         * The only assumption is all of tem will be set before build is invoked.
         * For clause that may add parameter, it can do it when parameter is set.
         * For clause that need hints, it should be done in build, because hints will only be ready
         * before build is called.
         */
        
        protected DatabaseCategory dbCategory;
        protected String logicDbName;
        protected DalHints hints;
        protected StatementParameters parameters;
        
        public void setLogicDbName(String logicDbName) {
            Objects.requireNonNull(logicDbName, "Logic Db Name can't be null.");
            this.logicDbName = logicDbName;
            this.dbCategory = DalClientFactory.getDalConfigure().getDatabaseSet(logicDbName).getDatabaseCategory();
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
        
        public void setLogicDbName(String logicDbName) {
            Objects.requireNonNull(logicDbName, "Logic Db Name can't be null.");
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
                if(logicDbName!=null)
                    c.setLogicDbName(logicDbName);

                c.setHints(hints);
                c.setParameters(parameters);
                
                list.add(c);
            }
            return this;
        }
        
        public boolean isEmpty() {
            return list.isEmpty();
        }
        
        public void nullable(Object o) {
            if(list.size() == 0)
                throw new IllegalStateException("There is no exitsing sql segement.");
            
            Clause last = list.get(list.size() - 1);
            
            if(last instanceof Expression) {
                ((Expression)last).nullable(o);
                return;
            }
                
            if(last instanceof ClauseList) {
                ((ClauseList)last).nullable(o);
                return;
            }
            
            throw new IllegalStateException("The last sql segement is not an expression.");
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
    
    private AbstractFreeSqlBuilder appendColumnExpression(String template, String columnName) {
        return append(Expressions.createColumnExpression(template, columnName));
    }
    
    public static Text text(String template) {
        return new Text(template);
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
