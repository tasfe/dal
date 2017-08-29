package com.ctrip.platform.dal.dao.sqlbuilder;

import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractSqlBuilder.wrapField;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.ctrip.platform.dal.common.enums.DatabaseCategory;
import com.ctrip.platform.dal.dao.DalClientFactory;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.StatementParameters;

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
    protected DatabaseCategory dbCategory = DatabaseCategory.MySql;

    private String logicDbName;
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
        clauses.setHints(hints);

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
        return clauses.build();
    }

    @Override
    public StatementParameters buildParameters() {
        // TODO Auto-generated method stub
        return null;
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
     * End of basic append methods definition
     */
    
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
        for (int i = 0; i < columnNames.length; i++) {
            appendColumn(columnNames[i]);
            if(i != columnNames.length -1)
                append(separator);    
        }
        
        return this;
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
    
    public AbstractFreeSqlBuilder groupBy(String column) {
        return this;
    }
    
    public AbstractFreeSqlBuilder having(String template) {
        return this;
    }
    
    public AbstractFreeSqlBuilder having(ExpressionClause clause) {
        return this;
    }
    
    public AbstractFreeSqlBuilder bracket(Clause clause) {
        return leftBracket().add(clause).rightBracket();
    }
    
    public AbstractFreeSqlBuilder leftBracket() {
        return add(new BracketClause(true));
    }

    public AbstractFreeSqlBuilder rightBracket() {
        return add(new BracketClause(false));
    }
    
    public AbstractFreeSqlBuilder bracket(ExpressionClause... clauses) {
        return leftBracket().append(clauses).rightBracket();
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
    
    public AbstractFreeSqlBuilder bracket(Clause... clauses) {
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

        public abstract String build();
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
            setParameters(parameters);
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
        
        @Override
        public String build() {
            StringBuilder sb = new StringBuilder();
            validate();
            for(Clause c: list)
                sb.append(c.build());
            return sb.toString();
        }
        
        private void validate() {
            
        }
    }
    
    public static class TextClause extends Clause {
        private String template;
        TextClause(String template) {
            this.template =template;
        }
        public String build() {
            return template;
        }
    }
    
    public static class ColumnClause extends Clause {
        private String columnName;
        private String alias;
        ColumnClause(String columnName) {
            this.columnName = columnName;
        }
        
        ColumnClause(String columnName, String alias) {
            this(columnName);
            this.alias = alias;
        }
        
        public String build() {
            return wrapField(dbCategory, columnName);
        }
    }
    
    public static class ExpressionClause extends Clause {
        private boolean valid;
        private String template;
        private String fieldName;
        
        ExpressionClause(String template) {
            this.template = template;
        }
        
        ExpressionClause(String template, String fieldName) {
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
    public static class NullClause extends ExpressionClause {
        public NullClause() {
            super("");
        }
        
        @Override
        public String build() {
            return "";
        }
    }
    
    public static final Clause NULL = new NullClause();
    
    public static class OperatorClause extends Clause {
        private String operator;
        OperatorClause(String operator) {
            this.operator = operator;
        }
        
        @Override
        public String build() {
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
    
    public static class BracketClause extends Clause {
        private boolean left;
        public BracketClause(boolean isLeft) {
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
        return add(new TextClause(template));
    }
    
    private AbstractFreeSqlBuilder addExpClause(String template, String fieldName) {
        return add(new ExpressionClause(template, fieldName));
    }
    
    public static TextClause text(String template) {
        return new TextClause(template);
    }
    
    public static Clause expression(String template) {
        return new ExpressionClause(template);
    }
    
    public static Clause expression(ExpressionClause... clauses) {
        return new ClauseList().add(clauses);
    }
    
    private static class TableClause extends Clause{
        private String tableName;
        private String tableShardId;
        private Object tableShardValue;
        
        TableClause(String tableName) {
            this.tableName = tableName;
        }
        
        @Override
        public String build() {
            if(hints == null)
                throw new RuntimeException("Just to remind that the hints s not set");
            
            // Check if table is sharded
            // compute the table shard if only value is provided
            // And need to wrap it against db category
            return null;
        }
    }
    
    public static ColumnClause as(String columnName, String alias) {
        return new ColumnClause(columnName, alias);
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
