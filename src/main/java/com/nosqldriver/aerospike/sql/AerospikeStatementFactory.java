package com.nosqldriver.aerospike.sql;

import com.aerospike.client.query.Statement;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

class AerospikeStatementFactory {
    private CCJSqlParserManager parserManager = new CCJSqlParserManager();

    Statement createStatement(String sql) throws SQLException {
        try {
            Statement statement = new Statement();
            Collection<String> selectedColumns = new ArrayList<>();
            parserManager.parse(new StringReader(sql)).accept(new StatementVisitorAdapter() {
                @Override
                public void visit(Select select) {
                    select.getSelectBody().accept(new SelectVisitorAdapter() {
                        @Override
                        public void visit(PlainSelect plainSelect) {
                            plainSelect.getFromItem().accept(new FromItemVisitorAdapter() {
                                @Override
                                public void visit(Table tableName) {
                                    statement.setNamespace(tableName.getSchemaName());
                                    statement.setSetName(tableName.getName());
                                }
                            });
                            plainSelect.getFromItem().accept(new FromItemVisitorAdapter() {
                                @Override
                                public void visit(Table table) {
                                    statement.setSetName(table.getName());
                                }
                            });

                            plainSelect.getSelectItems().forEach(si -> si.accept(new SelectItemVisitorAdapter() {
                                @Override
                                public void visit(SelectExpressionItem selectExpressionItem) {
                                    selectedColumns.add(selectExpressionItem.getAlias().getName());
                                }
                            }));
                        }
                    });
                }
            });

            statement.setBinNames(selectedColumns.toArray(new String[0]));
            return statement;
        } catch (JSQLParserException e) {
            throw new SQLException(e);
        }
    }
}
