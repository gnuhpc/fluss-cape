package org.gnuhpc.fluss.cape.pg.executor;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.gnuhpc.fluss.cape.pg.protocol.PgRowDescriptionBuilder;
import org.gnuhpc.fluss.cape.pg.sql.PgQueryResult;

public final class PgCompatStubResults {

    private PgCompatStubResults() {
    }

    public static PgQueryResult schemas() {
        return singleColumn("schema", List.of("public", "pg_catalog"));
    }

    public static PgQueryResult namespaces() {
        return singleColumn("nspname", List.of("pg_catalog", "public"));
    }

    public static PgQueryResult tables() {
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType rowType = typeFactory.createStructType(
                List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR),
                        typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                List.of("schema_name", "table_name"));
        return new PgQueryResult(PgRowDescriptionBuilder.fromRelType(rowType), List.of());
    }

    public static PgQueryResult singleColumn(String columnName, List<String> values) {
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType rowType = typeFactory.createStructType(
                List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)), List.of(columnName));
        List<Object[]> rows = new ArrayList<>();
        for (String value : values) {
            rows.add(new Object[]{value});
        }
        return new PgQueryResult(PgRowDescriptionBuilder.fromRelType(rowType), rows);
    }

    public static PgQueryResult singleColumn(String columnName, String value) {
        return singleColumn(columnName, List.of(value));
    }

    public static PgQueryResult describeTableColumns() {
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType rowType = typeFactory.createStructType(
                List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR),
                        typeFactory.createSqlType(SqlTypeName.VARCHAR),
                        typeFactory.createSqlType(SqlTypeName.VARCHAR),
                        typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                List.of("column_name", "data_type", "is_nullable", "column_key"));
        return new PgQueryResult(PgRowDescriptionBuilder.fromRelType(rowType), List.of());
    }

    public static PgQueryResult pgType() {
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType rowType = typeFactory.createStructType(
                List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR),
                        typeFactory.createSqlType(SqlTypeName.INTEGER)),
                List.of("typname", "oid"));
        
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{"bool", 16});
        rows.add(new Object[]{"int4", 23});
        rows.add(new Object[]{"int8", 20});
        rows.add(new Object[]{"varchar", 1043});
        rows.add(new Object[]{"text", 25});
        rows.add(new Object[]{"timestamp", 1114});
        rows.add(new Object[]{"date", 1082});
        rows.add(new Object[]{"bytea", 17});
        
        return new PgQueryResult(PgRowDescriptionBuilder.fromRelType(rowType), rows);
    }

    public static PgQueryResult pgAttribute() {
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType rowType = typeFactory.createStructType(
                List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR),
                        typeFactory.createSqlType(SqlTypeName.INTEGER),
                        typeFactory.createSqlType(SqlTypeName.INTEGER)),
                List.of("attname", "attnum", "atttypid"));
        return new PgQueryResult(PgRowDescriptionBuilder.fromRelType(rowType), List.of());
    }
}
