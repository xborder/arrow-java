# Arrow Java FlightInfo call-site inventory

Baseline: Apache Arrow Java `91b4a2ca418ecb47cb8871d97588b19444337eb4`. The baseline `FlightSqlClient` had 14 `FlightClient.getInfo` call shapes representing 13 distinct Flight SQL command families; the two XDBC overloads share one wire command family. Schema-only `getSchema` calls, updates/ingest, actions, `DoPut`, and `DoGet` are not FlightInfo-producing call sites.

The POC routes all 13 families through `FlightSqlClient.getInfo(FlightDescriptor, CallOption...)`. The default hook still makes one `GetFlightInfo` call. JDBC installs a polling subclass that can retain continuation state and append newly published endpoints to the existing ResultSet on demand.

| # | Wire command family | Baseline `FlightSqlClient` API/call shape | JDBC exposure | POC routing and applicability |
| --- | --- | --- | --- | --- |
| 1 | `CommandStatementQuery` | `FlightSqlClient.execute(String, Transaction, CallOption...)` | Yes: direct `Statement.executeQuery/execute`; baseline JDBC implicitly prepared even direct statements. | `ArrowFlightMetaImpl.prepareAndExecute` selects `prepareDirect`; schema discovery remains prepared, query execution sends the original direct command once through PollInfo. T1, T2, T5-T9. |
| 2 | `CommandStatementSubstraitPlan` | `FlightSqlClient.executeSubstrait(SubstraitPlan, Transaction, CallOption...)` | No JDBC API path. | Shared hook covered for FlightSqlClient consumers; not exercised by the JDBC fixture. |
| 3 | `CommandGetCatalogs` | `FlightSqlClient.getCatalogs(CallOption...)` | Yes: `ArrowDatabaseMetadata.getCatalogs`. | PollInfo through the handler. T4 and the family-isolation half of T5. |
| 4 | `CommandGetDbSchemas` | `FlightSqlClient.getSchemas(String, String, CallOption...)` | Yes: `ArrowDatabaseMetadata.getSchemas`. | PollInfo through the handler; covered by JDBC regression suite. |
| 5 | `CommandGetSqlInfo` | `FlightSqlClient.getSqlInfo(Iterable<Integer>, CallOption...)`; array/enum overloads delegate to it. | Yes: lazy SQL-info cache used by `ArrowDatabaseMetadata`. | PollInfo through the handler; covered by JDBC metadata regressions. |
| 6 | `CommandGetXdbcTypeInfo` | `FlightSqlClient.getXdbcTypeInfo(int, CallOption...)` and `getXdbcTypeInfo(CallOption...)` (two call shapes, one family). | No current JDBC handler/`DatabaseMetaData` path. | Shared hook covered for FlightSqlClient consumers; not exercised by the JDBC fixture. |
| 7 | `CommandGetTables` | `FlightSqlClient.getTables(...)` | Yes: `ArrowDatabaseMetadata.getTables`; also `getColumns` with `includeSchema=true`. | PollInfo through the handler; covered by JDBC metadata regressions. |
| 8 | `CommandGetPrimaryKeys` | `FlightSqlClient.getPrimaryKeys(TableRef, CallOption...)` | Yes: `ArrowDatabaseMetadata.getPrimaryKeys`. | PollInfo through the handler; covered by JDBC metadata regressions. |
| 9 | `CommandGetExportedKeys` | `FlightSqlClient.getExportedKeys(TableRef, CallOption...)` | Yes: `ArrowDatabaseMetadata.getExportedKeys`. | PollInfo through the handler; covered by JDBC metadata regressions. |
| 10 | `CommandGetImportedKeys` | `FlightSqlClient.getImportedKeys(TableRef, CallOption...)` | Yes: `ArrowDatabaseMetadata.getImportedKeys`. | PollInfo through the handler; covered by JDBC metadata regressions. |
| 11 | `CommandGetCrossReference` | `FlightSqlClient.getCrossReference(TableRef, TableRef, CallOption...)` | Yes: `ArrowDatabaseMetadata.getCrossReference`. | PollInfo through the handler; covered by JDBC metadata regressions. |
| 12 | `CommandGetTableTypes` | `FlightSqlClient.getTableTypes(CallOption...)` | Yes: `ArrowDatabaseMetadata.getTableTypes`. | PollInfo through the handler; covered by JDBC metadata regressions. |
| 13 | `CommandPreparedStatementQuery` | `FlightSqlClient.PreparedStatement.execute(CallOption...)` after optional parameter `DoPut`. | Yes: explicit JDBC `PreparedStatement.executeQuery/execute`. | The prepared object retains its parent FlightSqlClient and resolves the original prepared descriptor once through the hook. Binding stays before PollInfo and occurs once. T3. |

## JDBC-exposed summary

JDBC exposes 11 of the 13 wire families: direct statement query, prepared statement query, and nine metadata families (catalogs, schemas, SQL info, tables, primary keys, exported keys, imported keys, cross reference, and table types). JDBC does not expose statement Substrait or XDBC type-info commands. The handler's `getInfo(String)` is an internal direct-query convenience but had no live JDBC caller at baseline.

Direct and prepared updates are deliberately outside the table: they return update counts through prepared `executeUpdate`/Flight SQL update actions rather than FlightInfo. `prepareDirect` preserves that update branch and only changes the query-result branch to `CommandStatementQuery`. Parameter upload remains the existing single prepared-statement `DoPut`.
