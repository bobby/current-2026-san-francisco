# The Iceberg REST catalog for the local runtime, backed by Postgres rather
# than the fixture's default SQLite: with several jobs committing at each
# checkpoint, SQLite's file lock made commits fail with SQLITE_BUSY.
FROM apache/iceberg-rest-fixture:1.10.1

# The fixture bundles only the SQLite driver. Checked against Maven Central
# (SHA-1 0e7c94ce80cb1dd9875ff44ffe6a40a8aaa3eefd).
ADD --chown=iceberg:iceberg \
    --checksum=sha256:73914527305a40cce504b0d3d90b23caf565136912d607ae8ae7c5895512332c \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar \
    /usr/lib/iceberg-rest/postgresql.jar

CMD ["java", "-cp", "iceberg-rest-adapter.jar:postgresql.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
