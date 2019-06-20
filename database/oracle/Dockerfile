FROM odo-docker-local.artifactory.oci.oraclecorp.com/iod-opdb121-base:12.1.0.2.190115_1
MAINTAINER Steven Kolev <steven.k.kolev@oracle.com>

COPY example.sql /tmp/

RUN su oracle -c ' \
        export ORACLE_SID=orcl && \
        export ORAENV_ASK=NO && \
        . /usr/local/bin/oraenv && \
        echo "startup" | $ORACLE_HOME/bin/sqlplus -s / as sysdba && \
        echo "@/tmp/example.sql" | $ORACLE_HOME/bin/sqlplus -s / as sysdba \
    ;'
