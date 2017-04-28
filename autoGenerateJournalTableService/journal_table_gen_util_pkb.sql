CREATE OR REPLACE PACKAGE BODY journal_table_gen_util
IS
/*
    Author: Yen Xu
    Created: 2003 

		Purpose: Generate audit table and trigger.
				The script generates script to create a journal table and a trigger that moves any changes on the table to the journal table.
				If no changes is made on any of columns of original table but modified_date or modified_by, then the update will not be performed.

        1) A journal table will be created with name <table_name>_JN.
        2) A trigger will be created on the table with name <table_name>_JN_TRG
        3) A sequence will be created with name <table_name>_JN_SEQ

		    The journal table will make a copy of existing table and add following columns to the table:
		     1) JN_OPERATION  VARCHAR2(10) --  UPD, DEL
		     2) JN_MODBY --the name of the user computer e.g. Yen@Yens-MacBook-Pro.local
		     3) JN_ID --sequence

		  If you modify the table after creation, e.g. add new columns, just rerun the methods.

		Requirement:
		Create table and sequence privileges. Must be granted separately
		grant create table to $user;
		grant create sequence to $user;

		HOW TO USE:
		begin
			journal_table_gen_util.create_audit_table('mytable');
			journal_table_gen_util.auto_modify_archive_table('mytable');
			journal_table_gen_util.gen_audit_trg('mytable');
		end;

    
FREEWARE LICENSE
Licensor hereby grants you the following rights, provided that you comply with all of the restrictions set forth in this License and provided, further, that you distribute an unmodified copy of this License with the Program:
Permission is granted to use the Program for non-commercial, non-military purposes;
You may copy and distribute literal (i.e. verbatim) copies of the Program as you receive it throughout the world, in any medium;
You may create works based on the Program and distribute copies of such throughout the world, in any medium.

*/
  FUNCTION md5key(i_string VARCHAR2)
    RETURN VARCHAR2
  IS
    BEGIN
      RETURN DBMS_OBFUSCATION_TOOLKIT.md5(input =>UTL_RAW.cast_to_raw(i_string));
    END;

  PROCEDURE print(i_text VARCHAR2)
  IS
    BEGIN
      dbms_output.put_line(i_text);
--rasklog.plog.debug(i_text);
    END;
  PROCEDURE remove_not_null_check_aud_tab(
    i_table_name IN VARCHAR2)
  IS
    l_sql VARCHAR2(4000);
    BEGIN
      print('remove_not_null_check_aud_tab');
      FOR r IN
      (SELECT *
       FROM user_constraints uc
       WHERE constraint_type = 'C'
             AND table_name = upper(i_table_name)
      )
      LOOP
        l_sql := 'ALTER TABLE ' || i_table_name || ' drop constraint ' || r.constraint_name;
        EXECUTE IMMEDIATE l_sql;
      END LOOP;
    END;
----------------------------------------------------
---        object_exists
----------------------------------------------------
  FUNCTION object_exists(
    i_object_name IN VARCHAR2)
    RETURN BOOLEAN
  IS
    l_exists PLS_INTEGER;
    BEGIN
      SELECT 1
      INTO l_exists
      FROM user_objects
      WHERE object_name = upper(i_object_name);
      RETURN TRUE;
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
      RETURN FALSE;
    END;
  FUNCTION hasNotAllColumns(i_table_name VARCHAR2)
    RETURN BOOLEAN
  IS
    l_audit_tab_name VARCHAR2(30) := UPPER(SUBSTR(i_table_name, 1, 27) || '_JN');
    l_exists         PLS_INTEGER;
    BEGIN
      SELECT count(*)
      INTO
        l_exists
      FROM user_tab_columns tab
      WHERE tab.table_name = upper(i_table_name)
            AND NOT exists(SELECT 1
                           FROM user_tab_columns t2
                           WHERE t2.table_name = l_audit_tab_name
                                 AND t2.column_name = tab.column_name
      );

      IF l_exists > 0
      THEN RETURN TRUE;
      ELSE RETURN FALSE;
      END IF;
    END;

----------------------------------------------------
---        create_audit_table
----------------------------------------------------
  PROCEDURE create_audit_table(
    i_table_name IN VARCHAR2)
  IS
    l_sql            VARCHAR2(4000);
    l_audit_tab_name VARCHAR2(30) := UPPER(SUBSTR(i_table_name, 1, 27) || '_JN');
    l_audit_seq_name VARCHAR2(53) := UPPER(SUBSTR(i_table_name, 1, 23) || '_JN_SEQ');
    l_exists         PLS_INTEGER;
      jnTableNotCorrect EXCEPTION;
      table_not_exists EXCEPTION;
    PROCEDURE create_tab_jn_seq
    IS
      l_max_id PLS_INTEGER;
      l_sql    VARCHAR2(200);
      BEGIN
        EXECUTE IMMEDIATE 'select NVL(max(jn_id),0)+1  from ' || l_audit_tab_name INTO l_max_id;
        IF object_exists(l_audit_seq_name)
        THEN
          EXECUTE IMMEDIATE 'drop SEQUENCE ' || l_audit_seq_name;
        END IF;
        l_sql := 'CREATE SEQUENCE ' || l_audit_seq_name || ' START WITH ' || l_max_id || ' MINVALUE 1  NOCYCLE CACHE 50  NOORDER';
        print(l_sql);
        EXECUTE IMMEDIATE l_sql;
        IF hasNotAllColumns(i_table_name)
        THEN
          RAISE jnTableNotCorrect;
        END IF;
      END;
    PROCEDURE add_audit_column(
      i_col_name IN VARCHAR2,
      i_type     IN VARCHAR2)
    IS
      BEGIN
        SELECT 1
        INTO l_exists
        FROM user_tab_columns
        WHERE column_name = UPPER(i_col_name)
              AND table_name = l_audit_tab_name;
        print('Audit_tab:' || l_audit_tab_name || ' has ' || i_col_name);
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
        l_sql := 'alter table ' || l_audit_tab_name || ' add ' || i_col_name || '  ' || i_type;
        print(l_sql);
        EXECUTE IMMEDIATE l_sql;
      END;
    PROCEDURE create_audit_table
    IS
      BEGIN
        IF NOT object_exists(i_table_name)
        THEN
          RAISE table_not_exists;
        END IF;
        IF NOT object_exists(l_audit_tab_name)
        THEN
          l_sql := 'create table ' || l_audit_tab_name || ' as select * from ' || i_table_name || ' WHERE 1=2';
          print(l_sql);
          EXECUTE IMMEDIATE l_sql;
        END IF;
        remove_not_null_check_aud_tab(l_audit_tab_name);
      END;
    BEGIN
      print('create_audit_table ' || i_table_name);
      create_audit_table;
      --add_audit_column('MD5KEY', 'VARCHAR2(32)');
      add_audit_column('JN_OPERATION', 'VARCHAR2(10)');
      add_audit_column('JN_DATETIME', 'DATE');
      add_audit_column('JN_MODBY', 'VARCHAR2(100)');
      add_audit_column('JN_ID', 'NUMBER(20)');
      create_tab_jn_seq;
      EXCEPTION
      WHEN jnTableNotCorrect THEN
      auto_modify_archive_table(i_table_name);
      WHEN table_not_exists THEN
      print('Table ' || i_table_name || ' does not exists');
    END;

---Generate MD5KEY based on table columns automatically
  FUNCTION gen_md5key(i_table_name VARCHAR2)
    RETURN VARCHAR2
  IS
    l_str varchar2 (1000);
    BEGIN
      FOR r_col IN
      (SELECT *
       FROM user_tab_columns tc
       WHERE tc.table_name = upper(i_table_name)
        and tc.column_name not in ('CREATED_BY','CREATION_DATE','MODIFIED_BY','MODIFIED_DATE')
       ORDER BY COLUMN_ID
      ) LOOP
        IF r_col.data_type IN ('VARCHAR2', 'CHAR','NCHAR','NVARCHAR2')
        THEN
          l_str := l_str || '||trim(upper('||r_col.COLUMN_NAME||'))';
        ELSIF r_col.DATA_TYPE = 'NUMBER'
          THEN
            l_str := l_str ||'||'||r_col.COLUMN_NAME;
        elsif r_col.DATA_TYPE in ('DATE','TIMESTAMP') THEN
          l_str:=l_str||'||to_char('||r_col.column_name||',''yyyymmddhh24missss'')';
        END IF;
      END LOOP;
      return substr(l_str,3);
    END;
----------------------------------------------------
---        gen_audit_trg
----------------------------------------------------
  PROCEDURE gen_audit_trg(i_table_name IN VARCHAR2)
  IS
    i                PLS_INTEGER;
    l_sql            VARCHAR2(10000);
    l_data_type      VARCHAR2(50);
    l_audit_tab_name VARCHAR2(30) := UPPER(SUBSTR(i_table_name, 1, 27) || '_JN');
    l_trg_name       VARCHAR2(30) := UPPER(SUBSTR(i_table_name, 1, 23) || '_JN_TRG');
    l_audit_seq_name VARCHAR2(30) := UPPER(SUBSTR(i_table_name, 1, 23) || '_JN_SEQ');
    FUNCTION get_user
      RETURN VARCHAR2
    IS
      l_sql VARCHAR2(10000);
      BEGIN
        IF upper(i_table_name) IN ('EPI_CONTRACT')
        THEN
          l_sql := '      l_tab_jn.Jn_modby := :NEW.changed_by;';
        ELSE
          l_sql := '     SELECT NVL (USERENV (''CLIENT_INFO''), sys_context(''USERENV'', ''OS_USER'') ||''@''||sys_context(''USERENV'', ''HOST'' ))
           INTO l_tab_jn.Jn_modby
           FROM DUAL; ';
        END IF;
        RETURN l_sql;
      END;
    PROCEDURE drop_trg(i_trg_name IN VARCHAR2)
    IS
      l_exists NUMBER(1) := 0;
      BEGIN
        SELECT count(*)
        INTO l_exists
        FROM user_triggers
        WHERE TRIGGER_NAME = upper(i_trg_name);
        IF l_exists = 1
        THEN
          EXECUTE IMMEDIATE 'drop trigger ' || i_trg_name;
        END IF;
      END;
    BEGIN
      drop_trg(l_trg_name);
      print('Create TRIGGER ' || l_trg_name);
      l_sql := 'CREATE OR REPLACE TRIGGER ' || l_trg_name || ' BEFORE DELETE OR UPDATE ON ' || i_table_name || ' REFERENCING NEW AS New OLD AS Old FOR EACH ROW
            DECLARE
            l_tab_jn ' || l_audit_tab_name || '%ROWTYPE;
            l_is_changed boolean := FALSE;
            BEGIN
            l_tab_jn.jn_operation := case when UPDATING THEN ''UPD'' WHEN DELETING THEN ''DEL''  WHEN INSERTING THEN ''INS'' END;
            l_tab_jn.JN_DATETIME := sysdate;
            SELECT ' || l_audit_seq_name || '.nextval INTO l_tab_jn.jn_id FROM DUAL;
            IF  DELETING THEN l_is_changed := TRUE;
            ELSIF UPDATING THEN
            IF ';
      i := 0;
      FOR r_col IN
      (SELECT c.*
       FROM user_tab_columns c
       WHERE c.table_name = UPPER(i_table_name)
       ORDER BY column_name
      )
      LOOP
        i := i + 1;
        IF r_col.column_name NOT LIKE '%MODIFIED%' AND r_col.column_name != 'VERSION'
        THEN
          l_data_type :=
          CASE r_col.data_type
          WHEN 'DATE'
            THEN
              'sysdate'
          WHEN 'VARCHAR2'
            THEN
              '''' || '0' || ''''
          ELSE
            '0'
          END;
          l_sql := l_sql || CHR(10) || '              ' ||
                   CASE
                   WHEN i > 1
                     THEN
                       'OR '
                   ELSE
                     NULL
                   END || 'NVL(:NEW.' || r_col.column_name || ',' || l_data_type || ') != NVL(:OLD.' || r_col.column_name || ',' || l_data_type || ')';
        END IF;
      END LOOP;
      l_sql := l_sql || ' THEN l_is_changed := TRUE;
          ELSE
          NULL;';
      FOR r IN
      (SELECT c.column_name
       FROM user_tab_columns c
       WHERE c.table_name = UPPER(i_table_name)
             AND c.column_name LIKE '%MODIFIED%'
      )
      LOOP
        l_sql := l_sql || ' :new.' || r.column_name || ' := :old.' || r.column_name || ';';
      END LOOP;
      l_sql := l_sql || 'END IF;
			  END IF;
        IF l_is_changed THEN' || CHR(10);
      l_sql := l_sql || get_user || CHR(10);
      FOR r_col IN
      (SELECT c.column_name
       FROM user_tab_columns c
       WHERE c.table_name = UPPER(i_table_name)
      )
      LOOP
        l_sql := l_sql || '         l_tab_jn.' || r_col.column_name || ':=:OLD.' || r_col.column_name || ';' || CHR(10);
      END LOOP;
      l_sql := l_sql || '         INSERT INTO ' || l_audit_tab_name || ' VALUES l_tab_jn;' || CHR(10);
      l_sql := l_sql || 'END IF; ' || CHR(10) || ' END;';
-- print (l_sql);
      EXECUTE IMMEDIATE l_sql;
      EXCEPTION
      WHEN OTHERS THEN
      print('create trigger ' || l_trg_name || ' error:' || sqlerrm);
    END;
  PROCEDURE auto_add_new_columns(
    i_table_name IN all_tables.table_name%TYPE)
/*
En arkiverings tabell skal være eksakt lik tabellen den skal kopieres fra.
Metoden sjekker om fra-tabellen differ fra arkiveringstabellen ved at det fins
kolonner som ikke fins i ark-tabellen.
I så fall legges den nye kolonnen til ved ALTER TABLE MODIFY.
*/
  IS
    l_data_length   VARCHAR2(300);
    l_sql           VARCHAR2(2000);
    l_archive_table VARCHAR2(30) := UPPER(SUBSTR(i_table_name, 1, 27) || '_JN');
    BEGIN
      print('auto_add_new_columns to ' || l_archive_table || ' of ' || i_table_name);
      FOR r_col IN
      (SELECT *
       FROM user_tab_columns tc
       WHERE tc.table_name = upper(i_table_name)
             AND NOT EXISTS
       (SELECT 1
        FROM user_tab_columns tc2
        WHERE tc2.table_name = l_archive_table
              AND tc.column_name = tc2.column_name
       )
      )
      LOOP
        IF r_col.data_type IN ('VARCHAR2', 'NUMBER', 'CHAR')
        THEN
          l_data_length := '(' || r_col.data_length || ')';
        ELSE
          l_data_length := NULL;
        END IF;
        l_sql := 'alter table ' || l_archive_table || ' add ' || r_col.column_name || ' ' || r_col.data_type;
        IF r_col.data_type IN ('NUMBER', 'VARCHAR2', 'CHAR')
        THEN
          l_sql := l_sql || l_data_length;
        END IF;
        print(l_sql);
        EXECUTE IMMEDIATE l_sql;
      END LOOP;
      FOR r_col IN
      (SELECT *
       FROM user_tab_columns tc
       WHERE tc.table_name = l_archive_table
             AND tc.column_name NOT LIKE 'JN_%'
             AND NOT EXISTS
       (SELECT 1
        FROM user_tab_columns tc2
        WHERE tc2.table_name = upper(i_table_name)
              AND tc.column_name = tc2.column_name
       )
      )
      LOOP
        IF r_col.data_type IN ('VARCHAR2', 'NUMBER')
        THEN
          l_data_length := '(' || r_col.data_length || ')';
        ELSE
          l_data_length := NULL;
        END IF;
        l_sql := 'alter table ' || l_archive_table || ' drop column ' || r_col.column_name;
        print(l_sql);
        EXECUTE IMMEDIATE l_sql;
      END LOOP;
      EXCEPTION WHEN NO_DATA_FOUND THEN NULL;
    END;
  PROCEDURE auto_modify_datatype(
    i_table_name IN all_tables.table_name%TYPE)
  IS
    l_audit_tab_name VARCHAR2(30) := UPPER(SUBSTR(i_table_name, 1, 27) || '_JN');
    l_sql            VARCHAR2(4000);
    BEGIN
      FOR r IN
      (SELECT tc.column_name
              || ' '
              || tc.data_type
              || ' ('
              || tc.data_length
              || ')' data_mod
       FROM user_tab_columns tc
         JOIN user_tab_columns tca
           ON tca.column_name = tc.column_name
       WHERE tc.table_name = upper(i_table_name)
             AND tca.table_name = l_audit_tab_name
             AND (tca.data_type != tc.data_Type
                  OR tca.data_length != tc.data_length
             )
      )
      LOOP
        l_sql := 'alter table ' || l_audit_tab_name || ' modify (' || r.data_mod || ')';
        print(l_sql);
        EXECUTE IMMEDIATE l_sql;
      END LOOP;
    END;
  PROCEDURE auto_modify_archive_table(
    i_table_name IN all_tables.table_name%TYPE)
  IS
    BEGIN
      auto_add_new_columns(upper(i_table_name));
      auto_modify_datatype(upper(i_table_name));
    END;
  PROCEDURE create_or_modify_archive_table(i_table_name IN all_tables.table_name%TYPE)
  IS
    BEGIN
      print('create_or_modify_archive_table for ' || i_table_name);
      create_audit_table(upper(i_table_name));
      print('before auto_add_new_columns ' || upper(i_table_name));
      auto_add_new_columns(upper(i_table_name));
      auto_modify_datatype(upper(i_table_name));
      gen_audit_trg(upper(i_table_name));
    END;
END;
