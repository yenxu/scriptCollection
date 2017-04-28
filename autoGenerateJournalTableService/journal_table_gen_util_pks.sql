CREATE OR REPLACE
PACKAGE journal_table_gen_util
IS
	PROCEDURE remove_not_null_check_aud_tab(i_table_name IN VARCHAR2);
	PROCEDURE gen_audit_trg(i_table_name IN VARCHAR2);
	PROCEDURE create_audit_table(i_table_name IN VARCHAR2);
	PROCEDURE auto_modify_archive_table(i_table_name IN all_tables.table_name%TYPE);
	PROCEDURE create_or_modify_archive_table(i_table_name IN all_tables.table_name%TYPE);
END;

