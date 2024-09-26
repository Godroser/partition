
def connect_db:    
  source_db_credentials = {
      "host": args.db_host,
      "port": args.db_port,
      "dbname": args.database,
      "user": args.db_user,
      "password": args.db_password,
  }

  destination_db_credentials = {
      "host": args.sample_db_host,
      "port": args.sample_db_port,
      "dbname": args.sample_db_name,
      "user": args.sample_db_user,
      "password": args.sample_db_password,
  }

  source_conn = psycopg2.connect(**source_db_credentials)
  destination_conn = psycopg2.connect(**destination_db_credentials)

  source_cursor = source_conn.cursor()
  destination_cursor = destination_conn.cursor()

  # Get the list of tables in the source database
  source_cursor.execute("""
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
  """)
  tables = source_cursor.fetchall()

if 