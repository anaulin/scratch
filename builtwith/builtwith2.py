# Script to import some BuiltWith exports into a Postgres DB

import csv
import datetime
import dateutil.parser
import psycopg2
import signal
import sys


DIR_PREFIX = ('/Users/anaulin/builtwith-exports/')
# A map from a csv filename to the column name for its technology
TECH_FILES = {
  'AB - Adobe Test Target.csv' : 'adobe_test',
  'AB - Monetate.csv' : 'monetate',
  'AB - Optimizely.csv' : 'optimizely',
  'Ads - Adroll.csv' : 'adroll',
  'Ads - Facebook Custom Audiences.csv' : 'fb_custom_audiences',
  'Ads - Google Remarketing.csv' : 'google_remarketing',
  'Ads - Perfect Audience.csv' : 'perfect_audience',
  'Analytics - Heap.csv' : 'heap',
  'Analytics - Mixpanel.csv' : 'mixpanel',
  'Analytics - Segment.csv' : 'segment',
  'CRM - Marketo Mail.csv' : 'marketo_mail',
  'CRM - Marketo.csv' : 'marketo',
  'CRM - Salesforce SPF.csv' : 'salesforce_spf',
  'CRM - Salesforce.csv' : 'salesforce',
  'CRM - Zendesk.csv' : 'zendesk',
  'Payment - Braintree.csv' : 'braintree',
  'Payment - PayPal.csv' : 'paypal',
  'Payment - Stripe.csv' : 'stripe',
  'Survey - Mailchimp.csv' : 'mailchimp',
  'Survey - Qualaroo.csv' : 'qualaroo',
  'Survey - Wufoo.csv' : 'wufoo',
}

TECH = TECH_FILES.values()

DATA_TEMPLATE = {
  'domain' : None,
  'vertical' : None,
  'quantcast': None,
  'alexa': None,
  'first_indexed': None,
  'last_indexed': None
}
for t in TECH:
  DATA_TEMPLATE[t] = None
  DATA_TEMPLATE['first_detected_%s' % t] = None
  DATA_TEMPLATE['last_found_%s' % t] = None

TABLE = 'builtwith'

def build_insert_sql():
  tech_cols = TECH_FILES.values()
  first_detected_cols = ['first_detected_%s' % (t) for t in tech_cols]
  last_found_cols = ['last_found_%s' % (t) for t in tech_cols]

  INSERT_SQL = """INSERT INTO %s (domain, vertical, quantcast,
    alexa, first_indexed, last_indexed, """ % TABLE
  INSERT_SQL += ', '.join(tech_cols)
  INSERT_SQL += ', '
  INSERT_SQL += ', '.join(first_detected_cols)
  INSERT_SQL += ', '
  INSERT_SQL += ', '.join(last_found_cols)
  INSERT_SQL += ") VALUES "
  return INSERT_SQL
INSERT_SQL = build_insert_sql()

def build_values_sql():
  tech_cols = TECH_FILES.values()
  first_detected_cols = ['first_detected_%s' % (t) for t in tech_cols]
  last_found_cols = ['last_found_%s' % (t) for t in tech_cols]

  VALUES_SQL = """(%(domain)s, %(vertical)s, %(quantcast)s,
    %(alexa)s, %(first_indexed)s, %(last_indexed)s, """
  VALUES_SQL += ', '.join(['%(' + t + ')s' for t in tech_cols])
  VALUES_SQL += ', '
  VALUES_SQL += ', '.join(['%(first_detected_' + t + ')s' 
    for t in tech_cols])
  VALUES_SQL += ', '
  VALUES_SQL += ', '.join(['%(last_found_' + t + ')s' for t in tech_cols])
  VALUES_SQL += ")"
  return VALUES_SQL
VALUES_SQL = build_values_sql()

def connect():
  conn = psycopg2.connect(
    host='104.199.143.144',
    port='5432',
    dbname='postgres',
    user='postgres',
    password='dUsa4iom'
  )
  cursor = conn.cursor()
  return conn, cursor

def drop_table():
  conn, cursor = connect()
  cursor.execute("DROP TABLE " + TABLE)
  conn.commit()
  cursor.close()
  conn.close()
  print 'Dropped table'

def create_table():
  conn, cursor = connect()
  print 'Creating table'
  CREATE_SQL = "CREATE TABLE " + TABLE
  CREATE_SQL += """ (
    domain           varchar(200) PRIMARY KEY,
    vertical         varchar(300),
    quantcast        int,
    alexa            int,
    first_indexed    date,
    last_indexed     date
  )"""
  cursor.execute(CREATE_SQL)
  conn.commit()

  create_columns(cursor)
  conn.commit()

  cursor.close()
  conn.close()

def create_columns(cursor):
  tech = TECH_FILES.values()
  for t in tech:
    COLUMNS = "ALTER TABLE " + TABLE
    COLUMNS += " ADD COLUMN " + t + " boolean,"
    COLUMNS += " ADD COLUMN first_detected_" + t + " date,"
    COLUMNS += " ADD COLUMN last_found_" + t + " date"
    cursor.execute(COLUMNS, (t, t, t))

def valid_int(value):
  try:
    return int(value)
  except ValueError:
    return None

def valid_date(value):
  if not value:
    return None
  try:
    dateutil.parser.parse(value)
    return value
  except ValueError:
    return None

def process_file(filename, tech):
  print 'Processing file: %s as Tech: %s' % (filename, tech)
  with open(DIR_PREFIX + filename, 'rU') as csvfile:
    reader = csv.reader(csvfile)
    data = {}
    malformed_rows = 0
    for row in reader:
      if row[0] == 'Domain':
        continue
      if not row[0] or not row[0].strip():
        malformed_rows += 1
        continue
      datum = {
        'domain': row[0],
        tech : True,
        'vertical': row[4],
        'quantcast': valid_int(row[5]),
        'alexa': valid_int(row[6]),
        'first_detected_%s' % tech: valid_date(row[24]),
        'last_found_%s' % tech: valid_date(row[25]),
        'first_indexed': valid_date(row[26]),
        'last_indexed': valid_date(row[27])
      }
      data[datum['domain']] = datum
    print '%s: rows: %s' % (tech, len(data.keys()))
    print '%s: malformed rows skipped: %s' % (tech, malformed_rows)
    sys.stdout.flush()
    return data

def insert_batch(batch, cursor, conn):
  args_str = ', '.join(cursor.mogrify(VALUES_SQL, data) for data in batch)
  print datetime.datetime.now()
  sys.stdout.flush()
  cursor.execute(INSERT_SQL + args_str)
  conn.commit()
  print datetime.datetime.now(), ' done'
  sys.stdout.flush()

def merge_data(new_data, merged_data):
  for domain, datum in new_data.iteritems():
    if domain in merged_data:
      merged_data[domain].update(datum)
    else:
      merged_data[domain] = datum
  return merged_data

def main():
  signal.signal(signal.SIGINT, signal.SIG_DFL)
  merged_data = {}
  # 1. Process all files and built unified dictionary with data.
  for filename, tech in TECH_FILES.iteritems():
    print datetime.datetime.now()
    sys.stdout.flush()
    new_data = process_file(filename, tech)
    merged_data = merge_data(new_data, merged_data)
    print 'Merged data rows: %s' % (len(merged_data.keys()))
    sys.stdout.flush()

  # 2. Create the table and columns.
  drop_table()
  create_table()

  # 3. Insert the data into the DB in batches
  conn, cursor = connect()
  batch_size = 5000
  print 'Inserting rows in batches'
  sys.stdout.flush()
  batch = []
  for domain in merged_data.keys():
    # Pad the dict with None for keys where there is no data.
    padded_data = DATA_TEMPLATE.copy()
    padded_data.update(merged_data[domain])
    batch.append(padded_data)
    if (len(batch)) == batch_size:
      insert_batch(batch, cursor, conn)
      batch = []
  print 'Inserting final partial batch...'
  sys.stdout.flush()
  if batch:
    insert_batch(batch, cursor, conn)

if __name__ == "__main__":
    main()
