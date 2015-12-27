# Script to import some BuiltWith exports into a Postgres DB

import csv
import datetime
import dateutil.parser
import psycopg2
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
  'Survey - Wufoo.csv' : 'wufoo'
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

TABLE = 'builtwith2'

INSERT_SQL = build_insert_sql()

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
    domain           varchar(200),
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
  INSERT_SQL += """) VALUES (%(domain)s, %(vertical)s, %(quantcast)s,
    %(alexa)s, %(first_indexed)s, %(last_indexed)s, """
  INSERT_SQL += ', '.join(['%(' + t + ')s' for t in tech_cols])
  INSERT_SQL += ', '
  INSERT_SQL += ', '.join(['%(first_detected_' + t + ')s' 
    for t in tech_cols])
  INSERT_SQL += ', '
  INSERT_SQL += ', '.join(['%(last_found_' + t + ')s' for t in tech_cols])
  INSERT_SQL += ")"
  return INSERT_SQL

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
    for row in reader:
      if not row[0] or row[0] == 'Domain':
        continue
      datum = DATA_TEMPLATE.copy()
      datum.update({
        'domain': row[0],
        tech : True,
        'vertical': row[4],
        'quantcast': valid_int(row[5]),
        'alexa': valid_int(row[6]),
        'first_detected_%s' % tech: valid_date(row[24]),
        'last_found_%s' % tech: valid_date(row[25]),
        'first_indexed': valid_date(row[26]),
        'last_indexed': valid_date(row[27])
      })
      data[datum['domain']] = datum
    return data

def main():
  data = {}
  # 1. Process all files and built unified dictionary with data.
  for filename, tech in TECH_FILES.iteritems():
    data.update(process_file(filename, tech))
    print 'Data rows: %s' % (len(data.keys()))
    sys.stdout.flush()

  # 2. Create the table and columns.
  drop_table()
  create_table()

  # 3. Insert the data into the DB in batches
  conn, cursor = connect()
  batch_size = 500
  batch = []
  print 'Inserting rows in batches'
  for domain in data.keys():
    batch.append[data[domain]]
    if (len(batch)) == batch_size:
      cursor.executemany(INSERT_SQL, batch)
      conn.commit()
      batch = []
      print datetime.datetime.now()
      sys.stdout.flush()

if __name__ == "__main__":
    main()