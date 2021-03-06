# Script to import some BuiltWith exports into a Postgres DB

import csv
import dateutil.parser
import psycopg2

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
#  'Analytics - Heap.csv' : 'heap',
  'Analytics - Mixpanel.csv' : 'mixpanel',
  'Analytics - Segment.csv' : 'segment',
#  'CRM - Marketo Mail.csv' : 'marketo_mail',
#  'CRM - Marketo.csv' : 'marketo',
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

def create_table():
  conn, cursor = connect()
  try:
    print 'Creating table'
    cursor.execute("""CREATE TABLE builtwith (
      domain           varchar(200),
      vertical         varchar(300),
      quantcast        int,
      alexa            int,
      first_indexed    date,
      last_indexed     date
    )""")
    conn.commit()
  except psycopg2.Error, e:
    print 'Creation failed!'
    print e
  cursor.close()
  conn.close()

def drop_table():
  conn, cursor = connect()
  cursor.execute("""DROP TABLE builtwith""")
  conn.commit()
  cursor.close()
  conn.close()
  print 'Dropped table'

def upsert_rows(tech, data, cursor):
  print '.'
  UPSERT_SQL = """ WITH upd AS (
    UPDATE builtwith b
    SET (%s, first_detected_%s, last_found_%s)
  """ % (tech, tech, tech)
  UPSERT_SQL += """ WHERE b.domain = %(domain)s)"""
  UPSERT_SQL += """
  """
  INSERT_SQL = """INSERT INTO builtwith (domain, vertical, quantcast,
    alexa, first_indexed, last_indexed, %s, first_detected_%s,
    last_found_%s) """ % (tech, tech, tech)
  INSERT_SQL += """VALUES (%(domain)s, %(vertical)s, %(quantcast)s,
    %(alexa)s, %(first_indexed)s, %(last_indexed)s, %(tech)s,
    %(first_detected)s, %(last_found)s) """
  INSERT_SQL += """ ON CONFLICT DO UPDATE SET (%s, first_detected_%s,
        last_found_%s) """ % (tech, tech, tech)
  INSERT_SQL += """  = (%(tech)s, %(first_detected)s, %(last_found)s)"""
  print INSERT_SQL
  print len(data)
  print data[0]
  cursor.executemany(INSERT_SQL, data)

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

def import_file(filename, tech):
  print 'Importing: %s as tech %s' % (filename, tech)
  conn, cursor = connect()
  # Create columns for this tech
  try:
    tech_column = "ALTER TABLE builtwith ADD COLUMN %s boolean" % tech
    cursor.execute(tech_column)
    first_detected_column = (
      "ALTER TABLE builtwith ADD COLUMN first_detected_%s date" % tech)
    cursor.execute(first_detected_column)
    last_found_column = (
      "ALTER TABLE builtwith ADD COLUMN last_found_%s date" % tech)
    cursor.execute(last_found_column)
    conn.commit()
  except:
    print 'Columns for tech %s already exist. Not creating.' % tech
    conn.commit()

  # Insert all data from file
  with open(DIR_PREFIX + filename, 'rU') as csvfile:
    reader = csv.reader(csvfile)
    data = []
    for row in reader:
      if not row[0] or row[0] == 'Domain':
        continue
      data.append({
        'domain': row[0],
        'tech': True,
        'vertical': row[4],
        'quantcast': valid_int(row[5]),
        'alexa': valid_int(row[6]),
        'first_detected': valid_date(row[24]),
        'last_found': valid_date(row[25]),
        'first_indexed': valid_date(row[26]),
        'last_indexed': valid_date(row[27])
      })
      if len(data) == 1000:
        upsert_rows(tech, data, cursor)
        conn.commit()
        data = []
  conn.commit()
  cursor.close()
  conn.close()

def main():
  #for filename, tech in TECH_FILES.iteritems():
  import_file('Survey - Mailchimp.csv', 'mailchimp')


if __name__ == "__main__":
    main()
