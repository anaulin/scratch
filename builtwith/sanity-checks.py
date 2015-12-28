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
  'Survey - Wufoo.csv' : 'wufoo'
}

TECH = TECH_FILES.values()

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

def load_file(filename, tech):
  print 'Checking file: ', filename
  with open(DIR_PREFIX + filename, 'rU') as csvfile:
    reader = csv.reader(csvfile)
    data = {}
    malformed_rows = 0
    for row in reader:
      if row[0] == 'Domain':
        print 'Found header row: ', row
        sys.stdout.flush()
        continue
      if not row[0]:
        print 'Found row with no domain: ', row
        malformed_rows += 1
        sys.stdout.flush()
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
      if datum['domain'] in data:
        print 'Duplicate domain: ' + datum['domain']
        sys.stdout.flush()
      data[datum['domain']] = datum
    print '%s: malformed rows: %d' % (filename, malformed_rows)
    print '%s: total rows: %d' % (filename, len(data.keys()))
    sys.stdout.flush()
    return data

def main():
  signal.signal(signal.SIGINT, signal.SIG_DFL)
  all_data = {}
  files = 0
  for filename, tech in TECH_FILES.iteritems():
    new_data = load_file(filename, tech)
    # Merge datasets
    for domain, data in new_data.iteritems():
      if domain in all_data:
        all_data[domain].update(data)
      else:
        all_data[domain] = data
    print 'All data rows: ', len(all_data.keys())
    sys.stdout.flush()
    files += 1
    if files == 3:
      break

  counters = {}
  for domain, data in all_data.iteritems():
    for key in data.keys():
      if key in counters.keys():
        counters[key] += 1
      else:
        counters[key] = 1
  print 'Counters: ', counters

if __name__ == "__main__":
    main()
