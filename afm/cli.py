"""Main Entry Point."""

import os

from collections import defaultdict
import csv
from pprint import pprint
from urllib.parse import urlparse
import requests

import click
from dotenv import load_dotenv, find_dotenv
from twilio.rest import Client
import psycopg2
import psycopg2.extras


load_dotenv(find_dotenv())
VAN_API_KEY = os.getenv('VAN_API_KEY', None)
DATABASE_URL = os.getenv('DATABASE_URL', None)
ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID', None)
AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN', None)
client = Client(ACCOUNT_SID, AUTH_TOKEN)    # pylint:disable=invalid-name


@click.group()
def cli():
    """Helper scripts for the Abdul for Michigan campaign."""
    pass

@cli.group()
def analysis():
    """Tools for analysis."""
    pass


@analysis.command()
@click.argument('csv-input', type=click.File('r'))
def number_stats(csv_input):
    """Breakdown Twilio send/receive logs to find number of texts sent from each Twilio number."""
    reader = csv.DictReader(csv_input)

    from_number_count = defaultdict(int)

    for row in reader:
        if row['Direction'] != 'inbound':
            from_number_count[row['From']] += 1

    click.echo('Breakdown by number sent from:')
    for key, value in from_number_count.items():
        click.echo(f'{key}: {value}')


@analysis.command()
@click.argument('superset-csv', type=click.File('r'))
@click.argument('subset-csv', type=click.File('r'))
@click.argument('csv-output', type=click.File('w'))
def dedup(superset_csv, subset_csv, csv_output):
    """Remove all the numbers in subset-csv from superset-csv, saving the result to the output."""
    superset_reader = csv.DictReader(superset_csv)
    subset_reader = csv.DictReader(subset_csv)

    fieldnames = superset_reader.fieldnames
    writer = csv.DictWriter(csv_output,
                            lineterminator='\n',
                            fieldnames=fieldnames)
    writer.writeheader()

    def format_cell(cell):
        """Ensure cell numbers have a leading +1."""
        cell_ten = cell[-10:]
        return f'+1{cell_ten}'

    subset_numbers = set([format_cell(row['contact[cell]'])
                          for row in subset_reader])

    dup_count = 0
    untouched_count = 0
    for row in superset_reader:
        cell = format_cell(row['cell'])
        if cell not in subset_numbers:
            writer.writerow(row)
            untouched_count += 1
        else:
            dup_count += 1

    click.echo(f'Removed {dup_count} duplicate numbers.')
    click.echo(f'There were {untouched_count} remaining numbers.')


@cli.group()
def twilio():
    """Commands for interacting with Twilio."""
    pass


@twilio.command()
@click.option('--group-by-area-code', '-g', is_flag=True, help='Group by area code.')
def count(group_by_area_code):
    """Return the number of phone numbers."""
    incoming_phone_numbers = client.incoming_phone_numbers.list()
    phone_number_count = len(incoming_phone_numbers)
    click.echo(f'Number of Twilio SMS Numbers: {phone_number_count}')

    if group_by_area_code:
        click.echo('\nBy area code:')
        area_codes = defaultdict(lambda: 0)
        for number in incoming_phone_numbers:
            area_code = number.phone_number[2:5]
            area_codes[area_code] += 1
        for area_code, number_count in area_codes.items():
            click.echo(f'({area_code}): {number_count}')


@twilio.command()
@click.argument('csv-input', type=click.File('r'))
@click.argument('csv-output', type=click.File('w'))
@click.option('--auto-purchase', '-y', is_flag=True,
              help=('Purchase maximum available numbers if less than the requested '
                    'requested_quantity.'))
@click.option('--service-sid', '-s', help='Messaging service SID to add new numbers to.')
def purchase(csv_input, csv_output, auto_purchase, service_sid):
    """Purchase new Twilio numbers based on the specified area code counts.

    Accepts a csv with 'area_code' and 'quantity' columns.
    """
    reader = csv.DictReader(csv_input)
    purchase_order = {}
    for row in reader:
        area_code = row['area_code']
        requested_quantity = int(row['quantity'])
        numbers = client.available_phone_numbers('US').local.list(area_code=area_code)
        available_count = len(numbers)
        if available_count == 0:
            click.echo((f'Area code ({area_code}) has {available_count} available numbers. '
                        'Skipping this area code.'))
            continue
        elif available_count < requested_quantity:
            prompt = (f'Area code ({area_code}) only has {available_count} available numbers.'
                      f'You requested {requested_quantity}.\n\n'
                      f'Would you like to purchase {available_count} instead?')
            if not auto_purchase and not click.confirm(prompt):
                continue
        purchase_order[area_code] = numbers[0:requested_quantity]

    click.echo('Please confirm your order:')
    for area_code, number_list in purchase_order.items():
        number_count = len(number_list)
        click.echo(f'({area_code}): {number_count}')
    if click.confirm('\nIs this correct?', abort=True):
        fieldnames = ['area_code', 'number', 'purchase_status', 'service_status', 'message']
        writer = csv.DictWriter(csv_output,
                                lineterminator='\n',
                                fieldnames=fieldnames)
        writer.writeheader()

        results = {'success': 0, 'error': 0}
        for area_code, number_list in purchase_order.items():
            results[area_code] = {}
            for number in number_list:
                phone_number = number.phone_number
                row = {
                    'area_code': area_code,
                    'number': phone_number
                }

                # Purchase number
                try:
                    client.incoming_phone_numbers.create(phone_number=phone_number)
                    row['purchase_status'] = 'success'
                except Exception as exc:        # pylint:disable=broad-except
                    row['purchase_status'] = 'error'
                    row['message'] = str(exc)

                # Add to messaging service
                if service_sid:
                    try:
                        incoming_phone_number = client.incoming_phone_numbers.list(
                            phone_number=phone_number
                        )[0]
                        phone_number_sid = incoming_phone_number.sid
                        phone_number = client.messaging \
                            .services(service_sid) \
                            .phone_numbers \
                            .create(phone_number_sid=phone_number_sid)
                        row['service_status'] = 'success'
                    except Exception as exc:    # pylint:disable=broad-except
                        row['service_status'] = 'error'
                        row['message'] = str(exc)

                writer.writerow(row)


@twilio.group()
def service():
    """Commands for interacting with Twilio MMS service."""
    pass


@service.command()
@click.argument('service-sid')
def count(service_sid):
    """Get number of phone numbers in a service."""
    phone_numbers = client.messaging \
        .services(service_sid) \
        .phone_numbers \
        .list()
    number_count = len(phone_numbers)
    click.echo(f'Found {number_count} numbers in service {service_sid}')


@service.command()
@click.argument('csv-input', type=click.File('r'))
@click.argument('service-sid')
def add(csv_input, service_sid):
    """Add numbers to a messaging service."""
    reader = csv.DictReader(csv_input)
    for row in reader:
        phone_number = row['number']
        incoming_phone_number = client.incoming_phone_numbers.list(phone_number=phone_number)[0]
        phone_number_sid = incoming_phone_number.sid
        phone_number = client.messaging \
            .services(service_sid) \
            .phone_numbers \
            .create(phone_number_sid=phone_number_sid)


@twilio.command()
@click.argument('csv-input', type=click.File('r'))
@click.argument('csv-output', type=click.File('w'))
@click.option('--quiet', '-q', is_flag=True, help='Do not print stats. Only write to output csv.')
def sms(csv_input, csv_output, quiet):
    """Lookup carrier information from an input Twilio error log csv and
    write an Output csv with additional 'Carrier' column.
    """
    reader = csv.DictReader(csv_input)
    fieldnames = reader.fieldnames + ['Carrier']
    writer = csv.DictWriter(csv_output,
                            lineterminator='\n',
                            fieldnames=fieldnames)
    writer.writeheader()

    error_count = defaultdict(int)
    carrier_count = defaultdict(int)

    for row in reader:
        error_code = row['ErrorCode']
        error_count[error_code] += 1

        if error_code == '30007':
            destination = row['To']
            phone_number = client.lookups.phone_numbers(f'+1{destination}').fetch(type='carrier')
            carrier_name = phone_number.carrier['name']
            row['Carrier'] = carrier_name
            carrier_count[carrier_name] += 1

        writer.writerow(row)

    if not quiet:
        click.echo('Results')
        click.echo('(full results in text_errors_carrier.csv)\n')

        click.echo('Breakdown by error type:')
        for key, value in error_count.items():
            click.echo(f'{key}: {value}')

        click.echo('')

        print('30007 breakdown by carrier:')
        for key, value in carrier_count.items():
            click.echo(f'{key}: {value}')


@cli.group()
def van():
    """Tools for working with VAN."""
    pass


@van.command()
@click.argument('campaign-id')
def sync_responses(campaign_id):
    """Re-send survey responses to VAN."""
    if not DATABASE_URL:
        raise click.Abort('DATABASE_URL environment variable is required!')
    if not VAN_API_KEY:
        raise click.Abort('VAN_API_KEY environment variable is required!')

    result = urlparse(DATABASE_URL)
    username = result.username
    password = result.password
    database = result.path[1:]
    hostname = result.hostname
    connection = psycopg2.connect(
        database=database,
        user=username,
        password=password,
        host=hostname
    )
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # 1. Limit question responses to the specified campaign via `campaign_contact.campaign_id`
    # 2. Limit interaction steps to the specified campaign
    # 3. Find the interaction step corresponding to the question response by comparing `value` and
    #    `answer_option` (seems like this should really be a foreign key...)
    # 4. Ignore question responses that don't map to an external response
    # 5. Grab the external question from the parent interaction step
    # 6. Select fields necessary to submit to external system
    cursor.execute(f'''
        SELECT qr.id AS qr_id,
            to_json(qr.created_at) AS qr_created_at,
            qr.value AS qr_value,
            cc.external_id AS cc_external_id,
            istep.external_response,
            pstep.external_question AS external_question
        FROM question_response AS qr
        INNER JOIN campaign_contact AS cc
            ON qr.campaign_contact_id = cc.id
        INNER JOIN interaction_step AS istep
            ON qr.value = istep.answer_option
        INNER JOIN interaction_step AS pstep
            ON istep.parent_interaction_id = pstep.id
        WHERE cc.campaign_id = {campaign_id}
            AND istep.campaign_id = {campaign_id}
            AND istep.external_response != '';
        ''')

    records = cursor.fetchall()
    errors = []

    click.echo(f'There are {len(records)} records')

    with click.progressbar(records, label='Updating records') as progess_bar:
        for record in progess_bar:
            cc_external_id = record['cc_external_id']
            action_date = record['qr_created_at']
            external_question = record['external_question']
            external_response = int(record['external_response'])

            url = f'https://osdi.ngpvan.com/api/v1/people/{cc_external_id}/record_canvass_helper/'

            headers = {
                'OSDI-Api-Token': VAN_API_KEY,
                'Content-type': 'application/hal+json',
            }

            body = {
                'canvass': {
                    'action_date': action_date,
                    'contact_type': 'SMS Text',
                    'success': True,
                    'status_code': '',
                },
                'add_answers': [{
                    'question': external_question,
                    'responses': [external_response],
                }],
            }

            result = requests.post(url, headers=headers, json=body)

            if result.status_code != 200:
                errors.append((cc_external_id, result.status_code, result.reason))

    click.echo('Completed')
    if errors:
        click.echo('Erros:')
        pprint(errors)

    connection.close()
