"""Main Entry Point."""

import os

from collections import defaultdict
import csv
from pprint import pprint

import click
from dotenv import load_dotenv, find_dotenv
from twilio.rest import Client


load_dotenv(find_dotenv())
account_sid = os.getenv('TWILIO_ACCOUNT_SID', None)
auth_token = os.getenv('TWILIO_AUTH_TOKEN', None)
client = Client(account_sid, auth_token)


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
        for area_code, count in area_codes.items():
            click.echo(f'({area_code}): {count}')


@twilio.command()
@click.argument('csv-input', type=click.File('r'))
@click.argument('csv-output', type=click.File('w'))
@click.option('--auto-purchase', '-y', is_flag=True,
              help='Purchase maximum available numbers if less than the requested requested_quantity.')
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
            click.echo(f'Area code ({area_code}) has {available_count} available numbers. Skipping this area code.')
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
        count = len(number_list)
        click.echo(f'({area_code}): {count}')
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
                except Exception as exc:
                    row['purchase_status'] = 'error'
                    row['message'] = str(exc)

                # Add to messaging service
                if service_sid:
                    try:
                        incoming_phone_number = client.incoming_phone_numbers.list(phone_number=phone_number)[0]
                        phone_number_sid = incoming_phone_number.sid
                        phone_number = client.messaging \
                            .services(service_sid) \
                            .phone_numbers \
                            .create(phone_number_sid=phone_number_sid)
                        row['service_status'] = 'success'
                    except Exception:
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
    count = len(phone_numbers)
    click.echo(f'Found {count} numbers in service {service_sid}')


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
def sms(csv_input, csv_output):
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
