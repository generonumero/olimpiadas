#!/usr/bin/env python

import csv
import glob
import os

from collections import namedtuple, OrderedDict
from io import BytesIO
from pathlib import Path
from re import compile as regexp_compile

import requests
import requests_cache
import rows
import rows.utils


URL_YEARS = 'http://www.sports-reference.com/olympics/summer/'
URL_COUNTRIES = 'http://www.sports-reference.com/olympics/countries/'
URL_DATA = 'http://www.sports-reference.com/olympics/countries/{country_code}/summer/{year}/'
REGEXP_COUNTRY = regexp_compile(r'/olympics/countries/([A-Z]{3})/">([^<]+)<')
FIELDS = OrderedDict([
    ('rk', rows.fields.IntegerField),
    ('athlete', rows.fields.TextField),
    ('gender', rows.fields.TextField),
    ('age', rows.fields.IntegerField),
    ('sport', rows.fields.TextField),
    ('gold', rows.fields.IntegerField),
    ('silver', rows.fields.IntegerField),
    ('bronze', rows.fields.IntegerField),
    ('total', rows.fields.IntegerField),
])
FULL_FIELDS = OrderedDict([
    ('year', rows.fields.IntegerField),
    ('country_code', rows.fields.TextField),
    ('country_name', rows.fields.TextField),
])
FULL_FIELDS.update(FIELDS)
del FULL_FIELDS['rk']
Country = namedtuple('Country', ['code', 'name'])


def download_years():
    "Return a list with the game's years as integers"

    response = requests.get(URL_YEARS)
    html = response.content
    games = rows.import_from_html(BytesIO(html), encoding='utf-8')
    return [game.year for game in games]


def download_countries():
    "Return the countries who played the games"

    response = requests.get(URL_COUNTRIES)
    html = response.text
    return {result[0]: Country(*result)
            for result in REGEXP_COUNTRY.findall(html)}


def _make_filename(year, country_code):
    return '{}-{}.csv'.format(year, country_code)


def _parse_filename(filename):
    return filename.name.split('.')[0].split('-')


def download_game_data_for_country(path, year, country_code):
    'Download country athlete data for a specific year if not downloaded yet'

    filename = path.joinpath(_make_filename(year, country_code))
    if filename.exists():
        print(' (already downloaded, skipping)')
        return

    url = URL_DATA.format(year=year, country_code=country_code)
    response = requests.get(url)
    if '404' in response.url:  # country didn't played this year
        print(" (didn't play this year, skipping)")
        return

    html = response.content
    table = rows.import_from_html(BytesIO(html),
                                  encoding='utf-8',
                                  fields=FIELDS)
    rows.export_to_csv(table, str(filename.absolute()), encoding='utf-8')
    print(' ok')


def download_all(path):
    'Download data for all countries during all years'

    if not path.exists():
        path.mkdir()

    years = download_years()
    countries_by_code = download_countries()
    for year in years:
        for country in countries_by_code.values():
            # TODO: instead of trying every possible country we could optimize
            # this by listing the countries who attended to this specific year
            # (is there any page which have this index?)
            print('Downloading year: {}, country: {}...'
                    .format(year, country.name), end='')
            download_game_data_for_country(path, year, country.code)


def merge_files(filenames, output):
    'Merge all game files into one CSV file, adding year and country columns'

    if not output.parent.exists():
        output.parent.mkdir()

    countries_by_code = download_countries()
    games = rows.Table(fields=FULL_FIELDS)

    for filename in filenames:
        year, country_code = _parse_filename(filename)
        country = countries_by_code[country_code]
        print('Merging year: {}, country: {}...'.format(year, country.name))
        game = rows.import_from_csv(
                str(filename.absolute()),
                fields=FIELDS,
                dialect=csv.excel,
                encoding='utf-8'
        )
        for row in game:
            data = row._asdict()
            data['year'] = year
            data['country_code'] = country_code
            data['country_name'] = country.name
            del data['rk']
            games.append(data)
    games.order_by('-year')
    rows.utils.export_to_uri(games, str(output.absolute()))


def main():
    data_path = Path('./data')
    output = Path('./output/all-games.csv')
    requests_cache.install_cache('olympic-games')

    download_all(data_path)
    merge_files(data_path.glob('*.csv'), output)


if __name__ == '__main__':
    main()
