from xmljson import BadgerFish, Parker
import logging
import json


def purge_namespaces(root):
    for element in root.getiterator():
        if element.tag.startswith('{'):
            element.tag = element.tag.split('}', 1)[1]
    return root


def xml2json(element, conv='parker'):
    """Takes an XML record and returns the json representation of it."""
    if conv == 'bf':
        convention = BadgerFish(xml_fromstring=str)
    elif conv == 'parker':
        convention = Parker(xml_fromstring=str)
    else:
        logging.critical('Invalid XML2JSON Convention: ' + conv)
        raise ValueError('The parameter @conv should be "bf" or "parker" not ' + conv)

    data = convention.data(element)
    return json.dumps(data, indent='    ', ensure_ascii=False)


def clean_data(record, id_name: str):
    """Cleans FDB data of some common invalid values. May need to be extended in the future."""
    logging.info('Fixing isses with publication data.')
    if 'enddate' in record:
        if record['enddate'] == '0000-00-00':
            del record['enddate']
            logging.warning(
                'Removed end date in publication ' + record[id_name] + ' because the date was 0000-00-00.')
    if 'startdate' in record:
        if record['startdate'] == '0000-00-00':
            del record['startdate']
            logging.warning(
                'Removed start date in publication ' + record[id_name] + ' because the date was 0000-00-00.')
    if 'date' in record:
        if record['date'] == '0000-00-00':
            del record['date']
            logging.warning(
                'Removed date in publication ' + record[id_name] + ' because the date was 0000-00-00.')
    if 'unibascreator' in record:
        if not isinstance(record['unibascreator'], dict):
            del record['unibascreator']
            logging.warning(
                'Removed unibascreator in publication ' + record[id_name] + ' it was an empty field.')