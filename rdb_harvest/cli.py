from argparse import ArgumentParser

from .harvest import HarvestFDBData


def main():
    parser = ArgumentParser(description='CLI for harvesting a RDB endpoint.')
    parser.add_argument('harvest', action='store', help='Which dataset should be harvested: ach, proj, person, org, pub.')
    parser.add_argument('user', action='store', help='The user name of the OAI Service.')
    parser.add_argument('password', action='store', help='The password of the OAI Service.')

    parser.add_argument('-d', dest='dest_path', action='store', help='Path where the harvested records will be stored.',
                        default='')

    parser.add_argument('-u', dest='upload', action='store_true', help='If set the harvested records will '
                                                                       'be uploaded to elastic search.')
    parser.add_argument('url', action='store', help='URL of the elastic server the harvester will upload to.',
                        default='https://localhost:9200/')
    parser.add_argument('-b', dest='elastic_base', action='store', help='The base of the elastic index name the harvested '
                                                                        'data will be stored in (default fdb adds harvest '
                                                                        'short-name and current date).',
                        default='fdb-')

    args = parser.parse_args()

    harvester = HarvestFDBData(args.user, args.password, args.url)

    if args.harvest in ['ach', 'proj', 'person', 'org', 'pub']:
        # TODO: Implement a good way to add a date...
        harvester.harvest(args.harvest)
    else:
        raise Exception('Havest needs to be one of ach, proj, person, org, pub. Not %s.' % args.harvest)

    if args.upload:
        harvester.upload_to_elastic(args.harvest, 'identifier' if args.harvest in ['ach', 'proj', 'pub'] else 'mcss_id')
