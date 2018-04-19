from simple_elastic import ElasticIndex

from xml.etree import ElementTree
from datetime import date
import os

from oai import OAIHarvester
from utility import *


class HarvestFDBData(object):
    """
    Can harvest all the data from the Uni-Basel Research Database OAI-API and uploads them to
    a elastic search index.


    Usage
    -----
    Harvest all records from the FDB.
    >>> harvester = HarvestFDBData('user', 'password')
    >>> harvester.harvest_all()
    >>> harvester.upload_all()
    
    # Harvest achievements, replace with 'proj' , 'person', 'org' or 'pub' for other downloads.
    >>> harvester = HarvestFDBData('user', 'password')
    >>> harvester.harvest('ach')
    >>> harvester.upload_to_elastic('ach')
    """

    def __init__(self, user, password, elastic_url, base_path='', elastic_index_base='fdb-',
                 logger=logging.getLogger('fdb-harvester')):
        """
        Sets up an harvester for the FDB Data.
        

        :param base_path:           Base path where the harvest should be saved (default local).
        :param elastic_index_base:  The base of the elastic index name. The used type is added as index name.
        :param elastic_url:         Full URL for the index the harvest should be stored in.
        :param logger:              The logger used by this instance.
        """
        self.user = user
        self.password = password
        self.base_path = base_path
        self.logger = logger

        self.elastic_index = elastic_index_base
        self.elastic_url = elastic_url

        self.harvester_info = dict()
        self.harvester_info['ach'] = ['forschdb2.unibas.ch/inf2/c/oai/achievements.php', 'fdb_ach',
                                      base_path + 'achievement-harvest/', 'achievement-records']
        self.harvester_info['proj'] = ['forschdb2.unibas.ch/inf2/c/oai/projects.php', 'fdb_proj',
                                       base_path + 'project-harvest/', 'project-records']
        self.harvester_info['person'] = ['forschdb2.unibas.ch/inf2/c/oai/persons.php', 'fdb_pers',
                                         base_path + 'person-harvest/', 'person-records']
        self.harvester_info['org'] = ['forschdb2.unibas.ch/inf2/c/oai/organizations.php', 'fdb_org',
                                      base_path + 'organization-harvest/', 'organization-records']
        self.harvester_info['pub'] = ['forschdb2.unibas.ch/inf2/c/oai/publications.php', 'fdb_pub',
                                      base_path + 'publication-harvest/', 'publication-records']

    def upload_to_elastic(self, what: str, identifier='identifier'):
        """
        Uploads a harvest to a elastic search index.

        :param what:        Which harvest it should upload 'ach', 'proj', 'person', 'org', 'pub'
        :param identifier:  What the identifier inside the data is called (default 'identifier')
        """
        data = list()
        for root_dir, _, files in os.walk(self.harvester_info[what][2]):
            for file in files:
                tree = ElementTree.parse(root_dir + '/' + file)
                root = purge_namespaces(tree.getroot())
                for element in root.findall('./ListRecords/record/metadata/'):
                    data.append(json.loads(xml2json(element, 'parker')))

        for item in data:
            clean_data(item, identifier)

        index = ElasticIndex(self.elastic_index + what + '_' + date.today().isoformat(), 'publication',
                             self.elastic_url)
        index.bulk(data, identifier)

    def upload_all(self):
        """Uploads all harvests to the elastic search index."""
        self.logger.info('Upload all records to elasticsearch index at %s.', self.elastic_url)
        signatures = ['ach', 'proj', 'person', 'org', 'pub']
        for signature in signatures:
            self.logger.info('Begin upload of %s', signature)
            self.upload_to_elastic(signature, 'identifier' if signature in ['ach', 'proj', 'pub'] else 'mcss_id')
            self.logger.info('Upload of %s has ended.', signature)

    def harvest(self, what: str, date=None):
        """
        Harvests a single repository of the Research Database and stores them in files.

        :param what:        Which harvest it should upload 'ach', 'proj', 'person', 'org', 'pub'
        :param date     if date is given only records from after this point are harvested. (default None)
        :type date      str with format YYYY-MM-D
        """
        if what in self.harvester_info:
            harvester = OAIHarvester(self.harvester_info[what][0], self.harvester_info[what][1],
                                     self.harvester_info[what][2],
                                     base_file_name=self.harvester_info[what][3],
                                     user=self.user, password=self.password)
            harvester.store_records(date=date)
        else:
            self.logger.critical('This is not a valid signature: %s. Select one of %s.', what,
                                 str(self.harvester_info.keys()))

    def harvest_all(self, date=None):
        """
        Harvests all repositories of the FDB.

        :param date     if date is given only records from after this point are harvested. (default None)
        :type date      str with format YYYY-MM-DD
        """
        self.logger.info('Harvest all records in Research Database.')
        signatures = ['ach', 'proj', 'person', 'org', 'pub']
        for signature in signatures:
            self.logger.info('Begin harvest of %s.', signature)
            self.harvest(signature, date=date)
            self.logger.info('Harvest of %s has ended.', signature)
