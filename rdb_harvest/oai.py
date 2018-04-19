from xml.etree import ElementTree
from sickle import Sickle
from sickle.iterator import OAIResponseIterator, OAIItemIterator
from sickle.oaiexceptions import BadArgument, BadResumptionToken, NoSetHierarchy

import urllib.parse
import logging
import random
import os


class InvalidPrefixError(Exception):
    pass
    

class OAIHarvester(object):
    """Downloads files from a OAI-PMH 2.0 API and stores them as xml."""

    def __init__(self, base_url: str, metadata_prefix: str, path: str,
                 base_file_name='harvest-result', user='', password='',
                 logger=logging.getLogger('oai'), encoding='iso-8859-1'):
        """
        Configure a basic connection to the OAI-Server. Sets up the sickle instance with appropriate settings
        and checks if the metadata prefix is valid. Creates a directory at path if no such path exists.

        :param base_url:        Base url for the oai request without http://
        :param metadata_prefix:  Metadata-Prefix for the api_response to be harvested.
        :param path:            Directory path where the files should be stored.
        :param base_file_name:  Downloads are saved in this file. If several downloads are made the resumption token
                                or a random number is added.
        :param user:            User name for basic http authentication (unescaped)
        :param password:        Password for basic http authentication (unescaped)
        :param logger:          Logger used to log all actions and errors of this class.
        :param encoding:        The encoding used to store elements

        :raises InvalidPrefixError if the given prefix is not valid.
        """
        self.encoding = encoding
        self.logger = logger
        self.use_authentication = False
        if user != '':
            assert password != ''
            self.user = urllib.parse.quote(user)
            self.encoded_password = urllib.parse.quote(password)
            self.use_authentication = True
            self.logger.info('Uses authentication with credentials: user: %s, password: %s.',
                             self.user, self.encoded_password)
        else:
            self.logger.info('No authentication given.')

        self.url = base_url
        self.path = path
        self.base_file_name = base_file_name
        self.metadataPrefix = metadata_prefix
        self.api_response = None
        self.data = list()

        if self.use_authentication:
            self.sickle = Sickle('https://' + self.user + ':' + self.encoded_password + '@' + self.url,
                                 iterator=OAIResponseIterator)
        else:
            self.sickle = Sickle('https://' + self.url, iterator=OAIResponseIterator)

        self._verify_metadata_prefix()

        if not os.path.exists(self.path):
            self.logger.info('Create directory at %s.', self.path)
            os.makedirs(self.path)

    def _verify_metadata_prefix(self):
        """
        Verifies that the used metadata prefix is valid for this OAI repository.

        :raises InvalidPrefixError  if the given prefix is not valid.
        """
        # changes the sickle iterator to item to easily access metadata prefix.
        self.sickle.iterator = OAIItemIterator
        valid_prefix_list = list()
        metadata = self.sickle.ListMetadataFormats()
        is_valid_prefix = False
        while True:
            try:
                prefix = metadata.next().metadataPrefix
            except StopIteration:
                break
            valid_prefix_list.append(prefix)
            if prefix == self.metadataPrefix:
                is_valid_prefix = True

        if not is_valid_prefix:
            self.logger.critical('Given metadata prefix (%s) was not valid. Select one of these: %s',
                                 self.metadataPrefix, str(valid_prefix_list))
            raise InvalidPrefixError('Invalid metadataPrefix: ' + self.metadataPrefix + '.\n' +
                                     ' A list of the available prefixes: ' + str(valid_prefix_list))
        else:
            self.logger.info('The prefix given is valid.')

    def store_records(self, set_id=None, date=None, ignore_deleted=False):
        """
        Downloads all records found on the OAI-API or all records from a given set.

        :param set_id:          determine what set to download if a given set should be downloaded (default None)
        :type set_id:           str
        :param date:            Only records added/changed after this date will be downloaded (default None)
        :type date:             str 'YYYY-MM-DD'
        :param ignore_deleted:  When true ignores all deleted records. This may not be a
                                feature available in all OAI archives.
        :type ignore_deleted    bool
        """
        self.sickle.iterator = OAIResponseIterator
        params = {'metadataPrefix': self.metadataPrefix, 'from': date, 'set': set_id, 'ignore_deleted': ignore_deleted}
        self.api_response = self.sickle.ListRecords(**params)
        self._write_all_records()

    def store_record(self, identifier: int):
        """
        Downloads a single record with the given id and stores it in a file at the given place.

        :param identifier: the id which should be retrieved.
        """
        self.sickle.iterator = OAIResponseIterator
        record = self.sickle.GetRecord(identifier=identifier, metadataPrefix=self.metadataPrefix)
        temp_xml = record.raw
        with open(self.path + self.base_file_name + str(identifier) + '.xml', 'w', encoding=self.encoding) as file:
            file.write(temp_xml)

    def iterate_sets(self):
        """Iterate through all sets available at the OAI repository.

        :return List of all sets as tupels (id, name)
        :rtype: iterator tuple (str, str)
        """
        self.sickle.iterator = OAIItemIterator
        try:
            sets = self.sickle.ListSets()
            for s in sets:
                yield (s.setSpec, s.setName)
        except NoSetHierarchy as error:
            self.logger.warning(str(error))
            raise NoSetHierarchy(error)

    def _write_all_records(self):
        """Writes all downloaded api_response into xml files."""
        if self.api_response is None:
            self.logger.critical('No response loaded.')
            raise Exception('No response loaded.')
        record = self.api_response.next()
        last_count = 0
        while record:
            temp_xml = record.raw
            if isinstance(temp_xml, str):
                root = ElementTree.fromstring(temp_xml)
                self.data.append(root)

                download_count = len(root[2]) - 1
                last_count += download_count
                token = root[2][-1]
                total = 0
                file = None
                try:
                    file = open(self.path + self.base_file_name + '-' + token.text + '.xml', 'w',
                                encoding=self.encoding)
                    total = int(root[2][-1].get('completeListSize'))
                    self.logger.info('Downloaded %s records from repository. Still %s to go.',
                                     download_count, total - last_count)
                    file.write(temp_xml)
                    record = self.api_response.next()
                except TypeError:  # no resumption token found.
                    file = open(self.path + self.base_file_name + '-' + str(random.randrange(100000)) + '.xml', 'w',
                                encoding=self.encoding)
                    self.logger.info('No resumption token found. Stopping Download. '
                                     'Downloaded %s from this repository.', total)
                    file.write(temp_xml)
                    record = None
                except (BadArgument, BadResumptionToken) as error:
                    self.logger.critical('Stopped Download: "%s"', str(error))
                    record = None
                finally:
                    if file is not None:
                        file.close()
