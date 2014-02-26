"""
Utilities for real-case harvesting scenario
"""

from collections import Mapping
import os
import json

import pytest

from ckan_api_client import CkanClient


HERE = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(os.path.dirname(HERE), 'data', 'datitrentino')

HARVEST_SOURCE_NAME = 'dummy-harvest-source'


class HarvestSource(Mapping):
    """Provides dict-like access to harvest sources
    """

    source_name = HARVEST_SOURCE_NAME

    def __init__(self, base_dir, day):
        """
        :param day:
            The day from which to get data.
            Full name, like 'day-00', 'day-01', ..
        """
        self.base_dir = base_dir
        self.day = day

    # def iter_objects(self, objtype):
    #     folder = os.path.join(DATA_DIR, self.day, objtype)
    #     for filename in os.listdir(folder):
    #         filepath = os.path.join(folder, filename)
    #         with open(filepath, 'r') as f:
    #             data = json.load(f)
    #         yield data

    def __getitem__(self, name):
        if name not in self.__iter__():
            raise KeyError("No such object type: {0!r}".format(name))
        return HarvestSourceCollection(self, name)

    def __iter__(self):
        """List object types"""

        folder = os.path.join(self.base_dir, self.day)
        for name in os.listdir(folder):
            ## Skip hidden files
            if name.startswith('.'):
                continue

            ## Skip non-directories
            path = os.path.join(folder, name)
            if not os.path.isdir(path):
                continue

            yield name

    def __len__(self):
        return len(self.__iter__())


class HarvestSourceCollection(Mapping):
    def __init__(self, source, name):
        self.source = source
        self.name = name

    def __getitem__(self, name):
        if name not in self.__iter__():
            raise KeyError("There is no object of type={0!r} id={1!r}"
                           .format(self.name, name))
        folder = os.path.join(self.source.base_dir, self.source.day, self.name)
        path = os.path.join(folder, name)

        with open(path, 'r') as f:
            data = json.load(f)

        # todo: apply customizations/cleanup, if needed?

        return data

    def __iter__(self):
        """List object ids"""

        folder = os.path.join(self.source.base_dir, self.source.day, self.name)
        for name in os.listdir(folder):
            ## Skip hidden files
            if name.startswith('.'):
                continue

            ## Skip non-directories
            path = os.path.join(folder, name)
            if not os.path.isdir(path):
                continue

            yield name

    def __len__(self):
        return len(self.__iter__())
