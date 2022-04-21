import json
import tempfile

import pytest

from .._internal.config import Config, parse_config

class TestConfig:
    def build_args(self, seeds_file="", page_limit="", debug=""):
        args = []
        if seeds_file != '':
            args.extend(['-s', seeds_file])
        if page_limit != '':
            args.extend(['-n', page_limit])
        if debug != '':
            args.extend(['-d'])
        return args

    def test_parse_config_empty(self):
        args = []
        # Hopefully doesn't throw any exceptions
        parse_config(args)

    def test_parse_config_valid(self):
        # Create seeds file
        seeds_file = tempfile.TemporaryFile()

        page_limit = '10000'
        args = self.build_args(seeds_file.name, page_limit, "True")

        config = parse_config(args)

        assert int(page_limit) == config.page_limit
        assert seeds_file.name == config.seeds_file
        assert True == config.debug

    def test_parse_config_file_not_exists(self):
        args = self.build_args(seeds_file='this-file-does-not-exist')
        with pytest.raises(FileNotFoundError):
            parse_config(args)

    def test_parse_config_invalid_page_limit_below(self):
        args = self.build_args(page_limit='0')
        with pytest.raises(ValueError):
            parse_config(args)

    def test_parse_config_invalid_page_limit_above(self):
        args = self.build_args(page_limit='1_000_000_000_000')
        with pytest.raises(ValueError):
            parse_config(args)

    def test_to_json_parseable(self):
        config = Config()
        # Hopefully does not throw any exceptions
        json.loads(config.to_json(), strict=False)
