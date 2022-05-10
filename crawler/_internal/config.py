from .utils import file_exists, between

class Config:
    seeds_file_key = 'seeds_file'
    page_limit_key = 'page_limit'
    debug_key = 'debug'

    output_pages_path = "pages.warc.gz"

    min_page_limit = 1
    max_page_limit = 1_000_000

    def __init__(self, seeds_file="", page_limit=1000, debug=False):
        self.seeds_file = seeds_file
        self.page_limit = page_limit
        self.debug = debug

        self.output_pages_path = Config.output_pages_path

    def to_json(self):
        def json_mapping(key, val):
            return f'"{key}": "{val}"'

        return (
            '{\n' +
            json_mapping(self.seeds_file_key, self.seeds_file) + ',\n' +
            json_mapping(self.page_limit_key, self.page_limit) + ',\n' +
            json_mapping(self.debug_key, self.debug) +
            '\n}'
        )

def parse_config(args):
    options = {}
    for arg_idx in range(len(args)):
        if '-s' == args[arg_idx]:
            # TODO: catch exception if index is overflow
            fpath = args[arg_idx + 1]
            
            if not file_exists(fpath):
                raise FileNotFoundError(fpath)

            options[Config.seeds_file_key] = fpath

        elif '-n' == args[arg_idx]:
            page_limit = int(args[arg_idx + 1])
            if not between(page_limit,
                           Config.min_page_limit,
                           Config.max_page_limit):
                raise ValueError(f"page limit must be between "+
                                 "{Config.min_page_limit} and "+
                                 "{Config.max_page_limit}.")

            options[Config.page_limit_key] = page_limit

        elif '-d' == args[arg_idx]:
            options[Config.debug_key] = True

    return Config(**options)
