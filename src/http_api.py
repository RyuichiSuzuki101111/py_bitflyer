import json
from typing import Final, Literal
from urllib.parse import urlencode

from requests import request, Response

Region: Final = Literal['JP', 'USA', 'EU']


class Market:

    __slots__ = ('product_code', 'alias', 'market_type')
    _markets: dict[str, 'Market']

    product_code: str
    alias: str
    market_type: Literal['Spot', 'FX', 'Futures']

    def __new__(cls, cxt: 'Context', *, product_code: str = None, alias: str = None):

        try:
            assert (product_code is None) != (alias is None)
        except AssertionError:
            raise TypeError(
                'Market() takes 1 positional argument and just 1 keyword-only argument.')

        if not hasattr(cls, '_markets'):

            cls._markets: dict[str, 'Market'] = {}
            res = cxt.getmarket()

            # TODO: add error handling.
            _markets: list[dict[str, str]] = json.loads(res.text)

            for market_data in _markets:
                market = object.__new__(cls)
                for attr in cls.__slots__:
                    setattr(market, attr, market_data.get(attr))
                cls._markets[market.product_code] = market

        if product_code is not None:
            try:
                return cls._markets[product_code]
            except KeyError:
                available = ', '.join(
                    f"'{prod}'" for prod in cls._markets.keys())
                raise KeyError(f"given: {product_code=}",
                               f"available: {available}")

        elif alias is not None:
            for market in cls._markets.values():
                if market.alias == alias:
                    return market
            available = ', '.join(
                f"'{m.alias}'" for m in cls._markets.values() if m.alias is not None)
            raise KeyError(f"given: {alias=}",
                           f"available: {available}")

    def __init__(cls, cxt: 'Context', *, product_code: str = None, alias: str = None):
        pass


class Context:

    __slots__ = ('region', 'market', 'key', 'secret')

    region: Region
    market: Market
    key: str
    secret: bytes

    def __init__(self, region: Region,
                 product_code: str = None,
                 alias: str = None,
                 api_key: str = None,
                 api_secret: str = None):
        """Context class maneges data of region, market and API key.
        region data is used to determine http request paths,
        market data is used to complete 'product_code' if it is required as request body or query parameter,
        API key and secret is used to create headers of private API requests.
        Arguments 'product_code' and 'alias' can be omitted if only market-independent requests are used,
        and arguments 'api_key' and 'api_secret' can be omitted if only public requests are used.
        These data can also be set later using 'set_market' or 'set_api_key' methods.
        """

        self.region: Final[str] = region

        if not (product_code is None and alias is None):
            self.set_market(product_code=product_code, alias=alias)

        if not (api_key is None or api_secret is None):
            self.set_api_key(api_key, api_secret)

    @property
    def endpoint(self) -> str:
        match self.region:
            case 'JP':
                return 'https://api.bitflyer.com'
            case 'USA':
                return 'https://api.bitflyer.com'
            case 'EU':
                return 'https://api.bitflyer.com'

    def set_market(self, *, product_code: str = None, alias: str = None):
        self.market = Market(self, product_code=product_code, alias=alias)

    def set_api_key(self, key: str, secret: str):
        self.key: str = key
        self.secret: bytes = secret.encode('utf8')

    def send_public_request(self, depends_on_market: bool, method: str, path: str, query: dict = {}, data: dict = {}):

        url = f'{self.endpoint}{path}'

        if depends_on_market:
            # NOTE: If both 'product_code' and 'alias' are not specified in 'query',
            #       add suitable 'product_code' to 'query' by context.
            if (query.get('product_code') is None) and (query.get('alias') is None):
                query['product_code'] = self.market.product_code

        if len(query) == 0:
            url += '?' + urlencode(query)

        # NOTE: Unlike private requests, this conversion is possibly meaningless.
        if len(data) == 0:
            data_str = ''
        else:
            data_str = json.dumps(data)

        return request(method, url, data=data_str)

    def _get_regionwise_path(self, base_path: str) -> str:
        # REVIEW: This probably works fine.
        match self.region:
            case 'JP':
                return base_path
            case _:
                return f'{base_path}/{self.region.lower()}'

    def getmarket(self) -> Response:
        path = self._get_regionwise_path('/v1/markets')
        return self.send_public_request(False, 'GET', path)

    def getboard(self, *, product_code: str = None, alias: str = None) -> Response:
        """
        Send the getboard request.
        If specified, product_code is used in preference to the context.
        """
        path = '/v1/getboard'
        assert (product_code is not None) + (alias is not None) <= 1
        query = {}
        if product_code is not None:
            query['product_code'] = product_code
        if alias is not None:
            query['alias'] = alias

        return self.send_public_request(True, 'GET', path, query)

    def getticker(self, *, product_code: str = None, alias: str = None) -> Response:
        """
        Send the getticker request.
        If specified, product_code is used in preference to the context.
        """
        path = '/v1/getticker'
        query = {}
        if product_code is not None:
            query['product_code'] = product_code
        return self.send_public_request(True, 'GET', path, query)
