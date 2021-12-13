import json
from datetime import datetime
from hashlib import sha256
from hmac import HMAC
from itertools import chain
from typing import Final, Generator, Literal
from urllib.parse import urlencode

from requests import Response, request

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


def gen_market_data(cxt: 'Context', product_code: str = None, alias: str = None) -> Generator[tuple[str, str], None, None]:
    # used to add product_code or alias to request body or query.

    if product_code is not None:
        yield 'product_code', product_code
    elif alias is not None:
        yield 'alias', alias
    else:
        yield 'product_code', cxt.market.product_code


def gen_pagenation(count: int = None, before: int = None, after: int = None) -> Generator[tuple[str, str], None, None]:
    # used to add pagenation to query.

    if count is not None:
        yield 'count', count
    if before is not None:
        yield 'before', before
    if after is not None:
        yield 'after', after


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

    def _create_header(self, method: Literal['GET', 'POST'], path: str, data: str):

        timestamp = str(datetime.now().timestamp())
        msg = f'{timestamp}{method}{path}{data}'.encode('utf8')
        sign = HMAC(self.secret, msg, sha256).hexdigest()

        return {
            'ACCESS-KEY': self.key,
            'ACCESS-TIMESTAMP': timestamp,
            'ACCESS-SIGN': sign,
            'Content-Type': 'application/json'
        }

    def send_request(self, method: str, path: str, query: dict = {},
                     data: dict = {}, add_headers: bool = False) -> Response:

        url = f'{self.endpoint}{path}'

        if len(query) == 0:
            url += '?' + urlencode(query)

        # NOTE: Unlike private requests, this conversion is possibly meaningless.
        if len(data) == 0:
            data_str = ''
        else:
            data_str = json.dumps(data)

        if add_headers:
            headers = self._create_header(method, path, data_str)
        else:
            headers = None

        return request(method, url, data=data_str, headers=headers)

    def _get_regionwise_path(self, base_path: str) -> str:
        # REVIEW: This probably works fine.
        match self.region:
            case 'JP':
                return base_path
            case _:
                return f'{base_path}/{self.region.lower()}'

    def getmarket(self) -> Response:
        path = self._get_regionwise_path('/v1/markets')
        return self.send_request('GET', path)

    def getboard(self, *, product_code: str = None, alias: str = None) -> Response:
        """
        Send the getboard request.
        If specified, product_code or alias are used in preference to the context.
        """
        path = '/v1/getboard'
        query = {key: value for key, value in
                 gen_market_data(self, product_code, alias)}
        return self.send_request('GET', path, query)

    def getticker(self, *, product_code: str = None, alias: str = None) -> Response:
        """
        Send the getticker request.
        If specified, product_code or alias are used in preference to the context.
        """
        path = '/v1/getticker'
        query = {key: value for key, value in
                 gen_market_data(self, product_code, alias)}
        return self.send_request('GET', path, query)

    def getexecutions(self, *, product_code: str = None, alias: str = None,
                      count: int = None, before: int = None, after: int = None) -> Response:
        """
        Send the getexecutions request.
        If specified, product_code or alias are used in preference to the context.
        """
        path = '/v1/getexecutions'

        query = {key: value for key, value in
                 chain(gen_market_data(self, product_code, alias),
                       gen_pagenation(count, before, after))}

        return self.send_request('GET', path, query)

    def getboardstate(self, *, product_code: str = None, alias: str = None) -> Response:
        """
        Send the getboardstate request.
        If specified, product_code or alias are used in preference to the context.
        """
        path = '/v1/getboardstate'
        query = {key: value for key, value in
                 gen_market_data(self, product_code, alias)}
        return self.send_request('GET', path, query)

    def gethealth(self, *, product_code: str = None, alias: str = None) -> Response:
        """
        Send the getboardstate request.
        If specified, product_code or alias are used in preference to the context.
        """
        path = '/v1/gethealth'
        query = {key: value for key, value in
                 gen_market_data(self, product_code, alias)}
        return self.send_request('GET', path, query)

    def getcorporateleverage(self) -> Response:
        """
        Send the getcorporateleverage request.
        """
        path = '/v1/getcorporateleverage'
        return self.send_request('GET', path)

    def getchats(self, from_date: str = None) -> Response:
        """
        Send the getchats request.
        query parameter from_date is expected to be of the form 'yyyy-mm-dd'.
        """
        if from_date is not None:
            query = {'from_date': from_date}
        else:
            query = {}

        path = self._get_regionwise_path('/v1/getchats')
        return self.send_request('GET', path, query)

    def me_getpermissions(self) -> Response:
        """
        Send the getpermissions request.
        """
        path = '/v1/me/getpermissionss'
        return self.send_request('GET', path, add_headers=True)

    def me_getbalance(self) -> Response:
        """
        Send the getpermissions request.
        """
        path = '/v1/me/getbalance'
        return self.send_request('GET', path, add_headers=True)

    def me_getcollateral(self) -> Response:
        """
        Send the getcollateral request.
        """
        path = '/v1/me/getcollateral'
        return self.send_request('GET', path, add_headers=True)

    def me_getcollateralaccounts(self) -> Response:
        """
        Send the getcollateralaccounts request.
        """
        path = '/v1/me/getcollateralaccounts'
        return self.send_request('GET', path, add_headers=True)

    def me_getaddresses(self) -> Response:
        """
        Send the getaddresses request.
        """
        path = '/v1/me/getaddresses'
        return self.send_request('GET', path, add_headers=True)

    def me_getcoinins(self, count: int = None,
                      before: int = None,
                      after: int = None) -> Response:
        """
        Send the getcoinins request.
        """
        path = '/v1/me/getcoinins'
        query = {key: value for key, value
                 in gen_pagenation(count, before, after)}
        return self.send_request('GET', path, query, add_header=True)

    def me_getcoinouts(self, count: int = None,
                       before: int = None,
                       after: int = None) -> Response:
        """
        Send the getcoinouts request.
        """
        path = '/v1/me/getcoinouts'
        query = {key: value for key, value
                 in gen_pagenation(count, before, after)}
        return self.send_request('GET', path, query, add_header=True)

    def me_getbankaccounts(self):
        """
        Send the getbankaccounts request.
        """
        path = '/v1/me/getbankaccounts'
        return self.send_request('GET', path, add_header=True)

    def me_getdeposits(self, count: int = None,
                       before: int = None,
                       after: int = None):
        """
        Send the getdeposits request.
        """
        path = '/v1/me/getdeposits'
        return self.send_request('GET', path, add_header=True)

    def me_withdraw(self, currency_code: Literal['JPY'], bank_account_id: int, amount: int, code: str):
        """
        Send the withdraw request.
        """
        data = {
            'currency_code': currency_code,
            'bank_account_id': bank_account_id,
            'amount': amount,
            'code': code
        }
        path = '/v1/me/withdraw'
        return self.send_request('POST', path, data=data, add_headers=True)
