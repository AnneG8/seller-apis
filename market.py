import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров Яндекс Маркета.

    Обращается к API Яндекс Маркета за списком созданных товаров.

    Args:
        page (str): Идентификатор последнего значения на странице.
        campaign_id (str): Идентификатор кампании и идентификатор магазина
        Яндекс Маркета.
        access_token (str): API-токен продавца Яндекс Маркета.

    Returns:
        dict: Ответ Яндекс Маркета.

    Raises:
        HTTPError: Если код ответа не 200.

    Examples:
        >>> get_product_list(page, campaign_id, access_token)
        {
            "status": "OK",
            "result": {
                "paging": {
                    "nextPageToken": "string",
                    "prevPageToken": "string"
                },
                "offerMappingEntries": [
                    {
                        "offer": {
                            "name": "Ударная дрель Makita HP1630, 710 Вт",
                            "shopSku": "string",
                            "category": "string",
                            "vendor": "LEVENHUK",
                            "vendorCode": "VNDR-0005A",
                            "description": "string",
                            "id": "string",
                            "feedId": 0,
                            "price": 0,
                            "barcodes": [
                                46012300000000
                            ],
                            "urls": [
                                "string"
                            ],
                            "pictures": [
                                "string"
                            ],
                            "manufacturer": "string",
                            "manufacturerCountries": [
                                "string"
                            ],
                            "minShipment": 0,
                            "transportUnitSize": 0,
                            "quantumOfSupply": 0,
                            "deliveryDurationDays": 0,
                            "boxCount": 0,
                            "customsCommodityCodes": [
                                "string"
                            ],
                            "weightDimensions": {
                                "length": 65.55,
                                "width": 50.7,
                                "height": 20,
                                "weight": 1.001
                            },
                            "supplyScheduleDays": [
                                "MONDAY"
                            ],
                            "shelfLifeDays": 0,
                            "lifeTimeDays": 0,
                            "guaranteePeriodDays": 0,
                            "processingState": {
                                "status": "UNKNOWN",
                                "notes": [
                                    {
                                        "type": "ASSORTMENT",
                                        "payload": "string"
                                    }
                                ]
                            },
                            "availability": "ACTIVE",
                            "shelfLife": {
                                "timePeriod": 0,
                                "timeUnit": "HOUR",
                                "comment": "string"
                            },
                            "lifeTime": {
                                "timePeriod": 0,
                                "timeUnit": "HOUR",
                                "comment": "string"
                            },
                            "guaranteePeriod": {
                                "timePeriod": 0,
                                "timeUnit": "HOUR",
                                "comment": "string"
                            },
                            "certificate": "string"
                        },
                        "mapping": {
                            "marketSku": 0,
                            "modelId": 0,
                            "categoryId": 0
                        },
                        "awaitingModerationMapping": {
                            "marketSku": 0,
                            "modelId": 0,
                            "categoryId": 0
                        },
                        "rejectedMapping": {
                            "marketSku": 0,
                            "modelId": 0,
                            "categoryId": 0
                        }
                    }
                ]
            }
        }
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновить остатки.

    Обновляет на Яндекс Маркете информацию о количестве товаров.

    Args:
        stocks (list): Список обновленного количества товаров.
        campaign_id (str): Идентификатор кампании и идентификатор магазина
        Яндекс Маркета.
        access_token (str): API-токен продавца Яндекс Маркета.

    Returns:
        response_object (dict): Ответ Яндекс Маркета Ozon в формате json об
        успешности операции.

    Raises:
        HTTPError: Если код ответа не 200.

    Examples:
        >>> update_stocks(stocks, campaign_id, access_token)
        {
            "status": "OK"
        }

        >>> update_stocks(stocks, campaign_id, access_token)
        {
            "status": "OK",
            "errors": [
                {
                    "code": "string",
                    "message": "string"
                }
            ]
        }
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновить цены товаров.

    Обновляет на Яндекс Маркете информацию о стоимости остатков.

    Args:
        prices (list): Список новых цен товаров.
        campaign_id (str): Идентификатор кампании и идентификатор магазина
        Яндекс Маркета.
        access_token (str): API-токен продавца Яндекс Маркета.

    Returns:
        response_object (dict): Ответ Яндекс Маркета в формате json об
        успешности операции.

    Raises:
        HTTPError: Если код ответа не 200.

    Examples:
        >>> update_price(prices, campaign_id, access_token)
        {
            "status": "OK"
        }

        >>> update_price(prices, campaign_id, access_token)
        {
            "status": "OK",
            "errors": [
                {
                    "code": "string",
                    "message": "string"
                }
            ]
        }
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров Яндекс Маркета.

    Постранично запрашивает артикулы созданных товаров у Яндекс Маркета.

    Args:
        campaign_id (str): Идентификатор кампании и идентификатор магазина
        Яндекс Маркета.
        market_token (str): API-токен продавца Яндекс Маркета.

    Returns:
        offer_ids (list): Список артикулов товаров Яндекс Маркета.

    Raises:
        HTTPError: Если код ответа не 200.

    Examples:
        >>> get_offer_ids(campaign_id, market_token)
        ["136748", "168448", "528148", "236297"]
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Сформировать список товаров в наличии и их количества.

    На основании данных с сайта timeworld.ru заполняет информацию об
    остатках для Яндекс Маркета.

    Args:
        watch_remnants (list): Список словарей с информацией о товарах.
        offer_ids (list): Список артикулов товаров Яндекс Маркета.
        warehouse_id (str): ID склада.

    Returns:
        stocks (list): Список обновленного количества товаров.

    Examples:
        >>> create_stocks(watch_remnants, offer_ids, warehouse_id)
        [
            {
                "sku": "48852",
                "warehouseId": "143645",
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": "2023-08-13T19:50:21Z",
                    }
                ],
            },
            {
                "sku": "48857",
                "warehouseId": "143645",
                "items": [
                    {
                        "count": 2,
                        "type": "FIT",
                        "updatedAt": "2023-08-13T19:50:21Z",
                    }
                ],
            }
        ]
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Обновление цен товаров.

    На основании данных с сайта timeworld.ru заполняет информацию о новых
    ценах для Яндекс Маркета.

    Args:
        watch_remnants (list): Список словарей с информацией о товарах.
        offer_ids (list): Список артикулов товаров Яндекс Маркета.

    Returns:
        prices (list): Список новых цен товаров.

    Examples:
        >>> create_prices(watch_remnants, offer_ids)
        [
            {
                "id": "48852",
                "price": {
                    "value": 24570,
                    "currencyId": "RUR",
                },
            }
        ]

    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Загрузить список цен на Яндекс Маркет.

    Обновляет цены товаров на Яндекс Маркет в соответствии с полученными в
    watch_remnants данными.

    Args:
        watch_remnants (list): Список словарей с информацией о товарах.
        campaign_id (str): Идентификатор кампании и идентификатор магазина
        Яндекс Маркета.
        market_token (str): API-токен продавца Яндекс Маркета.

    Returns:
        prices (list): Список новых цен товаров.

    Raises:
        HTTPError: Если код ответа не 200.

    Examples:
        >>> upload_prices(watch_remnants, campaign_id, market_token)
        [
            {
                "id": "48852",
                "price": {
                    "value": 24570,
                    "currencyId": "RUR",
                },
            }
        ]
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Загрузить количество товаров на Яндекс Маркет.

    Обновляет количество товаров на Яндекс Маркет в соответствии с
    полученными в watch_remnants данными. Формирует отдельным списком
    товары, которые есть в наличии.

    Args:
        watch_remnants (list): Список словарей с информацией о товарах.
        campaign_id (str): Идентификатор кампании и идентификатор магазина
        Яндекс Маркета.
        market_token (str): API-токен продавца Яндекс Маркета.
        warehouse_id (): ID склада.

    Returns:
        not_empty (list): Список товаров, которые есть в наличии.
        stocks (list): Список обновленного количества товаров.

    Raises:
        HTTPError: Если код ответа не 200.

    Examples:
        >>> upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id)
        [
            {
                "sku": "48857",
                "warehouseId": "143645",
                "items": [
                    {
                        "count": 2,
                        "type": "FIT",
                        "updatedAt": "2023-08-13T19:50:21Z",
                    }
                ],
            }
        ],
        [
            {
                "sku": "48852",
                "warehouseId": "143645",
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": "2023-08-13T19:50:21Z",
                    }
                ],
            },
            {
                "sku": "48857",
                "warehouseId": "143645",
                "items": [
                    {
                        "count": 2,
                        "type": "FIT",
                        "updatedAt": "2023-08-13T19:50:21Z",
                    }
                ],
            }
        ]
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
